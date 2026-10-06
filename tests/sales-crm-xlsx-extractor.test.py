#!/usr/bin/env python3
import importlib.util
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

MODULE_PATH = Path('scripts/extract-photo-sales-xlsx.py')
spec = importlib.util.spec_from_file_location('sales_xlsx', MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError('extractor_module_load_failed')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

passed = 0

def check(name, fn):
    global passed
    fn()
    passed += 1
    print(f'PASS {passed}: {name}')


def minimal_workbook(path: Path, date1904: bool):
    flag = ' date1904="1"' if date1904 else ''
    workbook = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <workbookPr{flag}/>
  <sheets>
    <sheet name="【2024】日程入力" sheetId="1" r:id="rId1"/>
    <sheet name="【2025】日程入力" sheetId="2" r:id="rId2"/>
    <sheet name="【2026】日程入力" sheetId="3" r:id="rId3"/>
  </sheets>
</workbook>'''
    rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet3.xml"/>
</Relationships>'''
    empty_sheet = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData/></worksheet>'''
    with zipfile.ZipFile(path, 'w') as zf:
        zf.writestr('xl/workbook.xml', workbook)
        zf.writestr('xl/_rels/workbook.xml.rels', rels)
        for idx in range(1, 4):
            zf.writestr(f'xl/worksheets/sheet{idx}.xml', empty_sheet)


def test_1904_flag_is_read():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / 'book.xlsx'
        minimal_workbook(p, True)
        sheets, date_1904 = mod.load_workbook(p, include_metadata=True)
        assert date_1904 is True
        assert set(sheets) == set(mod.SHEETS)


def test_1900_flag_default():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / 'book.xlsx'
        minimal_workbook(p, False)
        _, date_1904 = mod.load_workbook(p, include_metadata=True)
        assert date_1904 is False


def test_date_system_shift():
    d1900 = datetime.strptime(mod.excel_date('45000', False), '%Y/%m/%d')
    d1904 = datetime.strptime(mod.excel_date('45000', True), '%Y/%m/%d')
    assert (d1904 - d1900).days == 1462


def test_populated_nameless_row_is_preserved():
    original = mod.load_workbook
    try:
        sheets = {name: {} for name in mod.SHEETS}
        # 2025 date column is J=10. Name G=7 is intentionally blank.
        sheets['【2025】日程入力'][15] = {10: '45000', 8: '七五三', 9: '撮影済'}
        sheets['【2025】日程入力'][16] = {}
        mod.load_workbook = lambda path, include_metadata=False: (sheets, False) if include_metadata else sheets
        rows, metadata = mod.extract_sales_records(Path('unused.xlsx'), include_metadata=True)
        assert metadata['date_system'] == '1900'
        assert len(rows) == 1
        assert rows[0]['source_row'] == 15
        assert rows[0]['name'] == ''
        assert rows[0]['shoot_date']
        assert rows[0]['genre'] == '七五三'
    finally:
        mod.load_workbook = original


def test_completely_empty_rows_are_skipped():
    original = mod.load_workbook
    try:
        sheets = {name: {} for name in mod.SHEETS}
        sheets['【2026】日程入力'][15] = {}
        mod.load_workbook = lambda path, include_metadata=False: (sheets, True) if include_metadata else sheets
        rows, metadata = mod.extract_sales_records(Path('unused.xlsx'), include_metadata=True)
        assert metadata['date_system'] == '1904'
        assert rows == []
    finally:
        mod.load_workbook = original


check('workbook date1904 flag is honored', test_1904_flag_is_read)
check('workbook defaults to 1900 date system', test_1900_flag_default)
check('1904 serial date epoch differs by 1462 days', test_date_system_shift)
check('populated nameless sales row is preserved for fail-closed validation', test_populated_nameless_row_is_preserved)
check('wholly empty schedule row is skipped', test_completely_empty_rows_are_skipped)
print(f'SALES_CRM_XLSX_EXTRACTOR={passed}/{passed} PASS')
