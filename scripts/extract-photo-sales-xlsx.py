#!/usr/bin/env python3
import argparse
import json
import posixpath
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

NS_MAIN = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

SHEETS = {
    "【2024】日程入力": {"start_row": 15, "name_col": 5, "genre_col": 6, "status_col": 7, "date_col": 8, "repeat_col": None, "customer_id_col": None},
    "【2025】日程入力": {"start_row": 15, "name_col": 7, "genre_col": 8, "status_col": 9, "date_col": 10, "repeat_col": 4, "customer_id_col": 5},
    "【2026】日程入力": {"start_row": 15, "name_col": 7, "genre_col": 8, "status_col": 9, "date_col": 10, "repeat_col": 5, "customer_id_col": None},
}


def col_index(cell_ref: str) -> int:
    match = re.match(r"([A-Z]+)", cell_ref or "")
    if not match:
        return 0
    value = 0
    for ch in match.group(1):
        value = value * 26 + (ord(ch) - 64)
    return value


def excel_date(value, date_1904=False):
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        number = float(raw)
    except ValueError:
        return raw
    if not 20000 <= number <= 80000:
        return raw
    epoch = datetime(1904, 1, 1) if date_1904 else datetime(1899, 12, 30)
    return (epoch + timedelta(days=number)).strftime("%Y/%m/%d")


def load_workbook(path: Path, include_metadata=False):
    with zipfile.ZipFile(path) as archive:
        shared = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for si in root.findall("m:si", NS_MAIN):
                shared.append("".join(node.text or "" for node in si.iter("{%s}t" % NS_MAIN["m"])))

        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        workbook_pr = workbook.find("m:workbookPr", NS_MAIN)
        date_1904 = False
        if workbook_pr is not None:
            date_1904 = str(workbook_pr.attrib.get("date1904", "")).strip().lower() in {"1", "true"}

        rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        relmap = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        sheets = {}
        sheet_nodes = workbook.find("m:sheets", NS_MAIN)
        if sheet_nodes is None:
            return (sheets, date_1904) if include_metadata else sheets

        for sheet in sheet_nodes:
            name = sheet.attrib["name"]
            rid = sheet.attrib["{%s}id" % REL_NS]
            target = relmap[rid]
            entry = target.lstrip("/") if target.startswith("/") else posixpath.normpath(posixpath.join("xl", target))
            root = ET.fromstring(archive.read(entry))
            rows = {}
            for row in root.findall(".//m:sheetData/m:row", NS_MAIN):
                row_num = int(row.attrib["r"])
                cells = {}
                for cell in row.findall("m:c", NS_MAIN):
                    col = col_index(cell.attrib.get("r", ""))
                    cell_type = cell.attrib.get("t", "")
                    value = ""
                    if cell_type == "inlineStr":
                        value = "".join(node.text or "" for node in cell.iter("{%s}t" % NS_MAIN["m"]))
                    else:
                        node = cell.find("m:v", NS_MAIN)
                        if node is not None:
                            raw = node.text or ""
                            if cell_type == "s":
                                try:
                                    value = shared[int(raw)]
                                except (ValueError, IndexError):
                                    value = raw
                            else:
                                value = raw
                    cells[col] = value
                rows[row_num] = cells
            sheets[name] = rows
        return (sheets, date_1904) if include_metadata else sheets


def extract_sales_records(path: Path, include_metadata=False):
    sheets, date_1904 = load_workbook(path, include_metadata=True)
    missing = [name for name in SHEETS if name not in sheets]
    if missing:
        raise RuntimeError("missing_required_sheets:" + ",".join(missing))

    records = []
    for sheet_name, spec in SHEETS.items():
        year = int(re.search(r"(20\d{2})", sheet_name).group(1))
        for row_num, row in sheets[sheet_name].items():
            if row_num < spec["start_row"]:
                continue

            name = str(row.get(spec["name_col"], "") or "").strip()
            raw_date = str(row.get(spec["date_col"], "") or "").strip()
            genre = str(row.get(spec["genre_col"], "") or "").strip()
            status = str(row.get(spec["status_col"], "") or "").strip()
            repeat_flag = str(row.get(spec["repeat_col"], "") or "").strip() if spec["repeat_col"] else ""
            sales_customer_id = str(row.get(spec["customer_id_col"], "") or "").strip() if spec["customer_id_col"] else ""

            # Skip only rows that are empty across every reconciliation-relevant field.
            # Populated rows with a blank name must reach analyzeSalesHistory so it can fail closed.
            if not any([name, raw_date, genre, status, repeat_flag, sales_customer_id]):
                continue

            records.append({
                "year": year,
                "source_sheet": sheet_name,
                "source_row": row_num,
                "name": name,
                "shoot_date": excel_date(raw_date, date_1904=date_1904),
                "genre": genre,
                "status": status,
                "repeat_flag": repeat_flag,
                "sales_customer_id": sales_customer_id,
            })

    if include_metadata:
        return records, {"date_system": "1904" if date_1904 else "1900"}
    return records


def main():
    parser = argparse.ArgumentParser(description="Extract Photo sales schedule rows for local CRM reconciliation.")
    parser.add_argument("--xlsx", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    source = Path(args.xlsx).expanduser().resolve()
    output = Path(args.out).expanduser().resolve()
    if not source.is_file():
        raise SystemExit("RESULT=STOP_SALES_XLSX_MISSING")
    try:
        rows, metadata = extract_sales_records(source, include_metadata=True)
    except Exception as exc:
        print("RESULT=STOP_SALES_XLSX_PARSE_FAILED")
        print("ERROR_TYPE=" + type(exc).__name__)
        raise
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"source_kind": "photo_sales_schedule_xlsx", "record_count": len(rows), "workbook_date_system": metadata["date_system"], "records": rows}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("RESULT=PHOTO_SALES_XLSX_EXTRACTED")
    print("SALES_RECORDS=" + str(len(rows)))
    print("WORKBOOK_DATE_SYSTEM=" + metadata["date_system"])
    print("NETWORK_ACCESS=0")
    print("PRODUCTION_D1_READ=0")
    print("PRODUCTION_D1_WRITE=0")
    print("PRIVATE_VALUES_PRINTED=0")


if __name__ == "__main__":
    main()
