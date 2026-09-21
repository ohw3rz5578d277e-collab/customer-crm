#!/usr/bin/env python3
import argparse
import json
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

NS_MAIN = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

SHEETS = {
    "【2024】日程入力": {
        "start_row": 15,
        "name_col": 5,
        "genre_col": 6,
        "status_col": 7,
        "date_col": 8,
        "repeat_col": None,
        "customer_id_col": None,
    },
    "【2025】日程入力": {
        "start_row": 15,
        "name_col": 7,
        "genre_col": 8,
        "status_col": 9,
        "date_col": 10,
        "repeat_col": 4,
        "customer_id_col": 5,
    },
    "【2026】日程入力": {
        "start_row": 15,
        "name_col": 7,
        "genre_col": 8,
        "status_col": 9,
        "date_col": 10,
        "repeat_col": 5,
        "customer_id_col": None,
    },
}


def col_index(cell_ref: str) -> int:
    match = re.match(r"([A-Z]+)", cell_ref or "")
    if not match:
        return 0
    value = 0
    for ch in match.group(1):
        value = value * 26 + (ord(ch) - 64)
    return value


def excel_date(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        number = float(raw)
    except ValueError:
        return raw
    if not 20000 <= number <= 80000:
        return raw
    dt = datetime(1899, 12, 30) + timedelta(days=number)
    return dt.strftime("%Y/%m/%d")


def load_workbook(path: Path):
    with zipfile.ZipFile(path) as archive:
        shared = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for si in root.findall("m:si", NS_MAIN):
                shared.append(
                    "".join(
                        node.text or ""
                        for node in si.iter(
                            "{%s}t" % NS_MAIN["m"]
                        )
                    )
                )

        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        relmap = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}

        sheets = {}
        sheet_nodes = workbook.find("m:sheets", NS_MAIN)
        if sheet_nodes is None:
            return sheets

        for sheet in sheet_nodes:
            name = sheet.attrib["name"]
            rid = sheet.attrib["{%s}id" % REL_NS]
            target = relmap[rid]
            entry = target.lstrip("/") if target.startswith("/") else "xl/" + target
            root = ET.fromstring(archive.read(entry))
            rows = {}

            for row in root.findall(".//m:sheetData/m:row", NS_MAIN):
                row_num = int(row.attrib["r"])
                cells = {}

                for cell in row.findall("m:c", NS_MAIN):
                    ref = cell.attrib.get("r", "")
                    col = col_index(ref)
                    cell_type = cell.attrib.get("t", "")
                    value = ""

                    if cell_type == "inlineStr":
                        value = "".join(
                            node.text or ""
                            for node in cell.iter(
                                "{%s}t" % NS_MAIN["m"]
                            )
                        )
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

        return sheets


def extract_sales_records(path: Path):
    sheets = load_workbook(path)
    missing = [name for name in SHEETS if name not in sheets]
    if missing:
        raise RuntimeError("missing_required_sheets:" + ",".join(missing))

    records = []

    for sheet_name, spec in SHEETS.items():
        year = int(re.search(r"(20\d{2})", sheet_name).group(1))
        rows = sheets[sheet_name]

        for row_num, row in rows.items():
            if row_num < spec["start_row"]:
                continue

            name = str(row.get(spec["name_col"], "") or "").strip()
            if not name:
                continue

            repeat_flag = ""
            if spec["repeat_col"]:
                repeat_flag = str(row.get(spec["repeat_col"], "") or "").strip()

            sales_customer_id = ""
            if spec["customer_id_col"]:
                sales_customer_id = str(
                    row.get(spec["customer_id_col"], "") or ""
                ).strip()

            records.append(
                {
                    "year": year,
                    "source_sheet": sheet_name,
                    "source_row": row_num,
                    "name": name,
                    "shoot_date": excel_date(row.get(spec["date_col"], "")),
                    "genre": str(row.get(spec["genre_col"], "") or "").strip(),
                    "status": str(row.get(spec["status_col"], "") or "").strip(),
                    "repeat_flag": repeat_flag,
                    "sales_customer_id": sales_customer_id,
                }
            )

    return records


def main():
    parser = argparse.ArgumentParser(
        description="Extract Photo sales schedule rows for local CRM reconciliation."
    )
    parser.add_argument("--xlsx", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    source = Path(args.xlsx).expanduser().resolve()
    output = Path(args.out).expanduser().resolve()

    if not source.is_file():
        raise SystemExit("RESULT=STOP_SALES_XLSX_MISSING")

    try:
        rows = extract_sales_records(source)
    except Exception as exc:
        print("RESULT=STOP_SALES_XLSX_PARSE_FAILED")
        print("ERROR_TYPE=" + type(exc).__name__)
        raise

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "source_kind": "photo_sales_schedule_xlsx",
                "record_count": len(rows),
                "records": rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print("RESULT=PHOTO_SALES_XLSX_EXTRACTED")
    print("SALES_RECORDS=" + str(len(rows)))
    print("PRIVATE_VALUES_PRINTED=0")
    print("PRODUCTION_D1_WRITE=0")


if __name__ == "__main__":
    main()
