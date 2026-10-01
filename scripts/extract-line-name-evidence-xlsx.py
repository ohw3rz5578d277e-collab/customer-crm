#!/usr/bin/env python3
import argparse
import importlib.util
import json
import re
import unicodedata
from pathlib import Path


def load_xlsx_helper():
    helper_path = Path(__file__).with_name("extract-photo-sales-xlsx.py")
    spec = importlib.util.spec_from_file_location("photo_sales_xlsx_helper", helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("xlsx_helper_load_failed")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def normalize_name(value):
    return re.sub(
        r"[\s\u3000]+",
        "",
        unicodedata.normalize("NFKC", str(value or "")).strip().lower(),
    )


def primary_name(value):
    return re.split(r"[\r\n]+", str(value or "").strip())[0].strip()


def main():
    parser = argparse.ArgumentParser(
        description="Extract exact full-name LINE evidence for sales reconciliation."
    )
    parser.add_argument("--line-history-xlsx", required=True)
    parser.add_argument("--sales-records", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    line_path = Path(args.line_history_xlsx).expanduser().resolve()
    sales_path = Path(args.sales_records).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    if not line_path.is_file():
        raise SystemExit("RESULT=STOP_LINE_HISTORY_XLSX_MISSING")
    if not sales_path.is_file():
        raise SystemExit("RESULT=STOP_SALES_RECORDS_MISSING")

    helper = load_xlsx_helper()
    sheets = helper.load_workbook(line_path)

    if "LINE_log" not in sheets:
        raise SystemExit("RESULT=STOP_LINE_LOG_SHEET_MISSING")

    sales_payload = json.loads(sales_path.read_text(encoding="utf-8"))
    records = sales_payload.get("records")
    if not isinstance(records, list):
        raise SystemExit("RESULT=STOP_SALES_RECORDS_INVALID")

    names = {}
    for row in records:
        raw = primary_name(row.get("name", ""))
        key = normalize_name(raw)
        if not key:
            continue
        names.setdefault(key, raw)

    # LINE_log columns:
    # A=datetime, B=customer_id, C=name, D=content, E=type,
    # F=AI, G=sender, H=LINE UserID
    evidence = {}
    scanned_rows = 0

    for row_num, row in sheets["LINE_log"].items():
        if row_num < 2:
            continue

        content = str(row.get(4, "") or "")
        line_user_id = str(row.get(8, "") or "").strip()

        if not content or not line_user_id:
            continue

        scanned_rows += 1
        normalized_content = normalize_name(content)

        for key, display_name in names.items():
            # Short/generic names create false positives in free text.
            if len(key) < 4:
                continue

            if key not in normalized_content:
                continue

            evidence.setdefault(key, {
                "name": display_name,
                "line_user_ids": set(),
                "source_rows": [],
            })

            evidence[key]["line_user_ids"].add(line_user_id)
            evidence[key]["source_rows"].append(row_num)

    rows = []
    ambiguous_name_count = 0

    for key, item in sorted(evidence.items()):
        line_ids = sorted(item["line_user_ids"])

        if len(line_ids) > 1:
            ambiguous_name_count += 1

        for line_id in line_ids:
            rows.append({
                "name": item["name"],
                "name_key": key,
                "line_user_id": line_id,
                "source": "line_body_exact_full_name",
                "ambiguous_across_line_ids": len(line_ids) > 1,
                "source_row_count": len(item["source_rows"]),
            })

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(
            {
                "source_kind": "line_body_exact_full_name_evidence",
                "line_log_rows_scanned": scanned_rows,
                "matched_sales_names": len(evidence),
                "ambiguous_sales_names": ambiguous_name_count,
                "evidence_rows": rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print("RESULT=LINE_NAME_EVIDENCE_EXTRACTED")
    print("LINE_LOG_ROWS_SCANNED=" + str(scanned_rows))
    print("MATCHED_SALES_NAMES=" + str(len(evidence)))
    print("AMBIGUOUS_SALES_NAMES=" + str(ambiguous_name_count))
    print("EVIDENCE_ROWS=" + str(len(rows)))
    print("LINE_BODY_AUTO_LINK=0")
    print("PRODUCTION_D1_WRITE=0")
    print("PRIVATE_VALUES_PRINTED=0")


if __name__ == "__main__":
    main()
