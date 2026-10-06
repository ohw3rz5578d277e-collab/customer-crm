#!/usr/bin/env bash
set -euo pipefail

SALES_XLSX="${1:-}"
CUSTOMER_MASTER="${2:-}"
PRODUCTION_SNAPSHOT="${3:-}"
OUT_DIR="${4:-}"
LINE_HISTORY_XLSX="${5:-}"

if [ -z "$SALES_XLSX" ] || [ -z "$CUSTOMER_MASTER" ] || [ -z "$PRODUCTION_SNAPSHOT" ]; then
  echo "Usage: $0 <Photo売上管理.xlsx> <customer-master.json> <production-customers-snapshot.json> [output-dir] [LINE履歴.xlsx]"
  echo "NOTE=Production snapshot must be obtained separately under an authorized read-only flow and supplied as complete JSON envelope customer-crm-production-identity-snapshot-v1."
  exit 2
fi

test -f "$SALES_XLSX" || { echo "RESULT=STOP_SALES_XLSX_MISSING"; exit 3; }
test -f "$CUSTOMER_MASTER" || { echo "RESULT=STOP_CUSTOMER_MASTER_MISSING"; exit 4; }
test -f "$PRODUCTION_SNAPSHOT" || { echo "RESULT=STOP_PRODUCTION_SNAPSHOT_MISSING"; exit 5; }

LOCAL_HEAD="$(git rev-parse HEAD 2>/dev/null || printf 'unknown')"
echo "=================================================="
echo " CUSTOMER CRM — SALES RECONCILIATION RUNNER"
echo " LOCAL ONLY / EXACT IDENTITY EVIDENCE ONLY"
echo "=================================================="
echo "LOCAL_HEAD=$LOCAL_HEAD"
echo "PRODUCTION_NETWORK_ACCESS=0"
echo "PRODUCTION_D1_READ=0"
echo "PRODUCTION_D1_WRITE=0"

if [ -z "$OUT_DIR" ]; then
  OUT_DIR="$HOME/customer-crm-sales-reconciliation-$(date '+%Y%m%d-%H%M%S')"
fi
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
TMP="$OUT_DIR/tmp"
mkdir -p "$TMP"
SALES_RECORDS="$TMP/sales-records.json"
LINE_EVIDENCE="$TMP/line-name-evidence.json"

echo
echo "=== 1. Extract Photo sales schedule rows locally ==="
python3 scripts/extract-photo-sales-xlsx.py --xlsx "$SALES_XLSX" --out "$SALES_RECORDS"

LINE_EVIDENCE_ARGS=()
if [ -n "$LINE_HISTORY_XLSX" ]; then
  test -f "$LINE_HISTORY_XLSX" || { echo "RESULT=STOP_LINE_HISTORY_XLSX_MISSING"; exit 6; }
  python3 scripts/extract-line-name-evidence-xlsx.py \
    --line-history-xlsx "$LINE_HISTORY_XLSX" \
    --sales-records "$SALES_RECORDS" \
    --out "$LINE_EVIDENCE"
  LINE_EVIDENCE_ARGS=(--line-evidence "$LINE_EVIDENCE")
  echo "LINE_NAME_EVIDENCE=READY"
else
  echo "LINE_NAME_EVIDENCE=NOT_PROVIDED"
fi

echo
echo "=== 2. Validate supplied Production identity snapshot locally ==="
python3 - "$PRODUCTION_SNAPSHOT" <<'PY'
import json, pathlib, sys
path=pathlib.Path(sys.argv[1])
raw=path.read_text(encoding='utf-8-sig')
if not raw.strip():
    print('RESULT=STOP_PRODUCTION_SNAPSHOT_EMPTY')
    raise SystemExit(7)
try:
    data=json.loads(raw)
except json.JSONDecodeError:
    print('RESULT=STOP_PRODUCTION_SNAPSHOT_INVALID_JSON')
    raise SystemExit(8)
if not isinstance(data, dict):
    print('RESULT=STOP_PRODUCTION_SNAPSHOT_ENVELOPE_REQUIRED')
    raise SystemExit(9)
if data.get('snapshot_format') != 'customer-crm-production-identity-snapshot-v1':
    print('RESULT=STOP_PRODUCTION_SNAPSHOT_FORMAT_INVALID')
    raise SystemExit(10)
if data.get('complete') is not True:
    print('RESULT=STOP_PRODUCTION_SNAPSHOT_NOT_COMPLETE')
    raise SystemExit(11)
if data.get('query_scope') != 'all_customer_identities':
    print('RESULT=STOP_PRODUCTION_SNAPSHOT_SCOPE_INVALID')
    raise SystemExit(12)
customers=data.get('customers')
count=data.get('customer_count')
if isinstance(count, bool) or not isinstance(count, int) or not isinstance(customers, list) or count != len(customers) or count <= 0:
    print('RESULT=STOP_PRODUCTION_SNAPSHOT_COUNT_INVALID')
    raise SystemExit(13)
print('PRODUCTION_SNAPSHOT_FORMAT=customer-crm-production-identity-snapshot-v1')
print('PRODUCTION_SNAPSHOT_SCOPE=all_customer_identities')
print('PRODUCTION_SNAPSHOT_COMPLETE=PASS')
print('PRODUCTION_SNAPSHOT_INPUT=PASS')
print('PRODUCTION_NETWORK_ACCESS=0')
PY

echo
echo "=== 3. Reconcile locally ==="
node scripts/reconcile-photo-sales-readonly.mjs \
  --sales-records "$SALES_RECORDS" \
  --customer-master "$CUSTOMER_MASTER" \
  --production-customers "$PRODUCTION_SNAPSHOT" \
  --out-dir "$OUT_DIR" \
  "${LINE_EVIDENCE_ARGS[@]}"

if command -v open >/dev/null 2>&1; then
  open "$OUT_DIR/review.html" >/dev/null 2>&1 || true
  echo "LOCAL_REVIEW_HTML=OPEN_REQUESTED"
fi

echo
echo "=================================================="
echo " RESULT=SALES_CRM_LOCAL_RUNNER_COMPLETE"
echo "=================================================="
echo "OUTPUT_DIR=$OUT_DIR"
echo "PRODUCTION_SNAPSHOT_SOURCE=SUPPLIED_COMPLETE_ENVELOPE_FILE_ONLY"
echo "PRODUCTION_NETWORK_ACCESS=0"
echo "PRODUCTION_D1_READ=0"
echo "PRODUCTION_D1_WRITE=0"
echo "CUSTOMER_CREATION=0"
echo "CUSTOMER_ID_GENERATION=0"
echo "CUSTOMER_UPDATE=0"
echo "CUSTOMER_DELETE=0"
echo "CUSTOMER_MERGE=0"
echo "FUZZY_AUTO_LINK=0"
echo "LINE_BODY_AUTO_LINK=0"
echo "LINE_SEND=0"
echo "PRODUCTION_DEPLOY=0"
echo "=================================================="
