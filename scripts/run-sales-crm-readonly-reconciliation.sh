#!/usr/bin/env bash
set -euo pipefail

SALES_XLSX="${1:-}"
CUSTOMER_MASTER="${2:-}"
OUT_DIR="${3:-}"
LINE_HISTORY_XLSX="${4:-}"

if [ -z "$SALES_XLSX" ] || [ -z "$CUSTOMER_MASTER" ]; then
  echo "Usage: $0 <Photo売上管理.xlsx> <customer-master.json> [output-dir] [LINE履歴.xlsx]"
  exit 2
fi

test -f "$SALES_XLSX" || { echo "RESULT=STOP_SALES_XLSX_MISSING"; exit 3; }
test -f "$CUSTOMER_MASTER" || { echo "RESULT=STOP_CUSTOMER_MASTER_MISSING"; exit 4; }

REMOTE_MAIN="$(git ls-remote origin refs/heads/main | awk '{print $1}')"
LOCAL_HEAD="$(git rev-parse HEAD)"

echo "=================================================="
echo " CUSTOMER CRM — SALES RECONCILIATION RUNNER"
echo " READ ONLY / EXACT IDENTITY EVIDENCE ONLY"
echo "=================================================="
echo "LOCAL_HEAD=$LOCAL_HEAD"
echo "REMOTE_MAIN=$REMOTE_MAIN"

test "$LOCAL_HEAD" = "$REMOTE_MAIN" || {
  echo "RESULT=STOP_LOCAL_NOT_CURRENT_MAIN"
  exit 5
}

test -z "$(git status --porcelain)" || {
  echo "RESULT=STOP_WORKTREE_DIRTY"
  git status --short
  exit 6
}

if [ -z "$OUT_DIR" ]; then
  OUT_DIR="$HOME/customer-crm-sales-reconciliation-$(date '+%Y%m%d-%H%M%S')"
fi

mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
TMP="$OUT_DIR/tmp"
mkdir -p "$TMP"

SALES_RECORDS="$TMP/sales-records.json"
PRODUCTION_RAW="$TMP/production-customers.raw"
LINE_EVIDENCE="$TMP/line-name-evidence.json"

echo
echo "=== 1. Extract Photo sales schedule rows ==="

python3 scripts/extract-photo-sales-xlsx.py   --xlsx "$SALES_XLSX"   --out "$SALES_RECORDS"

LINE_EVIDENCE_ARGS=()
if [ -n "$LINE_HISTORY_XLSX" ]; then
  test -f "$LINE_HISTORY_XLSX" || { echo "RESULT=STOP_LINE_HISTORY_XLSX_MISSING"; exit 7; }
  python3 scripts/extract-line-name-evidence-xlsx.py \
    --line-history-xlsx "$LINE_HISTORY_XLSX" \
    --sales-records "$SALES_RECORDS" \
    --out "$LINE_EVIDENCE"
  LINE_EVIDENCE_ARGS=(--line-evidence "$LINE_EVIDENCE")
  echo "LINE_NAME_EVIDENCE=READY"
else
  echo "LINE_NAME_EVIDENCE=NOT_PROVIDED"
fi

SQL="SELECT customer_id,name,line_user_id FROM customers WHERE COALESCE(deleted_at,'')='' ORDER BY customer_id;"

python3 - "$SQL" <<'PY'
import re,sys
sql=sys.argv[1]
if not re.match(r"^\s*SELECT\b",sql,re.I):
    print("RESULT=STOP_PRODUCTION_QUERY_NOT_SELECT")
    raise SystemExit(10)
if re.search(r"\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE|UPSERT)\b",sql,re.I):
    print("RESULT=STOP_PRODUCTION_QUERY_WRITE_TOKEN")
    raise SystemExit(11)
print("PRODUCTION_QUERY_STATIC_AUDIT=PASS")
print("QUERY_MODE=SELECT_ONLY")
PY

echo
echo "=== 2. Read active Production CRM identity fields ==="

DB_NAME="$(
node - <<'NODE'
const fs=require('fs');
const cfg=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
const db=(cfg.d1_databases||[]).find(x=>x.binding==='DB');
if(!db?.database_name)process.exit(2);
process.stdout.write(String(db.database_name));
NODE
)"

if [ -n "${CRM_CF_ACCOUNT_ID:-}" ]; then
  DB_ID="$(
  node - <<'NODE'
const fs=require('fs');
const cfg=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
const db=(cfg.d1_databases||[]).find(x=>x.binding==='DB');
if(!db?.database_id)process.exit(2);
process.stdout.write(String(db.database_id));
NODE
  )"

  CFG="$TMP/wrangler-readonly.jsonc"
  cat >"$CFG" <<JSON
{
  "name": "customer-crm-sales-reconciliation-readonly",
  "compatibility_date": "2026-09-21",
  "account_id": "${CRM_CF_ACCOUNT_ID}",
  "d1_databases": [
    {
      "binding": "DB",
      "database_name": "$DB_NAME",
      "database_id": "$DB_ID"
    }
  ]
}
JSON

  env     -u CLOUDFLARE_API_TOKEN     -u CLOUDFLARE_API_KEY     -u CLOUDFLARE_EMAIL     CLOUDFLARE_ACCOUNT_ID="$CRM_CF_ACCOUNT_ID"     npx --yes wrangler@4.107.0 d1 execute "$DB_NAME"       --config "$CFG"       --remote       --json       --command "$SQL"       >"$PRODUCTION_RAW" 2>&1
else
  env     -u CLOUDFLARE_API_TOKEN     -u CLOUDFLARE_API_KEY     -u CLOUDFLARE_EMAIL     npx --yes wrangler@4.107.0 d1 execute "$DB_NAME"       --remote       --json       --command "$SQL"       >"$PRODUCTION_RAW" 2>&1
fi

echo "PRODUCTION_D1_READ=PASS"

echo
echo "=== 3. Reconcile locally ==="

node scripts/reconcile-photo-sales-readonly.mjs \
  --sales-records "$SALES_RECORDS" \
  --customer-master "$CUSTOMER_MASTER" \
  --production-customers "$PRODUCTION_RAW" \
  --out-dir "$OUT_DIR" \
  "${LINE_EVIDENCE_ARGS[@]}"

if command -v open >/dev/null 2>&1; then
  open "$OUT_DIR/review.html"
  echo "LOCAL_REVIEW_HTML=OPENED"
fi

echo
echo "=================================================="
echo " RESULT=SALES_CRM_READONLY_RUNNER_COMPLETE"
echo "=================================================="
echo "OUTPUT_DIR=$OUT_DIR"
echo "PRODUCTION_D1_READ=YES"
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
