#!/usr/bin/env bash
set -euo pipefail

ACCOUNT_ID="799b203a471e51a791129a5ca97a9b2b"
CRM_DB_ID="1ae3e0d9-72c0-47ad-8fc1-fed9d15ec70f"
WRANGLER_VERSION="4.107.0"

PREVIEW_DIR="${1:-}"
OUT_DIR="${2:-}"

if [ -z "$PREVIEW_DIR" ]; then
  echo "Usage: $0 <owner-backfill-preview-dir> [output-dir]"
  exit 2
fi

test -d "$PREVIEW_DIR" || { echo "RESULT=STOP_PREVIEW_DIR_MISSING"; exit 3; }

SUMMARY="$PREVIEW_DIR/owner-backfill-preview-summary.json"
SQL="$PREVIEW_DIR/owner-backfill-preview.sql"

test -f "$SUMMARY" || { echo "RESULT=STOP_PREVIEW_SUMMARY_MISSING"; exit 4; }
test -f "$SQL" || { echo "RESULT=STOP_PREVIEW_SQL_MISSING"; exit 5; }

LOCAL_HEAD="$(git rev-parse HEAD)"
REMOTE_MAIN="$(git ls-remote origin refs/heads/main | awk '{print $1}')"

echo "=================================================="
echo " CUSTOMER CRM — OWNER BACKFILL D1 READONLY PREVIEW"
echo " SELECT ONLY / NO PRODUCTION WRITE"
echo "=================================================="
echo "LOCAL_HEAD=$LOCAL_HEAD"
echo "REMOTE_MAIN=$REMOTE_MAIN"

test "$LOCAL_HEAD" = "$REMOTE_MAIN" || {
  echo "RESULT=STOP_MAIN_DRIFT"
  exit 6
}
echo "MAIN_SHA_GUARD=PASS"

if [ -z "$OUT_DIR" ]; then
  OUT_DIR="$PREVIEW_DIR/d1-readonly-result"
fi
if [ -e "$OUT_DIR" ] && [ -n "$(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
  echo "RESULT=STOP_OUTPUT_DIR_NOT_EMPTY"
  exit 7
fi
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
test "$OUT_DIR" != "/" || { echo "RESULT=STOP_UNSAFE_OUTPUT_DIR"; exit 8; }

echo
echo "=== 1. Verify preview artifact hashes and READ ONLY SQL ==="

python3 - "$SUMMARY" "$SQL" <<'PY'
import hashlib,json,re,sys
from pathlib import Path

summary=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
sql_bytes=Path(sys.argv[2]).read_bytes()
actual=hashlib.sha256(sql_bytes).hexdigest()
expected=str(summary.get("preview_sql_sha256") or "")

if actual!=expected:
    print("RESULT=STOP_PREVIEW_SQL_HASH_MISMATCH")
    raise SystemExit(10)

sql=sql_bytes.decode("utf-8",errors="strict")
body=re.sub(r"^--.*$","",sql,flags=re.M)
if re.search(r"\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE|UPSERT)\b",body,re.I):
    print("RESULT=STOP_PREVIEW_SQL_NOT_READ_ONLY")
    raise SystemExit(11)

if not summary.get("preview_ready") or int(summary.get("validation_error_count") or 0)!=0:
    print("RESULT=STOP_PREVIEW_SUMMARY_NOT_READY")
    raise SystemExit(12)

print("PREVIEW_SQL_SHA256="+actual)
print("PREVIEW_SQL_READ_ONLY=PASS")
PY

CFG="$OUT_DIR/wrangler-readonly.jsonc"
cat >"$CFG" <<JSON
{
  "name": "customer-crm-owner-backfill-readonly-preview",
  "compatibility_date": "2026-09-18",
  "account_id": "$ACCOUNT_ID",
  "d1_databases": [
    {
      "binding": "CRM_DB",
      "database_name": "customer-crm-db",
      "database_id": "$CRM_DB_ID"
    }
  ]
}
JSON

wrangler_clean() {
  env     -u CLOUDFLARE_API_TOKEN     -u CLOUDFLARE_API_KEY     -u CLOUDFLARE_EMAIL     CLOUDFLARE_ACCOUNT_ID="$ACCOUNT_ID"     npx --yes "wrangler@$WRANGLER_VERSION" "$@"
}

auth_probe() {
  wrangler_clean d1 execute CRM_DB     --config "$CFG"     --remote     --json     --command "SELECT 1 AS auth_probe;"     >"$OUT_DIR/auth.raw" 2>&1
}

echo
echo "=== 2. Cloudflare D1 auth probe — SELECT ONLY ==="

set +e
auth_probe
AUTH_RC=$?
set -e

if [ "$AUTH_RC" -ne 0 ]; then
  if grep -Eqi 'code[^0-9]*10000|authentication error|unauthorized|forbidden' "$OUT_DIR/auth.raw"; then
    echo "CRM_D1_AUTH=REFRESH_REQUIRED"
    env       -u CLOUDFLARE_API_TOKEN       -u CLOUDFLARE_API_KEY       -u CLOUDFLARE_EMAIL       npx --yes "wrangler@$WRANGLER_VERSION" login
    auth_probe || {
      echo "RESULT=STOP_CRM_D1_AUTH_FAILED_AFTER_REFRESH"
      exit 20
    }
  else
    echo "RESULT=STOP_CRM_D1_AUTH_FAILED"
    exit 21
  fi
fi

echo "CRM_D1_AUTH=PASS"
echo "SECRET_VALUES_PRINTED=NO"

echo
echo "=== 3. Execute exact backfill preview — SELECT ONLY ==="

wrangler_clean d1 execute CRM_DB   --config "$CFG"   --remote   --json   --file "$SQL"   >"$OUT_DIR/preview.raw" 2>&1

python3 - "$OUT_DIR/preview.raw" "$OUT_DIR/preview.clean.json" <<'PY'
import json,re,sys
from pathlib import Path

text=Path(sys.argv[1]).read_text(encoding="utf-8",errors="replace")
text=re.sub(r"\x1b\[[0-9;?]*[ -/]*[@-~]","",text)
dec=json.JSONDecoder()
root=None
for i,ch in enumerate(text):
    if ch not in "[{":
        continue
    try:
        value,_=dec.raw_decode(text[i:])
    except Exception:
        continue
    if isinstance(value,(list,dict)):
        root=value
        break

if root is None:
    print("RESULT=STOP_D1_PREVIEW_JSON_PARSE")
    raise SystemExit(30)

Path(sys.argv[2]).write_text(json.dumps(root,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print("D1_PREVIEW_JSON=PASS")
PY

echo
echo "=== 4. Aggregate privacy-safe D1 preview result ==="

node scripts/summarize-line-history-owner-backfill-preview.mjs   --preview-summary "$SUMMARY"   --d1-raw "$OUT_DIR/preview.clean.json"   --out "$OUT_DIR/owner-backfill-preview-result.json"

echo
echo "=================================================="
echo " RESULT=OWNER_BACKFILL_D1_READONLY_PREVIEW_COMPLETE"
echo "=================================================="
echo "PREVIEW_RESULT=$OUT_DIR/owner-backfill-preview-result.json"
echo "PRODUCTION_D1_READ=YES"
echo "PRODUCTION_D1_WRITE=0"
echo "WRITE_SQL_GENERATED=0"
echo "CUSTOMER_ID_GENERATION=0"
echo "CUSTOMER_UPDATE=0"
echo "CUSTOMER_DELETE=0"
echo "CUSTOMER_MERGE=0"
echo "LINE_SEND=0"
echo "WORKER_DEPLOY=0"
echo "PRODUCTION_DEPLOY=0"
echo "=================================================="
