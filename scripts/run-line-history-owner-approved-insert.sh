#!/usr/bin/env bash
set -euo pipefail

ACCOUNT_ID="799b203a471e51a791129a5ca97a9b2b"
CRM_DB_ID="1ae3e0d9-72c0-47ad-8fc1-fed9d15ec70f"
WRANGLER_VERSION="4.107.0"

PREAUTH_DIR="${1:-}"
D1_PREVIEW_DIR="${2:-}"
APPROVAL_FILE="${3:-}"
OUT_DIR="${4:-}"

if [ -z "$PREAUTH_DIR" ] || [ -z "$D1_PREVIEW_DIR" ] || [ -z "$APPROVAL_FILE" ]; then
  echo "Usage: $0 <owner-backfill-preauth-dir> <d1-preview-result-dir> <owner-exact-approval.txt> [output-dir]"
  exit 2
fi

test -d "$PREAUTH_DIR" || { echo "RESULT=STOP_PREAUTH_DIR_MISSING"; exit 3; }
test -d "$D1_PREVIEW_DIR" || { echo "RESULT=STOP_D1_PREVIEW_DIR_MISSING"; exit 4; }
test -f "$APPROVAL_FILE" || { echo "RESULT=STOP_EXACT_APPROVAL_FILE_MISSING"; exit 5; }

PREVIEW_DIR="$PREAUTH_DIR/readonly-preview"
PLAN="$PREAUTH_DIR/decision-plan-private.json"
SUMMARY="$PREVIEW_DIR/owner-backfill-preview-summary.json"
PREVIEW_SQL="$PREVIEW_DIR/owner-backfill-preview.sql"
PRIVATE_ROWS="$PREVIEW_DIR/owner-backfill-selected-private.json"
AUTHORIZED_PREVIEW_RESULT="$D1_PREVIEW_DIR/owner-backfill-preview-result.json"
PACKET="$D1_PREVIEW_DIR/write-authorization-packet.json"

for file in "$PLAN" "$SUMMARY" "$PREVIEW_SQL" "$PRIVATE_ROWS" "$AUTHORIZED_PREVIEW_RESULT" "$PACKET"; do
  test -f "$file" || { echo "RESULT=STOP_REQUIRED_ARTIFACT_MISSING"; exit 6; }
done

LOCAL_HEAD="$(git rev-parse HEAD)"
REMOTE_MAIN="$(git ls-remote origin refs/heads/main | awk '{print $1}')"

echo "=================================================="
echo " CUSTOMER CRM — OWNER APPROVED LINE HISTORY INSERT"
echo " EXACT APPROVAL + FRESH PREVIEW + INSERT-ONLY"
echo "=================================================="
echo "LOCAL_HEAD=$LOCAL_HEAD"
echo "REMOTE_MAIN=$REMOTE_MAIN"

test "$LOCAL_HEAD" = "$REMOTE_MAIN" || {
  echo "RESULT=STOP_MAIN_DRIFT"
  exit 7
}

PACKET_MAIN="$(python3 - "$PACKET" <<'PY'
import json,sys
print(str(json.load(open(sys.argv[1],encoding="utf-8")).get("source_main_sha") or ""))
PY
)"

test "$LOCAL_HEAD" = "$PACKET_MAIN" || {
  echo "RESULT=STOP_PACKET_MAIN_SHA_MISMATCH"
  exit 8
}
echo "MAIN_SHA_GUARD=PASS"

if [ -z "$OUT_DIR" ]; then
  OUT_DIR="$D1_PREVIEW_DIR/approved-insert-run"
fi

if [ -e "$OUT_DIR" ] && [ -n "$(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
  echo "RESULT=STOP_OUTPUT_DIR_NOT_EMPTY"
  exit 9
fi
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
test "$OUT_DIR" != "/" || { echo "RESULT=STOP_UNSAFE_OUTPUT_DIR"; exit 10; }

echo
echo "=== 1. Verify exact external Owner approval ==="

python3 - "$PACKET" "$APPROVAL_FILE" <<'PY'
import json,sys
from pathlib import Path

packet=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
approval=Path(sys.argv[2]).read_text(encoding="utf-8").strip()
expected=str(packet.get("approval_text") or "").strip()

if packet.get("planner")!="line_history_owner_write_authorization_packet_v2":
    print("RESULT=STOP_PACKET_FORMAT_INVALID")
    raise SystemExit(11)
if not packet.get("packet_ready") or not packet.get("authorization_required"):
    print("RESULT=STOP_PACKET_NOT_AUTHORIZABLE")
    raise SystemExit(12)
if packet.get("authorization_granted") is not False:
    print("RESULT=STOP_PACKET_AUTHORIZATION_STATE_INVALID")
    raise SystemExit(13)
if not expected or approval!=expected:
    print("RESULT=STOP_EXACT_OWNER_APPROVAL_MISMATCH")
    raise SystemExit(14)

print("EXACT_OWNER_APPROVAL=PASS")
print("APPROVAL_VALUE_PRINTED=NO")
PY

echo
echo "=== 2. Verify frozen artifact hashes ==="

python3 - "$PACKET" "$PLAN" "$AUTHORIZED_PREVIEW_RESULT" "$PRIVATE_ROWS" "$PREVIEW_SQL" <<'PY'
import hashlib,json,sys
from pathlib import Path

packet=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))

def h(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

checks=[
    ("PLAN_SHA",h(sys.argv[2]),str(packet.get("source_plan_sha256") or "")),
    ("PREVIEW_RESULT_SHA",h(sys.argv[3]),str(packet.get("source_preview_result_sha256") or "")),
    ("PRIVATE_ROWS_SHA",h(sys.argv[4]),str(packet.get("selected_private_rows_sha256") or "")),
    ("PREVIEW_SQL_SHA",h(sys.argv[5]),str(packet.get("source_preview_sql_sha256") or ""))
]

for name,actual,expected in checks:
    if actual!=expected:
        print("RESULT=STOP_"+name+"_MISMATCH")
        raise SystemExit(20)

sql=Path(sys.argv[5]).read_text(encoding="utf-8")
import re
body=re.sub(r"^--.*$","",sql,flags=re.M)
if re.search(r"\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE|UPSERT)\b",body,re.I):
    print("RESULT=STOP_PREVIEW_SQL_NOT_READ_ONLY")
    raise SystemExit(21)

print("FROZEN_ARTIFACT_HASHES=PASS")
print("PREVIEW_SQL_READ_ONLY=PASS")
PY

CFG="$OUT_DIR/wrangler-approved-insert.jsonc"
cat >"$CFG" <<JSON
{
  "name": "customer-crm-owner-approved-line-history-insert",
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
echo "=== 3. Cloudflare D1 authentication — SELECT ONLY ==="

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
      exit 30
    }
  else
    echo "RESULT=STOP_CRM_D1_AUTH_FAILED"
    exit 31
  fi
fi

echo "CRM_D1_AUTH=PASS"
echo "SECRET_VALUES_PRINTED=NO"

echo
echo "=== 4. Mandatory fresh Production D1 preview — SELECT ONLY ==="

wrangler_clean d1 execute CRM_DB   --config "$CFG"   --remote   --json   --file "$PREVIEW_SQL"   >"$OUT_DIR/fresh-preview.raw" 2>&1

python3 - "$OUT_DIR/fresh-preview.raw" "$OUT_DIR/fresh-preview.clean.json" <<'PY'
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
    print("RESULT=STOP_FRESH_PREVIEW_JSON_PARSE")
    raise SystemExit(40)
Path(sys.argv[2]).write_text(json.dumps(root,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print("FRESH_D1_PREVIEW_JSON=PASS")
PY

node scripts/summarize-line-history-owner-backfill-preview.mjs   --preview-summary "$SUMMARY"   --d1-raw "$OUT_DIR/fresh-preview.clean.json"   --out "$OUT_DIR/fresh-preview-result.json"

echo
echo "=== 5. Require fresh preview to be byte-identical to authorized preview result ==="

python3 - "$PACKET" "$OUT_DIR/fresh-preview-result.json" <<'PY'
import hashlib,json,sys
from pathlib import Path

packet=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
raw=Path(sys.argv[2]).read_bytes()
actual=hashlib.sha256(raw).hexdigest()
expected=str(packet.get("source_preview_result_sha256") or "")

if actual!=expected:
    print("RESULT=STOP_FRESH_PREVIEW_DRIFT")
    raise SystemExit(50)

fresh=json.loads(raw)
expected_rows=int(packet.get("exact_physical_insert_rows") or 0)
if int(fresh.get("exact_physical_insert_rows") or 0)!=expected_rows:
    print("RESULT=STOP_FRESH_PREVIEW_EXACT_ROWS_DRIFT")
    raise SystemExit(51)
if not fresh.get("preview_pass") or int(fresh.get("validation_error_count") or 0)!=0:
    print("RESULT=STOP_FRESH_PREVIEW_NOT_PASS")
    raise SystemExit(52)

print("FRESH_PREVIEW_MATCH_AUTHORIZED_RESULT=PASS")
print("EXACT_PHYSICAL_INSERT_ROWS="+str(expected_rows))
PY

echo
echo "=== 6. Generate exact-approved INSERT-only SQL locally ==="

node scripts/build-approved-line-history-insert.mjs   --packet "$PACKET"   --preview-result "$OUT_DIR/fresh-preview-result.json"   --private-rows "$PRIVATE_ROWS"   --approval-file "$APPROVAL_FILE"   --main-sha "$LOCAL_HEAD"   --out-sql "$OUT_DIR/approved-insert.sql"   --out-manifest "$OUT_DIR/approved-insert-manifest.json"

echo
echo "=== 7. Static SQL safety audit before Production write ==="

python3 - "$OUT_DIR/approved-insert.sql" "$OUT_DIR/approved-insert-manifest.json" <<'PY'
import json,re,sys
from pathlib import Path

sql=Path(sys.argv[1]).read_text(encoding="utf-8")
manifest=json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
body=re.sub(r"^--.*$","",sql,flags=re.M)

if not manifest.get("ready") or manifest.get("authorization_granted") is not True:
    print("RESULT=STOP_APPROVED_INSERT_MANIFEST_NOT_READY")
    raise SystemExit(60)

if re.search(r"\b(?:UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE|UPSERT)\b",body,re.I):
    print("RESULT=STOP_APPROVED_SQL_FORBIDDEN_WRITE")
    raise SystemExit(61)

targets=re.findall(r"INSERT\s+(?:OR\s+IGNORE\s+)?INTO\s+([A-Za-z0-9_]+)",body,re.I)
if targets!=["customer_line_messages"]:
    print("RESULT=STOP_APPROVED_SQL_TARGET_INVALID")
    raise SystemExit(62)

if re.search(r"INSERT\s+(?:OR\s+IGNORE\s+)?INTO\s+customers\b",body,re.I):
    print("RESULT=STOP_CUSTOMER_TABLE_WRITE")
    raise SystemExit(63)

print("APPROVED_SQL_STATIC_AUDIT=PASS")
print("CUSTOMER_TABLE_WRITE=0")
PY

echo
echo "=== 8. Execute exact-approved Production D1 INSERT-only write ==="

wrangler_clean d1 execute CRM_DB   --config "$CFG"   --remote   --json   --file "$OUT_DIR/approved-insert.sql"   >"$OUT_DIR/write.raw" 2>&1

python3 - "$OUT_DIR/write.raw" "$PACKET" "$OUT_DIR/write-result.json" <<'PY'
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
    print("RESULT=STOP_WRITE_RESULT_JSON_PARSE")
    raise SystemExit(70)

packet=json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
expected=int(packet.get("exact_physical_insert_rows") or 0)

changes=[]
def walk(v):
    if isinstance(v,dict):
        meta=v.get("meta")
        if isinstance(meta,dict) and isinstance(meta.get("changes"),(int,float)):
            changes.append(int(meta["changes"]))
        elif isinstance(v.get("changes"),(int,float)):
            changes.append(int(v["changes"]))
        for x in v.values():
            walk(x)
    elif isinstance(v,list):
        for x in v: walk(x)
walk(root)

actual=sum(changes)
result={
    "planner":"line_history_owner_approved_insert_result_v1",
    "expected_physical_insert_rows":expected,
    "reported_change_rows":actual,
    "change_metadata_found":bool(changes),
    "write_command_completed":True,
    "exact_change_count_match":bool(changes) and actual==expected
}
Path(sys.argv[3]).write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8")

print("WRITE_COMMAND_COMPLETED=YES")
print("EXPECTED_PHYSICAL_INSERT_ROWS="+str(expected))
print("REPORTED_CHANGE_ROWS="+str(actual))
print("CHANGE_METADATA_FOUND="+("YES" if changes else "NO"))
PY

echo
echo "=== 9. Mandatory post-write READ ONLY verification ==="

wrangler_clean d1 execute CRM_DB   --config "$CFG"   --remote   --json   --file "$PREVIEW_SQL"   >"$OUT_DIR/post-preview.raw" 2>&1

python3 - "$OUT_DIR/post-preview.raw" "$OUT_DIR/post-preview.clean.json" <<'PY'
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
    print("RESULT=STOP_POST_PREVIEW_JSON_PARSE")
    raise SystemExit(80)
Path(sys.argv[2]).write_text(json.dumps(root,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
PY

node scripts/summarize-line-history-owner-backfill-preview.mjs   --preview-summary "$SUMMARY"   --d1-raw "$OUT_DIR/post-preview.clean.json"   --out "$OUT_DIR/post-preview-result.json"

python3 - "$OUT_DIR/post-preview-result.json" "$OUT_DIR/write-result.json" <<'PY'
import json,sys
from pathlib import Path

post=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
write=json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))

if not post.get("preview_pass"):
    print("RESULT=STOP_POST_WRITE_PREVIEW_NOT_PASS")
    raise SystemExit(90)
if int(post.get("target_missing_rows") or 0)!=0:
    print("RESULT=STOP_POST_WRITE_TARGET_MISSING")
    raise SystemExit(91)
if int(post.get("target_line_conflict_rows") or 0)!=0:
    print("RESULT=STOP_POST_WRITE_TARGET_LINE_CONFLICT")
    raise SystemExit(92)
if int(post.get("would_insert_rows") or 0)!=0:
    print("RESULT=STOP_POST_WRITE_REMAINING_INSERTS")
    raise SystemExit(93)
if write.get("change_metadata_found") and not write.get("exact_change_count_match"):
    print("RESULT=STOP_WRITE_CHANGE_COUNT_MISMATCH")
    raise SystemExit(94)

print("POST_WRITE_PREVIEW=PASS")
print("REMAINING_WOULD_INSERT_ROWS=0")
print("TARGET_MISSING_ROWS=0")
print("TARGET_LINE_CONFLICT_ROWS=0")
PY

echo
echo "=================================================="
echo " RESULT=OWNER_APPROVED_LINE_HISTORY_INSERT_COMPLETE"
echo "=================================================="
echo "PRODUCTION_D1_READ=YES"
echo "PRODUCTION_D1_WRITE=YES"
echo "TARGET_TABLE=customer_line_messages"
echo "CUSTOMER_TABLE_WRITE=0"
echo "CUSTOMER_ID_GENERATION=0"
echo "CUSTOMER_UPDATE=0"
echo "CUSTOMER_DELETE=0"
echo "CUSTOMER_MERGE=0"
echo "LINE_SEND=0"
echo "WORKER_DEPLOY=0"
echo "PRODUCTION_DEPLOY=0"
echo "WRITE_RESULT=$OUT_DIR/write-result.json"
echo "POST_PREVIEW_RESULT=$OUT_DIR/post-preview-result.json"
echo "=================================================="
