#!/usr/bin/env bash
set -euo pipefail

REPO="ohw3rz5578d277e-collab/customer-crm"
ACCOUNT_ID="799b203a471e51a791129a5ca97a9b2b"
RESERVATION_DB_ID="507be6dd-7c94-4c84-ae28-81624834f84a"
CRM_DB_ID="1ae3e0d9-72c0-47ad-8fc1-fed9d15ec70f"
WRANGLER_VERSION="4.107.0"

CANDIDATES="${1:-}"
CUSTOMER_MASTER="${2:-}"
OUT_DIR="${3:-line-history-exact-reservation-resume-results}"

if [ -z "$CANDIDATES" ] || [ -z "$CUSTOMER_MASTER" ]; then
  echo "Usage: $0 candidate-snapshot.json customer-master.json [output-dir]"
  exit 2
fi

test -f "$CANDIDATES" || { echo "RESULT=STOP_CANDIDATE_SNAPSHOT_MISSING"; exit 3; }
test -f "$CUSTOMER_MASTER" || { echo "RESULT=STOP_CUSTOMER_MASTER_MISSING"; exit 4; }

echo "=================================================="
echo " CUSTOMER CRM — EXACT RESERVATION READONLY RESUME"
echo " AUTH -> D1 SELECT -> LOCAL JOIN -> TRIAGE"
echo " NO PRODUCTION WRITE / NO DEPLOY / NO LINE SEND"
echo "=================================================="

LOCAL_HEAD="$(git rev-parse HEAD)"
REMOTE_MAIN="$(git ls-remote origin refs/heads/main | awk '{print $1}')"

echo "LOCAL_HEAD=$LOCAL_HEAD"
echo "REMOTE_MAIN=$REMOTE_MAIN"

test "$LOCAL_HEAD" = "$REMOTE_MAIN" || {
  echo "RESULT=STOP_MAIN_DRIFT"
  exit 5
}
echo "MAIN_SHA_GUARD=PASS"

if [ -e "$OUT_DIR" ] && [ -n "$(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
  echo "RESULT=STOP_OUTPUT_DIR_NOT_EMPTY"
  exit 6
fi
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
test "$OUT_DIR" != "/" || { echo "RESULT=STOP_UNSAFE_OUTPUT_DIR"; exit 7; }

CFG="$OUT_DIR/wrangler-readonly.jsonc"
cat >"$CFG" <<JSON
{
  "name": "customer-crm-exact-reservation-readonly-resume",
  "compatibility_date": "2026-09-18",
  "account_id": "$ACCOUNT_ID",
  "d1_databases": [
    {
      "binding": "RESERVATION_DB",
      "database_name": "reservation-app-db",
      "database_id": "$RESERVATION_DB_ID"
    },
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

normalize_rows() {
  local src="$1"
  local dst="$2"
  python3 - "$src" "$dst" <<'PY'
import json,re,sys
from pathlib import Path

src,dst=sys.argv[1],sys.argv[2]
text=Path(src).read_text(encoding="utf-8",errors="replace")
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
    raise SystemExit("RESULT=STOP_WRANGLER_JSON_PARSE")

rows=[]
def walk(v):
    if isinstance(v,dict):
        if isinstance(v.get("results"),list):
            rows.extend(x for x in v["results"] if isinstance(x,dict))
        for x in v.values():
            walk(x)
    elif isinstance(v,list):
        for x in v:
            walk(x)
walk(root)

Path(dst).write_text(json.dumps(rows,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print("ROWS="+str(len(rows)))
PY
}

auth_probe() {
  local binding="$1"
  local raw="$2"
  wrangler_clean d1 execute "$binding"     --config "$CFG"     --remote     --json     --command "SELECT 1 AS auth_probe;"     >"$raw" 2>&1
}

echo
echo "=== 1. Cloudflare READ ONLY auth probes ==="

set +e
auth_probe RESERVATION_DB "$OUT_DIR/reservation-auth.raw"
RES_AUTH_RC=$?
set -e

if [ "$RES_AUTH_RC" -ne 0 ]; then
  if grep -Eqi 'code[^0-9]*10000|authentication error|unauthorized|forbidden' "$OUT_DIR/reservation-auth.raw"; then
    echo "RESERVATION_D1_AUTH=REFRESH_REQUIRED"
    echo "CLOUDFLARE_LOGIN=START"
    env       -u CLOUDFLARE_API_TOKEN       -u CLOUDFLARE_API_KEY       -u CLOUDFLARE_EMAIL       npx --yes "wrangler@$WRANGLER_VERSION" login
    echo "CLOUDFLARE_LOGIN=FINISHED"
    auth_probe RESERVATION_DB "$OUT_DIR/reservation-auth.raw" || {
      echo "RESULT=STOP_RESERVATION_D1_AUTH_FAILED_AFTER_REFRESH"
      exit 10
    }
  else
    echo "RESULT=STOP_RESERVATION_D1_AUTH_FAILED"
    exit 11
  fi
fi

echo "RESERVATION_D1_AUTH=PASS"

auth_probe CRM_DB "$OUT_DIR/crm-auth.raw" || {
  echo "RESULT=STOP_CRM_D1_AUTH_FAILED"
  exit 12
}
echo "CRM_D1_AUTH=PASS"
echo "SECRET_VALUES_PRINTED=NO"

echo
echo "=== 2. Baseline Customer CRM triage — existing READ ONLY runner ==="

BASELINE_DIR="$OUT_DIR/baseline"
env   -u CLOUDFLARE_API_TOKEN   -u CLOUDFLARE_API_KEY   -u CLOUDFLARE_EMAIL   CLOUDFLARE_ACCOUNT_ID="$ACCOUNT_ID"   bash scripts/run-line-history-unresolved-readonly.sh     "$CANDIDATES"     "$CUSTOMER_MASTER"     "$BASELINE_DIR"

test -f "$BASELINE_DIR/triage.json" || {
  echo "RESULT=STOP_BASELINE_TRIAGE_MISSING"
  exit 20
}

echo
echo "=== 3. Extract REVIEW_REQUIRED source hints locally ==="

python3 - "$BASELINE_DIR/triage.json" "$OUT_DIR/review-hints.json" <<'PY'
import json,sys
from pathlib import Path

src,out=sys.argv[1],sys.argv[2]
triage=json.loads(Path(src).read_text(encoding="utf-8"))

hints=[]
for row in triage.get("classifications",[]):
    if row.get("category")!="REVIEW_REQUIRED":
        continue
    for key in ("customer_id_hints","legacy_customer_id_hints"):
        for value in row.get(key,[]) or []:
            value=str(value or "").strip()
            if value and value not in hints:
                hints.append(value)

Path(out).write_text(json.dumps(hints,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print("REVIEW_REQUIRED_SOURCE_HINTS="+str(len(hints)))
print("RAW_HINT_VALUES_PRINTED=NO")
PY

HINT_COUNT="$(python3 - "$OUT_DIR/review-hints.json" <<'PY'
import json,sys
print(len(json.load(open(sys.argv[1],encoding="utf-8"))))
PY
)"

if [ "$HINT_COUNT" -eq 0 ]; then
  cp "$BASELINE_DIR/triage.json" "$OUT_DIR/final-triage.json"
  printf '[]\n' >"$OUT_DIR/exact-reservation-evidence.json"
  echo "RESULT=NO_REVIEW_REQUIRED_HINTS"
  echo "SAFE_EXACT_RESERVATION_GROUPS=0"
  echo "SAFE_EXACT_RESERVATION_MESSAGES=0"
  echo "PRODUCTION_D1_WRITE=0"
  echo "LINE_SEND=0"
  exit 0
fi

echo
echo "=== 4. Reservation Production schema — SELECT ONLY ==="

wrangler_clean d1 execute RESERVATION_DB   --config "$CFG"   --remote   --json   --command "PRAGMA table_info(app_reservations);"   >"$OUT_DIR/reservation-schema.raw" 2>&1

normalize_rows "$OUT_DIR/reservation-schema.raw" "$OUT_DIR/reservation-schema.json" >/dev/null

python3 - "$OUT_DIR/reservation-schema.json" "$OUT_DIR/reservation-schema-flags.json" <<'PY'
import json,sys
from pathlib import Path

rows=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
cols={str(x.get("name") or "") for x in rows}
required={"reservation_id","customer_id"}
missing=required-cols
if missing:
    raise SystemExit("RESULT=STOP_RESERVATION_SCHEMA_MISSING_REQUIRED_COLUMNS")

flags={"deleted_at":"deleted_at" in cols}
Path(sys.argv[2]).write_text(json.dumps(flags,indent=2)+"\n",encoding="utf-8")
print("RESERVATION_SCHEMA=PASS")
print("RESERVATION_DELETED_AT="+("PRESENT" if flags["deleted_at"] else "ABSENT"))
PY

echo
echo "=== 5. Build Reservation SELECT chunks locally ==="

python3 -   "$OUT_DIR/review-hints.json"   "$OUT_DIR/reservation-schema-flags.json"   "$OUT_DIR" <<'PY'
import json,sys
from pathlib import Path

hints=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
flags=json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
out=Path(sys.argv[3])

def q(v):
    return "'" + str(v).replace("'","''") + "'"

chunks=[hints[i:i+25] for i in range(0,len(hints),25)]
for idx,chunk in enumerate(chunks,1):
    where="customer_id IN ("+",".join(q(x) for x in chunk)+")"
    if flags.get("deleted_at"):
        where+=" AND COALESCE(deleted_at,'')=''"
    sql=(
        "SELECT reservation_id, customer_id AS source_customer_id "
        "FROM app_reservations WHERE "+where+";"
    )
    (out/f"reservation-query-{idx:02d}.sql").write_text(sql+"\n",encoding="utf-8")

print("RESERVATION_SQL_CHUNKS="+str(len(chunks)))
print("SQL_MODE=SELECT_ONLY")
print("PRODUCTION_WRITE_SQL=0")
PY

echo
echo "=== 6. Reservation Production history — SELECT ONLY ==="

RES_RAW_LIST=()
for sql in "$OUT_DIR"/reservation-query-*.sql; do
  name="$(basename "$sql" .sql)"
  raw="$OUT_DIR/$name.raw"
  echo "READING_RESERVATION_CHUNK=$name"
  wrangler_clean d1 execute RESERVATION_DB     --config "$CFG"     --remote     --json     --file "$sql"     >"$raw" 2>&1
  RES_RAW_LIST+=("$raw")
done

python3 - "$OUT_DIR/reservation-history.json" "${RES_RAW_LIST[@]}" <<'PY'
import json,re,sys
from pathlib import Path

dst=Path(sys.argv[1])
rows=[]

def extract(path):
    text=Path(path).read_text(encoding="utf-8",errors="replace")
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
        raise SystemExit("RESULT=STOP_RESERVATION_JSON_PARSE")

    found=[]
    def walk(v):
        if isinstance(v,dict):
            if isinstance(v.get("results"),list):
                found.extend(x for x in v["results"] if isinstance(x,dict))
            for x in v.values():
                walk(x)
        elif isinstance(v,list):
            for x in v:
                walk(x)
    walk(root)
    return found

for path in sys.argv[2:]:
    rows.extend(extract(path))

unique=[]
seen=set()
for row in rows:
    rid=str(row.get("reservation_id") or "").strip()
    sid=str(row.get("source_customer_id") or "").strip()
    key=(rid,sid)
    if not rid or not sid or key in seen:
        continue
    seen.add(key)
    unique.append({"reservation_id":rid,"source_customer_id":sid})

dst.write_text(json.dumps(unique,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print("RESERVATION_HISTORY_ROWS="+str(len(unique)))
print("RAW_RESERVATION_IDS_PRINTED=NO")
print("RAW_SOURCE_IDS_PRINTED=NO")
PY

RESERVATION_HISTORY_COUNT="$(python3 - "$OUT_DIR/reservation-history.json" <<'PY'
import json,sys
print(len(json.load(open(sys.argv[1],encoding="utf-8"))))
PY
)"

if [ "$RESERVATION_HISTORY_COUNT" -eq 0 ]; then
  cp "$BASELINE_DIR/triage.json" "$OUT_DIR/final-triage.json"
  printf '[]\n' >"$OUT_DIR/exact-reservation-evidence.json"
  echo "RESULT=EXACT_RESERVATION_NO_HISTORY_MATCH"
  echo "SAFE_EXACT_RESERVATION_GROUPS=0"
  echo "SAFE_EXACT_RESERVATION_MESSAGES=0"
  echo "PRODUCTION_D1_WRITE=0"
  echo "LINE_SEND=0"
  exit 0
fi

echo
echo "=== 7. Customer CRM reservation schema — SELECT ONLY ==="

wrangler_clean d1 execute CRM_DB   --config "$CFG"   --remote   --json   --command "PRAGMA table_info(customer_reservations);"   >"$OUT_DIR/crm-reservation-schema.raw" 2>&1

normalize_rows "$OUT_DIR/crm-reservation-schema.raw" "$OUT_DIR/crm-reservation-schema.json" >/dev/null

python3 - "$OUT_DIR/crm-reservation-schema.json" "$OUT_DIR/crm-reservation-schema-flags.json" <<'PY'
import json,sys
from pathlib import Path

rows=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
cols={str(x.get("name") or "") for x in rows}
required={"reservation_id","customer_id"}
missing=required-cols
if missing:
    raise SystemExit("RESULT=STOP_CRM_RESERVATION_SCHEMA_MISSING_REQUIRED_COLUMNS")

flags={"deleted_at":"deleted_at" in cols}
Path(sys.argv[2]).write_text(json.dumps(flags,indent=2)+"\n",encoding="utf-8")
print("CRM_RESERVATION_SCHEMA=PASS")
print("CRM_RESERVATION_DELETED_AT="+("PRESENT" if flags["deleted_at"] else "ABSENT"))
PY

echo
echo "=== 8. Build Customer CRM exact reservation SELECT chunks locally ==="

python3 -   "$OUT_DIR/reservation-history.json"   "$OUT_DIR/crm-reservation-schema-flags.json"   "$OUT_DIR" <<'PY'
import json,sys
from pathlib import Path

history=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
flags=json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
out=Path(sys.argv[3])

ids=[]
for row in history:
    rid=str(row.get("reservation_id") or "").strip()
    if rid and rid not in ids:
        ids.append(rid)

def q(v):
    return "'" + str(v).replace("'","''") + "'"

chunks=[ids[i:i+25] for i in range(0,len(ids),25)]
for idx,chunk in enumerate(chunks,1):
    where="reservation_id IN ("+",".join(q(x) for x in chunk)+")"
    if flags.get("deleted_at"):
        where+=" AND COALESCE(deleted_at,'')=''"
    sql=(
        "SELECT reservation_id, customer_id "
        "FROM customer_reservations WHERE "+where+";"
    )
    (out/f"crm-reservation-query-{idx:02d}.sql").write_text(sql+"\n",encoding="utf-8")

print("CRM_RESERVATION_SQL_CHUNKS="+str(len(chunks)))
print("SQL_MODE=SELECT_ONLY")
print("PRODUCTION_WRITE_SQL=0")
PY

echo
echo "=== 9. Customer CRM exact reservation targets — SELECT ONLY ==="

CRM_RAW_LIST=()
for sql in "$OUT_DIR"/crm-reservation-query-*.sql; do
  name="$(basename "$sql" .sql)"
  raw="$OUT_DIR/$name.raw"
  echo "READING_CRM_RESERVATION_CHUNK=$name"
  wrangler_clean d1 execute CRM_DB     --config "$CFG"     --remote     --json     --file "$sql"     >"$raw" 2>&1
  CRM_RAW_LIST+=("$raw")
done

python3 - "$OUT_DIR/crm-reservations.json" "${CRM_RAW_LIST[@]}" <<'PY'
import json,re,sys
from pathlib import Path

dst=Path(sys.argv[1])
rows=[]

def extract(path):
    text=Path(path).read_text(encoding="utf-8",errors="replace")
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
        raise SystemExit("RESULT=STOP_CRM_RESERVATION_JSON_PARSE")

    found=[]
    def walk(v):
        if isinstance(v,dict):
            if isinstance(v.get("results"),list):
                found.extend(x for x in v["results"] if isinstance(x,dict))
            for x in v.values():
                walk(x)
        elif isinstance(v,list):
            for x in v:
                walk(x)
    walk(root)
    return found

for path in sys.argv[2:]:
    rows.extend(extract(path))

unique=[]
seen=set()
for row in rows:
    rid=str(row.get("reservation_id") or "").strip()
    cid=str(row.get("customer_id") or "").strip()
    key=(rid,cid)
    if not rid or key in seen:
        continue
    seen.add(key)
    unique.append({"reservation_id":rid,"customer_id":cid})

dst.write_text(json.dumps(unique,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print("CRM_EXACT_RESERVATION_ROWS="+str(len(unique)))
print("RAW_RESERVATION_IDS_PRINTED=NO")
print("RAW_CUSTOMER_IDS_PRINTED=NO")
PY

echo
echo "=== 10. Build privacy-safe exact reservation evidence locally ==="

node scripts/build-line-history-exact-reservation-evidence.mjs   --reservation-history "$OUT_DIR/reservation-history.json"   --crm-reservations "$OUT_DIR/crm-reservations.json"   --out "$OUT_DIR/exact-reservation-evidence.json"

echo
echo "=== 11. Re-classify locally with exact reservation evidence ==="

node scripts/classify-line-history-unresolved.mjs   --candidates "$CANDIDATES"   --customers "$BASELINE_DIR/customers.json"   --customer-master "$CUSTOMER_MASTER"   --reviews "$BASELINE_DIR/reviews.json"   --exact-reservation-evidence "$OUT_DIR/exact-reservation-evidence.json"   --out "$OUT_DIR/final-triage.json"

echo
echo "=== 12. Privacy-safe before / after summary ==="

python3 -   "$BASELINE_DIR/triage.json"   "$OUT_DIR/final-triage.json" <<'PY'
import json,sys
from pathlib import Path

before=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
after=json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))

def rows(reason=None,category=None):
    out=[]
    for row in after.get("classifications",[]):
        if reason is not None and row.get("reason")!=reason:
            continue
        if category is not None and row.get("category")!=category:
            continue
        out.append(row)
    return out

safe=rows(reason="EXACT_RESERVATION_ID_UNIQUE_CURRENT_TARGET")
line_conflict=rows(reason="EXACT_RESERVATION_LINE_CONFLICT")
multi=rows(reason="MULTIPLE_EXACT_RESERVATION_TARGETS")
missing=rows(reason="EXACT_RESERVATION_TARGET_MISSING")+rows(reason="EXACT_RESERVATION_TARGET_NOT_CURRENT")
no_line=rows(reason="EXACT_RESERVATION_REQUIRES_LINE_ID")

print("BEFORE_AUTO_CONFIRMABLE_GROUPS="+str(before.get("auto_confirmable_groups",0)))
print("AFTER_AUTO_CONFIRMABLE_GROUPS="+str(after.get("auto_confirmable_groups",0)))
print("BEFORE_REVIEW_REQUIRED_GROUPS="+str(before.get("review_required_groups",0)))
print("AFTER_REVIEW_REQUIRED_GROUPS="+str(after.get("review_required_groups",0)))
print("BEFORE_BLOCKED_CONFLICT_GROUPS="+str(before.get("blocked_conflict_groups",0)))
print("AFTER_BLOCKED_CONFLICT_GROUPS="+str(after.get("blocked_conflict_groups",0)))
print("SAFE_EXACT_RESERVATION_GROUPS="+str(len(safe)))
print("SAFE_EXACT_RESERVATION_MESSAGES="+str(sum(int(x.get("message_rows") or 0) for x in safe)))
print("EXACT_RESERVATION_LINE_CONFLICT_GROUPS="+str(len(line_conflict)))
print("MULTIPLE_EXACT_RESERVATION_TARGET_GROUPS="+str(len(multi)))
print("EXACT_RESERVATION_TARGET_MISSING_GROUPS="+str(len(missing)))
print("EXACT_RESERVATION_NO_LINE_GROUPS="+str(len(no_line)))
print("MESSAGE_TEXT_OUTPUT=0")
print("RAW_LINE_USER_ID_OUTPUT=0")
print("RAW_RESERVATION_ID_OUTPUT=0")
PY

echo
echo "=================================================="
echo " RESULT=EXACT_RESERVATION_READONLY_RESUME_COMPLETE"
echo "=================================================="
echo "PRODUCTION_D1_READ=YES"
echo "PRODUCTION_D1_WRITE=0"
echo "CUSTOMER_ID_GENERATION=0"
echo "CUSTOMER_UPDATE=0"
echo "CUSTOMER_DELETE=0"
echo "CUSTOMER_MERGE=0"
echo "LINE_SEND=0"
echo "WORKER_DEPLOY=0"
echo "PRODUCTION_DEPLOY=0"
echo "EVIDENCE=$OUT_DIR/exact-reservation-evidence.json"
echo "FINAL_TRIAGE=$OUT_DIR/final-triage.json"
echo "=================================================="
