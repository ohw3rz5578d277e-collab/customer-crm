#!/usr/bin/env bash
set -euo pipefail

REPO="ohw3rz5578d277e-collab/customer-crm"
ACCOUNT_ID="799b203a471e51a791129a5ca97a9b2b"
DB="customer-crm-db"
WRANGLER_VERSION="4.107.0"

CANDIDATES="${1:-}"
CUSTOMER_MASTER="${2:-}"
OUT_DIR="${3:-line-history-unresolved-triage-results}"

if [ -z "$CANDIDATES" ] || [ -z "$CUSTOMER_MASTER" ]; then
  echo "Usage: $0 candidate-snapshot.json customer-master.json [output-dir]"
  exit 2
fi

test -f "$CANDIDATES" || { echo "RESULT=STOP_CANDIDATE_SNAPSHOT_MISSING"; exit 3; }
test -f "$CUSTOMER_MASTER" || { echo "RESULT=STOP_CUSTOMER_MASTER_MISSING"; exit 4; }

echo "=================================================="
echo " CUSTOMER CRM — UNRESOLVED LINE HISTORY TRIAGE"
echo " READ ONLY / NO PRODUCTION WRITE"
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

export CLOUDFLARE_ACCOUNT_ID="$ACCOUNT_ID"
mkdir -p "$OUT_DIR"

CUSTOMERS_SQL="SELECT customer_id,COALESCE(line_user_id,'') AS line_user_id,COALESCE(name,'') AS name,COALESCE(phone,'') AS phone,COALESCE(email,'') AS email,COALESCE(deleted_at,'') AS deleted_at FROM customers WHERE COALESCE(deleted_at,'')='';"
REVIEWS_SQL="SELECT reservation_customer_id,crm_candidate_customer_id,decision FROM customer_id_reconciliation_reviews;"

echo "D1_ACTION=READ_ONLY_CUSTOMERS"
npx --yes "wrangler@${WRANGLER_VERSION}" d1 execute "$DB" --remote --json --command="$CUSTOMERS_SQL" >"$OUT_DIR/customers.raw" 2>&1

echo "D1_ACTION=READ_ONLY_RECONCILIATION_REVIEWS"
npx --yes "wrangler@${WRANGLER_VERSION}" d1 execute "$DB" --remote --json --command="$REVIEWS_SQL" >"$OUT_DIR/reviews.raw" 2>&1

python3 - "$OUT_DIR/customers.raw" "$OUT_DIR/customers.json" "$OUT_DIR/reviews.raw" "$OUT_DIR/reviews.json" <<'PY'
import json,re,sys
from pathlib import Path

def strip_ansi(s):
    return re.sub(r"\x1b\[[0-9;?]*[ -/]*[@-~]","",s)

def extract(path):
    text=strip_ansi(Path(path).read_text(encoding="utf-8",errors="replace"))
    dec=json.JSONDecoder()
    for i,ch in enumerate(text):
        if ch not in "[{":
            continue
        try:
            obj,_=dec.raw_decode(text[i:])
        except Exception:
            continue
        if isinstance(obj,(list,dict)):
            return obj
    raise SystemExit("RESULT=STOP_WRANGLER_JSON_PARSE")

for src,dst in [(sys.argv[1],sys.argv[2]),(sys.argv[3],sys.argv[4])]:
    Path(dst).write_text(json.dumps(extract(src),ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print("WRANGLER_JSON_NORMALIZE=PASS")
PY

node scripts/classify-line-history-unresolved.mjs   --candidates "$CANDIDATES"   --customers "$OUT_DIR/customers.json"   --customer-master "$CUSTOMER_MASTER"   --reviews "$OUT_DIR/reviews.json"   --out "$OUT_DIR/triage.json"

echo
echo "=================================================="
echo " RESULT=LINE_HISTORY_UNRESOLVED_READONLY_TRIAGE_COMPLETE"
echo " PRODUCTION_D1_READ=YES"
echo " PRODUCTION_D1_WRITE=0"
echo " CUSTOMER_ID_GENERATION=0"
echo " CUSTOMER_UPDATE=0"
echo " CUSTOMER_DELETE=0"
echo " LINE_SEND=0"
echo " WORKER_DEPLOY=0"
echo " PRODUCTION_DEPLOY=0"
echo " OUTPUT=$OUT_DIR/triage.json"
echo "=================================================="
