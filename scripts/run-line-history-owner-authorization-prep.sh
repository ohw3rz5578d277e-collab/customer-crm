#!/usr/bin/env bash
set -euo pipefail

RESUME_DIR="${1:-}"
CUSTOMER_MASTER="${2:-}"
DECISIONS="${3:-}"
OUT_DIR="${4:-}"

if [ -z "$RESUME_DIR" ] || [ -z "$CUSTOMER_MASTER" ] || [ -z "$DECISIONS" ]; then
  echo "Usage: $0 <resume-output-dir> <customer-master.json> <owner-decisions.json> [output-dir]"
  exit 2
fi

test -d "$RESUME_DIR" || { echo "RESULT=STOP_RESUME_DIR_MISSING"; exit 3; }
test -f "$CUSTOMER_MASTER" || { echo "RESULT=STOP_CUSTOMER_MASTER_MISSING"; exit 4; }
test -f "$DECISIONS" || { echo "RESULT=STOP_DECISIONS_MISSING"; exit 5; }

TRIAGE="$RESUME_DIR/final-triage.json"
CUSTOMERS="$RESUME_DIR/baseline/customers.json"

test -f "$TRIAGE" || { echo "RESULT=STOP_FINAL_TRIAGE_MISSING"; exit 6; }
test -f "$CUSTOMERS" || { echo "RESULT=STOP_BASELINE_CUSTOMERS_MISSING"; exit 7; }

LOCAL_HEAD="$(git rev-parse HEAD)"
REMOTE_MAIN="$(git ls-remote origin refs/heads/main | awk '{print $1}')"

echo "=================================================="
echo " CUSTOMER CRM — OWNER DECISION AUTHORIZATION PREP"
echo " LOCAL ONLY / NO SQL / NO PRODUCTION WRITE"
echo "=================================================="
echo "LOCAL_HEAD=$LOCAL_HEAD"
echo "REMOTE_MAIN=$REMOTE_MAIN"

test "$LOCAL_HEAD" = "$REMOTE_MAIN" || {
  echo "RESULT=STOP_MAIN_DRIFT"
  exit 8
}
echo "MAIN_SHA_GUARD=PASS"

if [ -z "$OUT_DIR" ]; then
  OUT_DIR="$RESUME_DIR/owner-authorization-prep"
fi

if [ -e "$OUT_DIR" ] && [ -n "$(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
  echo "RESULT=STOP_OUTPUT_DIR_NOT_EMPTY"
  exit 9
fi

mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
test "$OUT_DIR" != "/" || { echo "RESULT=STOP_UNSAFE_OUTPUT_DIR"; exit 10; }

PLAN="$OUT_DIR/decision-plan-private.json"
PACKET="$OUT_DIR/write-authorization-packet.json"

echo
echo "=== 1. Build private local decision plan ==="

node scripts/plan-line-history-owner-decisions.mjs   --triage "$TRIAGE"   --decisions "$DECISIONS"   --customer-master "$CUSTOMER_MASTER"   --customers "$CUSTOMERS"   --out "$PLAN"

echo
echo "=== 2. Validate decision plan ==="

python3 - "$PLAN" <<'PY'
import json,sys
from pathlib import Path

plan=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
errors=int(plan.get("validation_error_count") or 0)
proposed=int(plan.get("proposed_write_actions") or 0)
ready=bool(plan.get("ready_for_separate_write_authorization"))

print("PLAN_VALIDATION_ERROR_COUNT="+str(errors))
print("PLAN_PROPOSED_WRITE_ACTIONS="+str(proposed))
print("PLAN_READY_FOR_SEPARATE_WRITE_AUTHORIZATION="+("YES" if ready else "NO"))

if errors:
    print("RESULT=STOP_OWNER_DECISION_PLAN_VALIDATION")
    raise SystemExit(20)

if proposed>0 and not ready:
    print("RESULT=STOP_OWNER_DECISION_PLAN_NOT_READY")
    raise SystemExit(21)
PY

echo
echo "=== 3. Build public authorization packet ==="

node scripts/build-line-history-owner-authorization-packet.mjs   --plan "$PLAN"   --main-sha "$LOCAL_HEAD"   --out "$PACKET"

echo
echo "=== 4. Final non-write gate ==="

python3 - "$PACKET" <<'PY'
import json,sys
from pathlib import Path

packet=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
blockers=packet.get("blockers") or []
proposed=int(packet.get("proposed_write_actions") or 0)
ready=bool(packet.get("packet_ready"))

if blockers or not ready:
    print("RESULT=STOP_AUTHORIZATION_PACKET_NOT_READY")
    raise SystemExit(30)

if proposed==0:
    print("RESULT=OWNER_DECISIONS_NO_PRODUCTION_WRITE_REQUIRED")
else:
    print("RESULT=OWNER_WRITE_AUTHORIZATION_PACKET_READY")
PY

echo
echo "=================================================="
echo " DECISION AUTHORIZATION PREP COMPLETE"
echo "=================================================="
echo "PRIVATE_PLAN=$PLAN"
echo "PUBLIC_PACKET=$PACKET"
echo "AUTHORIZATION_GRANTED=NO"
echo "SQL_GENERATED=0"
echo "SQL_EXECUTED=0"
echo "PRODUCTION_D1_READ=0"
echo "PRODUCTION_D1_WRITE=0"
echo "CUSTOMER_ID_GENERATION=0"
echo "CUSTOMER_UPDATE=0"
echo "CUSTOMER_DELETE=0"
echo "CUSTOMER_MERGE=0"
echo "LINE_SEND=0"
echo "WORKER_DEPLOY=0"
echo "PRODUCTION_DEPLOY=0"
echo "=================================================="
