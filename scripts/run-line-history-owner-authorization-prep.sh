#!/usr/bin/env bash
set -euo pipefail

RESUME_DIR="${1:-}"
CUSTOMER_MASTER="${2:-}"
DECISIONS="${3:-}"
CANDIDATES="${4:-}"
OUT_DIR="${5:-}"

if [ -z "$RESUME_DIR" ] || [ -z "$CUSTOMER_MASTER" ] || [ -z "$DECISIONS" ] || [ -z "$CANDIDATES" ]; then
  echo "Usage: $0 <resume-output-dir> <customer-master.json> <owner-decisions.json> <candidate-snapshot.json> [output-dir]"
  exit 2
fi

test -d "$RESUME_DIR" || { echo "RESULT=STOP_RESUME_DIR_MISSING"; exit 3; }
test -f "$CUSTOMER_MASTER" || { echo "RESULT=STOP_CUSTOMER_MASTER_MISSING"; exit 4; }
test -f "$DECISIONS" || { echo "RESULT=STOP_DECISIONS_MISSING"; exit 5; }
test -f "$CANDIDATES" || { echo "RESULT=STOP_CANDIDATE_SNAPSHOT_MISSING"; exit 6; }

TRIAGE="$RESUME_DIR/final-triage.json"
CUSTOMERS="$RESUME_DIR/baseline/customers.json"

test -f "$TRIAGE" || { echo "RESULT=STOP_FINAL_TRIAGE_MISSING"; exit 7; }
test -f "$CUSTOMERS" || { echo "RESULT=STOP_BASELINE_CUSTOMERS_MISSING"; exit 8; }

LOCAL_HEAD="$(git rev-parse HEAD)"
REMOTE_MAIN="$(git ls-remote origin refs/heads/main | awk '{print $1}')"

echo "=================================================="
echo " CUSTOMER CRM — OWNER BACKFILL PREAUTH PREP"
echo " LOCAL ONLY -> READONLY PREVIEW SQL"
echo " NO WRITE AUTH / NO WRITE SQL / NO PRODUCTION WRITE"
echo "=================================================="
echo "LOCAL_HEAD=$LOCAL_HEAD"
echo "REMOTE_MAIN=$REMOTE_MAIN"

test "$LOCAL_HEAD" = "$REMOTE_MAIN" || {
  echo "RESULT=STOP_MAIN_DRIFT"
  exit 9
}
echo "MAIN_SHA_GUARD=PASS"

if [ -z "$OUT_DIR" ]; then
  OUT_DIR="$RESUME_DIR/owner-backfill-preauth"
fi

if [ -e "$OUT_DIR" ] && [ -n "$(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
  echo "RESULT=STOP_OUTPUT_DIR_NOT_EMPTY"
  exit 10
fi

mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
test "$OUT_DIR" != "/" || { echo "RESULT=STOP_UNSAFE_OUTPUT_DIR"; exit 11; }

PLAN="$OUT_DIR/decision-plan-private.json"
PREVIEW_DIR="$OUT_DIR/readonly-preview"
NO_WRITE_RECEIPT="$OUT_DIR/no-write-completion-receipt.json"

echo
echo "=== 1. Build private local Owner decision plan ==="

node scripts/plan-line-history-owner-decisions.mjs   --triage "$TRIAGE"   --decisions "$DECISIONS"   --customer-master "$CUSTOMER_MASTER"   --customers "$CUSTOMERS"   --out "$PLAN"

echo
echo "=== 2. Validate identity mapping plan ==="

python3 - "$PLAN" <<'PY'
import json,sys
from pathlib import Path

plan=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
errors=int(plan.get("validation_error_count") or 0)
identity_actions=int(plan.get("proposed_backfill_identity_actions") or 0)
physical_writes=int(plan.get("proposed_write_actions") or 0)
preview_ready=bool(plan.get("ready_for_readonly_backfill_preview"))
write_ready=bool(plan.get("ready_for_separate_write_authorization"))

print("PLAN_VALIDATION_ERROR_COUNT="+str(errors))
print("BACKFILL_IDENTITY_ACTIONS="+str(identity_actions))
print("PLAN_PHYSICAL_WRITE_ACTIONS="+str(physical_writes))
print("READY_FOR_READONLY_BACKFILL_PREVIEW="+("YES" if preview_ready else "NO"))
print("READY_FOR_SEPARATE_WRITE_AUTHORIZATION="+("YES" if write_ready else "NO"))

if errors:
    print("RESULT=STOP_OWNER_DECISION_PLAN_VALIDATION")
    raise SystemExit(20)

if physical_writes!=0 or write_ready:
    print("RESULT=STOP_PREVIEW_GATE_BYPASSED")
    raise SystemExit(21)

if identity_actions>0 and not preview_ready:
    print("RESULT=STOP_OWNER_DECISION_PLAN_NOT_READY_FOR_PREVIEW")
    raise SystemExit(22)
PY

echo
echo "=== 3. Zero-action completion gate ==="

if python3 - "$PLAN" "$NO_WRITE_RECEIPT" "$LOCAL_HEAD" <<'PY'
import json,sys
from pathlib import Path
from datetime import datetime, timezone

plan_path=Path(sys.argv[1])
receipt_path=Path(sys.argv[2])
main_sha=str(sys.argv[3])

plan=json.loads(plan_path.read_text(encoding="utf-8"))

groups=int(plan.get("review_queue_groups") or 0)
submitted=int(plan.get("submitted_decisions") or 0)
decided=int(plan.get("decided_known_groups") or 0)
undecided=int(plan.get("undecided_groups") or 0)
errors=int(plan.get("validation_error_count") or 0)
actions=int(plan.get("proposed_backfill_identity_actions") or 0)
physical=int(plan.get("proposed_write_actions") or 0)
no_write=int(plan.get("accepted_no_write_decisions") or 0)
preview_ready=bool(plan.get("ready_for_readonly_backfill_preview"))
write_ready=bool(plan.get("ready_for_separate_write_authorization"))

complete_no_write=(
    groups>0 and
    submitted==groups and
    decided==groups and
    undecided==0 and
    errors==0 and
    actions==0 and
    physical==0 and
    no_write==groups and
    not preview_ready and
    not write_ready
)

if not complete_no_write:
    raise SystemExit(1)

receipt={
    "complete":True,
    "completion_type":"OWNER_DECISIONS_NO_WRITE",
    "completed_at":datetime.now(timezone.utc).isoformat(),
    "source_main_sha":main_sha,
    "review_queue_groups":groups,
    "submitted_decisions":submitted,
    "accepted_no_write_decisions":no_write,
    "proposed_backfill_identity_actions":0,
    "proposed_write_actions":0,
    "decision_summary":plan.get("decision_summary") or {},
    "authorization_granted":False,
    "production_d1_read":0,
    "production_d1_write":0,
    "customer_id_generation":0,
    "customer_update":0,
    "customer_delete":0,
    "customer_merge":0,
    "line_send":0,
    "worker_deploy":0,
    "production_deploy":0
}

receipt_path.write_text(
    json.dumps(receipt,ensure_ascii=False,indent=2)+"\n",
    encoding="utf-8"
)

print("NO_WRITE_COMPLETION_RECEIPT=READY")
print("REVIEW_QUEUE_GROUPS="+str(groups))
print("ACCEPTED_NO_WRITE_DECISIONS="+str(no_write))
print("PROPOSED_BACKFILL_IDENTITY_ACTIONS=0")
print("PROPOSED_WRITE_ACTIONS=0")
PY
then
  echo
  echo "=================================================="
  echo " RESULT=OWNER_BACKFILL_COMPLETE_NO_WRITE"
  echo "=================================================="
  echo "NO_WRITE_COMPLETION_RECEIPT=$NO_WRITE_RECEIPT"
  echo "NEXT=NONE"
  echo "AUTHORIZATION_GRANTED=NO"
  echo "APPROVAL_TEXT_GENERATED=0"
  echo "WRITE_SQL_GENERATED=0"
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
  exit 0
fi

echo "ZERO_ACTION_COMPLETION=NO"
echo "READONLY_PREVIEW_REQUIRED=YES"

echo
echo "=== 4. Prepare exact selected-message READ ONLY preview ==="

node scripts/prepare-line-history-owner-backfill-preview.mjs   --plan "$PLAN"   --candidates "$CANDIDATES"   --out-dir "$PREVIEW_DIR"

echo
echo "=== 5. Final preauthorization gate ==="

python3 - "$PREVIEW_DIR/owner-backfill-preview-summary.json" <<'PY'
import json,sys
from pathlib import Path

summary=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if int(summary.get("validation_error_count") or 0)!=0 or not summary.get("preview_ready"):
    print("RESULT=STOP_OWNER_BACKFILL_PREVIEW_PREP_NOT_READY")
    raise SystemExit(30)

print("PREVIEW_SELECTED_CANDIDATE_ROWS="+str(int(summary.get("selected_candidate_rows") or 0)))
print("PREVIEW_BATCH_DUPLICATE_ROWS="+str(int(summary.get("batch_duplicate_rows") or 0)))
print("PREVIEW_CANDIDATE_ROWS="+str(int(summary.get("preview_candidate_rows") or 0)))
PY

echo
echo "=================================================="
echo " RESULT=OWNER_BACKFILL_READONLY_PREVIEW_PREPARED"
echo "=================================================="
echo "PRIVATE_PLAN=$PLAN"
echo "READONLY_PREVIEW_DIR=$PREVIEW_DIR"
echo "NEXT=RUN_PRODUCTION_D1_READONLY_PREVIEW"
echo "AUTHORIZATION_GRANTED=NO"
echo "APPROVAL_TEXT_GENERATED=0"
echo "WRITE_SQL_GENERATED=0"
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
