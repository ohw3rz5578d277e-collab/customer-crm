#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

EXPECTED_HEAD="${EXPECTED_HEAD:-}"

stop() {
  echo "STOP: $1"
  exit "${2:-1}"
}

command -v git >/dev/null 2>&1 || stop GIT_NOT_FOUND
command -v node >/dev/null 2>&1 || stop NODE_NOT_FOUND
command -v bash >/dev/null 2>&1 || stop BASH_NOT_FOUND

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || stop NOT_A_GIT_WORKTREE
CURRENT_HEAD="$(git rev-parse HEAD)"

if [ -n "$EXPECTED_HEAD" ] && [ "$CURRENT_HEAD" != "$EXPECTED_HEAD" ]; then
  echo "EXPECTED_HEAD=$EXPECTED_HEAD"
  echo "CURRENT_HEAD=$CURRENT_HEAD"
  stop EXACT_HEAD_MISMATCH 10
fi

if [ -n "$(git status --porcelain)" ]; then
  stop WORKTREE_NOT_CLEAN 11
fi

node -e 'const major=Number(process.versions.node.split(".")[0]); if(!Number.isInteger(major)||major<18){console.error(`STOP: NODE_VERSION_TOO_OLD=${process.versions.node}`); process.exit(12)}'

echo "=================================================="
echo " LINE HISTORY NO-WRITE LOCAL FINAL GATE"
echo " HEAD=$CURRENT_HEAD"
echo " PLATFORM=$(uname -s)"
echo " NODE=$(node --version)"
echo " PRODUCTION_D1_READ=0"
echo " PRODUCTION_D1_WRITE=0"
echo " DEPLOY=0"
echo " LINE_SEND=0"
echo " CRM_MUTATION=0"
echo "=================================================="

node --check src/crm-line-history-recovery-next-phase.mjs
node --check src/crm-line-history-recovery-next-command.mjs
node --check src/crm-line-history-recovery-discovery.mjs
node --check src/crm-line-history-owner-decision-plan.mjs
node --check src/crm-line-history-owner-review-queue.mjs
node --check scripts/discover-line-history-recovery-artifacts.mjs
node --check scripts/inspect-line-history-recovery-status.mjs
node --check scripts/plan-line-history-owner-decisions.mjs
node --check scripts/verify-line-history-no-write-safety.mjs
node --check tests/line-history-recovery-next-phase.test.mjs
node --check tests/line-history-recovery-artifact-discovery.test.mjs
node --check tests/line-history-owner-decision-plan.test.mjs
node --check tests/line-history-owner-review-queue.test.mjs
bash -n scripts/run-line-history-recovery-operator.sh
bash -n scripts/run-line-history-owner-authorization-prep.sh
bash -n scripts/run-line-history-no-write-local-final-gate.sh

echo 'LOCAL_SYNTAX_CHECKS=PASS'

node scripts/verify-line-history-no-write-safety.mjs
node tests/line-history-recovery-next-phase.test.mjs
node tests/line-history-recovery-artifact-discovery.test.mjs
node tests/line-history-owner-decision-plan.test.mjs
node tests/line-history-owner-review-queue.test.mjs

FINAL_HEAD="$(git rev-parse HEAD)"
[ "$FINAL_HEAD" = "$CURRENT_HEAD" ] || stop HEAD_CHANGED_DURING_GATE 13

if [ -n "$EXPECTED_HEAD" ] && [ "$FINAL_HEAD" != "$EXPECTED_HEAD" ]; then
  stop EXACT_HEAD_CHANGED_DURING_GATE 14
fi

if [ -n "$(git status --porcelain)" ]; then
  stop WORKTREE_CHANGED_DURING_GATE 15
fi

echo
echo "=================================================="
echo " RESULT=LINE_HISTORY_NO_WRITE_LOCAL_FINAL_GATE_PASS"
echo " EXACT_HEAD=$FINAL_HEAD"
echo " CROSS_PLATFORM_NODE_SAFETY_CHECK=PASS"
echo " PRODUCTION_D1_READ=0"
echo " PRODUCTION_D1_WRITE=0"
echo " PRODUCTION_FETCH=0"
echo " R2_ACCESS=0"
echo " CRM_MUTATION=0"
echo " LINE_SEND=0"
echo " CUSTOMER_ID_GENERATION=0"
echo " DEPLOY=0"
echo " SECRET_CHANGE=0"
echo " ROUTE_CHANGE=0"
echo " TRAFFIC_CHANGE=0"
echo " MERGE=0"
echo "=================================================="
