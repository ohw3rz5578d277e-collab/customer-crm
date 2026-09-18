#!/usr/bin/env bash
set -euo pipefail

PHASE=""
CANDIDATES=""
CUSTOMER_MASTER=""
RESUME_DIR=""
DECISIONS=""
PREAUTH_DIR=""
D1_PREVIEW_DIR=""
APPROVAL_FILE=""
OUT_DIR=""
EXECUTE_PRODUCTION_WRITE="NO"
AUTO_DISCOVER="NO"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/run-line-history-recovery-operator.sh --phase status [options]

Phases:
  status
  readonly-resume
  preauth
  d1-preview
  approved-write

Common:
  --out-dir <dir>
  --auto-discover
    status phase only. Searches safe local recovery roots for missing non-approval artifacts.

readonly-resume:
  --candidates <candidate-snapshot.json>
  --customer-master <customer-master.json>

preauth:
  --resume-dir <readonly-resume-output-dir>
  --customer-master <customer-master.json>
  --decisions <line-history-owner-review-decisions.json>
  --candidates <candidate-snapshot.json>

d1-preview:
  --preauth-dir <owner-backfill-preauth-dir>

approved-write:
  --preauth-dir <owner-backfill-preauth-dir>
  --d1-preview-dir <d1-readonly-result-dir>
  --approval-file <owner-exact-approval.txt>
  --execute-production-write YES

Important:
  approved-write never runs unless all four are explicit:
  --phase approved-write
  --approval-file ...
  --execute-production-write YES
  exact approval text inside the file must also match the frozen packet.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --phase) PHASE="${2:-}"; shift 2 ;;
    --candidates) CANDIDATES="${2:-}"; shift 2 ;;
    --customer-master) CUSTOMER_MASTER="${2:-}"; shift 2 ;;
    --resume-dir) RESUME_DIR="${2:-}"; shift 2 ;;
    --decisions) DECISIONS="${2:-}"; shift 2 ;;
    --preauth-dir) PREAUTH_DIR="${2:-}"; shift 2 ;;
    --d1-preview-dir) D1_PREVIEW_DIR="${2:-}"; shift 2 ;;
    --approval-file) APPROVAL_FILE="${2:-}"; shift 2 ;;
    --out-dir) OUT_DIR="${2:-}"; shift 2 ;;
    --execute-production-write) EXECUTE_PRODUCTION_WRITE="${2:-NO}"; shift 2 ;;
    --auto-discover) AUTO_DISCOVER="YES"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "RESULT=STOP_UNKNOWN_ARGUMENT"; echo "ARG=$1"; usage; exit 2 ;;
  esac
done

if [ -z "$PHASE" ]; then
  echo "RESULT=STOP_PHASE_REQUIRED"
  usage
  exit 2
fi

LOCAL_HEAD="$(git rev-parse HEAD)"
REMOTE_MAIN="$(git ls-remote origin refs/heads/main | awk '{print $1}')"

echo "=================================================="
echo " CUSTOMER CRM — LINE HISTORY RECOVERY OPERATOR"
echo "=================================================="
echo "PHASE=$PHASE"
echo "LOCAL_HEAD=$LOCAL_HEAD"
echo "REMOTE_MAIN=$REMOTE_MAIN"

test "$LOCAL_HEAD" = "$REMOTE_MAIN" || {
  echo "RESULT=STOP_MAIN_DRIFT"
  exit 3
}
echo "MAIN_SHA_GUARD=PASS"

status_path() {
  local label="$1"
  local path="$2"
  if [ -n "$path" ] && [ -e "$path" ]; then
    echo "${label}=PRESENT"
  else
    echo "${label}=MISSING"
  fi
}

case "$PHASE" in
  status)
    if [ "$AUTO_DISCOVER" = "YES" ]; then
      echo
      echo "=== Safe local artifact auto-discovery ==="

      DISCOVERY_TMP="$(mktemp "${TMPDIR:-/tmp}/customer-crm-line-history-discovery.XXXXXX")"
      DISCOVERY_ROOT_ARGS=(--root "$PWD")

      if [ -d "$HOME/Downloads" ]; then
        DISCOVERY_ROOT_ARGS+=(--root "$HOME/Downloads")
      fi

      shopt -s nullglob
      for d in "$HOME"/customer-crm-*; do
        [ -d "$d" ] && DISCOVERY_ROOT_ARGS+=(--root "$d")
      done
      shopt -u nullglob

      node scripts/discover-line-history-recovery-artifacts.mjs \
        "${DISCOVERY_ROOT_ARGS[@]}" \
        --out "$DISCOVERY_TMP"

      discovery_value() {
        local key="$1"
        python3 - "$DISCOVERY_TMP" "$key" <<'PY'
import json,sys
x=json.load(open(sys.argv[1],encoding="utf-8"))
node=x.get(sys.argv[2]) or {}
print(str(node.get("path") or ""))
PY
      }

      [ -z "$CANDIDATES" ] && CANDIDATES="$(discovery_value candidates)"
      [ -z "$CUSTOMER_MASTER" ] && CUSTOMER_MASTER="$(discovery_value customer_master)"
      [ -z "$RESUME_DIR" ] && RESUME_DIR="$(discovery_value resume_dir)"
      [ -z "$DECISIONS" ] && DECISIONS="$(discovery_value decisions)"
      [ -z "$PREAUTH_DIR" ] && PREAUTH_DIR="$(discovery_value preauth_dir)"
      [ -z "$D1_PREVIEW_DIR" ] && D1_PREVIEW_DIR="$(discovery_value d1_preview_dir)"

      rm -f "$DISCOVERY_TMP"

      echo "AUTO_DISCOVER=COMPLETE"
      echo "APPROVAL_FILE_AUTO_DISCOVERY=NO"
    fi

    echo
    echo "=== Operator status ==="
    status_path "CANDIDATES" "$CANDIDATES"
    status_path "CUSTOMER_MASTER" "$CUSTOMER_MASTER"
    status_path "RESUME_DIR" "$RESUME_DIR"
    status_path "DECISIONS" "$DECISIONS"
    status_path "PREAUTH_DIR" "$PREAUTH_DIR"
    status_path "D1_PREVIEW_DIR" "$D1_PREVIEW_DIR"
    status_path "APPROVAL_FILE" "$APPROVAL_FILE"

    if [ -n "$D1_PREVIEW_DIR" ] && [ -f "$D1_PREVIEW_DIR/write-authorization-packet.json" ]; then
      PACKET_STATE="$(python3 - "$D1_PREVIEW_DIR/write-authorization-packet.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1],encoding="utf-8"))
print("READY" if p.get("packet_ready") else "BLOCKED")
PY
)"
      echo "WRITE_AUTHORIZATION_PACKET=$PACKET_STATE"
    else
      echo "WRITE_AUTHORIZATION_PACKET=MISSING"
    fi

    STATUS_ARGS=()
    [ -n "$CANDIDATES" ] && STATUS_ARGS+=(--candidates "$CANDIDATES")
    [ -n "$CUSTOMER_MASTER" ] && STATUS_ARGS+=(--customer-master "$CUSTOMER_MASTER")
    [ -n "$RESUME_DIR" ] && STATUS_ARGS+=(--resume-dir "$RESUME_DIR")
    [ -n "$DECISIONS" ] && STATUS_ARGS+=(--decisions "$DECISIONS")
    [ -n "$PREAUTH_DIR" ] && STATUS_ARGS+=(--preauth-dir "$PREAUTH_DIR")
    [ -n "$D1_PREVIEW_DIR" ] && STATUS_ARGS+=(--d1-preview-dir "$D1_PREVIEW_DIR")
    [ -n "$APPROVAL_FILE" ] && STATUS_ARGS+=(--approval-file "$APPROVAL_FILE")

    STATUS_ARGS+=(--main-sha "$LOCAL_HEAD")
    node scripts/inspect-line-history-recovery-status.mjs "${STATUS_ARGS[@]}"

    echo "PRODUCTION_D1_WRITE=0"
    echo "LINE_SEND=0"
    echo "RESULT=LINE_HISTORY_RECOVERY_OPERATOR_STATUS"
    ;;

  readonly-resume)
    test "$AUTO_DISCOVER" = "NO" || { echo "RESULT=STOP_AUTO_DISCOVER_STATUS_ONLY"; exit 9; }
    test -n "$CANDIDATES" || { echo "RESULT=STOP_CANDIDATES_REQUIRED"; exit 10; }
    test -n "$CUSTOMER_MASTER" || { echo "RESULT=STOP_CUSTOMER_MASTER_REQUIRED"; exit 11; }
    test -f "$CANDIDATES" || { echo "RESULT=STOP_CANDIDATES_MISSING"; exit 12; }
    test -f "$CUSTOMER_MASTER" || { echo "RESULT=STOP_CUSTOMER_MASTER_MISSING"; exit 13; }

    if [ -z "$OUT_DIR" ]; then
      OUT_DIR="line-history-exact-reservation-resume-results"
    fi

    echo "WRITE_PHASE=NO"
    echo "PRODUCTION_D1_WRITE=0"
    bash scripts/run-line-history-exact-reservation-readonly-resume.sh       "$CANDIDATES" "$CUSTOMER_MASTER" "$OUT_DIR"
    ;;

  preauth)
    test "$AUTO_DISCOVER" = "NO" || { echo "RESULT=STOP_AUTO_DISCOVER_STATUS_ONLY"; exit 19; }
    test -n "$RESUME_DIR" || { echo "RESULT=STOP_RESUME_DIR_REQUIRED"; exit 20; }
    test -n "$CUSTOMER_MASTER" || { echo "RESULT=STOP_CUSTOMER_MASTER_REQUIRED"; exit 21; }
    test -n "$DECISIONS" || { echo "RESULT=STOP_DECISIONS_REQUIRED"; exit 22; }
    test -n "$CANDIDATES" || { echo "RESULT=STOP_CANDIDATES_REQUIRED"; exit 23; }

    if [ -z "$OUT_DIR" ]; then
      OUT_DIR="$RESUME_DIR/owner-backfill-preauth"
    fi

    echo "WRITE_PHASE=NO"
    echo "APPROVAL_TEXT_GENERATED=0"
    echo "PRODUCTION_D1_WRITE=0"
    bash scripts/run-line-history-owner-authorization-prep.sh       "$RESUME_DIR" "$CUSTOMER_MASTER" "$DECISIONS" "$CANDIDATES" "$OUT_DIR"
    ;;

  d1-preview)
    test "$AUTO_DISCOVER" = "NO" || { echo "RESULT=STOP_AUTO_DISCOVER_STATUS_ONLY"; exit 29; }
    test -n "$PREAUTH_DIR" || { echo "RESULT=STOP_PREAUTH_DIR_REQUIRED"; exit 30; }
    test -d "$PREAUTH_DIR" || { echo "RESULT=STOP_PREAUTH_DIR_MISSING"; exit 31; }
    PLAN="$PREAUTH_DIR/decision-plan-private.json"
    PREVIEW_DIR="$PREAUTH_DIR/readonly-preview"
    test -f "$PLAN" || { echo "RESULT=STOP_DECISION_PLAN_MISSING"; exit 32; }
    test -d "$PREVIEW_DIR" || { echo "RESULT=STOP_READONLY_PREVIEW_DIR_MISSING"; exit 33; }

    if [ -z "$OUT_DIR" ]; then
      OUT_DIR="$PREVIEW_DIR/d1-readonly-result"
    fi

    echo "WRITE_PHASE=NO"
    echo "AUTHORIZATION_GRANTED=NO"
    echo "PRODUCTION_D1_WRITE=0"
    bash scripts/run-line-history-owner-backfill-readonly-preview.sh       "$PREVIEW_DIR" "$PLAN" "$OUT_DIR"
    ;;

  approved-write)
    test "$AUTO_DISCOVER" = "NO" || { echo "RESULT=STOP_AUTO_DISCOVER_STATUS_ONLY"; exit 39; }
    test -n "$PREAUTH_DIR" || { echo "RESULT=STOP_PREAUTH_DIR_REQUIRED"; exit 40; }
    test -n "$D1_PREVIEW_DIR" || { echo "RESULT=STOP_D1_PREVIEW_DIR_REQUIRED"; exit 41; }
    test -n "$APPROVAL_FILE" || { echo "RESULT=STOP_APPROVAL_FILE_REQUIRED"; exit 42; }
    test -f "$APPROVAL_FILE" || { echo "RESULT=STOP_APPROVAL_FILE_MISSING"; exit 43; }

    if [ "$EXECUTE_PRODUCTION_WRITE" != "YES" ]; then
      echo "RESULT=STOP_EXPLICIT_PRODUCTION_WRITE_CONFIRMATION_REQUIRED"
      echo "REQUIRED_FLAG=--execute-production-write YES"
      echo "PRODUCTION_D1_WRITE=0"
      exit 44
    fi

    if [ -z "$OUT_DIR" ]; then
      OUT_DIR="$D1_PREVIEW_DIR/approved-insert-run"
    fi

    echo "WRITE_PHASE=YES"
    echo "EXPLICIT_WRITE_CONFIRMATION=YES"
    echo "EXACT_APPROVAL_FILE=PRESENT"
    echo "APPROVAL_VALUE_PRINTED=NO"

    bash scripts/run-line-history-owner-approved-insert.sh       "$PREAUTH_DIR" "$D1_PREVIEW_DIR" "$APPROVAL_FILE" "$OUT_DIR"
    ;;

  *)
    echo "RESULT=STOP_INVALID_PHASE"
    usage
    exit 50
    ;;
esac
