#!/usr/bin/env bash
set -euo pipefail

EXPECTED_SHA="${1:-}"
OWNER_ACK="${2:-}"

ACCOUNT_ID="799b203a471e51a791129a5ca97a9b2b"
WRANGLER_VERSION="4.107.0"
EXPECTED_SQL='SELECT customer_id, line_user_id, name, deleted_at FROM customers ORDER BY customer_id;'
PRIVATE_DIR="${HOME}/.customer-crm-private"
OUT_FILE="${PRIVATE_DIR}/production-customer-identity-snapshot-${EXPECTED_SHA}.json"

stop(){ echo "RESULT=STOP_$1"; exit "${2:-1}"; }

[ "$(uname -s)" = "Darwin" ] || stop "MACOS_REQUIRED" 10
[[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || stop "EXPECTED_SHA_INVALID" 11
EXPECTED_ACK="AUTHORIZE_PRODUCTION_CUSTOMER_IDENTITY_SNAPSHOT_READ:${EXPECTED_SHA}"
[ "$OWNER_ACK" = "$EXPECTED_ACK" ] || stop "OWNER_ACK_MISMATCH" 12

command -v git >/dev/null 2>&1 || stop "GIT_MISSING" 14
command -v node >/dev/null 2>&1 || stop "NODE_MISSING" 15
command -v npx >/dev/null 2>&1 || stop "NPX_MISSING" 16
command -v python3 >/dev/null 2>&1 || stop "PYTHON3_MISSING" 17
command -v shasum >/dev/null 2>&1 || stop "SHASUM_MISSING" 18

NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]')"
[ "$NODE_MAJOR" = "22" ] || stop "NODE_22_REQUIRED" 19

LOCAL_HEAD="$(git rev-parse HEAD)"
REMOTE_MAIN="$(git ls-remote origin refs/heads/main | awk '{print $1}')"
REPO_ROOT="$(cd "$(git rev-parse --show-toplevel)" && pwd)"
[ "$LOCAL_HEAD" = "$EXPECTED_SHA" ] || stop "LOCAL_HEAD_MISMATCH" 20
[ "$REMOTE_MAIN" = "$EXPECTED_SHA" ] || stop "MAIN_DRIFT" 21

[ -f wrangler.jsonc ] || stop "WRANGLER_CONFIG_MISSING" 22
[ -f scripts/build-production-identity-snapshot.mjs ] || stop "SNAPSHOT_BUILDER_MISSING" 23

grep -Eq '"binding"[[:space:]]*:[[:space:]]*"DB"' wrangler.jsonc || stop "DB_BINDING_CHANGED" 24
grep -Eq '"database_name"[[:space:]]*:[[:space:]]*"customer-crm-db"' wrangler.jsonc || stop "DB_NAME_CHANGED" 25
grep -Eq '"database_id"[[:space:]]*:[[:space:]]*"1ae3e0d9-72c0-47ad-8fc1-fed9d15ec70f"' wrangler.jsonc || stop "DB_ID_CHANGED" 26
node --check scripts/build-production-identity-snapshot.mjs

umask 077
mkdir -p "$PRIVATE_DIR"
chmod 700 "$PRIVATE_DIR"
PRIVATE_DIR="$(cd "$PRIVATE_DIR" && pwd)"
OUT_FILE="${PRIVATE_DIR}/production-customer-identity-snapshot-${EXPECTED_SHA}.json"
case "$PRIVATE_DIR/" in
  "$REPO_ROOT/"*) stop "PRIVATE_DIR_INSIDE_REPOSITORY" 27 ;;
esac
DIR_MODE="$(stat -f '%Lp' "$PRIVATE_DIR")"
[ "$DIR_MODE" = "700" ] || stop "PRIVATE_DIR_MODE_NOT_700" 28
[ ! -e "$OUT_FILE" ] || stop "OUTPUT_ALREADY_EXISTS" 29

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/crm-production-identity-read.XXXXXX")"
chmod 700 "$TMP_DIR"
RAW="$TMP_DIR/production-customer-identity.raw.json"
cleanup(){ rm -rf "$TMP_DIR"; }
trap cleanup EXIT

echo "=================================================="
echo " CUSTOMER CRM — PRODUCTION IDENTITY SNAPSHOT READ"
echo " PHYSICAL MAC / ONE SELECT / LOCAL PRIVATE OUTPUT"
echo "=================================================="
echo "EXACT_MAIN_SHA=$EXPECTED_SHA"
echo "NODE_VERSION=$(node -p 'process.versions.node')"
echo "PRODUCTION_D1_READ_AUTHORIZED=1"
echo "PRODUCTION_D1_WRITE=0"
echo "GITHUB_ARTIFACT_UPLOAD=0"
echo "PRIVATE_VALUES_PRINTED=0"
echo "MAIN_SHA_GUARD=PASS"
echo "OWNER_ACK_GATE=PASS"
echo "PRODUCTION_D1_TARGET_GUARD=PASS"
echo "PRIVATE_DIR_MODE=700"

echo
echo "=== Execute exactly one Production D1 SELECT ==="

env \
  -u CLOUDFLARE_API_TOKEN \
  -u CLOUDFLARE_API_KEY \
  -u CLOUDFLARE_EMAIL \
  CLOUDFLARE_ACCOUNT_ID="$ACCOUNT_ID" \
  npx --yes "wrangler@${WRANGLER_VERSION}" d1 execute DB \
    --config wrangler.jsonc \
    --remote \
    --json \
    --command "$EXPECTED_SQL" \
    >"$RAW"

python3 - "$RAW" <<'PY'
import json,sys
from pathlib import Path
p=Path(sys.argv[1])
raw=json.loads(p.read_text(encoding='utf-8'))
blocks=raw if isinstance(raw,list) else [raw]
if not blocks:
    raise SystemExit('STOP_D1_RESULT_EMPTY')
count=0
for block in blocks:
    if not isinstance(block,dict):
        raise SystemExit('STOP_D1_RESULT_BLOCK_INVALID')
    if 'error' in block:
        raise SystemExit('STOP_D1_RESULT_ERROR_PRESENT')
    if block.get('success') is not True:
        raise SystemExit('STOP_D1_RESULT_UNSUCCESSFUL')
    results=block.get('results')
    if not isinstance(results,list):
        raise SystemExit('STOP_D1_RESULTS_REQUIRED')
    meta=block.get('meta') if isinstance(block.get('meta'),dict) else {}
    if int(meta.get('rows_written') or 0) != 0:
        raise SystemExit('STOP_D1_ROWS_WRITTEN_NONZERO')
    if meta.get('changed_db') is True:
        raise SystemExit('STOP_D1_CHANGED_DB_TRUE')
    for row in results:
        if not isinstance(row,dict):
            raise SystemExit('STOP_D1_ROW_INVALID')
        for key in ('customer_id','line_user_id','name','deleted_at'):
            if key not in row:
                raise SystemExit('STOP_D1_IDENTITY_FIELD_MISSING')
        count += 1
if count < 1:
    raise SystemExit('STOP_D1_CUSTOMER_ROWS_EMPTY')
print('PRODUCTION_D1_SELECT=PASS')
print('PRODUCTION_D1_ROWS_WRITTEN=0')
print('PRODUCTION_D1_CHANGED_DB=0')
print(f'PRODUCTION_IDENTITY_ROW_COUNT={count}')
print('PRIVATE_CUSTOMER_VALUES_PRINTED=0')
PY

echo
echo "=== Build canonical snapshot locally ==="
node scripts/build-production-identity-snapshot.mjs \
  --input "$RAW" \
  --output "$OUT_FILE" \
  --source-sha "$EXPECTED_SHA"
chmod 600 "$OUT_FILE"

SNAPSHOT_SHA256="$(shasum -a 256 "$OUT_FILE" | awk '{print $1}')"
[[ "$SNAPSHOT_SHA256" =~ ^[0-9a-f]{64}$ ]] || stop "SNAPSHOT_SHA256_INVALID" 40

FILE_MODE="$(stat -f '%Lp' "$OUT_FILE")"
[ "$FILE_MODE" = "600" ] || stop "SNAPSHOT_MODE_NOT_600" 41

echo "SNAPSHOT_SHA256=$SNAPSHOT_SHA256"
echo "SNAPSHOT_FILE_MODE=600"
echo "SNAPSHOT_LOCAL_PATH=$OUT_FILE"
echo "RAW_D1_RESULT_DELETED_ON_EXIT=YES"
echo "GITHUB_ARTIFACT_UPLOAD=0"
echo "PRIVATE_CUSTOMER_VALUES_PRINTED=0"

echo
echo "=================================================="
echo " RESULT=PRODUCTION_CUSTOMER_IDENTITY_SNAPSHOT_READ_COMPLETE"
echo " EXACT_MAIN_SHA=$EXPECTED_SHA"
echo " PRODUCTION_D1_READ=1"
echo " PRODUCTION_D1_WRITE=0"
echo " PRODUCTION_FETCH=0"
echo " R2_ACCESS=0"
echo " CRM_MUTATION=0"
echo " LINE_SEND=0"
echo " CUSTOMER_ID_GENERATION=0"
echo " CUSTOMER_CREATE_UPDATE_DELETE_MERGE=0"
echo " WORKER_DEPLOY=0"
echo " PRODUCTION_DEPLOY=0"
echo " WORKER_ACTIVATION=0"
echo " ROUTE_CHANGE=0"
echo " SECRET_CHANGE=0"
echo " SECURITY_POLICY_CHANGE=0"
echo " TRAFFIC_CHANGE=0"
echo " COMMERCE_ACTIVATION=0"
echo " PAID_SPEND=0"
echo " GITHUB_ARTIFACT_UPLOAD=0"
echo " PRIVATE_SNAPSHOT_LOCAL_ONLY=1"
echo "=================================================="
