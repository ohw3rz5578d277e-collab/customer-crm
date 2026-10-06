import assert from 'node:assert/strict';
import fs from 'node:fs';

const scriptPath='scripts/run-production-customer-identity-snapshot-read-mac.sh';
const script=fs.readFileSync(scriptPath,'utf8');

function pass(label){console.log(`PASS: ${label}`)}

assert.ok(script.startsWith('#!/usr/bin/env bash'));
assert.ok(script.includes('set -euo pipefail'));
assert.ok(script.includes('[ "$(uname -s)" = "Darwin" ]'));
assert.ok(script.includes('NODE_22_REQUIRED'));
pass('runtime is explicitly physical-Mac and Node-22 gated');

for(const token of ['EXPECTED_SHA="${1:-}"','OWNER_ACK="${2:-}"','OUT_FILE="${3:-}"']){
  assert.ok(script.includes(token),`missing required local input ${token}`);
}
assert.ok(script.includes('AUTHORIZE_PRODUCTION_CUSTOMER_IDENTITY_SNAPSHOT_READ:${EXPECTED_SHA}'));
assert.ok(script.includes('[ "$OWNER_ACK" = "$EXPECTED_ACK" ]'));
assert.ok(script.includes('LOCAL_HEAD="$(git rev-parse HEAD)"'));
assert.ok(script.includes('REMOTE_MAIN="$(git ls-remote origin refs/heads/main'));
assert.ok(script.includes('[ "$LOCAL_HEAD" = "$EXPECTED_SHA" ]'));
assert.ok(script.includes('[ "$REMOTE_MAIN" = "$EXPECTED_SHA" ]'));
pass('Owner acknowledgement and exact current-main guards are present');

for(const expected of [
  '"binding"[[:space:]]*:[[:space:]]*"DB"',
  '"database_name"[[:space:]]*:[[:space:]]*"customer-crm-db"',
  '"database_id"[[:space:]]*:[[:space:]]*"1ae3e0d9-72c0-47ad-8fc1-fed9d15ec70f"'
]){
  assert.ok(script.includes(expected),`missing canonical D1 target guard: ${expected}`);
}
assert.ok(script.includes('scripts/build-production-identity-snapshot.mjs'));
pass('canonical CRM D1 target and merged normalizer are required');

const sqlMatch=script.match(/EXPECTED_SQL='([^']+)'/);
assert.ok(sqlMatch,'fixed SQL assignment missing');
const sql=sqlMatch[1].trim();
assert.equal(sql,'SELECT customer_id, line_user_id, name, deleted_at FROM customers ORDER BY customer_id;');
assert.match(sql,/^SELECT\b/i);
assert.doesNotMatch(sql,/\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE|UPSERT|PRAGMA|VACUUM|REINDEX|ANALYZE|ATTACH|DETACH)\b/i);
assert.equal((sql.match(/;/g)||[]).length,1);
pass('Production SQL is exactly one fixed SELECT-only statement');

const d1Executes=script.match(/\bd1 execute\b/g)||[];
assert.equal(d1Executes.length,1,'exactly one D1 execute command is allowed');
assert.match(script,/d1 execute DB\s+\\\n\s+--config wrangler\.jsonc\s+\\\n\s+--remote\s+\\\n\s+--json\s+\\\n\s+--command "\$EXPECTED_SQL"/);
assert.doesNotMatch(script,/\bd1\s+migrations\b/i);
assert.doesNotMatch(script,/\bwrangler@?[^\n]*\bdeploy\b/i);
assert.doesNotMatch(script,/\bwrangler@?[^\n]*\bsecret\s+(?:put|bulk|delete)\b/i);
assert.doesNotMatch(script,/\s--file(?:\s|=)/);
pass('D1 execution surface is one remote JSON SELECT with no mutation/deploy path');

assert.ok(script.includes('-u CLOUDFLARE_API_TOKEN'));
assert.ok(script.includes('-u CLOUDFLARE_API_KEY'));
assert.ok(script.includes('-u CLOUDFLARE_EMAIL'));
assert.doesNotMatch(script,/echo[^\n]*\$CLOUDFLARE_API_TOKEN/);
assert.doesNotMatch(script,/echo[^\n]*\$CLOUDFLARE_API_KEY/);
pass('ambient Cloudflare API credentials are removed and never printed');

for(const marker of [
  'STOP_D1_RESULT_ERROR_PRESENT',
  'STOP_D1_RESULT_UNSUCCESSFUL',
  'STOP_D1_RESULTS_REQUIRED',
  'STOP_D1_ROWS_WRITTEN_NONZERO',
  'STOP_D1_CHANGED_DB_TRUE',
  "for key in ('customer_id','line_user_id','name','deleted_at')",
  'STOP_D1_CUSTOMER_ROWS_EMPTY'
]){
  assert.ok(script.includes(marker),`missing fail-closed result guard ${marker}`);
}
pass('D1 result is fail-closed on writes, malformed/empty results, and missing identity columns');

assert.match(script,/node scripts\/build-production-identity-snapshot\.mjs\s+\\\n\s+--input "\$RAW"\s+\\\n\s+--output "\$OUT_FILE"\s+\\\n\s+--source-sha "\$EXPECTED_SHA"/);
assert.ok(script.includes('umask 077'));
assert.ok(script.includes('chmod 600 "$OUT_FILE"'));
assert.ok(script.includes("stat -f '%Lp' \"$OUT_FILE\""));
assert.ok(script.includes('trap cleanup EXIT'));
assert.ok(script.includes('rm -rf "$TMP_DIR"'));
pass('private snapshot remains local with mode 600 and raw D1 temp data is cleaned');

for(const forbidden of [
  'actions/upload-artifact',
  'api.github.com',
  'github.com/repos/',
  'curl ',
  'wget ',
  'r2 object',
  'r2 bucket'
]){
  assert.ok(!script.toLowerCase().includes(forbidden.toLowerCase()),`forbidden transfer surface present: ${forbidden}`);
}
assert.ok(script.includes('GITHUB_ARTIFACT_UPLOAD=0'));
assert.ok(script.includes('PRIVATE_SNAPSHOT_LOCAL_ONLY=1'));
pass('private identity data has no GitHub artifact or secondary transfer path');

for(const marker of [
  'PRODUCTION_D1_READ=1',
  'PRODUCTION_D1_WRITE=0',
  'PRODUCTION_FETCH=0',
  'R2_ACCESS=0',
  'CRM_MUTATION=0',
  'LINE_SEND=0',
  'CUSTOMER_ID_GENERATION=0',
  'CUSTOMER_CREATE_UPDATE_DELETE_MERGE=0',
  'WORKER_DEPLOY=0',
  'PRODUCTION_DEPLOY=0',
  'WORKER_ACTIVATION=0',
  'ROUTE_CHANGE=0',
  'SECRET_CHANGE=0',
  'SECURITY_POLICY_CHANGE=0',
  'TRAFFIC_CHANGE=0',
  'COMMERCE_ACTIVATION=0',
  'PAID_SPEND=0'
]){
  assert.ok(script.includes(marker),`receipt marker missing ${marker}`);
}
pass('receipt makes the one Production read and all excluded mutations explicit');

assert.doesNotMatch(script,/echo[^\n]*(?:customer_id|line_user_id|deleted_at)=\$/i);
assert.ok(script.includes('PRIVATE_CUSTOMER_VALUES_PRINTED=0'));
pass('script does not intentionally print customer identity values');

console.log('RESULT=PRODUCTION_CUSTOMER_IDENTITY_SNAPSHOT_READ_CONTRACT_PASS');
console.log('RUNTIME_LOCATION=PHYSICAL_MAC_ONLY');
console.log('PRIVATE_SNAPSHOT_LOCAL_ONLY=1');
console.log('GITHUB_ARTIFACT_UPLOAD=0');
console.log('PRODUCTION_D1_READ_EXECUTED=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('PRODUCTION_FETCH=0');
console.log('R2_ACCESS=0');
console.log('CRM_MUTATION=0');
console.log('LINE_SEND=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('PRODUCTION_DEPLOY=0');
