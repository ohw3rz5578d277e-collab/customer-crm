import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflowPath='.github/workflows/production-customer-identity-snapshot-read.yml';
const workflow=fs.readFileSync(workflowPath,'utf8');

function pass(label){console.log(`PASS: ${label}`)}

assert.match(workflow,/^on:\n\s+workflow_dispatch:/m);
assert.doesNotMatch(workflow,/^\s{2}(?:push|pull_request|pull_request_target|schedule|workflow_run):/m);
pass('runtime workflow is manual-dispatch only');

for(const input of ['expected_sha:','owner_ack:']){
  assert.ok(workflow.includes(input),`missing workflow input ${input}`);
}
assert.ok(workflow.includes("github.ref == 'refs/heads/main'"));
assert.ok(workflow.includes("github.actor == 'ohw3rz5578d277e-collab'"));
assert.ok(workflow.includes("github.triggering_actor == 'ohw3rz5578d277e-collab'"));
assert.ok(workflow.includes('github.run_attempt == 1'));
assert.ok(workflow.includes("EXPECTED_ACK=\"AUTHORIZE_PRODUCTION_CUSTOMER_IDENTITY_SNAPSHOT_READ:${EXPECTED_SHA}\""));
assert.ok(workflow.includes('test "$OWNER_ACK" = "$EXPECTED_ACK"'));
pass('Owner, main-ref, exact-SHA, and replay gates are present');

assert.match(workflow,/permissions:\n\s+contents: read/);
assert.doesNotMatch(workflow,/contents:\s*write/);
assert.doesNotMatch(workflow,/actions:\s*write/);
assert.doesNotMatch(workflow,/issues:\s*write/);
assert.doesNotMatch(workflow,/pull-requests:\s*write/);
pass('GitHub token permissions remain read-only');

assert.ok(workflow.includes('ref: ${{ inputs.expected_sha }}'));
assert.ok(workflow.includes('persist-credentials: false'));
assert.ok(workflow.includes('REMOTE_MAIN="$(git ls-remote origin refs/heads/main'));
assert.ok(workflow.includes('test "$REMOTE_MAIN" = "$EXPECTED_SHA"'));
assert.ok(workflow.includes('test "$GITHUB_SHA" = "$EXPECTED_SHA"'));
pass('checkout and current-main drift guards are exact-SHA bound');

assert.ok(workflow.includes('"binding"[[:space:]]*:[[:space:]]*"DB"'));
assert.ok(workflow.includes('"database_name"[[:space:]]*:[[:space:]]*"customer-crm-db"'));
assert.ok(workflow.includes('scripts/build-production-identity-snapshot.mjs'));
pass('fixed CRM D1 target and canonical normalizer are required');

const sqlMatch=workflow.match(/SQL='([^']+)'/);
assert.ok(sqlMatch,'fixed SQL assignment missing');
const sql=sqlMatch[1].trim();
assert.equal(sql,'SELECT customer_id, line_user_id, name, deleted_at FROM customers ORDER BY customer_id;');
assert.match(sql,/^SELECT\b/i);
assert.doesNotMatch(sql,/\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE|UPSERT|PRAGMA|VACUUM|REINDEX|ANALYZE|ATTACH|DETACH)\b/i);
assert.equal((sql.match(/;/g)||[]).length,1);
pass('Production SQL is one fixed SELECT-only statement');

const d1Executes=workflow.match(/\bd1 execute\b/g)||[];
assert.equal(d1Executes.length,1,'exactly one D1 execute command is allowed');
assert.match(workflow,/d1 execute DB\s+\\\n\s+--remote\s+\\\n\s+--json\s+\\\n\s+--command "\$SQL"/);
assert.doesNotMatch(workflow,/\bwrangler@?[^\n]*\bdeploy\b/i);
assert.doesNotMatch(workflow,/\bwrangler@?[^\n]*\bsecret\s+(?:put|bulk|delete)\b/i);
assert.doesNotMatch(workflow,/\bd1\s+migrations\b/i);
assert.doesNotMatch(workflow,/\bd1\s+execute\b[^\n]*(?:--file|--command\s+["'][^"']*(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE))/i);
pass('D1 execution surface is one remote JSON SELECT with no deploy/migration/secret path');

for(const marker of [
  'D1_ROWS_WRITTEN_NONZERO',
  'D1_CHANGED_DB_TRUE',
  "for(const key of ['customer_id','line_user_id','name','deleted_at'])",
  'D1_CUSTOMER_ROWS_EMPTY',
  'PRIVATE_CUSTOMER_VALUES_PRINTED=0'
]){
  assert.ok(workflow.includes(marker),`missing fail-closed result guard ${marker}`);
}
pass('D1 result is fail-closed on writes, empty results, and missing identity columns');

assert.match(workflow,/node scripts\/build-production-identity-snapshot\.mjs\s+\\\n\s+--input "\$RAW"\s+\\\n\s+--output "\$OUT"\s+\\\n\s+--source-sha "\$EXPECTED_SHA"/);
assert.ok(workflow.includes('SNAPSHOT_SHA256='));
assert.ok(workflow.includes('RAW_D1_RESULT_UPLOAD=0'));
pass('canonical snapshot builder is exact-main bound and raw D1 output stays runner-local');

assert.ok(workflow.includes('uses: actions/upload-artifact@v4'));
assert.ok(workflow.includes('path: ${{ runner.temp }}/production-customer-identity-snapshot.json'));
assert.ok(workflow.includes('retention-days: 1'));
assert.doesNotMatch(workflow,/path:\s*\$\{\{ runner\.temp \}\}\/production-customer-identity\.raw\.json/);
pass('only canonical private snapshot is uploaded with one-day retention');

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
  assert.ok(workflow.includes(marker),`receipt marker missing ${marker}`);
}
pass('final receipt makes the single Production read and all excluded mutations explicit');

assert.doesNotMatch(workflow,/echo[^\n]*\$CLOUDFLARE_API_TOKEN/);
assert.doesNotMatch(workflow,/echo[^\n]*\$CLOUDFLARE_ACCOUNT_ID/);
assert.doesNotMatch(workflow,/console\.log\([^\n]*(?:customer_id|line_user_id|name|deleted_at)\s*\)/);
pass('workflow does not intentionally print secret or private identity values');

console.log('RESULT=PRODUCTION_CUSTOMER_IDENTITY_SNAPSHOT_READ_CONTRACT_PASS');
console.log('PRODUCTION_D1_READ_EXECUTED=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('PRODUCTION_FETCH=0');
console.log('R2_ACCESS=0');
console.log('CRM_MUTATION=0');
console.log('LINE_SEND=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('PRODUCTION_DEPLOY=0');
