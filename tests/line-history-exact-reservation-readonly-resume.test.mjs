import fs from 'node:fs';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';

const path='scripts/run-line-history-exact-reservation-readonly-resume.sh';
const src=fs.readFileSync(path,'utf8');

const syntax=spawnSync('bash',['-n',path],{encoding:'utf8'});
assert.equal(syntax.status,0,syntax.stderr||syntax.stdout);

assert.match(src,/^set -euo pipefail/m);
assert.match(src,/MAIN_SHA_GUARD=PASS/);
assert.match(src,/STOP_OUTPUT_DIR_NOT_EMPTY/);
assert.match(src,/STOP_UNSAFE_OUTPUT_DIR/);
assert.doesNotMatch(src,/rm\s+-rf/i);

assert.match(src,/RESERVATION_D1_AUTH=PASS/);
assert.match(src,/CRM_D1_AUTH=PASS/);
assert.match(src,/CLOUDFLARE_LOGIN=START/);
assert.match(src,/SECRET_VALUES_PRINTED=NO/);
assert.match(src,/env\s+\\[\s\S]*-u CLOUDFLARE_API_TOKEN/);

assert.match(src,/bash scripts\/run-line-history-unresolved-readonly\.sh/);
assert.match(src,/PRAGMA table_info\(app_reservations\)/);
assert.match(src,/PRAGMA table_info\(customer_reservations\)/);
assert.match(src,/SELECT reservation_id, customer_id AS source_customer_id/);
assert.match(src,/SELECT reservation_id, customer_id/);

assert.match(src,/build-line-history-exact-reservation-evidence\.mjs/);
assert.match(src,/classify-line-history-unresolved\.mjs/);
assert.match(src,/--exact-reservation-evidence/);
assert.match(src,/SAFE_EXACT_RESERVATION_GROUPS/);
assert.match(src,/SAFE_EXACT_RESERVATION_MESSAGES/);

const forbiddenSql=[
  /\bINSERT\s+INTO\b/i,
  /\bUPDATE\s+[A-Za-z_]/i,
  /\bDELETE\s+FROM\b/i,
  /\bDROP\s+TABLE\b/i,
  /\bALTER\s+TABLE\b/i,
  /\bCREATE\s+TABLE\b/i,
  /\bREPLACE\s+INTO\b/i,
  /\bTRUNCATE\b/i
];
for(const re of forbiddenSql){
  assert.doesNotMatch(src,re);
}

assert.doesNotMatch(src,/wrangler\s+deploy/i);
assert.doesNotMatch(src,/d1\s+migrations\s+apply/i);
assert.doesNotMatch(src,/api\.line\.me/i);
assert.doesNotMatch(src,/line\.me\/v2\/bot\/message/i);

for(const marker of [
  'PRODUCTION_D1_WRITE=0',
  'CUSTOMER_ID_GENERATION=0',
  'CUSTOMER_UPDATE=0',
  'CUSTOMER_DELETE=0',
  'CUSTOMER_MERGE=0',
  'LINE_SEND=0',
  'WORKER_DEPLOY=0',
  'PRODUCTION_DEPLOY=0'
]){
  assert.ok(src.includes(marker),marker+' missing');
}

assert.match(src,/RAW_RESERVATION_IDS_PRINTED=NO/);
assert.match(src,/RAW_SOURCE_IDS_PRINTED=NO/);
assert.match(src,/RAW_CUSTOMER_IDS_PRINTED=NO/);
assert.match(src,/RAW_LINE_USER_ID_OUTPUT=0/);
assert.match(src,/MESSAGE_TEXT_OUTPUT=0/);

console.log('EXACT_RESERVATION_RESUME_BASH_SYNTAX=PASS');
console.log('EXACT_RESERVATION_RESUME_MAIN_GUARD=PASS');
console.log('EXACT_RESERVATION_RESUME_NON_DESTRUCTIVE_LOCAL_OUTPUT=PASS');
console.log('EXACT_RESERVATION_RESUME_AUTH_RECOVERY=PASS');
console.log('EXACT_RESERVATION_RESUME_SCHEMA_AWARE=PASS');
console.log('EXACT_RESERVATION_RESUME_SELECT_ONLY=PASS');
console.log('EXACT_RESERVATION_RESUME_LOCAL_JOIN_AND_TRIAGE=PASS');
console.log('EXACT_RESERVATION_RESUME_PRIVACY_MARKERS=PASS');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
console.log('PRODUCTION_DEPLOY=0');
