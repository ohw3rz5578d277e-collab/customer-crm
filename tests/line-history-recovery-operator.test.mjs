import fs from 'node:fs';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';

const path='scripts/run-line-history-recovery-operator.sh';
const src=fs.readFileSync(path,'utf8');

const syntax=spawnSync('bash',['-n',path],{encoding:'utf8'});
assert.equal(syntax.status,0,syntax.stderr||syntax.stdout);

assert.match(src,/^set -euo pipefail/m);
assert.match(src,/MAIN_SHA_GUARD=PASS/);
assert.match(src,/STOP_MAIN_DRIFT/);

for(const phase of ['status','readonly-resume','preauth','d1-preview','approved-write']){
  assert.ok(src.includes(phase),'missing phase '+phase);
}

assert.match(src,/run-line-history-exact-reservation-readonly-resume\.sh/);
assert.match(src,/run-line-history-owner-authorization-prep\.sh/);
assert.match(src,/run-line-history-owner-backfill-readonly-preview\.sh/);
assert.match(src,/run-line-history-owner-approved-insert\.sh/);

const approvedIndex=src.indexOf('approved-write)');
const confirmationIndex=src.indexOf('STOP_EXPLICIT_PRODUCTION_WRITE_CONFIRMATION_REQUIRED');
const delegateIndex=src.indexOf('run-line-history-owner-approved-insert.sh');
assert.ok(approvedIndex>0);
assert.ok(confirmationIndex>approvedIndex);
assert.ok(delegateIndex>confirmationIndex);

assert.match(src,/--execute-production-write YES/);
assert.match(src,/EXACT_APPROVAL_FILE=PRESENT/);
assert.match(src,/APPROVAL_VALUE_PRINTED=NO/);

const readonlySection=src.slice(src.indexOf('readonly-resume)'),src.indexOf('preauth)'));
const preauthSection=src.slice(src.indexOf('preauth)'),src.indexOf('d1-preview)'));
const previewSection=src.slice(src.indexOf('d1-preview)'),src.indexOf('approved-write)'));

for(const section of [readonlySection,preauthSection,previewSection]){
  assert.match(section,/PRODUCTION_D1_WRITE=0/);
  assert.doesNotMatch(section,/run-line-history-owner-approved-insert\.sh/);
}

assert.doesNotMatch(src,/\bwrangler\b/i);
assert.doesNotMatch(src,/d1\s+execute/i);
assert.doesNotMatch(src,/INSERT\s+(?:OR\s+IGNORE\s+)?INTO/i);
assert.doesNotMatch(src,/UPDATE\s+customers/i);
assert.doesNotMatch(src,/DELETE\s+FROM/i);
assert.doesNotMatch(src,/api\.line\.me/i);

assert.match(src,/WRITE_AUTHORIZATION_PACKET=/);
assert.match(src,/RESULT=LINE_HISTORY_RECOVERY_OPERATOR_STATUS/);

console.log('LINE_HISTORY_OPERATOR_BASH_SYNTAX=PASS');
console.log('LINE_HISTORY_OPERATOR_MAIN_SHA_GUARD=PASS');
console.log('LINE_HISTORY_OPERATOR_PHASES=PASS');
console.log('LINE_HISTORY_OPERATOR_WRITE_EXPLICIT_CONFIRMATION=PASS');
console.log('LINE_HISTORY_OPERATOR_APPROVAL_FILE_GATE=PASS');
console.log('LINE_HISTORY_OPERATOR_READONLY_PHASES_NO_WRITE_DELEGATE=PASS');
console.log('LINE_HISTORY_OPERATOR_NO_DIRECT_D1_OR_SQL=PASS');
