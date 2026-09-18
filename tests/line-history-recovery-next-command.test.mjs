import fs from 'node:fs';
import assert from 'node:assert/strict';
import { buildLineHistoryRecoveryNextCommand } from '../src/crm-line-history-recovery-next-command.mjs';

const base={
  candidates:'/tmp/candidates.json',
  customerMaster:'/tmp/customer master.json',
  resumeDir:'/tmp/resume dir',
  decisions:'/tmp/decisions.json',
  preauthDir:'/tmp/preauth dir',
  d1PreviewDir:'/tmp/d1 preview',
  approvalFile:'/tmp/approval.txt'
};

let r=buildLineHistoryRecoveryNextCommand({
  stage:'READONLY_RESUME_REQUIRED',
  ...base
});
assert.equal(r.ready,true);
assert.equal(r.production_write,false);
assert.equal(r.requires_owner_approval,false);
assert.match(r.command,/--phase' 'readonly-resume/);
assert.match(r.command,/customer master\.json/);
assert.doesNotMatch(r.command,/execute-production-write/);

r=buildLineHistoryRecoveryNextCommand({
  stage:'OWNER_REVIEW_REQUIRED',
  ...base
});
assert.equal(r.ready,false);
assert.equal(r.reason,'OWNER_REVIEW_DECISIONS_REQUIRED');
assert.equal(r.command,'');

r=buildLineHistoryRecoveryNextCommand({
  stage:'PREAUTH_REQUIRED',
  ...base
});
assert.equal(r.ready,true);
assert.equal(r.production_write,false);
assert.match(r.command,/--phase' 'preauth/);
assert.doesNotMatch(r.command,/execute-production-write/);

for(const stage of ['D1_PREVIEW_REQUIRED','STALE_AUTHORIZATION_PACKET','BLOCKED_D1_PREVIEW']){
  r=buildLineHistoryRecoveryNextCommand({stage,...base});
  assert.equal(r.ready,true);
  assert.equal(r.production_write,false);
  assert.match(r.command,/--phase' 'd1-preview/);
  assert.doesNotMatch(r.command,/execute-production-write/);
}

r=buildLineHistoryRecoveryNextCommand({
  stage:'OWNER_EXACT_APPROVAL_REQUIRED',
  ...base
});
assert.equal(r.ready,false);
assert.equal(r.requires_owner_approval,true);
assert.equal(r.production_write,false);
assert.equal(r.command,'');

r=buildLineHistoryRecoveryNextCommand({
  stage:'APPROVED_WRITE_READY',
  ...base
});
assert.equal(r.ready,true);
assert.equal(r.requires_owner_approval,true);
assert.equal(r.production_write,true);
assert.match(r.command,/--phase' 'approved-write/);
assert.match(r.command,/--approval-file/);
assert.match(r.command,/--execute-production-write' 'YES/);

for(const stage of ['COMPLETE','COMPLETE_NO_REVIEW','COMPLETE_NO_WRITE']){
  r=buildLineHistoryRecoveryNextCommand({stage,...base});
  assert.equal(r.ready,false);
  assert.equal(r.command,'');
  assert.equal(r.production_write,false);
}

r=buildLineHistoryRecoveryNextCommand({
  stage:'APPROVED_WRITE_READY',
  preauthDir:'/tmp/preauth',
  d1PreviewDir:'/tmp/d1',
  approvalFile:''
});
assert.equal(r.ready,false);
assert.equal(r.production_write,false);
assert.equal(r.reason,'APPROVED_WRITE_PATHS_REQUIRED');

r=buildLineHistoryRecoveryNextCommand({
  stage:'READONLY_RESUME_REQUIRED',
  candidates:'/tmp/a',
  customerMaster:"/tmp/it's master.json"
});
assert.match(r.command,/it'\\''s master\.json/);

const cli=fs.readFileSync('scripts/inspect-line-history-recovery-status.mjs','utf8');
assert.match(cli,/buildLineHistoryRecoveryNextCommand/);
assert.match(cli,/NEXT_COMMAND_READY=/);
assert.match(cli,/NEXT_COMMAND_REASON=/);
assert.match(cli,/NEXT_COMMAND_PRODUCTION_WRITE=/);
assert.match(cli,/NEXT_COMMAND_REQUIRES_OWNER_APPROVAL=/);
assert.match(cli,/NEXT_COMMAND=/);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/d1\s+execute/i);

console.log('LINE_HISTORY_NEXT_COMMAND_READONLY=PASS');
console.log('LINE_HISTORY_NEXT_COMMAND_PREAUTH=PASS');
console.log('LINE_HISTORY_NEXT_COMMAND_D1_PREVIEW=PASS');
console.log('LINE_HISTORY_NEXT_COMMAND_OWNER_APPROVAL_GATE=PASS');
console.log('LINE_HISTORY_NEXT_COMMAND_APPROVED_WRITE_EXPLICIT_FLAG=PASS');
console.log('LINE_HISTORY_NEXT_COMMAND_COMPLETE_NOOP=PASS');
console.log('LINE_HISTORY_NEXT_COMMAND_SHELL_QUOTING=PASS');
