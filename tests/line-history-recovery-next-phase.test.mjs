import fs from 'node:fs';
import assert from 'node:assert/strict';
import { resolveLineHistoryRecoveryNextPhase } from '../src/crm-line-history-recovery-next-phase.mjs';

function expectStage(input,stage,nextPhase,writePossible=false){
  const r=resolveLineHistoryRecoveryNextPhase(input);
  assert.equal(r.stage,stage);
  assert.equal(r.next_phase,nextPhase);
  assert.equal(r.production_write_possible,writePossible);
  assert.ok(r.next_action);
}

expectStage({},'INPUTS_REQUIRED','readonly-resume');
expectStage({
  candidatesPresent:true,
  customerMasterPresent:true
},'READONLY_RESUME_REQUIRED','readonly-resume');

expectStage({
  resumeReady:true,
  reviewQueueGroups:5
},'OWNER_REVIEW_REQUIRED','preauth');

expectStage({
  resumeReady:true,
  reviewQueueGroups:null
},'OWNER_REVIEW_REQUIRED','preauth');

expectStage({
  resumeReady:true,
  reviewQueueGroups:0
},'COMPLETE_NO_REVIEW','none');

expectStage({
  resumeReady:true,
  reviewQueueGroups:4,
  decisionsPresent:true
},'PREAUTH_REQUIRED','preauth');

expectStage({
  resumeReady:true,
  reviewQueueGroups:4,
  decisionsPresent:true,
  preauthPreviewReady:true
},'D1_PREVIEW_REQUIRED','d1-preview');

expectStage({
  completionReceipt:{
    complete:true,
    completion_type:'OWNER_DECISIONS_NO_WRITE',
    review_queue_groups:4,
    accepted_no_write_decisions:4,
    proposed_backfill_identity_actions:0,
    proposed_write_actions:0
  }
},'COMPLETE_NO_WRITE','none');

expectStage({
  d1Packet:{
    packet_ready:true,
    authorization_required:true,
    source_main_sha:'1'.repeat(40)
  },
  currentMainSha:'2'.repeat(40),
  approvalFilePresent:true
},'STALE_AUTHORIZATION_PACKET','d1-preview');

expectStage({
  d1Packet:{
    packet_ready:false,
    authorization_required:true
  }
},'BLOCKED_D1_PREVIEW','d1-preview');

expectStage({
  d1Packet:{
    packet_ready:true,
    authorization_required:false
  }
},'COMPLETE_NO_WRITE','none');

expectStage({
  d1Packet:{
    packet_ready:true,
    authorization_required:true
  },
  approvalFilePresent:false
},'OWNER_EXACT_APPROVAL_REQUIRED','approved-write');

expectStage({
  d1Packet:{
    packet_ready:true,
    authorization_required:true
  },
  approvalFilePresent:true
},'APPROVED_WRITE_READY','approved-write',true);

expectStage({
  d1Packet:{
    packet_ready:true,
    authorization_required:true
  },
  approvalFilePresent:true,
  completionReceipt:{complete:true}
},'COMPLETE','none',false);

const cli=fs.readFileSync('scripts/inspect-line-history-recovery-status.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/d1\s+execute/i);
assert.doesNotMatch(cli,/INSERT\s+(?:OR\s+IGNORE\s+)?INTO/i);
assert.match(cli,/--main-sha/);
assert.match(cli,/no-write-completion-receipt\.json/);
assert.match(cli,/PRODUCTION_WRITE_POSSIBLE=/);
assert.match(cli,/PRIVATE_VALUES_PRINTED_TO_TERMINAL=0/);

const preauth=fs.readFileSync('scripts/run-line-history-owner-authorization-prep.sh','utf8');
assert.match(preauth,/no-write-completion-receipt\.json/);
assert.match(preauth,/OWNER_DECISIONS_NO_WRITE/);
assert.match(preauth,/RESULT=OWNER_BACKFILL_COMPLETE_NO_WRITE/);
assert.match(preauth,/PRODUCTION_D1_WRITE=0/);
assert.match(preauth,/PROPOSED_BACKFILL_IDENTITY_ACTIONS=0/);
assert.doesNotMatch(preauth,/--execute-production-write/);

const operator=fs.readFileSync('scripts/run-line-history-recovery-operator.sh','utf8');
assert.match(operator,/inspect-line-history-recovery-status\.mjs/);
assert.match(operator,/STATUS_ARGS=\(\)/);
assert.match(operator,/PRODUCTION_D1_WRITE=0/);

console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_INPUTS=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_REVIEW=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_PREAUTH=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_NO_WRITE_RECEIPT=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_D1_PREVIEW=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_STALE_PACKET=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_EXACT_APPROVAL=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_COMPLETE=PASS');
console.log('LINE_HISTORY_RECOVERY_STATUS_LOCAL_ONLY=PASS');
