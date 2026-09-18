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
assert.match(cli,/PRODUCTION_WRITE_POSSIBLE=/);
assert.match(cli,/PRIVATE_VALUES_PRINTED_TO_TERMINAL=0/);

const operator=fs.readFileSync('scripts/run-line-history-recovery-operator.sh','utf8');
assert.match(operator,/inspect-line-history-recovery-status\.mjs/);
assert.match(operator,/STATUS_ARGS=\(\)/);
assert.match(operator,/PRODUCTION_D1_WRITE=0/);

console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_INPUTS=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_REVIEW=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_PREAUTH=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_D1_PREVIEW=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_EXACT_APPROVAL=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_COMPLETE=PASS');
console.log('LINE_HISTORY_RECOVERY_STATUS_LOCAL_ONLY=PASS');
