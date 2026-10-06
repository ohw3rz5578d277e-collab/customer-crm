import fs from 'node:fs';
import assert from 'node:assert/strict';
import {
  isValidOwnerNoWriteCompletionReceipt,
  isValidOwnerWriteCompletionReceipt,
  resolveLineHistoryRecoveryNextPhase
} from '../src/crm-line-history-recovery-next-phase.mjs';

function expectStage(input,stage,nextPhase,writePossible=false){
  const r=resolveLineHistoryRecoveryNextPhase(input);
  assert.equal(r.stage,stage);
  assert.equal(r.next_phase,nextPhase);
  assert.equal(r.production_write_possible,writePossible);
  assert.ok(r.next_action);
}

function noWriteReceipt(mainSha='a'.repeat(40)){
  return {
    receipt_format:'customer-crm-line-history-no-write-completion-v1',
    complete:true,
    completion_type:'OWNER_DECISIONS_NO_WRITE',
    source_main_sha:mainSha,
    review_queue_groups:4,
    submitted_decisions:4,
    accepted_no_write_decisions:4,
    proposed_backfill_identity_actions:0,
    proposed_write_actions:0,
    decision_summary:{
      SAME_PERSON:0,
      DIFFERENT_PERSON:2,
      DEFERRED:1,
      NEEDS_MORE_EVIDENCE:1
    },
    authorization_granted:false,
    production_d1_read:0,
    production_d1_write:0,
    customer_id_generation:0,
    customer_update:0,
    customer_delete:0,
    customer_merge:0,
    line_send:0,
    worker_deploy:0,
    production_deploy:0
  };
}

function writeReceipt(mainSha='a'.repeat(40)){
  return {
    planner:'line_history_owner_write_completion_receipt_v1',
    complete:true,
    source_main_sha:mainSha,
    authorization_scope:'CUSTOMER_LINE_MESSAGES_INSERT_ONLY',
    exact_physical_insert_rows:2,
    post_preview_would_insert_rows:0,
    blocker_count:0,
    blockers:[],
    safety:{
      private_customer_id_output:false,
      private_line_user_id_output:false,
      message_text_output:false,
      customer_name_output:false,
      line_send:0,
      customer_id_generation:0,
      customer_update:0,
      customer_delete:0,
      customer_merge:0,
      worker_deploy:0,
      production_deploy:0
    }
  };
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

const exactMainSha='a'.repeat(40);
const validNoWrite=noWriteReceipt(exactMainSha);
assert.equal(isValidOwnerNoWriteCompletionReceipt(validNoWrite,exactMainSha),true);
expectStage({
  completionReceipt:validNoWrite,
  currentMainSha:exactMainSha
},'COMPLETE_NO_WRITE','none');

const wrongFormat={...validNoWrite,receipt_format:'legacy'};
const missingCompletionType={...validNoWrite};
delete missingCompletionType.completion_type;
const staleReceipt={...validNoWrite,source_main_sha:'b'.repeat(40)};
const countMismatch={...validNoWrite,submitted_decisions:3};
const actionMismatch={...validNoWrite,proposed_backfill_identity_actions:1};
const writeFlag={...validNoWrite,production_d1_write:1};
const authorizationFlag={...validNoWrite,authorization_granted:true};
const samePersonSummary={
  ...validNoWrite,
  decision_summary:{
    SAME_PERSON:1,
    DIFFERENT_PERSON:2,
    DEFERRED:1,
    NEEDS_MORE_EVIDENCE:0
  }
};
const summaryTotalMismatch={
  ...validNoWrite,
  decision_summary:{
    SAME_PERSON:0,
    DIFFERENT_PERSON:1,
    DEFERRED:1,
    NEEDS_MORE_EVIDENCE:1
  }
};
const summaryExtraKey={
  ...validNoWrite,
  decision_summary:{...validNoWrite.decision_summary,UNKNOWN:0}
};

for(const bad of [
  wrongFormat,
  missingCompletionType,
  staleReceipt,
  countMismatch,
  actionMismatch,
  writeFlag,
  authorizationFlag,
  samePersonSummary,
  summaryTotalMismatch,
  summaryExtraKey
]){
  assert.equal(isValidOwnerNoWriteCompletionReceipt(bad,exactMainSha),false);
  const stage=resolveLineHistoryRecoveryNextPhase({
    completionReceipt:bad,
    currentMainSha:exactMainSha
  }).stage;
  assert.notEqual(stage,'COMPLETE_NO_WRITE');
  assert.notEqual(stage,'COMPLETE');
}

assert.equal(isValidOwnerNoWriteCompletionReceipt(validNoWrite,''),false);

const validWrite=writeReceipt(exactMainSha);
assert.equal(isValidOwnerWriteCompletionReceipt(validWrite,exactMainSha),true);
expectStage({
  completionReceipt:validWrite,
  currentMainSha:exactMainSha
},'COMPLETE','none',false);

for(const bad of [
  {complete:true},
  {...validWrite,planner:'legacy'},
  {...validWrite,source_main_sha:'b'.repeat(40)},
  {...validWrite,blocker_count:1,blockers:['X']},
  {...validWrite,post_preview_would_insert_rows:1},
  {...validWrite,safety:{...validWrite.safety,line_send:1}}
]){
  assert.equal(isValidOwnerWriteCompletionReceipt(bad,exactMainSha),false);
  assert.notEqual(
    resolveLineHistoryRecoveryNextPhase({completionReceipt:bad,currentMainSha:exactMainSha}).stage,
    'COMPLETE'
  );
}

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
assert.match(preauth,/customer-crm-line-history-no-write-completion-v1/);
assert.match(preauth,/OWNER_DECISIONS_NO_WRITE/);
assert.match(preauth,/RESULT=OWNER_BACKFILL_COMPLETE_NO_WRITE/);
assert.match(preauth,/PRODUCTION_D1_READ=0/);
assert.match(preauth,/PRODUCTION_D1_WRITE=0/);
assert.match(preauth,/PROPOSED_BACKFILL_IDENTITY_ACTIONS=0/);
assert.match(preauth,/PROPOSED_WRITE_ACTIONS=0/);
assert.doesNotMatch(preauth,/--execute-production-write/);

const operator=fs.readFileSync('scripts/run-line-history-recovery-operator.sh','utf8');
assert.match(operator,/inspect-line-history-recovery-status\.mjs/);
assert.match(operator,/STATUS_ARGS=\(\)/);
assert.match(operator,/STATUS_ARGS\+=\(--main-sha "\$LOCAL_HEAD"\)/);
assert.match(operator,/PRODUCTION_D1_WRITE=0/);

const nextCommand=fs.readFileSync('src/crm-line-history-recovery-next-command.mjs','utf8');
assert.match(nextCommand,/COMPLETE_NO_WRITE/);
assert.match(nextCommand,/production_write:false/);

console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_INPUTS=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_REVIEW=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_PREAUTH=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_NO_WRITE_RECEIPT=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_DECISION_SUMMARY=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_WRITE_RECEIPT=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_D1_PREVIEW=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_STALE_PACKET=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_EXACT_APPROVAL=PASS');
console.log('LINE_HISTORY_RECOVERY_NEXT_PHASE_COMPLETE=PASS');
console.log('LINE_HISTORY_RECOVERY_STATUS_LOCAL_ONLY=PASS');
