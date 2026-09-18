import fs from 'node:fs';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { buildLineHistoryOwnerAuthorizationPacket } from '../src/crm-line-history-owner-authorization-packet.mjs';

const privatePlan={
  planner:'line_history_owner_decision_plan_v1',
  submitted_decisions:3,
  undecided_groups:1,
  decision_summary:{
    SAME_PERSON:1,
    DIFFERENT_PERSON:1,
    DEFERRED:1,
    NEEDS_MORE_EVIDENCE:0
  },
  proposed_backfill_identity_actions:1,
  proposed_write_actions:0,
  accepted_no_write_decisions:2,
  validation_error_count:0,
  proposed_actions:[
    {
      queue_id:'safe-queue-id',
      action:'BACKFILL_LINE_HISTORY_TO_EXISTING_CUSTOMER',
      target_customer_id:'26001234',
      line_user_id:'Uabcdefabcdefabcdefabcdefabcdef12',
      source_line_id_hash:'private-line-hash',
      message_rows:4,
      target_line_state:'EMPTY',
      source:'owner_review_same_person'
    }
  ],
  no_write_decisions:[
    {queue_id:'safe-no-write',decision:'DIFFERENT_PERSON'}
  ],
  validation_errors:[],
  ready_for_readonly_backfill_preview:true,
  ready_for_separate_write_authorization:false,
  authorization_granted:false,
  safety:{
    production_d1_read:0,
    production_d1_write:0,
    generated_sql:false,
    executed_sql:false,
    customer_id_generation:0,
    customer_update:0,
    customer_delete:0,
    customer_merge:0,
    line_send:0,
    worker_deploy:0,
    production_deploy:0
  }
};

const planRaw=Buffer.from(JSON.stringify(privatePlan,null,2)+'\n');
const planDigest=createHash('sha256').update(planRaw).digest('hex');

const previewResult={
  planner:'line_history_owner_backfill_preview_result_v1',
  source_plan_sha256:planDigest,
  source_candidate_snapshot_sha256:'b'.repeat(64),
  selected_private_rows_sha256:'c'.repeat(64),
  source_preview_sql_sha256:'d'.repeat(64),
  chunk_count:1,
  candidate_rows:4,
  batch_duplicate_rows:0,
  target_missing_rows:0,
  target_line_conflict_rows:0,
  already_present_rows:1,
  would_insert_rows:3,
  validation_error_count:0,
  validation_errors:[],
  preview_pass:true,
  write_required:true,
  exact_physical_insert_rows:3,
  authorization_granted:false,
  safety:{
    production_d1_read:1,
    production_d1_write:0,
    sql_executed_read_only:true,
    write_sql_generated:false,
    write_sql_executed:false,
    customer_id_generation:0,
    customer_update:0,
    customer_delete:0,
    customer_merge:0,
    line_send:0,
    worker_deploy:0,
    production_deploy:0,
    private_values_output:false
  }
};

const previewRaw=Buffer.from(JSON.stringify(previewResult,null,2)+'\n');
const mainSha='2df62bf3682ae0e0444e024605118098fc43e15f';

const packet=buildLineHistoryOwnerAuthorizationPacket({
  plan:privatePlan,
  sourcePlanBytes:planRaw,
  previewResult,
  sourcePreviewResultBytes:previewRaw,
  mainSha
});

assert.equal(packet.planner,'line_history_owner_write_authorization_packet_v2');
assert.match(packet.source_plan_sha256,/^[0-9a-f]{64}$/);
assert.match(packet.source_preview_result_sha256,/^[0-9a-f]{64}$/);
assert.equal(packet.source_preview_sql_sha256,'d'.repeat(64));
assert.equal(packet.source_candidate_snapshot_sha256,'b'.repeat(64));
assert.equal(packet.selected_private_rows_sha256,'c'.repeat(64));
assert.equal(packet.source_main_sha,mainSha);
assert.equal(packet.backfill_identity_actions,1);
assert.equal(packet.already_present_rows,1);
assert.equal(packet.exact_physical_insert_rows,3);
assert.equal(packet.authorization_scope,'CUSTOMER_LINE_MESSAGES_INSERT_ONLY');
assert.equal(packet.authorization_required,true);
assert.equal(packet.packet_ready,true);
assert.equal(packet.authorization_granted,false);
assert.equal(packet.blockers.length,0);
assert.match(
  packet.approval_text,
  /^PLAN_SHA [0-9a-f]{64} \/ PREVIEW_SHA [0-9a-f]{64} \/ MAIN_SHA [0-9a-f]{40} \/ ROWS 3 のcustomer_line_messages INSERT-only Production D1 writeを承認します$/
);

const serialized=JSON.stringify(packet);
for(const secret of [
  '26001234',
  'Uabcdefabcdefabcdefabcdefabcdef12',
  'owner_review_same_person',
  'BACKFILL_LINE_HISTORY_TO_EXISTING_CUSTOMER',
  'private-line-hash'
]){
  assert.ok(!serialized.includes(secret),'private action leaked: '+secret);
}

assert.equal(packet.safety.production_d1_write,0);
assert.equal(packet.safety.sql_generated,false);
assert.equal(packet.safety.sql_executed,false);
assert.equal(packet.safety.customer_update,0);
assert.equal(packet.safety.line_send,0);

const mismatchedPreview=buildLineHistoryOwnerAuthorizationPacket({
  plan:privatePlan,
  sourcePlanBytes:planRaw,
  previewResult:{...previewResult,source_plan_sha256:'0'.repeat(64)},
  sourcePreviewResultBytes:previewRaw,
  mainSha
});
assert.equal(mismatchedPreview.packet_ready,false);
assert.ok(mismatchedPreview.blockers.includes('PREVIEW_PLAN_SHA_MISMATCH'));
assert.equal(mismatchedPreview.approval_text,'');

const failedPreview=buildLineHistoryOwnerAuthorizationPacket({
  plan:privatePlan,
  sourcePlanBytes:planRaw,
  previewResult:{...previewResult,preview_pass:false,validation_error_count:1,target_line_conflict_rows:1},
  sourcePreviewResultBytes:previewRaw,
  mainSha
});
assert.equal(failedPreview.packet_ready,false);
assert.ok(failedPreview.blockers.includes('PREVIEW_NOT_PASS'));
assert.ok(failedPreview.blockers.includes('PREVIEW_TARGET_LINE_CONFLICT'));

const badMain=buildLineHistoryOwnerAuthorizationPacket({
  plan:privatePlan,
  sourcePlanBytes:planRaw,
  previewResult,
  sourcePreviewResultBytes:previewRaw,
  mainSha:'bad-sha'
});
assert.equal(badMain.packet_ready,false);
assert.ok(badMain.blockers.includes('MAIN_SHA_INVALID'));

const bypassedPlan=buildLineHistoryOwnerAuthorizationPacket({
  plan:{...privatePlan,ready_for_separate_write_authorization:true},
  sourcePlanBytes:planRaw,
  previewResult,
  sourcePreviewResultBytes:previewRaw,
  mainSha
});
assert.equal(bypassedPlan.packet_ready,false);
assert.ok(bypassedPlan.blockers.includes('PLAN_PREVIEW_GATE_BYPASSED'));

const noWritePreview={...previewResult,would_insert_rows:0,exact_physical_insert_rows:0,write_required:false};
const noWriteRaw=Buffer.from(JSON.stringify(noWritePreview));
const noWritePacket=buildLineHistoryOwnerAuthorizationPacket({
  plan:privatePlan,
  sourcePlanBytes:planRaw,
  previewResult:noWritePreview,
  sourcePreviewResultBytes:noWriteRaw,
  mainSha
});
assert.equal(noWritePacket.packet_ready,true);
assert.equal(noWritePacket.authorization_required,false);
assert.equal(noWritePacket.approval_text,'');

const cli=fs.readFileSync('scripts/build-line-history-owner-authorization-packet.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/INSERT\s+INTO/i);
assert.match(cli,/--preview-result/);
assert.match(cli,/EXACT_PHYSICAL_INSERT_ROWS/);
assert.match(cli,/AUTHORIZATION_GRANTED=NO/);
assert.match(cli,/SQL_GENERATED=0/);
assert.match(cli,/SQL_EXECUTED=0/);

const prep=fs.readFileSync('scripts/run-line-history-owner-authorization-prep.sh','utf8');
assert.match(prep,/prepare-line-history-owner-backfill-preview\.mjs/);
assert.match(prep,/READY_FOR_READONLY_BACKFILL_PREVIEW/);
assert.match(prep,/READY_FOR_SEPARATE_WRITE_AUTHORIZATION=/);
assert.match(prep,/APPROVAL_TEXT_GENERATED=0/);
assert.match(prep,/WRITE_SQL_GENERATED=0/);
assert.match(prep,/PRODUCTION_D1_WRITE=0/);
assert.doesNotMatch(prep,/build-line-history-owner-authorization-packet\.mjs/);
assert.doesNotMatch(prep,/\bwrangler\b/i);
assert.doesNotMatch(prep,/d1\s+execute/i);

const d1Runner=fs.readFileSync('scripts/run-line-history-owner-backfill-readonly-preview.sh','utf8');
assert.match(d1Runner,/summarize-line-history-owner-backfill-preview\.mjs/);
assert.match(d1Runner,/build-line-history-owner-authorization-packet\.mjs/);
assert.match(d1Runner,/PRODUCTION_D1_READ=YES/);
assert.match(d1Runner,/PRODUCTION_D1_WRITE=0/);
assert.match(d1Runner,/AUTHORIZATION_GRANTED=NO/);
assert.doesNotMatch(d1Runner,/wrangler\s+deploy/i);
assert.doesNotMatch(d1Runner,/d1\s+migrations\s+apply/i);

console.log('LINE_HISTORY_OWNER_AUTH_PACKET_PRIVATE_VALUES_HIDDEN=PASS');
console.log('LINE_HISTORY_OWNER_AUTH_PACKET_PLAN_PREVIEW_MAIN_BINDING=PASS');
console.log('LINE_HISTORY_OWNER_AUTH_PACKET_EXACT_PHYSICAL_ROWS=PASS');
console.log('LINE_HISTORY_OWNER_AUTH_PACKET_PREVIEW_GATE_REQUIRED=PASS');
console.log('LINE_HISTORY_OWNER_AUTH_PACKET_AUTH_NOT_GRANTED=PASS');
console.log('LINE_HISTORY_OWNER_PREAUTH_STOPS_BEFORE_D1=PASS');
console.log('SQL_GENERATED=0');
console.log('SQL_EXECUTED=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
