import fs from 'node:fs';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { buildLineHistoryOwnerBackfillReadonlyPreview } from '../src/crm-line-history-owner-backfill-preview.mjs';

const hash=v=>createHash('sha256').update(v).digest('hex').slice(0,16);
const line1='U11111111111111111111111111111111';
const line2='U22222222222222222222222222222222';

const plan={
  planner:'line_history_owner_decision_plan_v1',
  validation_error_count:0,
  authorization_granted:false,
  proposed_backfill_identity_actions:2,
  proposed_write_actions:0,
  ready_for_readonly_backfill_preview:true,
  ready_for_separate_write_authorization:false,
  proposed_actions:[
    {
      queue_id:'q1',
      action:'BACKFILL_LINE_HISTORY_TO_EXISTING_CUSTOMER',
      target_customer_id:'26000001',
      line_user_id:line1,
      source_line_id_hash:hash(line1),
      message_rows:2,
      target_line_state:'EMPTY',
      source:'owner_review_same_person'
    },
    {
      queue_id:'q2',
      action:'BACKFILL_LINE_HISTORY_TO_EXISTING_CUSTOMER',
      target_customer_id:'26000002',
      line_user_id:line2,
      source_line_id_hash:hash(line2),
      message_rows:2,
      target_line_state:'EXACT',
      source:'owner_review_same_person'
    }
  ]
};

const candidates=[
  {
    message_key:'m1',
    line_user_id:line1,
    direction:'inbound',
    message_type:'text',
    message_text:'hello',
    sent_at:'2026-01-01T10:00:00+09:00'
  },
  {
    message_key:'m2',
    line_user_id:line1,
    direction:'outbound',
    message_type:'text',
    message_text:'reply',
    sent_at:'2026-01-01T10:01:00+09:00'
  },
  {
    message_key:'m3',
    line_user_id:line2,
    direction:'inbound',
    message_type:'text',
    message_text:'same semantic',
    sent_at:'2026-01-02T10:00:00+09:00'
  },
  {
    message_key:'m4',
    line_user_id:line2,
    direction:'inbound',
    message_type:'text',
    message_text:'same semantic',
    sent_at:'2026-01-02T10:00:00+09:00'
  }
];

const result=buildLineHistoryOwnerBackfillReadonlyPreview({plan,candidates});

assert.equal(result.planner,'line_history_owner_backfill_readonly_preview_v1');
assert.equal(result.source_identity_actions,2);
assert.equal(result.selected_candidate_rows,4);
assert.equal(result.batch_duplicate_rows,1);
assert.equal(result.preview_candidate_rows,3);
assert.equal(result.validation_error_count,0);
assert.equal(result.preview_ready,true);
assert.equal(result.private_rows.length,4);
assert.equal(result.private_rows.filter(x=>x.batch_duplicate).length,1);

const sql=result.preview_sql.replace(/^--.*$/gm,'');
assert.match(sql,/WITH candidates\(/);
assert.match(sql,/customer_line_messages/);
assert.match(sql,/target_missing_rows/);
assert.match(sql,/target_line_conflict_rows/);
assert.match(sql,/already_present_rows/);
assert.match(sql,/would_insert_rows/);
assert.doesNotMatch(sql,/\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE)\b/i);

assert.equal(result.safety.production_d1_read,0);
assert.equal(result.safety.production_d1_write,0);
assert.equal(result.safety.sql_generated,true);
assert.equal(result.safety.generated_sql_read_only,true);
assert.equal(result.safety.write_sql_generated,false);
assert.equal(result.safety.customer_update,0);
assert.equal(result.safety.line_send,0);

const badCount=buildLineHistoryOwnerBackfillReadonlyPreview({
  plan:{...plan,proposed_actions:[{...plan.proposed_actions[0],message_rows:3}]},
  candidates
});
assert.equal(badCount.preview_ready,false);
assert.ok(badCount.validation_errors.some(x=>x.code==='CANDIDATE_MESSAGE_COUNT_MISMATCH'));
assert.equal(badCount.preview_sql,'');

const badHash=buildLineHistoryOwnerBackfillReadonlyPreview({
  plan:{...plan,proposed_actions:[{...plan.proposed_actions[0],source_line_id_hash:'deadbeef'}]},
  candidates
});
assert.equal(badHash.preview_ready,false);
assert.ok(badHash.validation_errors.some(x=>x.code==='SOURCE_LINE_HASH_MISMATCH'));

const badAction=buildLineHistoryOwnerBackfillReadonlyPreview({
  plan:{...plan,proposed_actions:[{...plan.proposed_actions[0],action:'LINK_LINE_ID_TO_EXISTING_CUSTOMER'}]},
  candidates
});
assert.equal(badAction.preview_ready,false);
assert.ok(badAction.validation_errors.some(x=>x.code==='ACTION_TYPE_INVALID'));

const preauthorized=buildLineHistoryOwnerBackfillReadonlyPreview({
  plan:{...plan,authorization_granted:true},
  candidates
});
assert.equal(preauthorized.preview_ready,false);
assert.ok(preauthorized.validation_errors.some(x=>x.code==='PLAN_ALREADY_AUTHORIZED'));

const cli=fs.readFileSync('scripts/prepare-line-history-owner-backfill-preview.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/https?:\/\//i);
assert.match(cli,/PREVIEW_SQL_READ_ONLY=YES/);
assert.match(cli,/WRITE_SQL_GENERATED=0/);
assert.match(cli,/PRODUCTION_D1_WRITE=0/);
assert.match(cli,/PRIVATE_VALUES_PRINTED_TO_TERMINAL=0/);

console.log('LINE_HISTORY_OWNER_BACKFILL_PREVIEW_SELECTION=PASS');
console.log('LINE_HISTORY_OWNER_BACKFILL_PREVIEW_BATCH_DEDUPE=PASS');
console.log('LINE_HISTORY_OWNER_BACKFILL_PREVIEW_COUNT_GUARD=PASS');
console.log('LINE_HISTORY_OWNER_BACKFILL_PREVIEW_READ_ONLY_SQL=PASS');
console.log('LINE_HISTORY_OWNER_BACKFILL_PREVIEW_PREAUTH_BLOCK=PASS');
console.log('WRITE_SQL_GENERATED=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
