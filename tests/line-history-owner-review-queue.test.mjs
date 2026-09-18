import fs from 'node:fs';
import assert from 'node:assert/strict';
import { buildLineHistoryOwnerReviewQueue } from '../src/crm-line-history-owner-review-queue.mjs';

const triage={
  candidate_message_rows:12,
  classifications:[
    {
      line_id_hash:'abc123hash',
      line_user_id_present:true,
      message_rows:5,
      customer_id_hints:['26001234'],
      legacy_customer_id_hints:['C-LEGACY-SECRET'],
      csv_file_names:['customer-private-name.csv'],
      category:'REVIEW_REQUIRED',
      reason:'WEAK_OR_INCOMPLETE_EVIDENCE',
      target_customer_id:'26005678',
      evidence:[
        'customer_master_exact_line_but_customer_missing_in_production:26001234',
        'name_only_unique:26005678'
      ],
      conflicts:[],
      message_text:'private-message-one',
      customer_name:'Private Name One'
    },
    {
      line_id_hash:'def456hash',
      line_user_id_present:true,
      message_rows:3,
      customer_id_hints:['26009991'],
      legacy_customer_id_hints:[],
      csv_file_names:['private-two.csv'],
      category:'BLOCKED_CONFLICT',
      reason:'EXACT_RESERVATION_LINE_CONFLICT',
      target_customer_id:'26009992',
      evidence:['reservation_id_exact','exact_reservation_unique_current_target'],
      conflicts:['explicit_different_person_review:C-SECRET->26009992'],
      message_text:'private-message-two',
      customer_name:'Private Name Two'
    },
    {
      line_id_hash:'',
      line_user_id_present:false,
      message_rows:1,
      customer_id_hints:[],
      legacy_customer_id_hints:['C-NO-LINE-SECRET'],
      csv_file_names:['private-three.csv'],
      category:'UNRESOLVED',
      reason:'NO_SAFE_IDENTITY_EVIDENCE',
      target_customer_id:'',
      evidence:[],
      conflicts:[],
      message_text:'private-message-three',
      customer_name:'Private Name Three'
    },
    {
      line_id_hash:'resolvedhash',
      line_user_id_present:true,
      message_rows:2,
      customer_id_hints:['26007777'],
      legacy_customer_id_hints:[],
      category:'ALREADY_RESOLVED',
      reason:'PRODUCTION_EXACT_LINE',
      target_customer_id:'26007777',
      evidence:['production_line_exact'],
      conflicts:[]
    },
    {
      line_id_hash:'autohash',
      line_user_id_present:true,
      message_rows:1,
      customer_id_hints:['26008888'],
      legacy_customer_id_hints:[],
      category:'AUTO_CONFIRMABLE',
      reason:'EXACT_RESERVATION_ID_UNIQUE_CURRENT_TARGET',
      target_customer_id:'26008888',
      evidence:['reservation_id_exact'],
      conflicts:[]
    }
  ]
};

const result=buildLineHistoryOwnerReviewQueue(triage);

assert.equal(result.planner,'line_history_owner_review_queue_v1');
assert.equal(result.source_candidate_message_rows,12);
assert.equal(result.review_queue_groups,3);
assert.equal(result.review_queue_message_rows,9);
assert.equal(result.blocked_conflict_groups,1);
assert.equal(result.blocked_conflict_message_rows,3);
assert.equal(result.review_required_groups,1);
assert.equal(result.review_required_message_rows,5);
assert.equal(result.unresolved_groups,1);
assert.equal(result.unresolved_message_rows,1);

assert.equal(result.items[0].category,'BLOCKED_CONFLICT');
assert.equal(result.items[0].review_action,'RESOLVE_CONFLICT');
assert.equal(result.items[1].category,'REVIEW_REQUIRED');
assert.equal(result.items[1].review_action,'OWNER_REVIEW');
assert.equal(result.items[2].category,'UNRESOLVED');
assert.equal(result.items[2].review_action,'NEEDS_MORE_EVIDENCE');

const review=result.items.find(x=>x.category==='REVIEW_REQUIRED');
assert.equal(review.current_hint_count,1);
assert.equal(review.legacy_hint_count,1);
assert.equal(review.target_customer_id_present,true);
assert.match(review.source_identity_hash,/^[0-9a-f]{16}$/);
assert.match(review.target_customer_id_hash,/^[0-9a-f]{16}$/);
assert.deepEqual(
  review.evidence_types,
  ['customer_master_exact_line_but_customer_missing_in_production','name_only_unique']
);

const conflict=result.items.find(x=>x.category==='BLOCKED_CONFLICT');
assert.deepEqual(conflict.conflict_types,['explicit_different_person_review']);

const serialized=JSON.stringify(result);
for(const secret of [
  '26001234','26005678','26009991','26009992','26007777','26008888',
  'C-LEGACY-SECRET','C-SECRET','C-NO-LINE-SECRET',
  'customer-private-name.csv','private-two.csv','private-three.csv',
  'private-message-one','private-message-two','private-message-three',
  'Private Name One','Private Name Two','Private Name Three'
]){
  assert.ok(!serialized.includes(secret),'private value leaked: '+secret);
}

assert.equal(result.safety.production_d1_read,0);
assert.equal(result.safety.production_d1_write,0);
assert.equal(result.safety.customer_id_generation,0);
assert.equal(result.safety.customer_update,0);
assert.equal(result.safety.customer_delete,0);
assert.equal(result.safety.customer_merge,0);
assert.equal(result.safety.line_send,0);
assert.equal(result.safety.raw_customer_id_output,false);
assert.equal(result.safety.raw_line_user_id_output,false);
assert.equal(result.safety.message_text_output,false);
assert.equal(result.safety.customer_name_output,false);
assert.equal(result.safety.csv_file_name_output,false);

const cli=fs.readFileSync('scripts/build-line-history-owner-review-queue.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/https?:\/\//i);
assert.match(cli,/PRODUCTION_D1_READ=0/);
assert.match(cli,/PRODUCTION_D1_WRITE=0/);
assert.match(cli,/RAW_CUSTOMER_ID_OUTPUT=0/);

console.log('LINE_HISTORY_OWNER_REVIEW_QUEUE_COUNTS=PASS');
console.log('LINE_HISTORY_OWNER_REVIEW_QUEUE_PRIORITY=PASS');
console.log('LINE_HISTORY_OWNER_REVIEW_QUEUE_RESOLVED_EXCLUDED=PASS');
console.log('LINE_HISTORY_OWNER_REVIEW_QUEUE_PRIVACY=PASS');
console.log('LINE_HISTORY_OWNER_REVIEW_QUEUE_LOCAL_ONLY=PASS');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
