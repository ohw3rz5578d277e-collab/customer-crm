import fs from 'node:fs';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { buildLineHistoryOwnerReviewQueue } from '../src/crm-line-history-owner-review-queue.mjs';
import { buildLineHistoryOwnerDecisionPlan } from '../src/crm-line-history-owner-decision-plan.mjs';

const hash=v=>createHash('sha256').update(v).digest('hex').slice(0,16);
const line1='U11111111111111111111111111111111';
const line2='U22222222222222222222222222222222';
const line3='U33333333333333333333333333333333';
const line4='U44444444444444444444444444444444';

const classifications=[
  {
    line_id_hash:hash(line1),
    line_user_id_present:true,
    message_rows:5,
    customer_id_hints:[],
    legacy_customer_id_hints:['C-ONE'],
    category:'REVIEW_REQUIRED',
    reason:'WEAK_OR_INCOMPLETE_EVIDENCE',
    target_customer_id:'26000001',
    evidence:['reservation_id_exact'],
    conflicts:[]
  },
  {
    line_id_hash:hash(line2),
    line_user_id_present:true,
    message_rows:3,
    customer_id_hints:[],
    legacy_customer_id_hints:['C-TWO'],
    category:'REVIEW_REQUIRED',
    reason:'WEAK_OR_INCOMPLETE_EVIDENCE',
    target_customer_id:'26000002',
    evidence:['reservation_id_exact'],
    conflicts:[]
  },
  {
    line_id_hash:hash(line3),
    line_user_id_present:true,
    message_rows:2,
    customer_id_hints:[],
    legacy_customer_id_hints:['C-THREE'],
    category:'BLOCKED_CONFLICT',
    reason:'EXACT_RESERVATION_LINE_CONFLICT',
    target_customer_id:'26000003',
    evidence:['reservation_id_exact'],
    conflicts:['target_line_conflict']
  },
  {
    line_id_hash:hash(line4),
    line_user_id_present:true,
    message_rows:1,
    customer_id_hints:[],
    legacy_customer_id_hints:['C-FOUR'],
    category:'REVIEW_REQUIRED',
    reason:'WEAK_OR_INCOMPLETE_EVIDENCE',
    target_customer_id:'26000004',
    evidence:['name_only_unique'],
    conflicts:[]
  },
  {
    line_id_hash:'',
    line_user_id_present:false,
    message_rows:1,
    customer_id_hints:[],
    legacy_customer_id_hints:['C-NO-LINE'],
    category:'UNRESOLVED',
    reason:'NO_SAFE_IDENTITY_EVIDENCE',
    target_customer_id:'',
    evidence:[],
    conflicts:[]
  }
];

const triage={candidate_message_rows:12,classifications};
const queue=buildLineHistoryOwnerReviewQueue(triage);
const qByTarget=new Map();
for(const row of classifications){
  const q=buildLineHistoryOwnerReviewQueue({classifications:[row]}).items[0];
  if(q)qByTarget.set(row.target_customer_id||'NO_TARGET',q.queue_id);
}

const customerMaster=[
  {customer_id:'C-ONE',line_user_id:line1},
  {customer_id:'C-TWO',line_user_id:line2},
  {customer_id:'C-THREE',line_user_id:line3},
  {customer_id:'C-FOUR',line_user_id:line4}
];

const customers=[
  {customer_id:'26000001',line_user_id:'',deleted_at:''},
  {customer_id:'26000002',line_user_id:line2,deleted_at:''},
  {customer_id:'26000003',line_user_id:'U99999999999999999999999999999999',deleted_at:''},
  {customer_id:'26000004',line_user_id:'',deleted_at:''}
];

const decisions={
  planner:'line_history_owner_review_decisions_v1',
  decisions:[
    {queue_id:qByTarget.get('26000001'),decision:'SAME_PERSON'},
    {queue_id:qByTarget.get('26000002'),decision:'SAME_PERSON'},
    {queue_id:qByTarget.get('26000003'),decision:'SAME_PERSON'},
    {queue_id:qByTarget.get('26000004'),decision:'DIFFERENT_PERSON'},
    {queue_id:qByTarget.get('NO_TARGET'),decision:'NEEDS_MORE_EVIDENCE'}
  ]
};

const plan=buildLineHistoryOwnerDecisionPlan({
  triage,decisions,customerMaster,customers
});

assert.equal(plan.planner,'line_history_owner_decision_plan_v1');
assert.equal(plan.review_queue_groups,5);
assert.equal(plan.submitted_decisions,5);
assert.equal(plan.undecided_groups,0);
assert.equal(plan.decision_summary.SAME_PERSON,3);
assert.equal(plan.decision_summary.DIFFERENT_PERSON,1);
assert.equal(plan.decision_summary.NEEDS_MORE_EVIDENCE,1);
assert.equal(plan.proposed_write_actions,1);
assert.equal(plan.accepted_no_write_decisions,2);
assert.equal(plan.validation_error_count,1);
assert.equal(plan.ready_for_readonly_backfill_preview,false);
assert.equal(plan.ready_for_separate_write_authorization,false);
assert.equal(plan.authorization_granted,false);

assert.deepEqual(plan.proposed_actions[0],{
  queue_id:qByTarget.get('26000001'),
  action:'BACKFILL_LINE_HISTORY_TO_EXISTING_CUSTOMER',
  target_customer_id:'26000001',
  line_user_id:line1,
  source_line_id_hash:hash(line1),
  message_rows:5,
  target_line_state:'EMPTY',
  source:'owner_review_same_person'
});

assert.ok(plan.proposed_actions.some(x=>
  x.queue_id===qByTarget.get('26000002')&&
  x.action==='BACKFILL_LINE_HISTORY_TO_EXISTING_CUSTOMER'&&
  x.target_line_state==='EXACT'&&
  x.message_rows===3
));
assert.ok(plan.no_write_decisions.some(x=>
  x.queue_id===qByTarget.get('26000004')&&x.decision==='DIFFERENT_PERSON'
));
assert.ok(plan.no_write_decisions.some(x=>
  x.queue_id===qByTarget.get('NO_TARGET')&&x.decision==='NEEDS_MORE_EVIDENCE'
));
assert.ok(plan.validation_errors.some(x=>
  x.queue_id===qByTarget.get('26000003')&&
  x.code==='SAME_PERSON_TARGET_LINE_CONFLICT'
));

const validPlan=buildLineHistoryOwnerDecisionPlan({
  triage,
  decisions:{decisions:[
    {queue_id:qByTarget.get('26000001'),decision:'SAME_PERSON'},
    {queue_id:qByTarget.get('26000004'),decision:'DEFERRED'}
  ]},
  customerMaster,
  customers
});
assert.equal(validPlan.proposed_write_actions,1);
assert.equal(validPlan.validation_error_count,0);
assert.equal(validPlan.ready_for_readonly_backfill_preview,true);
assert.equal(validPlan.ready_for_separate_write_authorization,false);
assert.equal(validPlan.authorization_granted,false);
assert.equal(validPlan.safety.production_d1_write,0);
assert.equal(validPlan.safety.generated_sql,false);
assert.equal(validPlan.safety.executed_sql,false);

const duplicatePlan=buildLineHistoryOwnerDecisionPlan({
  triage,
  decisions:{decisions:[
    {queue_id:qByTarget.get('26000001'),decision:'SAME_PERSON'},
    {queue_id:qByTarget.get('26000001'),decision:'DEFERRED'}
  ]},
  customerMaster,
  customers
});
assert.ok(duplicatePlan.validation_errors.some(x=>x.code==='DUPLICATE_QUEUE_DECISION'));

const unknownPlan=buildLineHistoryOwnerDecisionPlan({
  triage,
  decisions:{decisions:[{queue_id:'unknown-queue-id',decision:'SAME_PERSON'}]},
  customerMaster,
  customers
});
assert.ok(unknownPlan.validation_errors.some(x=>x.code==='QUEUE_ID_NOT_FOUND'));

const cli=fs.readFileSync('scripts/plan-line-history-owner-decisions.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/https?:\/\//i);
assert.doesNotMatch(cli,/INSERT\s+INTO/i);
assert.doesNotMatch(cli,/UPDATE\s+customers/i);
assert.match(cli,/AUTHORIZATION_GRANTED=NO/);
assert.match(cli,/SQL_GENERATED=0/);
assert.match(cli,/SQL_EXECUTED=0/);
assert.match(cli,/PRODUCTION_D1_WRITE=0/);
assert.match(cli,/PRIVATE_ACTION_VALUES_PRINTED_TO_TERMINAL=0/);

console.log('LINE_HISTORY_OWNER_DECISION_PLAN_SAFE_LINK=PASS');
console.log('LINE_HISTORY_OWNER_DECISION_PLAN_EXISTING_LINE_STILL_BACKFILLS_MESSAGES=PASS');
console.log('LINE_HISTORY_OWNER_DECISION_PLAN_CONFLICT_BLOCK=PASS');
console.log('LINE_HISTORY_OWNER_DECISION_PLAN_NON_WRITE_DECISIONS=PASS');
console.log('LINE_HISTORY_OWNER_DECISION_PLAN_DUPLICATE_UNKNOWN_BLOCK=PASS');
console.log('LINE_HISTORY_OWNER_DECISION_PLAN_READONLY_PREVIEW_GATE=PASS');
console.log('SQL_GENERATED=0');
console.log('SQL_EXECUTED=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
