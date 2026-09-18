import fs from 'node:fs';
import assert from 'node:assert/strict';
import { classifyLineHistoryUnresolved } from '../src/crm-line-history-unresolved-classifier.mjs';

const line=n=>'U'+String(n).padStart(32,'a').slice(-32);
const L1=line(1),L2=line(2),L3=line(3),L4=line(4),L5=line(5),L6=line(6),L7=line(7);

const candidates=[
  {message_key:'a1',line_user_id:L1,customer_id_hint:'26000001',message_text:'secret-a'},
  {message_key:'a2',line_user_id:L1,customer_id_hint:'26000001',message_text:'secret-a2'},
  {message_key:'b1',line_user_id:L2,customer_id_hint:'26000002',message_text:'secret-b'},
  {message_key:'c1',line_user_id:L3,legacy_customer_id_hint:'C-OLD-3',message_text:'secret-c'},
  {message_key:'d1',line_user_id:L4,legacy_customer_id_hint:'C-OLD-4',message_text:'secret-d'},
  {message_key:'e1',line_user_id:L5,legacy_customer_id_hint:'C-OLD-5',message_text:'secret-e'},
  {message_key:'f1',line_user_id:L6,legacy_customer_id_hint:'C-OLD-6',message_text:'secret-f'},
  {message_key:'g1',line_user_id:L7,customer_id_hint:'26000007',message_text:'secret-g'}
];

const customers=[
  {customer_id:'26000001',line_user_id:L1,name:'Direct'},
  {customer_id:'26000002',line_user_id:'',name:'Master Exact'},
  {customer_id:'26000003',line_user_id:'',name:'Reviewed'},
  {customer_id:'26000004',line_user_id:'',name:'Pending'},
  {customer_id:'26000005',line_user_id:'',name:'Name Only',phone:'09011112222'},
  {customer_id:'26000007',line_user_id:line(99),name:'Conflict'}
];

const customerMaster=[
  {customer_id:'26000002',line_user_id:L2,name:'Master Exact'},
  {customer_id:'26000007',line_user_id:L7,name:'Conflict'}
];

const reviews=[
  {reservation_customer_id:'C-OLD-3',crm_candidate_customer_id:'26000003',decision:'SAME_PERSON'},
  {reservation_customer_id:'C-OLD-4',crm_candidate_customer_id:'26000004',decision:'UNREVIEWED'}
];

const reservationIdentities=[
  {customer_id:'C-OLD-5',customer_name:'Name Only'},
  {customer_id:'C-OLD-6',customer_name:'No Match'}
];

const result=classifyLineHistoryUnresolved({candidates,customers,customerMaster,reviews,reservationIdentities});

assert.equal(result.candidate_message_rows,8);
assert.equal(result.identity_groups,7);
assert.equal(result.already_resolved_groups,1);
assert.equal(result.already_resolved_message_rows,2);
assert.equal(result.unresolved_identity_groups,6);
assert.equal(result.unresolved_message_rows,6);
assert.equal(result.auto_confirmable_groups,2);
assert.equal(result.auto_confirmable_message_rows,2);
assert.equal(result.review_required_groups,2);
assert.equal(result.review_required_message_rows,2);
assert.equal(result.unresolved_groups,1);
assert.equal(result.fully_unresolved_message_rows,1);
assert.equal(result.blocked_conflict_groups,1);
assert.equal(result.blocked_conflict_message_rows,1);

const byReason=new Map(result.classifications.map(x=>[x.reason,x]));
assert.equal(byReason.get('PRODUCTION_EXACT_LINE').target_customer_id,'26000001');
assert.equal(byReason.get('MASTER_EXACT_LINE_TO_EXISTING_CURRENT_ID').target_customer_id,'26000002');
assert.equal(byReason.get('EXPLICIT_SAME_PERSON_REVIEW').target_customer_id,'26000003');
assert.equal(byReason.get('EXISTING_REVIEW_NOT_CONFIRMED').category,'REVIEW_REQUIRED');
assert.equal(byReason.get('EXISTING_REVIEW_NOT_CONFIRMED').target_customer_id,'26000004');
assert.equal(byReason.get('WEAK_OR_INCOMPLETE_EVIDENCE').category,'REVIEW_REQUIRED');
assert.equal(byReason.get('NO_SAFE_IDENTITY_EVIDENCE').category,'UNRESOLVED');
assert.equal(byReason.get('MASTER_LINE_CONFLICTS_WITH_PRODUCTION_LINE').category,'BLOCKED_CONFLICT');

const serialized=JSON.stringify(result);
for(const secret of ['secret-a','secret-a2','secret-b','secret-c','secret-d','secret-e','secret-f','secret-g']){
  assert.ok(!serialized.includes(secret),'message text leaked: '+secret);
}
for(const rawLine of [L1,L2,L3,L4,L5,L6,L7]){
  assert.ok(!serialized.includes(rawLine),'raw LINE User ID leaked');
}
assert.equal(result.safety.production_d1_write,0);
assert.equal(result.safety.customer_id_generation,0);
assert.equal(result.safety.customer_update,0);
assert.equal(result.safety.customer_delete,0);
assert.equal(result.safety.line_send,0);
assert.equal(result.safety.name_only_auto_link,false);
assert.equal(result.safety.output_contains_message_text,false);

const exactLines={
  safeEmpty:line(8),
  safeSame:line(9),
  lineConflict:line(10),
  multiple:line(11),
  missing:line(12),
  differentReview:line(14)
};

const exactCandidates=[
  {message_key:'x8',line_user_id:exactLines.safeEmpty,legacy_customer_id_hint:'C-RES-8',message_text:'exact-secret-8'},
  {message_key:'x9',line_user_id:exactLines.safeSame,legacy_customer_id_hint:'C-RES-9',message_text:'exact-secret-9'},
  {message_key:'x10',line_user_id:exactLines.lineConflict,legacy_customer_id_hint:'C-RES-10',message_text:'exact-secret-10'},
  {message_key:'x11',line_user_id:exactLines.multiple,legacy_customer_id_hint:'C-RES-11',message_text:'exact-secret-11'},
  {message_key:'x12',line_user_id:exactLines.missing,legacy_customer_id_hint:'C-RES-12',message_text:'exact-secret-12'},
  {message_key:'x13',line_user_id:'',legacy_customer_id_hint:'C-RES-13',message_text:'exact-secret-13'},
  {message_key:'x14',line_user_id:exactLines.differentReview,legacy_customer_id_hint:'C-RES-14',message_text:'exact-secret-14'}
];

const exactCustomers=[
  {customer_id:'26000008',line_user_id:'',name:'Exact Empty'},
  {customer_id:'26000009',line_user_id:exactLines.safeSame,name:'Exact Same'},
  {customer_id:'26000010',line_user_id:line(99),name:'Exact Conflict'},
  {customer_id:'26000011',line_user_id:'',name:'Exact Multi A'},
  {customer_id:'26000012',line_user_id:'',name:'Exact Multi B'},
  {customer_id:'26000013',line_user_id:'',name:'Exact No Line'},
  {customer_id:'26000014',line_user_id:'',name:'Exact Review Block'}
];

const exactReviews=[
  {reservation_customer_id:'C-RES-14',crm_candidate_customer_id:'26000014',decision:'DIFFERENT_PERSON'}
];

const exactReservationEvidence=[
  {source_customer_id:'C-RES-8',reservation_id:'R-EXACT-8',target_customer_id:'26000008'},
  {source_customer_id:'C-RES-9',reservation_id:'R-EXACT-9',target_customer_id:'26000009'},
  {source_customer_id:'C-RES-10',reservation_id:'R-EXACT-10',target_customer_id:'26000010'},
  {source_customer_id:'C-RES-11',reservation_id:'R-EXACT-11A',target_customer_id:'26000011'},
  {source_customer_id:'C-RES-11',reservation_id:'R-EXACT-11B',target_customer_id:'26000012'},
  {source_customer_id:'C-RES-12',reservation_id:'R-EXACT-12',target_customer_id:'26009999'},
  {source_customer_id:'C-RES-13',reservation_id:'R-EXACT-13',target_customer_id:'26000013'},
  {source_customer_id:'C-RES-14',reservation_id:'R-EXACT-14',target_customer_id:'26000014'}
];

const exactResult=classifyLineHistoryUnresolved({
  candidates:exactCandidates,
  customers:exactCustomers,
  reviews:exactReviews,
  exactReservationEvidence
});

assert.equal(exactResult.planner,'line_history_unresolved_triage_v2');
assert.equal(exactResult.already_resolved_groups,1);
assert.equal(exactResult.auto_confirmable_groups,1);
assert.equal(exactResult.review_required_groups,2);
assert.equal(exactResult.blocked_conflict_groups,3);
assert.equal(exactResult.unresolved_groups,0);

const byLegacyHint=hint=>exactResult.classifications.find(x=>x.legacy_customer_id_hints.includes(hint));

const safeEmpty=byLegacyHint('C-RES-8');
assert.equal(safeEmpty.category,'AUTO_CONFIRMABLE');
assert.equal(safeEmpty.reason,'EXACT_RESERVATION_ID_UNIQUE_CURRENT_TARGET');
assert.equal(safeEmpty.target_customer_id,'26000008');
assert.ok(safeEmpty.evidence.includes('production_line_id_empty'));

const safeSame=byLegacyHint('C-RES-9');
assert.equal(safeSame.category,'ALREADY_RESOLVED');
assert.equal(safeSame.reason,'PRODUCTION_EXACT_LINE');
assert.equal(safeSame.target_customer_id,'26000009');

const lineConflict=byLegacyHint('C-RES-10');
assert.equal(lineConflict.category,'BLOCKED_CONFLICT');
assert.equal(lineConflict.reason,'EXACT_RESERVATION_LINE_CONFLICT');

const multiConflict=byLegacyHint('C-RES-11');
assert.equal(multiConflict.category,'BLOCKED_CONFLICT');
assert.equal(multiConflict.reason,'MULTIPLE_EXACT_RESERVATION_TARGETS');

const missingTarget=byLegacyHint('C-RES-12');
assert.equal(missingTarget.category,'REVIEW_REQUIRED');
assert.equal(missingTarget.reason,'EXACT_RESERVATION_TARGET_NOT_CURRENT');

const noLine=byLegacyHint('C-RES-13');
assert.equal(noLine.category,'REVIEW_REQUIRED');
assert.equal(noLine.reason,'EXACT_RESERVATION_REQUIRES_LINE_ID');

const differentReview=byLegacyHint('C-RES-14');
assert.equal(differentReview.category,'BLOCKED_CONFLICT');
assert.equal(differentReview.reason,'EXPLICIT_DIFFERENT_PERSON_REVIEW');

const exactSerialized=JSON.stringify(exactResult);
for(const secret of exactCandidates.map(x=>x.message_text)){
  assert.ok(!exactSerialized.includes(secret),'exact reservation message text leaked');
}
for(const rawLine of Object.values(exactLines)){
  assert.ok(!exactSerialized.includes(rawLine),'exact reservation raw LINE User ID leaked');
}
for(const reservationId of exactReservationEvidence.map(x=>x.reservation_id)){
  assert.ok(!exactSerialized.includes(reservationId),'reservation ID leaked');
}
assert.equal(exactResult.safety.production_d1_write,0);
assert.equal(exactResult.safety.customer_id_generation,0);
assert.equal(exactResult.safety.customer_update,0);
assert.equal(exactResult.safety.customer_delete,0);
assert.equal(exactResult.safety.line_send,0);
assert.equal(exactResult.safety.name_only_auto_link,false);

const cli=fs.readFileSync('scripts/classify-line-history-unresolved.mjs','utf8');
assert.match(cli,/--exact-reservation-evidence/);

const runner=fs.readFileSync('scripts/run-line-history-unresolved-readonly.sh','utf8');
assert.match(runner,/D1_ACTION=READ_ONLY_CUSTOMERS/);
assert.match(runner,/D1_ACTION=READ_ONLY_RECONCILIATION_REVIEWS/);
assert.match(runner,/EXACT_RESERVATION_EVIDENCE=LOCAL_FILE_ENABLED/);
assert.match(runner,/--exact-reservation-evidence/);
assert.match(runner,/--command=/);
const runnerSql=[...runner.matchAll(/(?:CUSTOMERS|REVIEWS)_SQL="([^"]*)"/g)].map(x=>x[1]);
assert.equal(runnerSql.length,2);
for(const sql of runnerSql){
  assert.match(sql,/^SELECT\b/i);
  assert.doesNotMatch(sql,/\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE)\b/i);
}
assert.doesNotMatch(runner,/wrangler\s+deploy/i);
assert.doesNotMatch(runner,/api\.line\.me/i);

console.log('LINE_HISTORY_UNRESOLVED_GROUPING=PASS');
console.log('LINE_HISTORY_UNRESOLVED_EXPLICIT_REVIEW=PASS');
console.log('LINE_HISTORY_UNRESOLVED_MASTER_EXACT_LINE=PASS');
console.log('LINE_HISTORY_UNRESOLVED_NAME_ONLY_NOT_AUTO=PASS');
console.log('LINE_HISTORY_UNRESOLVED_CONFLICT_BLOCK=PASS');
console.log('LINE_HISTORY_UNRESOLVED_PRIVACY=PASS');
console.log('LINE_HISTORY_UNRESOLVED_READONLY_RUNNER=PASS');
console.log('LINE_HISTORY_UNRESOLVED_EXACT_EVIDENCE_RUNNER=PASS');
console.log('LINE_HISTORY_EXACT_RESERVATION_SAFE_EMPTY_LINE=PASS');
console.log('LINE_HISTORY_EXACT_RESERVATION_SAFE_SAME_LINE=PASS');
console.log('LINE_HISTORY_EXACT_RESERVATION_CONFLICT_BLOCK=PASS');
console.log('LINE_HISTORY_EXACT_RESERVATION_REVIEW_GUARDS=PASS');
console.log('LINE_HISTORY_EXACT_RESERVATION_PRIVACY=PASS');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('LINE_SEND=0');
