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
assert.equal(byReason.get('EXISTING_REVIEW_NOT_CONFIRMED').category,'REVIEW_REQUIRED');\nassert.equal(byReason.get('EXISTING_REVIEW_NOT_CONFIRMED').target_customer_id,'26000004');
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

console.log('LINE_HISTORY_UNRESOLVED_GROUPING=PASS');
console.log('LINE_HISTORY_UNRESOLVED_EXPLICIT_REVIEW=PASS');
console.log('LINE_HISTORY_UNRESOLVED_MASTER_EXACT_LINE=PASS');
console.log('LINE_HISTORY_UNRESOLVED_NAME_ONLY_NOT_AUTO=PASS');
console.log('LINE_HISTORY_UNRESOLVED_CONFLICT_BLOCK=PASS');
console.log('LINE_HISTORY_UNRESOLVED_PRIVACY=PASS');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('LINE_SEND=0');
