import fs from 'node:fs';
import assert from 'node:assert/strict';
import { buildExactReservationEvidence } from '../src/crm-line-history-exact-reservation-evidence.mjs';
import { classifyLineHistoryUnresolved } from '../src/crm-line-history-unresolved-classifier.mjs';

const line=n=>'U'+String(n).padStart(32,'b').slice(-32);
const LA=line(21),LB=line(22),LC=line(23);

const reservationHistory=[
  {source_customer_id:'C-BUILD-A',reservation_id:'R-PRIVATE-1'},
  {source_customer_id:'C-BUILD-A',reservation_id:'R-PRIVATE-2'},
  {source_customer_id:'C-BUILD-B',reservation_id:'R-PRIVATE-3'},
  {source_customer_id:'C-BUILD-C',reservation_id:'R-PRIVATE-4'},
  {source_customer_id:'C-BUILD-NO-MATCH',reservation_id:'R-PRIVATE-5'}
];

const crmReservations=[
  {reservation_id:'R-PRIVATE-1',customer_id:'26000021',line_user_id:line(91)},
  {reservation_id:'R-PRIVATE-2',customer_id:'26000021',line_user_id:line(92)},
  {reservation_id:'R-PRIVATE-3',customer_id:'26000022',line_user_id:line(93)},
  {reservation_id:'R-PRIVATE-3',customer_id:'26000023',line_user_id:line(94)},
  {reservation_id:'R-PRIVATE-4',customer_id:'',line_user_id:line(95)},
  {reservation_id:'R-OTHER',customer_id:'26000024',line_user_id:line(96)}
];

const built=buildExactReservationEvidence({reservationHistory,crmReservations});

assert.equal(built.planner,'line_history_exact_reservation_evidence_v1');
assert.equal(built.reservation_history_rows,5);
assert.equal(built.crm_reservation_rows,6);
assert.equal(built.exact_evidence_rows,5);
assert.equal(built.source_groups_with_exact_match,3);
assert.equal(built.rows.length,5);

for(const row of built.rows){
  assert.match(row.reservation_id_hash,/^[0-9a-f]{24}$/);
  assert.ok(!Object.hasOwn(row,'reservation_id'));
}

const builtSerialized=JSON.stringify(built);
for(const rawReservationId of reservationHistory.map(x=>x.reservation_id)){
  assert.ok(!builtSerialized.includes(rawReservationId),'raw reservation ID leaked');
}
for(const rawLineId of crmReservations.map(x=>x.line_user_id)){
  assert.ok(!builtSerialized.includes(rawLineId),'raw LINE User ID leaked');
}

assert.equal(built.safety.production_d1_read,0);
assert.equal(built.safety.production_d1_write,0);
assert.equal(built.safety.customer_id_generation,0);
assert.equal(built.safety.customer_update,0);
assert.equal(built.safety.customer_delete,0);
assert.equal(built.safety.line_send,0);
assert.equal(built.safety.raw_reservation_id_output,false);
assert.equal(built.safety.raw_line_user_id_output,false);
assert.equal(built.safety.message_text_output,false);

const classification=classifyLineHistoryUnresolved({
  candidates:[
    {message_key:'build-a',line_user_id:LA,legacy_customer_id_hint:'C-BUILD-A',message_text:'private-message-a'},
    {message_key:'build-b',line_user_id:LB,legacy_customer_id_hint:'C-BUILD-B',message_text:'private-message-b'},
    {message_key:'build-c',line_user_id:LC,legacy_customer_id_hint:'C-BUILD-C',message_text:'private-message-c'}
  ],
  customers:[
    {customer_id:'26000021',line_user_id:'',name:'A'},
    {customer_id:'26000022',line_user_id:'',name:'B1'},
    {customer_id:'26000023',line_user_id:'',name:'B2'}
  ],
  exactReservationEvidence:built.rows
});

assert.equal(classification.auto_confirmable_groups,1);
assert.equal(classification.review_required_groups,1);
assert.equal(classification.blocked_conflict_groups,1);
assert.equal(classification.unresolved_groups,0);

const byHint=hint=>classification.classifications.find(x=>x.legacy_customer_id_hints.includes(hint));

const safe=byHint('C-BUILD-A');
assert.equal(safe.category,'AUTO_CONFIRMABLE');
assert.equal(safe.reason,'EXACT_RESERVATION_ID_UNIQUE_CURRENT_TARGET');
assert.equal(safe.target_customer_id,'26000021');

const multiple=byHint('C-BUILD-B');
assert.equal(multiple.category,'BLOCKED_CONFLICT');
assert.equal(multiple.reason,'MULTIPLE_EXACT_RESERVATION_TARGETS');

const missing=byHint('C-BUILD-C');
assert.equal(missing.category,'REVIEW_REQUIRED');
assert.equal(missing.reason,'EXACT_RESERVATION_TARGET_MISSING');

const classificationSerialized=JSON.stringify(classification);
for(const secret of ['private-message-a','private-message-b','private-message-c']){
  assert.ok(!classificationSerialized.includes(secret),'message text leaked');
}
for(const rawLine of [LA,LB,LC]){
  assert.ok(!classificationSerialized.includes(rawLine),'raw candidate LINE ID leaked');
}

const cli=fs.readFileSync('scripts/build-line-history-exact-reservation-evidence.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/https?:\/\//i);
assert.match(cli,/PRODUCTION_D1_READ=0/);
assert.match(cli,/PRODUCTION_D1_WRITE=0/);
assert.match(cli,/RAW_RESERVATION_ID_OUTPUT=0/);

console.log('LINE_HISTORY_EXACT_RESERVATION_BUILDER_JOIN=PASS');
console.log('LINE_HISTORY_EXACT_RESERVATION_BUILDER_HASH_PRIVACY=PASS');
console.log('LINE_HISTORY_EXACT_RESERVATION_BUILDER_MULTI_TARGET_BLOCK=PASS');
console.log('LINE_HISTORY_EXACT_RESERVATION_BUILDER_MISSING_TARGET_REVIEW=PASS');
console.log('LINE_HISTORY_EXACT_RESERVATION_BUILDER_LOCAL_ONLY=PASS');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('LINE_SEND=0');
