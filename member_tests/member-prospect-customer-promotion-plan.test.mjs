import assert from 'node:assert/strict';
import {planProspectCustomerPromotion} from '../src/member-prospect-customer-promotion-plan.mjs';
const base={
 prospect_id:'PID_abcdefghijklmnopqrstuvwxyz123456',member_identity_id:'MID_abcdefghijklmnopqrstuvwxyz123456',
 persisted_prospect_id:'PID_abcdefghijklmnopqrstuvwxyz123456',persisted_member_identity_id:'MID_abcdefghijklmnopqrstuvwxyz123456',
 canonical_customer_id:'12345678',customer_id_source:'customer_crm',existing_customer_member_binding_count:0,persisted_binding_count_customer_id:'12345678',
 prospect_status:'prospect',consent_history_preserved:true,acquisition_history_preserved:true,benefit_state_preserved:true
};
let p=planProspectCustomerPromotion(base);
assert.equal(p.status,'ready');
assert.equal(p.preserve_member_identity,true);
assert.equal(p.fuzzy_match_used,false);
assert.equal(p.customer_id_generation,false);
assert.equal(p.automatic_merge,false);
assert.equal(p.promotion_allowed,false);
assert.equal(planProspectCustomerPromotion({...base,customer_id_source:'member'}).status,'invalid_customer_id_source');
assert.equal(planProspectCustomerPromotion({...base,persisted_member_identity_id:'MID_zyxwvutsrqponmlkjihgfedcba654321'}).status,'binding_mismatch');
assert.equal(planProspectCustomerPromotion({...base,persisted_binding_count_customer_id:'87654321'}).status,'binding_count_customer_not_verified');
assert.equal(planProspectCustomerPromotion({...base,existing_customer_member_binding_count:1}).status,'customer_binding_collision');
assert.equal(planProspectCustomerPromotion({...base,benefit_state_preserved:'true'}).status,'continuity_not_verified');
const {existing_customer_member_binding_count,...missingCount}=base;
assert.equal(planProspectCustomerPromotion(missingCount).status,'missing_persisted_evidence');
const {prospect_status,...missingStatus}=base;
assert.equal(planProspectCustomerPromotion(missingStatus).status,'missing_persisted_evidence');
for(const missing of [null,'','   ',false]) assert.equal(planProspectCustomerPromotion({...base,existing_customer_member_binding_count:missing}).status,'missing_persisted_evidence');
for(const malformed of [['0'],{value:0},0n]) assert.equal(planProspectCustomerPromotion({...base,existing_customer_member_binding_count:malformed}).status,'invalid_binding_count');
assert.equal(planProspectCustomerPromotion({...base,existing_customer_member_binding_count:'0'}).status,'ready');
assert.equal(planProspectCustomerPromotion({...base,existing_customer_member_binding_count:'9'.repeat(400)}).status,'invalid_binding_count');
assert.equal(planProspectCustomerPromotion({...base,member_identity_id:'x',persisted_member_identity_id:'x'}).status,'invalid_identity');
console.log('PROSPECT_CUSTOMER_PROMOTION_PLAN=PASS');
console.log('CUSTOMER_ID_SOURCE=CRM_ONLY');
console.log('FUZZY_MERGE=0');
console.log('MEMBER_IDENTITY_PRESERVED=YES');
console.log('PROMOTION_EXECUTION=0');
