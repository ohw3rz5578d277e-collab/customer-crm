import assert from 'node:assert/strict';
import {planProspectCustomerPromotion} from '../src/member-prospect-customer-promotion-plan.mjs';
const base={
 prospect_id:'PID_abcdefghijklmnopqrstuvwxyz123456',member_identity_id:'MID_member',
 persisted_prospect_id:'PID_abcdefghijklmnopqrstuvwxyz123456',persisted_member_identity_id:'MID_member',
 canonical_customer_id:'12345678',customer_id_source:'customer_crm',existing_customer_member_binding_count:0,
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
assert.equal(planProspectCustomerPromotion({...base,persisted_member_identity_id:'MID_other'}).status,'binding_mismatch');
assert.equal(planProspectCustomerPromotion({...base,existing_customer_member_binding_count:1}).status,'customer_binding_collision');
assert.equal(planProspectCustomerPromotion({...base,benefit_state_preserved:'true'}).status,'continuity_not_verified');
console.log('PROSPECT_CUSTOMER_PROMOTION_PLAN=PASS');
console.log('CUSTOMER_ID_SOURCE=CRM_ONLY');
console.log('FUZZY_MERGE=0');
console.log('MEMBER_IDENTITY_PRESERVED=YES');
console.log('PROMOTION_EXECUTION=0');
