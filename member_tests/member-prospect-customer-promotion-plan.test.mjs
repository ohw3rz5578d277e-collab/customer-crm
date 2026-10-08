import assert from 'node:assert/strict';
import {planProspectCustomerPromotion,createProspectPromotionEventId} from '../src/member-prospect-customer-promotion-plan.mjs';

const member='MID_abcdefghijklmnopqrstuvwxyz123456';
const otherMember='MID_zyxwvutsrqponmlkjihgfedcba654321';
const prospect='PID_abcdefghijklmnopqrstuvwxyz123456';
const otherProspect='PID_zyxwvutsrqponmlkjihgfedcba654321';
const customer='12345678';
const otherCustomer='87654321';
const entitlement='BEN_abcdefghijklmnopqrstuvwxyz123456';

const base={
  prospect_id:prospect,
  member_identity_id:member,
  canonical_customer_id:customer,
  customer_id_source:'customer_crm',

  // Stable Member <-> Prospect identity evidence.
  persisted_prospect_id:prospect,
  persisted_member_identity_id:member,
  member_identity_record_verified:true,
  persisted_member_identity_record_id:member,
  persisted_member_identity_status:'active',
  persisted_member_identity_prospect_id:prospect,
  persisted_member_identity_customer_id:null,
  prospect_record_verified:true,
  persisted_prospect_record_id:prospect,
  persisted_prospect_member_identity_id:member,
  persisted_promoted_customer_id:null,
  member_prospect_binding_verified:true,
  active_member_prospect_binding_count:1,
  persisted_member_prospect_binding_count_prospect_id:prospect,

  // Exact canonical Customer CRM evidence.
  customer_crm_record_verified:true,
  persisted_crm_customer_id:customer,
  persisted_crm_customer_id_source:'customer_crm',
  canonical_customer_match_count:1,
  persisted_customer_match_count_customer_id:customer,
  crm_promotion_trigger_verified:true,
  persisted_trigger_prospect_id:prospect,
  persisted_trigger_member_identity_id:member,
  persisted_trigger_customer_id:customer,

  // Current promotable state.
  prospect_status:'prospect',
  prospect_status_binding_verified:true,
  persisted_status_prospect_id:prospect,
  persisted_status_member_identity_id:member,
  persisted_prospect_status:'prospect',
  existing_customer_member_binding_count:0,
  persisted_binding_count_customer_id:customer,

  // Deterministic append-only promotion event scope.
  existing_promotion_event_count:0,
  persisted_promotion_event_count_prospect_id:prospect,
  persisted_promotion_event_count_member_identity_id:member,
  persisted_promotion_event_count_customer_id:customer,

  // Durable continuity evidence.
  consent_history_verified:true,
  persisted_consent_member_identity_id:member,
  persisted_consent_prospect_id:prospect,
  acquisition_history_verified:true,
  persisted_acquisition_member_identity_id:member,
  persisted_acquisition_prospect_id:prospect,
  existing_signup_benefit_count:0,
  persisted_benefit_count_member_identity_id:member,
  persisted_benefit_count_prospect_id:prospect
};

const promotionId=createProspectPromotionEventId({prospect_id:prospect,member_identity_id:member,canonical_customer_id:customer});
assert.equal(promotionId,createProspectPromotionEventId({prospect_id:prospect,member_identity_id:member,canonical_customer_id:customer}));
assert.match(promotionId,/^PROM_[0-9a-f]{64}$/);
assert.equal(createProspectPromotionEventId({prospect_id:` ${prospect}`,member_identity_id:member,canonical_customer_id:customer}),null);

let p=planProspectCustomerPromotion(base);
assert.equal(p.status,'ready');
assert.equal(p.ready,true);
assert.equal(p.promotion_event_id,promotionId);
assert.equal(p.preserve_member_identity,true);
assert.equal(p.preserve_prospect_reference,true);
assert.equal(p.preserve_consent_history,true);
assert.equal(p.preserve_acquisition_history,true);
assert.equal(p.preserve_benefit_state,true);
assert.equal(p.benefit_carry_forward,null);
assert.deepEqual(p.transaction_operations,['bind_member_to_existing_crm_customer','mark_prospect_promoted','append_promotion_event','preserve_history_links']);
assert.equal(p.transaction_required,true);
assert.equal(p.partial_commit_allowed,false);
assert.equal(p.rollback_on_failure,true);
assert.equal(p.fuzzy_match_used,false);
assert.equal(p.customer_id_generation,false);
assert.equal(p.family_id_generation,false);
assert.equal(p.family_mutation,false);
assert.equal(p.customer_master_mutation,false);
assert.equal(p.history_rewrite,false);
assert.equal(p.automatic_merge,false);
assert.equal(p.promotion_allowed,false);
assert.equal(p.write_allowed,false);
assert.equal(p.execute,false);
assert.equal(p.production_write_authorized,false);

// Identity and canonical Customer evidence fail closed.
assert.equal(planProspectCustomerPromotion({...base,customer_id_source:'member'}).status,'invalid_customer_id_source');
assert.equal(planProspectCustomerPromotion({...base,persisted_member_identity_id:otherMember}).status,'binding_mismatch');
assert.equal(planProspectCustomerPromotion({...base,persisted_prospect_id:otherProspect}).status,'binding_mismatch');
assert.equal(planProspectCustomerPromotion({...base,member_identity_record_verified:false}).status,'member_identity_record_not_verified');
assert.equal(planProspectCustomerPromotion({...base,persisted_member_identity_status:'disabled'}).status,'member_identity_record_not_verified');
assert.equal(planProspectCustomerPromotion({...base,prospect_record_verified:false}).status,'prospect_record_not_verified');
assert.equal(planProspectCustomerPromotion({...base,persisted_prospect_member_identity_id:otherMember}).status,'prospect_record_not_verified');
assert.equal(planProspectCustomerPromotion({...base,member_prospect_binding_verified:false}).status,'member_prospect_binding_not_verified');
assert.equal(planProspectCustomerPromotion({...base,active_member_prospect_binding_count:0}).status,'ambiguous_member_prospect_binding');
assert.equal(planProspectCustomerPromotion({...base,persisted_member_prospect_binding_count_prospect_id:otherProspect}).status,'member_prospect_binding_count_scope_mismatch');
assert.equal(planProspectCustomerPromotion({...base,active_member_prospect_binding_count:'1'}).status,'invalid_member_prospect_binding_count');
assert.equal(planProspectCustomerPromotion({...base,customer_crm_record_verified:false}).status,'crm_customer_not_verified');
assert.equal(planProspectCustomerPromotion({...base,persisted_crm_customer_id:otherCustomer}).status,'crm_customer_not_verified');
assert.equal(planProspectCustomerPromotion({...base,canonical_customer_match_count:0}).status,'ambiguous_canonical_customer');
assert.equal(planProspectCustomerPromotion({...base,canonical_customer_match_count:2}).status,'ambiguous_canonical_customer');
assert.equal(planProspectCustomerPromotion({...base,canonical_customer_match_count:'1'}).status,'invalid_canonical_customer_match_count');
assert.equal(planProspectCustomerPromotion({...base,persisted_customer_match_count_customer_id:otherCustomer}).status,'canonical_customer_match_count_scope_mismatch');
assert.equal(planProspectCustomerPromotion({...base,persisted_trigger_customer_id:otherCustomer}).status,'crm_promotion_trigger_not_verified');
assert.equal(planProspectCustomerPromotion({...base,persisted_trigger_member_identity_id:otherMember}).status,'crm_promotion_trigger_not_verified');

// Current state must be genuinely unpromoted and collision-free.
assert.equal(planProspectCustomerPromotion({...base,persisted_status_member_identity_id:otherMember}).status,'prospect_status_binding_not_verified');
assert.equal(planProspectCustomerPromotion({...base,persisted_member_identity_customer_id:otherCustomer}).status,'member_already_customer_bound');
assert.equal(planProspectCustomerPromotion({...base,persisted_promoted_customer_id:otherCustomer}).status,'prospect_already_promoted');
const {persisted_member_identity_customer_id,...missingMemberCustomerState}=base;
assert.equal(planProspectCustomerPromotion(missingMemberCustomerState).status,'missing_member_customer_state_evidence');
const {persisted_promoted_customer_id,...missingPromotedState}=base;
assert.equal(planProspectCustomerPromotion(missingPromotedState).status,'missing_promoted_customer_state_evidence');
assert.equal(planProspectCustomerPromotion({...base,persisted_binding_count_customer_id:otherCustomer}).status,'customer_member_binding_count_scope_mismatch');
assert.equal(planProspectCustomerPromotion({...base,existing_customer_member_binding_count:1}).status,'customer_binding_collision');
assert.equal(planProspectCustomerPromotion({...base,existing_customer_member_binding_count:'0'}).status,'invalid_customer_member_binding_count');

// Historical continuity survives without rewriting/reissuing state.
assert.equal(planProspectCustomerPromotion({...base,consent_history_verified:false}).status,'consent_history_not_verified');
assert.equal(planProspectCustomerPromotion({...base,persisted_consent_prospect_id:otherProspect}).status,'consent_history_not_verified');
assert.equal(planProspectCustomerPromotion({...base,acquisition_history_verified:false}).status,'acquisition_history_not_verified');
assert.equal(planProspectCustomerPromotion({...base,persisted_acquisition_member_identity_id:otherMember}).status,'acquisition_history_not_verified');
assert.equal(planProspectCustomerPromotion({...base,existing_signup_benefit_count:2}).status,'signup_benefit_collision');
assert.equal(planProspectCustomerPromotion({...base,existing_signup_benefit_count:'0'}).status,'invalid_signup_benefit_count');
assert.equal(planProspectCustomerPromotion({...base,persisted_benefit_count_prospect_id:otherProspect}).status,'signup_benefit_count_scope_mismatch');

p=planProspectCustomerPromotion({
  ...base,
  existing_signup_benefit_count:1,
  benefit_state_verified:true,
  persisted_benefit_entitlement_id:entitlement,
  persisted_benefit_member_identity_id:member,
  persisted_benefit_prospect_id:prospect,
  persisted_benefit_state:'reserved'
});
assert.equal(p.status,'ready');
assert.deepEqual(p.benefit_carry_forward,{
  entitlement_id:entitlement,state:'reserved',member_identity_id:member,prospect_id:prospect,canonical_customer_id:customer,state_reset:false,reissue:false
});
assert.equal(planProspectCustomerPromotion({...base,existing_signup_benefit_count:1}).status,'signup_benefit_state_not_verified');
assert.equal(planProspectCustomerPromotion({...base,existing_signup_benefit_count:1,benefit_state_verified:true,persisted_benefit_entitlement_id:entitlement,persisted_benefit_member_identity_id:otherMember,persisted_benefit_prospect_id:prospect,persisted_benefit_state:'available'}).status,'signup_benefit_state_not_verified');

// Exact completed replay is idempotent: no second promotion is planned.
const replay={
  ...base,
  prospect_status:'promoted',
  persisted_prospect_status:'promoted',
  persisted_member_identity_customer_id:customer,
  persisted_promoted_customer_id:customer,
  existing_customer_member_binding_count:1,
  customer_member_binding_verified:true,
  persisted_bound_customer_id:customer,
  persisted_bound_member_identity_id:member,
  existing_promotion_event_count:1,
  existing_promotion_event_verified:true,
  persisted_promotion_event_id:promotionId,
  persisted_promotion_event_prospect_id:prospect,
  persisted_promotion_event_member_identity_id:member,
  persisted_promotion_event_customer_id:customer
};
p=planProspectCustomerPromotion(replay);
assert.equal(p.status,'promotion_already_recorded');
assert.equal(p.idempotent_replay,true);
assert.equal(p.preserve_member_identity,true);
assert.equal(p.preserve_prospect_reference,true);
assert.equal(p.promotion_allowed,false);
assert.equal(planProspectCustomerPromotion({...replay,persisted_bound_member_identity_id:otherMember}).status,'promotion_replay_evidence_mismatch');
assert.equal(planProspectCustomerPromotion({...replay,persisted_promotion_event_customer_id:otherCustomer}).status,'promotion_replay_evidence_mismatch');
assert.equal(planProspectCustomerPromotion({...replay,persisted_member_identity_customer_id:otherCustomer}).status,'promotion_replay_evidence_mismatch');
assert.equal(planProspectCustomerPromotion({...base,existing_promotion_event_count:2}).status,'promotion_event_collision');
assert.equal(planProspectCustomerPromotion({...base,persisted_promotion_event_count_customer_id:otherCustomer}).status,'promotion_event_count_scope_mismatch');
assert.equal(planProspectCustomerPromotion({...base,existing_promotion_event_count:'0'}).status,'invalid_existing_promotion_event_count');

for(const malformed of [null,false,[],{},0n,' 0']){
  assert.notEqual(planProspectCustomerPromotion({...base,existing_promotion_event_count:malformed}).status,'ready');
}
assert.equal(planProspectCustomerPromotion({...base,member_identity_id:['MID_abcdefghijklmnopqrstuvwxyz123456']}).status,'invalid_identity');

console.log('PROSPECT_CUSTOMER_PROMOTION_STEP7=PASS');
console.log('ACTIVE_MEMBER_PROSPECT_BINDING_EXACT=YES');
console.log('CRM_CUSTOMER_EXACT_MATCH_COUNT=1');
console.log('CURRENT_CUSTOMER_BINDING_REQUIRED_NULL=YES');
console.log('PROMOTED_CUSTOMER_REQUIRED_NULL=YES');
console.log('CONSENT_ACQUISITION_CONTINUITY_EXACT=YES');
console.log('SIGNUP_BENEFIT_REISSUE=0');
console.log('PROMOTION_REPLAY_IDEMPOTENT=YES');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('FUZZY_MERGE=0');
console.log('PRODUCTION_WRITE=0');
