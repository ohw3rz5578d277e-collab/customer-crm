import assert from 'node:assert/strict';
import {planSignupBenefitIssue as rawPlanSignupBenefitIssue,planSignupBenefitTransition,planBenefitPromotionCarryForward} from '../src/member-signup-benefit-foundation.mjs';
const member='MID_abcdefghijklmnopqrstuvwxyz123456';
const otherMember='MID_zyxwvutsrqponmlkjihgfedcba654321';
const prospect='PID_abcdefghijklmnopqrstuvwxyz123456';
const entitlement='BEN_abcdefghijklmnopqrstuvwxyz123456';
const planSignupBenefitIssue=(args={})=>rawPlanSignupBenefitIssue({
 persisted_benefit_count_member_identity_id:args.member_identity_id,
 persisted_benefit_count_prospect_id:args.prospect_id,
 registration_completion_binding_verified:true,
 persisted_registration_member_identity_id:args.member_identity_id,
 persisted_registration_prospect_id:args.prospect_id,
 consent_binding_verified:true,
 persisted_consent_member_identity_id:args.member_identity_id,
 persisted_consent_prospect_id:args.prospect_id,
 ...args
});
const planTransition=(args={})=>planSignupBenefitTransition({
 entitlement_id:entitlement,
 persisted_entitlement_id:entitlement,
 entitlement_member_binding_verified:true,
 persisted_entitlement_member_identity_id:args.entitlement_member_identity_id,
 entitlement_state_binding_verified:true,
 persisted_entitlement_state:args.current_state,
 ...args
});
const planReservedTransition=(args={})=>{
 const customer=args.canonical_customer_id??'12345678';
 const reservation=args.reservation_id??'R-RESERVE';
 const auth=args.authenticated_member_identity_id??args.entitlement_member_identity_id;
 return planTransition({
  authenticated_member_identity_id:auth,
  canonical_customer_id:customer,
  reservation_id:reservation,
  reservation_context_binding_verified:true,
  persisted_reservation_context_member_identity_id:auth,
  persisted_reservation_context_customer_id:customer,
  persisted_reservation_context_reservation_id:reservation,
  ...args
 });
};
const planReleasedTransition=(args={})=>{
 const customer=args.canonical_customer_id??'12345678';
 const reservation=args.reservation_id??'R-RELEASE';
 const auth=args.authenticated_member_identity_id??args.entitlement_member_identity_id;
 return planTransition({
  authenticated_member_identity_id:auth,
  canonical_customer_id:customer,
  reservation_id:reservation,
  reserved_entitlement_context_binding_verified:true,
  persisted_reserved_entitlement_id:entitlement,
  persisted_reserved_entitlement_member_identity_id:auth,
  persisted_reserved_entitlement_customer_id:customer,
  persisted_reserved_entitlement_reservation_id:reservation,
  reservation_release_authorized:true,
  release_authorization_binding_verified:true,
  persisted_release_entitlement_id:entitlement,
  persisted_release_member_identity_id:auth,
  persisted_release_customer_id:customer,
  persisted_release_reservation_id:reservation,
  ...args
 });
};
const planUsedTransition=(args={})=>{
 const auth=args.authenticated_member_identity_id;
 const customer=args.canonical_customer_id;
 const reservation=args.reservation_id;
 return planTransition({
  persisted_redemption_entitlement_id:entitlement,
  reserved_entitlement_context_binding_verified:true,
  persisted_reserved_entitlement_id:entitlement,
  persisted_reserved_entitlement_member_identity_id:auth,
  persisted_reserved_entitlement_customer_id:customer,
  persisted_reserved_entitlement_reservation_id:reservation,
  redemption_context_binding_verified:true,
  persisted_redemption_member_identity_id:auth,
  persisted_redemption_customer_id:customer,
  persisted_redemption_reservation_id:reservation,
  ...args
 });
};
const planCarryForward=(args={})=>planBenefitPromotionCarryForward({
 entitlement_id:entitlement,
 entitlement_binding_verified:true,
 persisted_entitlement_id:entitlement,
 persisted_entitlement_member_identity_id:args.member_identity_id,
 persisted_entitlement_state:args.current_state,
 ...args
});
assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,registration_completed:true,consent_current:true,existing_signup_benefit_count:0}).status,'member_prospect_binding_not_verified');
assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,persisted_member_identity_id:member,persisted_prospect_id:'PID_zyxwvutsrqponmlkjihgfedcba654321',member_prospect_binding_verified:true,registration_completed:true,consent_current:true,existing_signup_benefit_count:0}).status,'member_prospect_binding_not_verified');
assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,persisted_member_identity_id:'MID_zyxwvutsrqponmlkjihgfedcba654321',persisted_prospect_id:prospect,member_prospect_binding_verified:true,registration_completed:true,consent_current:true,existing_signup_benefit_count:0}).status,'member_prospect_binding_not_verified');
// BENEFIT_CROSS_MEMBER_BINDING_REGRESSION
let p=planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,member_prospect_binding_verified:true,persisted_member_identity_id:member,persisted_prospect_id:prospect,registration_completed:true,consent_current:true,existing_signup_benefit_count:0});
assert.equal(p.status,'ready');
assert.equal(p.state,'issued');
assert.equal(p.commercial_definition,null);
assert.equal(p.automatic_discount,false);
assert.equal(p.issue_allowed,false);
assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,member_prospect_binding_verified:true,persisted_member_identity_id:member,persisted_prospect_id:prospect,registration_completed:true,consent_current:true,existing_signup_benefit_count:0,persisted_registration_prospect_id:'PID_zyxwvutsrqponmlkjihgfedcba654321'}).status,'registration_completion_binding_not_verified');
assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,member_prospect_binding_verified:true,persisted_member_identity_id:member,persisted_prospect_id:prospect,registration_completed:true,consent_current:true,existing_signup_benefit_count:0,persisted_consent_member_identity_id:otherMember}).status,'consent_binding_not_verified');
assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,member_prospect_binding_verified:true,persisted_member_identity_id:member,persisted_prospect_id:prospect,registration_completed:true,consent_current:true,existing_signup_benefit_count:0,persisted_benefit_count_prospect_id:'PID_zyxwvutsrqponmlkjihgfedcba654321'}).status,'benefit_count_binding_not_verified');
assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,member_prospect_binding_verified:true,persisted_member_identity_id:member,persisted_prospect_id:prospect,registration_completed:true,consent_current:true}).status,'missing_existing_benefit_evidence');
for(const missing of [null,'','   ',false]) assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,member_prospect_binding_verified:true,persisted_member_identity_id:member,persisted_prospect_id:prospect,registration_completed:true,consent_current:true,existing_signup_benefit_count:missing}).status,'missing_existing_benefit_evidence');
assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,member_prospect_binding_verified:true,persisted_member_identity_id:member,persisted_prospect_id:prospect,registration_completed:true,consent_current:true,existing_signup_benefit_count:1}).status,'already_issued');
for(const malformed of [['0'],{value:0},0n]) assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,member_prospect_binding_verified:true,persisted_member_identity_id:member,persisted_prospect_id:prospect,registration_completed:true,consent_current:true,existing_signup_benefit_count:malformed}).status,'invalid_existing_benefit_count');
assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,member_prospect_binding_verified:true,persisted_member_identity_id:member,persisted_prospect_id:prospect,registration_completed:true,consent_current:true,existing_signup_benefit_count:'0'}).status,'ready');
assert.equal(planSignupBenefitIssue({member_identity_id:member,prospect_id:prospect,member_prospect_binding_verified:true,persisted_member_identity_id:member,persisted_prospect_id:prospect,registration_completed:true,consent_current:true,existing_signup_benefit_count:'9'.repeat(400)}).status,'invalid_existing_benefit_count');

p=planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0});
assert.equal(p.status,'ready');
assert.equal(p.transition_allowed,false);
assert.equal(p.commerce_write_allowed,false);
assert.equal(p.automatic_discount,false);
assert.equal(p.entitlement_id,entitlement);
assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:1}).status,'already_redeemed');
assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0,persisted_redemption_entitlement_id:'BEN_zyxwvutsrqponmlkjihgfedcba654321'}).status,'redemption_count_binding_not_verified');
assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:otherMember,authenticated_member_identity_id:otherMember,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0,persisted_entitlement_member_identity_id:member}).status,'entitlement_member_binding_not_verified');
assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0,entitlement_member_binding_verified:false}).status,'entitlement_member_binding_not_verified');
assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0,persisted_entitlement_state:'expired'}).status,'entitlement_state_binding_not_verified');
assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0,persisted_reserved_entitlement_reservation_id:'R-2'}).status,'reserved_entitlement_context_binding_not_verified');
assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0,persisted_reserved_entitlement_customer_id:'87654321'}).status,'reserved_entitlement_context_binding_not_verified');
assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0,persisted_redemption_customer_id:'87654321'}).status,'redemption_context_binding_not_verified');
assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0,persisted_redemption_reservation_id:'R-2'}).status,'redemption_context_binding_not_verified');
assert.equal(planSignupBenefitTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0}).status,'transition_entitlement_required');
assert.equal(planTransition({current_state:'used',target_state:'available',entitlement_member_identity_id:member,authenticated_member_identity_id:member,prior_redemption_count:1}).status,'invalid_transition');
for(const missing of [null,'','   ',false]) assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:missing}).status,'missing_redemption_evidence');
for(const malformed of [['0'],{value:0},0n]) assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:malformed}).status,'invalid_redemption_count');
assert.equal(planTransition({current_state:'issued',target_state:'available',entitlement_member_identity_id:member}).status,'ready');
assert.equal(planTransition({current_state:'reserved',target_state:'available',entitlement_member_identity_id:member}).status,'member_identity_mismatch');
p=planReleasedTransition({current_state:'reserved',target_state:'available',entitlement_member_identity_id:member});
assert.equal(p.status,'ready');
assert.equal(p.canonical_customer_id,'12345678');
assert.equal(p.reservation_id,'R-RELEASE');
assert.equal(planReleasedTransition({current_state:'reserved',target_state:'available',entitlement_member_identity_id:member,reservation_release_authorized:false}).status,'release_authorization_not_verified');
assert.equal(planReleasedTransition({current_state:'reserved',target_state:'available',entitlement_member_identity_id:member,persisted_reserved_entitlement_reservation_id:'R-OTHER'}).status,'reserved_entitlement_context_binding_not_verified');
assert.equal(planReleasedTransition({current_state:'reserved',target_state:'available',entitlement_member_identity_id:member,persisted_release_reservation_id:'R-OTHER'}).status,'release_authorization_not_verified');
assert.equal(planReleasedTransition({current_state:'reserved',target_state:'expired',entitlement_member_identity_id:member}).status,'ready');
assert.equal(planReleasedTransition({current_state:'reserved',target_state:'revoked',entitlement_member_identity_id:member}).status,'ready');
assert.equal(planTransition({current_state:'available',target_state:'reserved',entitlement_member_identity_id:member,persisted_entitlement_member_identity_id:otherMember}).status,'entitlement_member_binding_not_verified');
assert.equal(planTransition({current_state:'reserved',target_state:'revoked',entitlement_member_identity_id:member,persisted_entitlement_member_identity_id:otherMember}).status,'entitlement_member_binding_not_verified');
assert.equal(planTransition({current_state:'available',target_state:'reserved',entitlement_member_identity_id:member,authenticated_member_identity_id:member}).status,'reservation_context_required');
p=planReservedTransition({current_state:'available',target_state:'reserved',entitlement_member_identity_id:member});
assert.equal(p.status,'ready');
assert.equal(p.canonical_customer_id,'12345678');
assert.equal(p.reservation_id,'R-RESERVE');
assert.equal(planReservedTransition({current_state:'available',target_state:'reserved',entitlement_member_identity_id:member,persisted_reservation_context_customer_id:'87654321'}).status,'reservation_context_binding_not_verified');
assert.equal(planReservedTransition({current_state:'available',target_state:'reserved',entitlement_member_identity_id:member,persisted_reservation_context_reservation_id:'R-OTHER'}).status,'reservation_context_binding_not_verified');
assert.equal(planReservedTransition({current_state:'available',target_state:'reserved',entitlement_member_identity_id:member,authenticated_member_identity_id:otherMember}).status,'member_identity_mismatch');

p=planCarryForward({member_identity_id:member,prospect_id:prospect,canonical_customer_id:'12345678',current_state:'available',persisted_member_identity_id:member,persisted_prospect_id:prospect,persisted_canonical_customer_id:'12345678',promotion_verified:true});
assert.equal(p.status,'ready');
assert.equal(p.entitlement_id,entitlement);
assert.equal(p.state_reset,false);
assert.equal(p.reissue,false);
assert.equal(p.carry_forward_allowed,false);
assert.equal(planCarryForward({member_identity_id:member,prospect_id:prospect,canonical_customer_id:'12345678',current_state:'available',persisted_member_identity_id:member,persisted_prospect_id:prospect,persisted_canonical_customer_id:'12345678',promotion_verified:true,persisted_entitlement_state:'used'}).status,'entitlement_carry_forward_binding_not_verified');
assert.equal(planCarryForward({member_identity_id:member,prospect_id:prospect,canonical_customer_id:'12345678',current_state:'available',persisted_member_identity_id:member,persisted_prospect_id:prospect,persisted_canonical_customer_id:'12345678',promotion_verified:true,persisted_entitlement_member_identity_id:otherMember}).status,'entitlement_carry_forward_binding_not_verified');
assert.equal(planBenefitPromotionCarryForward({member_identity_id:member,prospect_id:prospect,canonical_customer_id:'12345678',current_state:'available',persisted_member_identity_id:member,persisted_prospect_id:prospect,persisted_canonical_customer_id:'12345678',promotion_verified:true}).status,'carry_forward_entitlement_required');
console.log('MEMBER_SIGNUP_BENEFIT_FOUNDATION=PASS');
console.log('ONE_TIME_ENTITLEMENT=YES');
console.log('DOUBLE_REDEMPTION=BLOCKED');
console.log('PROSPECT_PROMOTION_REISSUE=0');
console.log('AUTOMATIC_DISCOUNT=0');
console.log('COMMERCE_WRITE=0');

assert.equal(planCarryForward({member_identity_id:member,prospect_id:prospect,canonical_customer_id:'12345678',current_state:'available'}).status,'promotion_binding_not_verified');
assert.equal(planCarryForward({member_identity_id:member,prospect_id:prospect,canonical_customer_id:'12345678',current_state:'available',persisted_member_identity_id:member,persisted_prospect_id:prospect,persisted_canonical_customer_id:'87654321',promotion_verified:true}).status,'promotion_binding_not_verified');
