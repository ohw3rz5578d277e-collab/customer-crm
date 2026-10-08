import assert from 'node:assert/strict';
import {planSignupBenefitIssue as rawPlanSignupBenefitIssue,planSignupBenefitTransition,planBenefitPromotionCarryForward} from '../src/member-signup-benefit-foundation.mjs';
const member='MID_abcdefghijklmnopqrstuvwxyz123456';
const otherMember='MID_zyxwvutsrqponmlkjihgfedcba654321';
const prospect='PID_abcdefghijklmnopqrstuvwxyz123456';
const entitlement='BEN_abcdefghijklmnopqrstuvwxyz123456';
const planSignupBenefitIssue=(args={})=>rawPlanSignupBenefitIssue({persisted_benefit_count_member_identity_id:args.member_identity_id,persisted_benefit_count_prospect_id:args.prospect_id,...args});
const planUsedTransition=(args={})=>planSignupBenefitTransition({
 entitlement_id:entitlement,
 persisted_redemption_entitlement_id:entitlement,
 entitlement_member_binding_verified:true,
 persisted_entitlement_id:entitlement,
 persisted_entitlement_member_identity_id:args.entitlement_member_identity_id,
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
assert.equal(planSignupBenefitTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:0}).status,'redemption_entitlement_required');
assert.equal(planSignupBenefitTransition({current_state:'used',target_state:'available',entitlement_member_identity_id:member,authenticated_member_identity_id:member,prior_redemption_count:1}).status,'invalid_transition');
for(const missing of [null,'','   ',false]) assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:missing}).status,'missing_redemption_evidence');
for(const malformed of [['0'],{value:0},0n]) assert.equal(planUsedTransition({current_state:'reserved',target_state:'used',entitlement_member_identity_id:member,authenticated_member_identity_id:member,canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:malformed}).status,'invalid_redemption_count');
assert.equal(planSignupBenefitTransition({current_state:'issued',target_state:'available',entitlement_member_identity_id:member}).status,'ready');
assert.equal(planSignupBenefitTransition({current_state:'reserved',target_state:'available',entitlement_member_identity_id:member}).status,'ready');

p=planBenefitPromotionCarryForward({member_identity_id:member,prospect_id:prospect,canonical_customer_id:'12345678',current_state:'available',persisted_member_identity_id:member,persisted_prospect_id:prospect,persisted_canonical_customer_id:'12345678',promotion_verified:true});
assert.equal(p.status,'ready');
assert.equal(p.state_reset,false);
assert.equal(p.reissue,false);
assert.equal(p.carry_forward_allowed,false);
console.log('MEMBER_SIGNUP_BENEFIT_FOUNDATION=PASS');
console.log('ONE_TIME_ENTITLEMENT=YES');
console.log('DOUBLE_REDEMPTION=BLOCKED');
console.log('PROSPECT_PROMOTION_REISSUE=0');
console.log('AUTOMATIC_DISCOUNT=0');
console.log('COMMERCE_WRITE=0');

assert.equal(planBenefitPromotionCarryForward({member_identity_id:member,prospect_id:prospect,canonical_customer_id:'12345678',current_state:'available'}).status,'promotion_binding_not_verified');
assert.equal(planBenefitPromotionCarryForward({member_identity_id:member,prospect_id:prospect,canonical_customer_id:'12345678',current_state:'available',persisted_member_identity_id:member,persisted_prospect_id:prospect,persisted_canonical_customer_id:'87654321',promotion_verified:true}).status,'promotion_binding_not_verified');
