import assert from 'node:assert/strict';
import {planSignupBenefitTransition} from '../src/member-signup-benefit-foundation.mjs';

const member='MID_abcdefghijklmnopqrstuvwxyz123456';
const otherMember='MID_zyxwvutsrqponmlkjihgfedcba654321';
const entitlement='BEN_abcdefghijklmnopqrstuvwxyz123456';

const plan=(args={})=>planSignupBenefitTransition({
 entitlement_id:entitlement,
 entitlement_member_identity_id:member,
 persisted_entitlement_id:entitlement,
 persisted_entitlement_member_identity_id:member,
 entitlement_member_binding_verified:true,
 entitlement_state_binding_verified:true,
 persisted_entitlement_state:args.current_state,
 ...args
});

assert.equal(plan({current_state:'issued',target_state:'expired'}).status,'member_identity_mismatch');
assert.equal(plan({current_state:'available',target_state:'revoked',authenticated_member_identity_id:member}).status,'terminal_authorization_not_verified');

let p=plan({
 current_state:'issued',target_state:'expired',authenticated_member_identity_id:member,
 terminal_authorized:true,terminal_authorization_binding_verified:true,
 persisted_terminal_entitlement_id:entitlement,
 persisted_terminal_member_identity_id:member,
 persisted_terminal_target_state:'expired'
});
assert.equal(p.status,'ready');
assert.equal(p.transition_allowed,false);
assert.equal(p.to,'expired');

p=plan({
 current_state:'available',target_state:'revoked',authenticated_member_identity_id:member,
 terminal_authorized:true,terminal_authorization_binding_verified:true,
 persisted_terminal_entitlement_id:entitlement,
 persisted_terminal_member_identity_id:member,
 persisted_terminal_target_state:'revoked'
});
assert.equal(p.status,'ready');
assert.equal(p.to,'revoked');

assert.equal(plan({
 current_state:'available',target_state:'revoked',authenticated_member_identity_id:member,
 terminal_authorized:true,terminal_authorization_binding_verified:true,
 persisted_terminal_entitlement_id:entitlement,
 persisted_terminal_member_identity_id:member,
 persisted_terminal_target_state:'expired'
}).status,'terminal_authorization_not_verified');

assert.equal(plan({
 current_state:'issued',target_state:'expired',authenticated_member_identity_id:member,
 terminal_authorized:true,terminal_authorization_binding_verified:true,
 persisted_terminal_entitlement_id:entitlement,
 persisted_terminal_member_identity_id:otherMember,
 persisted_terminal_target_state:'expired'
}).status,'terminal_authorization_not_verified');

assert.equal(plan({
 current_state:'available',target_state:'revoked',authenticated_member_identity_id:otherMember,
 terminal_authorized:true,terminal_authorization_binding_verified:true,
 persisted_terminal_entitlement_id:entitlement,
 persisted_terminal_member_identity_id:otherMember,
 persisted_terminal_target_state:'revoked'
}).status,'member_identity_mismatch');

console.log('MEMBER_SIGNUP_BENEFIT_TERMINAL_AUTHORIZATION=PASS');
console.log('UNAUTHORIZED_TERMINATION=0');
console.log('PRODUCTION_WRITE=0');
