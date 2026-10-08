import assert from 'node:assert/strict';
import {buildMemberAdminReadModel as rawBuildMemberAdminReadModel} from '../src/crm-member-admin-read-model.mjs';
const customer='12345678';
const prospect='PID_abcdefghijklmnopqrstuvwxyz123456';
const member='MID_abcdefghijklmnopqrstuvwxyz123456';
const otherMember='MID_zyxwvutsrqponmlkjihgfedcba654321';
const buildMemberAdminReadModel=(args={})=>rawBuildMemberAdminReadModel({
 subject_member_binding_verified:true,
 persisted_member_identity_id:args.member_identity_id,
 persisted_subject_customer_id:args.canonical_customer_id,
 persisted_subject_prospect_id:args.prospect_id,
 ...args
});
let r=buildMemberAdminReadModel({subject_type:'customer',canonical_customer_id:customer,member_identity_id:member,member_status:'registered',consent_status:'current',invitation_status:'used',review_pending_count:1});
assert.equal(r.status,'ready');
assert.equal(r.needs_review,true);
assert.equal(r.customer_id_generation,false);
assert.equal(r.mutation_allowed,false);
assert.equal(r.line_send_allowed,false);
r=buildMemberAdminReadModel({subject_type:'prospect',prospect_id:prospect,member_identity_id:member,acquisition_source:'instagram',benefit_status:'available',promotion_status:'prospect'});
assert.equal(r.status,'ready');
assert.equal(r.canonical_customer_id,null);
assert.equal(r.invitation_status,'not_applicable');
assert.equal(r.promotion_status,'prospect');
assert.equal(buildMemberAdminReadModel({subject_type:'prospect',prospect_id:'bad',member_identity_id:member}).read_ready,false);
assert.equal(buildMemberAdminReadModel({subject_type:'prospect',prospect_id:prospect}).status,'invalid_member_identity');
const defaultProspect=buildMemberAdminReadModel({subject_type:'prospect',prospect_id:prospect,member_identity_id:member});
assert.equal(defaultProspect.promotion_status,'prospect');
assert.equal(rawBuildMemberAdminReadModel({subject_type:'prospect',prospect_id:prospect,member_identity_id:member}).status,'subject_member_binding_not_verified');
assert.equal(buildMemberAdminReadModel({subject_type:'prospect',prospect_id:prospect,member_identity_id:member,persisted_member_identity_id:otherMember}).status,'subject_member_binding_not_verified');
assert.equal(buildMemberAdminReadModel({subject_type:'customer',canonical_customer_id:customer,member_identity_id:member,persisted_subject_customer_id:'87654321'}).status,'subject_member_binding_not_verified');
assert.equal(buildMemberAdminReadModel({subject_type:'customer',canonical_customer_id:customer,member_identity_id:otherMember,persisted_member_identity_id:member}).status,'subject_member_binding_not_verified');
for(const malformed of [false,['0'],{value:0},0n,'9'.repeat(400),Number.MAX_SAFE_INTEGER+1]){
 assert.equal(buildMemberAdminReadModel({subject_type:'customer',canonical_customer_id:customer,member_identity_id:member,review_pending_count:malformed}).status,'invalid_review_count');
}
console.log('CRM_MEMBER_ADMIN_READ_MODEL=PASS');
console.log('UI_CHANGE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CRM_MUTATION=0');
console.log('LINE_SEND=0');
