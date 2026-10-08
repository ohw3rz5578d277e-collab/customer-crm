import assert from 'node:assert/strict';
import {buildMemberAdminReadModel} from '../src/crm-member-admin-read-model.mjs';
let r=buildMemberAdminReadModel({subject_type:'customer',canonical_customer_id:'12345678',member_status:'registered',consent_status:'current',invitation_status:'used',review_pending_count:1});
assert.equal(r.status,'ready');
assert.equal(r.needs_review,true);
assert.equal(r.customer_id_generation,false);
assert.equal(r.mutation_allowed,false);
assert.equal(r.line_send_allowed,false);
r=buildMemberAdminReadModel({subject_type:'prospect',prospect_id:'PID_abcdefghijklmnopqrstuvwxyz123456',member_identity_id:'MID_abcdefghijklmnopqrstuvwxyz123456',acquisition_source:'instagram',benefit_status:'available',promotion_status:'prospect'});
assert.equal(r.status,'ready');
assert.equal(r.canonical_customer_id,null);
assert.equal(r.invitation_status,'not_applicable');
assert.equal(r.promotion_status,'prospect');
assert.equal(buildMemberAdminReadModel({subject_type:'prospect',prospect_id:'bad',member_identity_id:'MID_abcdefghijklmnopqrstuvwxyz123456'}).read_ready,false);
assert.equal(buildMemberAdminReadModel({subject_type:'prospect',prospect_id:'PID_abcdefghijklmnopqrstuvwxyz123456'}).status,'invalid_member_identity');
const defaultProspect=buildMemberAdminReadModel({subject_type:'prospect',prospect_id:'PID_abcdefghijklmnopqrstuvwxyz123456',member_identity_id:'MID_abcdefghijklmnopqrstuvwxyz123456'});
assert.equal(defaultProspect.promotion_status,'prospect');
for(const malformed of [false,['0'],{value:0},0n,'9'.repeat(400),Number.MAX_SAFE_INTEGER+1]){
 assert.equal(buildMemberAdminReadModel({subject_type:'customer',canonical_customer_id:'12345678',review_pending_count:malformed}).status,'invalid_review_count');
}
console.log('CRM_MEMBER_ADMIN_READ_MODEL=PASS');
console.log('UI_CHANGE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CRM_MUTATION=0');
console.log('LINE_SEND=0');
