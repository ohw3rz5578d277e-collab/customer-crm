import assert from 'node:assert/strict';
import {planExactCustomerMemberFamilyBinding,planExactProspectMemberBinding} from '../src/member-exact-identity-binding-plan.mjs';

const mid='MID_abcdefghijklmnopqrstuvwxyz';
const pid='PID_abcdefghijklmnopqrstuvwxyz';
const cid='12345678';

const customerReady={
  member_identity_id:mid,
  canonical_customer_id:cid,
  family_id:'FAM-1',
  customer_id_source:'customer_crm',
  customer_record_verified:true,
  persisted_customer_record_id:cid,
  member_customer_binding_verified:true,
  persisted_member_identity_id:mid,
  persisted_member_customer_id:cid,
  active_member_customer_binding_count:1,
  persisted_member_binding_count_customer_id:cid,
  family_link_verified:true,
  persisted_family_id:'FAM-1',
  persisted_family_customer_id:cid,
  active_family_link_count:1,
  persisted_family_link_count_customer_id:cid
};

let r=planExactCustomerMemberFamilyBinding(customerReady);
assert.equal(r.status,'ready');
assert.equal(r.exact_binding_verified,true);
assert.equal(r.write_allowed,false);
assert.equal(r.customer_id_generation,false);
assert.equal(r.family_id_generation,false);
assert.equal(r.fuzzy_identity_linking,false);

r=planExactCustomerMemberFamilyBinding({...customerReady,customer_record_verified:'true'});
assert.equal(r.status,'customer_record_not_verified');

r=planExactCustomerMemberFamilyBinding({...customerReady,member_customer_binding_verified:'true'});
assert.equal(r.status,'member_customer_binding_not_verified');

r=planExactCustomerMemberFamilyBinding({...customerReady,persisted_member_customer_id:'87654321'});
assert.equal(r.status,'member_customer_binding_not_verified');
assert.equal(r.review_required,true);

r=planExactCustomerMemberFamilyBinding({...customerReady,active_member_customer_binding_count:2});
assert.equal(r.status,'ambiguous_member_customer_binding');
assert.equal(r.review_required,true);

r=planExactCustomerMemberFamilyBinding({...customerReady,active_member_customer_binding_count:Number.MAX_SAFE_INTEGER+1});
assert.equal(r.status,'invalid_member_binding_count');

r=planExactCustomerMemberFamilyBinding({...customerReady,family_link_verified:false});
assert.equal(r.status,'family_link_not_verified');

r=planExactCustomerMemberFamilyBinding({...customerReady,persisted_family_customer_id:'87654321'});
assert.equal(r.status,'family_link_not_verified');
assert.equal(r.review_required,true);

r=planExactCustomerMemberFamilyBinding({...customerReady,active_family_link_count:0});
assert.equal(r.status,'ambiguous_family_link');
assert.equal(r.review_required,true);

const prospectReady={
  member_identity_id:mid,
  prospect_id:pid,
  prospect_status:'prospect',
  member_prospect_binding_verified:true,
  persisted_member_identity_id:mid,
  persisted_prospect_id:pid,
  active_member_prospect_binding_count:1,
  persisted_member_binding_count_prospect_id:pid
};

r=planExactProspectMemberBinding(prospectReady);
assert.equal(r.status,'ready');
assert.equal(r.exact_binding_verified,true);
assert.equal(r.canonical_customer_id,null);
assert.equal(r.family_id,null);
assert.equal(r.prospect_access_to_customer_data,false);
assert.equal(r.customer_id_generation,false);
assert.equal(r.write_allowed,false);

r=planExactProspectMemberBinding({...prospectReady,canonical_customer_id:cid});
assert.equal(r.status,'prospect_scope_violation');
assert.equal(r.review_required,true);

r=planExactProspectMemberBinding({...prospectReady,family_id:'FAM-1'});
assert.equal(r.status,'prospect_scope_violation');

r=planExactProspectMemberBinding({...prospectReady,member_prospect_binding_verified:'true'});
assert.equal(r.status,'member_prospect_binding_not_verified');

r=planExactProspectMemberBinding({...prospectReady,persisted_member_identity_id:'MID_zyxwvutsrqponmlkjihgfedcba'});
assert.equal(r.status,'member_prospect_binding_not_verified');
assert.equal(r.review_required,true);

r=planExactProspectMemberBinding({...prospectReady,active_member_prospect_binding_count:2});
assert.equal(r.status,'ambiguous_member_prospect_binding');
assert.equal(r.review_required,true);

r=planExactProspectMemberBinding({...prospectReady,active_member_prospect_binding_count:'9007199254740992'});
assert.equal(r.status,'invalid_prospect_binding_count');

console.log('MEMBER_EXACT_IDENTITY_BINDING=PASS');
console.log('CUSTOMER_MEMBER_BINDING=EXACT_ONE');
console.log('CUSTOMER_FAMILY_BINDING=EXACT_ONE');
console.log('PROSPECT_CUSTOMER_DATA_ACCESS=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('FAMILY_ID_GENERATION=0');
console.log('FUZZY_IDENTITY_LINKING=0');
console.log('PRODUCTION_WRITE=0');
