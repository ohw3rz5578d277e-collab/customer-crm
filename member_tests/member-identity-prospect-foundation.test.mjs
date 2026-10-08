import assert from 'node:assert/strict';
import {
 createMemberIdentityId,
 createProspectId,
 createInvitationToken,
 invitationTokenDigest,
 buildExistingCustomerFirstLoginPlan,
 buildProspectRegistrationPlan,
 buildProspectPromotionPlan,
 __test
} from '../src/member-identity-prospect-foundation.mjs';

const mid=createMemberIdentityId();
const pid=createProspectId();
const token=createInvitationToken();
assert.match(mid,__test.MEMBER_ID_RE);
assert.match(pid,__test.PROSPECT_ID_RE);
assert.match(token,__test.INVITE_TOKEN_RE);
assert.notEqual(createMemberIdentityId(),mid);
assert.notEqual(createProspectId(),pid);
assert.notEqual(createInvitationToken(),token);
assert.match(invitationTokenDigest(token),/^[0-9a-f]{64}$/);

const existing=buildExistingCustomerFirstLoginPlan({customer_id:'12345678'});
assert.equal(existing.status,'ready');
assert.equal(existing.customer_id,'12345678');
assert.equal(existing.customer_id_generation,false);
assert.equal(existing.fuzzy_identity_linking,false);
assert.equal(existing.invitation.customer_id_exposed,false);
assert.equal(existing.invitation.single_use,true);
assert.equal(existing.write_allowed,false);
assert.equal('customer_id' in existing.invitation,false);

assert.equal(buildExistingCustomerFirstLoginPlan({customer_id:'山田'}).write_allowed,false);
assert.equal(buildExistingCustomerFirstLoginPlan({customer_id:'12345678',active_invitation_count:2}).status,'ambiguous_active_invitation');

const prospect=buildProspectRegistrationPlan();
assert.equal(prospect.status,'ready');
assert.equal(prospect.customer_id,null);
assert.equal(prospect.customer_id_generation,false);
assert.equal(prospect.write_allowed,false);

const promoted=buildProspectPromotionPlan({
 member_identity_id:prospect.member_identity_id,
 prospect_id:prospect.prospect_id,
 canonical_customer_id:'87654321'
});
assert.equal(promoted.status,'ready');
assert.equal(promoted.preserve_member_identity,true);
assert.equal(promoted.customer_id_generation,false);
assert.equal(promoted.automatic_merge,false);
assert.equal(promoted.write_allowed,false);

const collision=buildProspectPromotionPlan({
 member_identity_id:prospect.member_identity_id,
 prospect_id:prospect.prospect_id,
 canonical_customer_id:'87654321',
 collision_count:1
});
assert.equal(collision.status,'review_required');
assert.equal(collision.write_allowed,false);

console.log('MEMBER_IDENTITY_PROSPECT_FOUNDATION=PASS');
console.log('OPAQUE_INVITATION_TOKEN=PASS');
console.log('CUSTOMER_ID_IN_INVITATION_URL=0');
console.log('PROSPECT_CUSTOMER_ID_GENERATION=0');
console.log('FUZZY_IDENTITY_LINKING=0');
console.log('AUTOMATIC_CUSTOMER_MERGE=0');
console.log('PRODUCTION_WRITE=0');
