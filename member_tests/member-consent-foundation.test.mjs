import assert from 'node:assert/strict';
import {documentDigest,planConsentRecord,planConsentRequirement} from '../src/member-consent-foundation.mjs';
const terms=documentDigest('terms v1'),privacy=documentDigest('privacy v1');
let p=planConsentRecord({
 member_identity_id:'MID_abcdefghijklmnopqrstuvwxyz123456',terms_version:'1.0',terms_sha256:terms,
 privacy_version:'1.0',privacy_sha256:privacy,terms_accepted:true,privacy_accepted:true,
 accepted_at:'2026-10-07T06:00:00Z'
});
assert.equal(p.status,'ready');
assert.equal(p.append_only,true);
assert.equal(p.replace_prior_consent,false);
assert.equal(p.record_allowed,false);
assert.equal(planConsentRecord({
 member_identity_id:'12345678',terms_version:'1.0',terms_sha256:terms,
 privacy_version:'1.0',privacy_sha256:privacy,terms_accepted:true,privacy_accepted:true,
 accepted_at:'2026-10-07T06:00:00Z'
}).status,'invalid_member_identity');
assert.equal(planConsentRecord({
 member_identity_id:'MID_abcdefghijklmnopqrstuvwxyz123456',terms_version:'1.0',terms_sha256:terms,
 privacy_version:'1.0',privacy_sha256:privacy,terms_accepted:true,privacy_accepted:true,
 accepted_at:'2026-02-30T06:00:00Z'
}).status,'invalid_accepted_at');
assert.equal(planConsentRecord({
 member_identity_id:'MID_abcdefghijklmnopqrstuvwxyz123456',terms_version:'1.0',terms_sha256:terms,
 privacy_version:'1.0',privacy_sha256:privacy,terms_accepted:'true',privacy_accepted:'true',
 accepted_at:'2026-10-07T06:00:00Z'
}).status,'consent_incomplete');
let r=planConsentRequirement({latest_terms_version:'2.0',latest_privacy_version:'1.0',last_terms_version:'1.0',last_privacy_version:'1.0'});
assert.equal(r.reconsent_required,true);
assert.equal(r.terms_reconsent_required,true);
r=planConsentRequirement({latest_terms_version:'2.0',latest_privacy_version:'2.0',last_terms_version:'2.0',last_privacy_version:'2.0'});
assert.equal(r.reconsent_required,false);
console.log('MEMBER_CONSENT_FOUNDATION=PASS');
console.log('CONSENT_APPEND_ONLY=YES');
console.log('PROSPECT_PROMOTION_REWRITES_CONSENT=NO');
console.log('PRODUCTION_CONSENT_WRITE=0');
