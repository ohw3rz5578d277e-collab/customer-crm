import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {planProspectRegistrationConsent} from '../src/member-prospect-registration-consent-plan.mjs';

const member='MID_abcdefghijklmnopqrstuvwxyz123456';
const prospect='PID_abcdefghijklmnopqrstuvwxyz123456';
const terms=crypto.createHash('sha256').update('terms-v1').digest('hex');
const privacy=crypto.createHash('sha256').update('privacy-v1').digest('hex');
const acceptedAt='2026-10-09T02:45:00.000Z';
const idempotency='reg-20261009-00000001';
const profile={name:'Test Family',phone:'090-1234-5678',address:'Osaka, Japan',email:'family@example.com'};

const base={
  member_identity_id:member,
  prospect_id:prospect,
  server_generated_identity_verified:true,
  member_identity_source:'server_generated',
  prospect_id_source:'server_generated',
  canonical_customer_id:null,
  family_id:null,
  profile,
  terms_version:'terms-1',terms_sha256:terms,
  privacy_version:'privacy-1',privacy_sha256:privacy,
  terms_accepted:true,privacy_accepted:true,accepted_at:acceptedAt,
  registration_idempotency_key:idempotency,
  existing_member_identity_count:0,
  existing_member_identity_count_member_identity_id:member,
  existing_prospect_id_count:0,
  existing_prospect_id_count_prospect_id:prospect,
  existing_registration_event_count:0,
  existing_registration_event_count_member_identity_id:member,
  existing_registration_event_count_prospect_id:prospect,
  existing_registration_event_count_idempotency_key:idempotency,
  existing_consent_event_count:0,
  existing_consent_event_count_member_identity_id:member,
  existing_consent_event_count_terms_sha256:terms,
  existing_consent_event_count_privacy_sha256:privacy,
  existing_consent_event_count_accepted_at:acceptedAt
};

const ready=planProspectRegistrationConsent(base);
assert.equal(ready.status,'ready');
assert.equal(ready.ready,true);
assert.equal(ready.member_identity_id,member);
assert.equal(ready.prospect_id,prospect);
assert.equal(ready.canonical_customer_id,null);
assert.equal(ready.family_id,null);
assert.match(ready.profile_digest_sha256,/^[0-9a-f]{64}$/);
assert.match(ready.registration_event_id,/^REG_[0-9a-f]{64}$/);
assert.match(ready.consent_event_id,/^CONS_[0-9a-f]{64}$/);
assert.equal(ready.consent.append_only,true);
assert.equal(ready.consent.replace_prior_consent,false);
assert.equal(ready.transaction_required,true);
assert.equal(ready.partial_commit_allowed,false);
assert.equal(ready.registration_completed,false);
assert.equal(ready.completion_requires_atomic_execution,true);
assert.deepEqual(ready.transaction_operations,['create_member_identity','create_prospect','append_registration_event','append_consent_event']);
assert.equal(ready.customer_id_generation,false);
assert.equal(ready.customer_id_from_client,false);
assert.equal(ready.family_id_generation,false);
assert.equal(ready.fuzzy_identity_linking,false);
assert.equal(ready.raw_profile_output,false);
assert.equal(ready.profile_pii_long_term_storage_authorized,false);
assert.equal(ready.write_allowed,false);
assert.equal(ready.execute,false);
assert.equal(ready.production_write_authorized,false);
assert.equal(ready.execution_requires_separate_gate,true);
assert.ok(!JSON.stringify(ready).includes(profile.name));
assert.ok(!JSON.stringify(ready).includes(profile.phone));
assert.ok(!JSON.stringify(ready).includes(profile.address));
assert.ok(!JSON.stringify(ready).includes(profile.email));

const deterministic=planProspectRegistrationConsent({...base,profile:{...profile}});
assert.equal(deterministic.registration_event_id,ready.registration_event_id);
assert.equal(deterministic.consent_event_id,ready.consent_event_id);
assert.equal(deterministic.profile_digest_sha256,ready.profile_digest_sha256);
const changedProfile=planProspectRegistrationConsent({...base,profile:{...profile,address:'Kyoto, Japan'}});
assert.notEqual(changedProfile.profile_digest_sha256,ready.profile_digest_sha256);
assert.notEqual(changedProfile.registration_event_id,ready.registration_event_id);

assert.equal(planProspectRegistrationConsent({...base,server_generated_identity_verified:'true'}).status,'server_generated_identity_not_verified');
assert.equal(planProspectRegistrationConsent({...base,member_identity_source:'client'}).status,'invalid_identity_source');
assert.equal(planProspectRegistrationConsent({...base,prospect_id_source:'client'}).status,'invalid_identity_source');
assert.equal(planProspectRegistrationConsent({...base,canonical_customer_id:'12345678'}).status,'prospect_customer_scope_forbidden');
assert.equal(planProspectRegistrationConsent({...base,family_id:'FAM-1'}).status,'prospect_family_scope_forbidden');

for(const key of ['name','phone','address','email']){
  assert.match(planProspectRegistrationConsent({...base,profile:{...profile,[key]:[profile[key]]}}).status,new RegExp(`^invalid_profile_${key}$`));
  assert.match(planProspectRegistrationConsent({...base,profile:{...profile,[key]:` ${profile[key]}`}}).status,new RegExp(`^invalid_profile_${key}$`));
}
assert.equal(planProspectRegistrationConsent({...base,profile:{...profile,email:'not-an-email'}}).status,'invalid_profile_email');

assert.equal(planProspectRegistrationConsent({...base,terms_version:['terms-1']}).status,'consent_invalid_document_identity');
assert.equal(planProspectRegistrationConsent({...base,terms_sha256:[terms]}).status,'consent_invalid_document_identity');
assert.equal(planProspectRegistrationConsent({...base,privacy_version:{value:'privacy-1'}}).status,'consent_invalid_document_identity');
assert.equal(planProspectRegistrationConsent({...base,accepted_at:[acceptedAt]}).status,'consent_invalid_accepted_at');
assert.equal(planProspectRegistrationConsent({...base,terms_accepted:'true'}).status,'consent_consent_incomplete');
assert.equal(planProspectRegistrationConsent({...base,privacy_accepted:false}).status,'consent_consent_incomplete');
assert.equal(planProspectRegistrationConsent({...base,accepted_at:'2026-02-30T02:45:00.000Z'}).status,'consent_invalid_accepted_at');
assert.equal(planProspectRegistrationConsent({...base,registration_idempotency_key:'short'}).status,'invalid_registration_idempotency_key');

for(const key of ['existing_member_identity_count','existing_prospect_id_count','existing_registration_event_count','existing_consent_event_count']){
  for(const malformed of [[],{},Object.create(null),-1,1.5,Number.MAX_SAFE_INTEGER+1,'01','1.0','-1',' 1']){
    assert.doesNotThrow(()=>planProspectRegistrationConsent({...base,[key]:malformed}));
    assert.match(planProspectRegistrationConsent({...base,[key]:malformed}).status,/^invalid_/);
  }
}

assert.equal(planProspectRegistrationConsent({...base,existing_member_identity_count_member_identity_id:'MID_wrong_wrong_wrong_wrong'}).status,'existing_member_identity_count_member_identity_id_mismatch');
assert.equal(planProspectRegistrationConsent({...base,existing_prospect_id_count_prospect_id:'PID_wrong_wrong_wrong_wrong'}).status,'existing_prospect_id_count_prospect_id_mismatch');
assert.equal(planProspectRegistrationConsent({...base,existing_registration_event_count_prospect_id:'PID_wrong_wrong_wrong_wrong'}).status,'registration_event_count_prospect_scope_mismatch');
assert.equal(planProspectRegistrationConsent({...base,existing_registration_event_count_idempotency_key:'reg-20261009-wrong999'}).status,'registration_event_count_idempotency_scope_mismatch');
assert.equal(planProspectRegistrationConsent({...base,existing_consent_event_count_terms_sha256:'0'.repeat(64)}).status,'consent_event_count_terms_scope_mismatch');
assert.equal(planProspectRegistrationConsent({...base,existing_consent_event_count_accepted_at:'2026-10-09T02:46:00.000Z'}).status,'consent_event_count_time_scope_mismatch');

const partial=planProspectRegistrationConsent({...base,existing_member_identity_count:1});
assert.equal(partial.status,'registration_state_conflict');
assert.equal(partial.review_required,true);

const replayEvidence={
  ...base,
  existing_member_identity_count:1,
  existing_prospect_id_count:1,
  existing_registration_event_count:1,
  existing_consent_event_count:1,
  existing_registration_event_verified:true,
  persisted_registration_event_id:ready.registration_event_id,
  persisted_registration_member_identity_id:member,
  persisted_registration_prospect_id:prospect,
  persisted_registration_profile_digest_sha256:ready.profile_digest_sha256,
  existing_consent_event_verified:true,
  persisted_consent_event_id:ready.consent_event_id,
  persisted_consent_member_identity_id:member,
  persisted_consent_registration_event_id:ready.registration_event_id,
  persisted_consent_terms_sha256:terms,
  persisted_consent_privacy_sha256:privacy,
  persisted_consent_accepted_at:acceptedAt
};
const replay=planProspectRegistrationConsent(replayEvidence);
assert.equal(replay.status,'registration_already_recorded');
assert.equal(replay.idempotent_replay,true);
assert.equal(replay.write_allowed,false);
assert.equal(planProspectRegistrationConsent({...replayEvidence,persisted_registration_prospect_id:'PID_wrong_wrong_wrong_wrong'}).status,'registration_replay_evidence_mismatch');
assert.equal(planProspectRegistrationConsent({...replayEvidence,persisted_consent_registration_event_id:'REG_'+('0'.repeat(64))}).status,'registration_replay_evidence_mismatch');
assert.equal(planProspectRegistrationConsent({...replayEvidence,existing_consent_event_verified:'true'}).status,'registration_replay_evidence_mismatch');

console.log('MEMBER_PROSPECT_REGISTRATION_CONSENT_PLAN=PASS');
console.log('PROSPECT_CUSTOMER_ID_GENERATION=0');
console.log('CONSENT_APPEND_ONLY=PASS');
console.log('REGISTRATION_ATOMIC_EXECUTION_REQUIRED=YES');
console.log('RAW_PROFILE_OUTPUT=0');
console.log('PRODUCTION_WRITE=0');
