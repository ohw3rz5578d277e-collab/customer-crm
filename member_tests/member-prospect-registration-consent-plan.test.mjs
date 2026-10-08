import assert from 'node:assert/strict';
import fs from 'node:fs';
import {planProspectRegistrationWithConsent} from '../src/member-prospect-registration-consent-plan.mjs';

const memberId='MID_abcdefghijklmnopqrstuvwxyz';
const prospectId='PID_abcdefghijklmnopqrstuvwxyz';
const consentEventId='CE_abcdefghijklmnopqrstuvwxyz';
const termsSha='a'.repeat(64);
const privacySha='b'.repeat(64);

const base={
  member_identity_id:memberId,
  prospect_id:prospectId,
  consent_event_id:consentEventId,
  acquisition_source:'instagram_profile',
  ids_server_generated_verified:true,
  consent_event_id_server_generated_verified:true,
  registration_request_server_verified:true,
  member_identity_collision_count:0,
  member_identity_count_member_identity_id:memberId,
  prospect_collision_count:0,
  prospect_count_prospect_id:prospectId,
  consent_event_collision_count:0,
  consent_event_count_consent_event_id:consentEventId,
  terms_version:'2026.10',
  terms_sha256:termsSha,
  privacy_version:'2026.10',
  privacy_sha256:privacySha,
  terms_document_verified:true,
  privacy_document_verified:true,
  terms_accepted:true,
  privacy_accepted:true,
  accepted_at:'2026-10-09T00:00:00.000Z',
  accepted_at_server_recorded:true
};

const ready=planProspectRegistrationWithConsent(base);
assert.equal(ready.status,'ready');
assert.equal(ready.ready,true);
assert.equal(ready.member_identity_id,memberId);
assert.equal(ready.prospect_id,prospectId);
assert.equal(ready.consent_event_id,consentEventId);
assert.equal(ready.canonical_customer_id,null);
assert.equal(ready.family_id,null);
assert.equal(ready.promoted_customer_id,null);
assert.equal(ready.customer_id_generation,false);
assert.equal(ready.family_id_generation,false);
assert.equal(ready.fuzzy_identity_linking,false);
assert.equal(ready.consent.append_only,true);
assert.equal(ready.consent.replace_prior_consent,false);
assert.equal(ready.consent.update_allowed,false);
assert.equal(ready.consent.delete_allowed,false);
assert.equal(ready.transaction.mode,'all_or_nothing');
assert.equal(ready.transaction.statements.length,3);
assert.equal(ready.transaction.rollback_on_any_failure,true);
assert.equal(ready.transaction.retry_without_revalidation,false);
for(const statement of ready.transaction.statements){
  assert.equal(statement.success_requires_affected_rows,1);
}
assert.match(ready.transaction.statements[0].sql,/INSERT INTO member_identities/);
assert.match(ready.transaction.statements[1].sql,/INSERT INTO member_prospects/);
assert.match(ready.transaction.statements[2].sql,/INSERT INTO member_consent_evidence/);
assert.equal(ready.write_allowed,false);
assert.equal(ready.execute,false);
assert.equal(ready.production_write_authorized,false);
assert.equal(ready.execution_requires_separate_gate,true);
assert.equal(ready.crm_customer_mutation,false);
assert.equal(ready.customer_master_mutation,false);
assert.equal(ready.line_send,false);
assert.equal(ready.google_network_send,false);

for(const [key,value,status] of [
  ['member_identity_id',[memberId],'invalid_member_identity'],
  ['member_identity_id',` ${memberId}`,'invalid_member_identity'],
  ['prospect_id',[prospectId],'invalid_prospect_id'],
  ['prospect_id',`${prospectId} `,'invalid_prospect_id'],
  ['consent_event_id',[consentEventId],'invalid_consent_event_id'],
  ['acquisition_source',['instagram_profile'],'invalid_acquisition_source'],
  ['terms_version',['2026.10'],'invalid_consent_document_version'],
  ['terms_sha256',[termsSha],'invalid_consent_document_digest'],
  ['accepted_at','2026-10-09T00:00:00Z','invalid_accepted_at']
]){
  assert.doesNotThrow(()=>planProspectRegistrationWithConsent({...base,[key]:value}));
  assert.equal(planProspectRegistrationWithConsent({...base,[key]:value}).status,status);
}

assert.equal(planProspectRegistrationWithConsent({...base,ids_server_generated_verified:'true'}).status,'server_generated_identity_not_verified');
assert.equal(planProspectRegistrationWithConsent({...base,consent_event_id_server_generated_verified:'true'}).status,'server_generated_consent_event_not_verified');
assert.equal(planProspectRegistrationWithConsent({...base,registration_request_server_verified:false}).status,'registration_request_not_verified');
assert.equal(planProspectRegistrationWithConsent({...base,canonical_customer_id:'12345678'}).status,'prospect_scope_violation');
assert.equal(planProspectRegistrationWithConsent({...base,family_id:'FAM_example'}).status,'prospect_scope_violation');
assert.equal(planProspectRegistrationWithConsent({...base,promoted_customer_id:'12345678'}).status,'prospect_scope_violation');
assert.equal(planProspectRegistrationWithConsent({...base,terms_document_verified:'true'}).status,'consent_document_not_verified');
assert.equal(planProspectRegistrationWithConsent({...base,privacy_document_verified:false}).status,'consent_document_not_verified');
assert.equal(planProspectRegistrationWithConsent({...base,terms_accepted:'true'}).status,'consent_incomplete');
assert.equal(planProspectRegistrationWithConsent({...base,privacy_accepted:false}).status,'consent_incomplete');
assert.equal(planProspectRegistrationWithConsent({...base,accepted_at_server_recorded:'true'}).status,'accepted_at_not_server_recorded');
assert.equal(planProspectRegistrationWithConsent({...base,accepted_at:'2026-02-30T00:00:00.000Z'}).status,'invalid_accepted_at');

for(const [countKey,scopeKey,expectedScope,collisionStatus,scopeStatus] of [
  ['member_identity_collision_count','member_identity_count_member_identity_id',memberId,'member_identity_collision','member_identity_count_scope_mismatch'],
  ['prospect_collision_count','prospect_count_prospect_id',prospectId,'prospect_collision','prospect_count_scope_mismatch'],
  ['consent_event_collision_count','consent_event_count_consent_event_id',consentEventId,'consent_event_collision','consent_event_count_scope_mismatch']
]){
  assert.equal(planProspectRegistrationWithConsent({...base,[countKey]:1}).status,collisionStatus);
  assert.equal(planProspectRegistrationWithConsent({...base,[scopeKey]:`${expectedScope}x`}).status,scopeStatus);
  for(const malformed of [[],{},Object.create(null),-1,1.5,Number.MAX_SAFE_INTEGER+1,'01',' 0']){
    assert.doesNotThrow(()=>planProspectRegistrationWithConsent({...base,[countKey]:malformed}));
    assert.match(planProspectRegistrationWithConsent({...base,[countKey]:malformed}).status,/^invalid_/);
  }
}

const migration=fs.readFileSync(new URL('../migrations_managed/20261009_member_consent_evidence_foundation.sql',import.meta.url),'utf8');
assert.match(migration,/CREATE TABLE IF NOT EXISTS member_consent_evidence/);
assert.match(migration,/trg_member_consent_evidence_no_update/);
assert.match(migration,/trg_member_consent_evidence_no_delete/);
assert.match(migration,/RAISE\(ABORT, 'member_consent_evidence_append_only'\)/);
assert.ok(!migration.includes('DROP TABLE'));
assert.ok(!migration.includes('ALTER TABLE customers'));

console.log('MEMBER_PROSPECT_REGISTRATION_CONSENT_PLAN=PASS');
console.log('PROSPECT_REGISTRATION_ALL_OR_NOTHING=PASS');
console.log('CONSENT_EVENT_SERVER_GENERATED=PASS');
console.log('CONSENT_APPEND_ONLY_SCHEMA=PASS');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_MASTER_MUTATION=0');
console.log('PRODUCTION_WRITE=0');
console.log('MIGRATION_APPLY=0');
