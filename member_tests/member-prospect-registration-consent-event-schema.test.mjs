import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import {planProspectRegistrationConsent} from '../src/member-prospect-registration-consent-plan.mjs';

const migration=fs.readFileSync(
  new URL('../migrations_managed/20261009_member_registration_consent_event_foundation.sql',import.meta.url),
  'utf8'
);

assert.match(migration,/CREATE TABLE IF NOT EXISTS member_registration_events/);
assert.match(migration,/registration_event_id TEXT PRIMARY KEY/);
assert.match(migration,/registration_idempotency_key TEXT NOT NULL UNIQUE/);
assert.match(migration,/profile_digest_sha256 TEXT NOT NULL/);
assert.match(migration,/CREATE TABLE IF NOT EXISTS member_consent_evidence/);
assert.match(migration,/consent_event_id TEXT PRIMARY KEY/);
assert.match(migration,/registration_event_id TEXT NOT NULL UNIQUE/);
assert.match(migration,/trg_member_registration_events_no_update/);
assert.match(migration,/trg_member_registration_events_no_delete/);
assert.match(migration,/trg_member_consent_evidence_no_update/);
assert.match(migration,/trg_member_consent_evidence_no_delete/);
assert.doesNotMatch(migration,/\b(?:name|phone|address|email)\s+TEXT\b/i);
assert.doesNotMatch(migration,/customer_identity_(?:sequence|registry)/i);
assert.doesNotMatch(migration,/(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+(?:customers|customer_reservations|customer_delivery_links)\b/i);

const member='MID_abcdefghijklmnopqrstuvwxyz123456';
const prospect='PID_abcdefghijklmnopqrstuvwxyz123456';
const terms=crypto.createHash('sha256').update('terms-v1').digest('hex');
const privacy=crypto.createHash('sha256').update('privacy-v1').digest('hex');
const acceptedAt='2026-10-09T02:45:00.000Z';
const idempotency='reg-20261009-schema-0001';
const profile={name:'Test Family',phone:'090-1234-5678',address:'Osaka, Japan',email:'family@example.com'};

const ready=planProspectRegistrationConsent({
  member_identity_id:member,
  prospect_id:prospect,
  server_generated_identity_verified:true,
  member_identity_source:'server_generated',
  prospect_id_source:'server_generated',
  canonical_customer_id:null,
  family_id:null,
  profile,
  terms_version:'terms-1',
  terms_sha256:terms,
  privacy_version:'privacy-1',
  privacy_sha256:privacy,
  latest_terms_version:'terms-1',
  latest_terms_sha256:terms,
  latest_privacy_version:'privacy-1',
  latest_privacy_sha256:privacy,
  current_consent_documents_verified:true,
  terms_accepted:true,
  privacy_accepted:true,
  accepted_at:acceptedAt,
  accepted_at_source:'server',
  server_accepted_at_verified:true,
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
});

assert.equal(ready.status,'ready');
assert.match(ready.registration_event_id,/^REG_[0-9a-f]{64}$/);
assert.match(ready.consent_event_id,/^CONS_[0-9a-f]{64}$/);
assert.match(ready.profile_digest_sha256,/^[0-9a-f]{64}$/);
assert.deepEqual(ready.transaction_operations,[
  'create_member_identity',
  'create_prospect',
  'append_registration_event',
  'append_consent_event'
]);
assert.equal(ready.partial_commit_allowed,false);
assert.equal(ready.registration_completed,false);
assert.equal(ready.raw_profile_output,false);
assert.equal(ready.profile_pii_long_term_storage_authorized,false);
assert.equal(ready.write_allowed,false);
assert.equal(ready.execute,false);
assert.equal(ready.production_write_authorized,false);

console.log('MEMBER_PROSPECT_REGISTRATION_CONSENT_EVENT_SCHEMA=PASS');
console.log('REGISTRATION_EVENT_APPEND_ONLY=PASS');
console.log('CONSENT_EVENT_APPEND_ONLY=PASS');
console.log('RAW_PROFILE_SCHEMA_COLUMNS=0');
console.log('PRODUCTION_SCHEMA_APPLY=0');
console.log('PRODUCTION_WRITE=0');
