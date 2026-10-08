import assert from 'node:assert/strict';
import fs from 'node:fs';

const DOC='docs/member-app/member-identity-prospect-lifecycle-contract.md';
const FAMILY_IDENTITY='src/crm-member-family-identity.mjs';
const WRANGLER='wrangler.jsonc';

const doc=fs.readFileSync(DOC,'utf8');
const identity=fs.readFileSync(FAMILY_IDENTITY,'utf8');
const wrangler=fs.readFileSync(WRANGLER,'utf8');

for (const phrase of [
  'Two customer entry paths',
  'Existing customer: first login',
  'Prospect: new registration',
  'Customer ID remains owned exclusively by Customer CRM.',
  'Prospect registration must not generate a Customer ID.',
  'stable Member Identity',
  'single-use',
  'expires',
  'must not expose the Customer ID',
  'seven-day cache',
  'hard absolute maximum retention of 30 days',
  'cached PII must be deleted or made cryptographically inaccessible',
  'must not silently extend PII retention beyond 30 days',
  'do not overwrite Customer Master',
  'separate review queue',
  'human administrator',
  'Customer CRM remains the only Customer ID issuer.',
  'Consent history is append-only.',
  'does **not** authorize'
]) assert.ok(doc.includes(phrase), `missing contract phrase: ${phrase}`);

assert.ok(
  doc.includes('A Member Identity must not be inferred from name, address, phone number, email address, LINE display name, or fuzzy matching.'),
  'complete Member Identity no-inference rule must remain locked'
);
assert.ok(
  doc.includes('No name/phone/email/fuzzy match may silently merge a prospect into a customer.'),
  'Prospect promotion no-fuzzy-merge rule must remain locked'
);

assert.ok(identity.includes("customer_identity_source:'canonical_customer_id_only'"));
assert.ok(identity.includes('name_match:false'));
assert.ok(identity.includes('address_match:false'));
assert.ok(identity.includes('phone_match:false'));
assert.ok(identity.includes('email_match:false'));
assert.ok(identity.includes('line_display_name_match:false'));
assert.ok(identity.includes('customer_id_generation:false'));

assert.ok(!wrangler.includes('GOOGLE_CUSTOMER_MASTER'));
assert.ok(!wrangler.includes('MEMBER_PROSPECT_WRITE_MODE'));
assert.ok(!wrangler.includes('MEMBER_INVITATION_WRITE_MODE'));

for (const zeroBoundary of [
  'Production deploy or Worker activation',
  'Production D1 read/write or migration apply',
  'Google Sheets/GAS creation, read, write, deployment, or credential changes',
  'CRM customer create/update/delete/merge',
  'Customer ID generation',
  'LINE send or LINE Login activation',
  'R2 binding/object access',
  'BLACK entitlement writes',
  'historical MEMORY writes',
  'commerce activation or paid spend',
  'UI changes'
]) assert.ok(doc.includes(zeroBoundary), `authorization boundary missing: ${zeroBoundary}`);

console.log('MEMBER_IDENTITY_PROSPECT_LIFECYCLE_CONTRACT=PASS');
console.log('CUSTOMER_ID_OWNER=CRM_ONLY');
console.log('FIRST_LOGIN_EXISTING_CUSTOMER=YES');
console.log('NEW_REGISTRATION_PROSPECT=YES');
console.log('PROSPECT_CUSTOMER_ID_GENERATION=0');
console.log('CLIENT_SELECTED_WRITE_TARGET=0');
console.log('FUZZY_IDENTITY_LINKING=0');
console.log('PROFILE_CACHE_DAYS=7');
console.log('D1_PII_MAX_RETENTION_DAYS=30');
console.log('IDENTITY_MISMATCH_DIRECT_MASTER_WRITE=0');
console.log('REVIEW_QUEUE_REQUIRED=YES');
console.log('UI_CHANGE=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CRM_WRITE=0');
console.log('LINE_SEND=0');
console.log('GOOGLE_WRITE=0');
console.log('COMMERCE_WRITE=0');
