import assert from 'node:assert/strict';
import fs from 'node:fs';
const doc=fs.readFileSync('docs/member-app/google-customer-master-sync-contract.md','utf8');

for(const phrase of [
 'Customer CRM remains the sole issuer/owner of canonical Customer ID.',
 'Customer ID is the only Customer update key.',
 'A newly registered pre-booking Member has no canonical Customer ID yet',
 'Prospect ID is the only Prospect update key',
 'Prospect History is append-only',
 'explicit promotion event copies/transforms the current Prospect profile into Customer Master',
 'The customer browser never talks directly to Google Sheets or GAS.',
 'same sync_event_id replay -> no duplicate history row',
 'older profile_version -> reject/no overwrite',
 'same profile_version with different payload digest -> conflict/review',
 'scheduled daily reconciliation',
 '"Within one day" is the maximum normal synchronization objective',
 'bounded seven-day cache',
 'Never guess identity and never return another customer\'s row.',
 'History rows are append-only.',
 'not an independent backup',
 'physically/logically separate backup artifact',
 'does not authorize Google Sheet creation'
]) assert.ok(doc.includes(phrase),`missing: ${phrase}`);

assert.ok(doc.includes('Name, phone, email, address, LINE display name, or fuzzy similarity must never select the Customer row to overwrite.'));
assert.ok(doc.includes('name, phone, email, address, LINE display name, or fuzzy similarity must never select a Prospect row.'));
assert.ok(doc.includes('Spreadsheet ID'));
assert.ok(doc.includes('GAS deployment secret'));
assert.ok(doc.includes('HMAC/shared secret'));
assert.ok(doc.includes('Google API credential'));

console.log('GOOGLE_CUSTOMER_MASTER_SYNC_CONTRACT=PASS');
console.log('CUSTOMER_UPDATE_KEY=CANONICAL_CUSTOMER_ID_ONLY');
console.log('BROWSER_DIRECT_GOOGLE_ACCESS=0');
console.log('HISTORY_APPEND_ONLY=YES');
console.log('SYNC_IDEMPOTENT=YES');
console.log('DAILY_RECONCILIATION=YES');
console.log('INDEPENDENT_BACKUP_REQUIRED=YES');
console.log('GOOGLE_OPERATION=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CRM_WRITE=0');
console.log('LINE_SEND=0');
console.log('UI_CHANGE=0');
