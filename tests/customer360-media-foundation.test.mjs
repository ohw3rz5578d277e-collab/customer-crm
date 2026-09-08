import fs from 'node:fs';
import assert from 'node:assert/strict';

const migration=fs.readFileSync('migrations_managed/20260908_customer_media_delivery_links.sql','utf8');
const api=fs.readFileSync('src/crm-customer360-media.mjs','utf8');
const ui=fs.readFileSync('src/crm-customer360-media-ui.mjs','utf8');
const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');

assert.doesNotMatch(migration,/\bDROP\s+(?:TABLE|COLUMN)\b/i);
assert.doesNotMatch(migration,/\bALTER\s+TABLE\b/i);
for(const statement of migration.split(';').map(x=>x.trim()).filter(Boolean)){
  assert.match(statement,/^CREATE\s+(?:TABLE|INDEX)\s+IF\s+NOT\s+EXISTS\b/i);
}
assert.match(migration,/customer_profile_media/);
assert.match(migration,/customer_delivery_links/);
assert.match(api,/CRM_CUSTOMER360_WRITE_ENABLED/);
assert.match(api,/MAX_AVATAR_CHARS=96000/);
assert.match(api,/amazon_photos/);
assert.match(api,/^const CUSTOMER_ID_RE=\/\^\\d\{8\}\$\//);
assert.match(api,/customer360_media_customer_id_generation:false/);
assert.match(api,/customer360_media_paid_storage_required:false/);
assert.match(ui,/crm-customer360-media-ui-20260908-01/);
assert.match(ui,/写真・納品/);
assert.match(ui,/Amazon Photos 納品リンク/);
assert.match(ui,/toDataURL\('image\/jpeg',0\.72\)/);
assert.match(ui,/h\.querySelector\('#crmCustomerMediaCard'\)\?\.remove\(\)/);
assert.match(entry,/handleCustomer360MediaRequest/);
assert.match(entry,/injectCustomer360MediaUi/);
assert.match(entry,/customer360MediaUiHealth/);
assert.match(entry,/customer360_profile_media_schema_available/);
assert.match(entry,/customer360_delivery_links_schema_available/);

console.log('CUSTOMER360_MEDIA_FOUNDATION=PASS');
