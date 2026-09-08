import fs from 'node:fs';
import assert from 'node:assert/strict';

const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const ui=fs.readFileSync('src/crm-customer360-media-ui.mjs','utf8');
const api=fs.readFileSync('src/crm-customer360-media.mjs','utf8');

function occurrences(source,needle){return source.split(needle).length-1}

assert.equal(occurrences(entry,"./crm-customer360-media-ui.mjs"),1);
assert.equal(occurrences(entry,"./crm-customer360-media.mjs"),1);
assert.equal(occurrences(entry,'injectCustomer360MediaUi'),2); // import + single composition
assert.equal(occurrences(entry,'handleCustomer360MediaRequest'),2); // import + single routing call
assert.match(ui,/window\.__crmCustomerMediaUi20260908/);
assert.match(ui,/h\.querySelector\('#crmCustomerMediaCard'\)\?\.remove\(\)/);
assert.doesNotMatch(ui,/document\.documentElement/);
assert.doesNotMatch(ui,/addEventListener\(['\"]resize/);
assert.match(api,/CRM_CUSTOMER360_WRITE_ENABLED/);
assert.match(api,/customer360_media_customer_id_generation:false/);
assert.match(api,/customer360_media_paid_storage_required:false/);
assert.doesNotMatch(api,/INSERT\s+INTO\s+customers/i);
assert.doesNotMatch(api,/UPDATE\s+customers/i);

console.log('CUSTOMER360_MEDIA_DUPLICATION_AUDIT=PASS');
