import fs from 'node:fs';
import assert from 'node:assert/strict';

const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const client=fs.readFileSync('src/crm-customer360-ui-client.mjs','utf8');
const mediaUi=fs.readFileSync('src/crm-customer360-media-ui.mjs','utf8');
const editHandoff=fs.readFileSync('src/crm-customer360-exact-edit-handoff.mjs','utf8');

function occurrences(source,needle){return source.split(needle).length-1}

assert.match(client,/RESERVATION_ADMIN_URL/,'one-screen Customer360 reservation navigation missing');
assert.match(client,/CUSTOMER 360/,'one-screen Customer360 detail marker missing');
assert.match(client,/メモ/,'one-screen Customer360 memo section missing');
assert.equal(occurrences(entry,'injectCustomer360MediaUi'),2,'media UI must remain one import plus one composition');
assert.equal(occurrences(entry,'injectCustomer360ExactEditHandoff'),2,'exact edit handoff must remain one import plus one composition');
assert.match(mediaUi,/写真・納品/,'media section missing after one-screen integration');
assert.match(mediaUi,/Amazon Photos 納品リンク/,'Amazon Photos delivery UI missing');
assert.match(editHandoff,/edit_customer/,'exact edit handoff query missing');
assert.doesNotMatch(editHandoff,/fetch\s*\(/,'exact edit handoff must remain navigation-only');
assert.doesNotMatch(mediaUi,/document\.documentElement/,'media UI must not observe document root');
assert.doesNotMatch(mediaUi,/addEventListener\(['\"]resize['\"]/,'media UI must not add resize redecorate');

console.log('CUSTOMER360_MEDIA_ONE_SCREEN_DETAIL_INTEGRATION=PASS');
console.log('CUSTOMER360_MEDIA_ONE_SCREEN_DIRECT_WRITE=0');
