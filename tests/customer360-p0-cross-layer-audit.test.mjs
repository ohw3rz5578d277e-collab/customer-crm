import assert from 'node:assert/strict';
import fs from 'node:fs';

const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const media=fs.readFileSync('src/crm-customer360-media.mjs','utf8');
const mediaUi=fs.readFileSync('src/crm-customer360-media-ui.mjs','utf8');
const edit=fs.readFileSync('src/crm-customer360-exact-edit-handoff.mjs','utf8');
const profile=fs.readFileSync('src/crm-customer360-profile-enrichment.mjs','utf8');

assert.equal((entry.match(/injectCustomer360ExactEditHandoff/g)||[]).length,2,'exact edit handoff import/invocation count');
assert.equal((entry.match(/injectCustomer360MediaUi/g)||[]).length,2,'media UI import/invocation count');
assert.equal((entry.match(/handleCustomer360MediaRequest/g)||[]).length,2,'media API import/route count');
const principalCall='const effectiveRequest=await withOwnerPasswordPrincipal(request,env)';
const healthCall='handleProductionHealthRequest(effectiveRequest,env)';
const mediaCall='handleCustomer360MediaRequest(effectiveRequest,env)';
assert.ok(entry.indexOf(principalCall)>=0,'Owner principal normalization missing');
assert.ok(entry.indexOf(healthCall)>=0,'Production health route missing');
assert.ok(entry.indexOf(mediaCall)>=0,'Customer360 media route missing');
assert.ok(entry.indexOf(principalCall)<entry.indexOf(healthCall),'Owner principal normalization must precede Production health');
assert.ok(entry.indexOf(healthCall)<entry.indexOf(mediaCall),'Production health must remain ahead of media route');
assert.ok(entry.includes('customer360ExactEditHandoffHealth()'),'exact edit handoff health missing');

assert.ok(edit.includes("edit_customer')==='1"),'edit handoff query gate missing');
assert.ok(edit.includes('/^[0-9]{8}$/'),'exact Customer ID validation missing');
assert.ok(edit.includes("document.getElementById('crmPeEdit')"),'handoff must reuse existing Owner editor');
assert.ok(!/fetch\s*\(/.test(edit),'handoff must not introduce direct API writes or reads');
assert.ok(!edit.includes('document.documentElement'),'handoff must not observe document root');
assert.ok(!edit.includes("addEventListener('resize'"),'handoff must not add resize redecorate');

assert.ok(profile.includes("CRM_CUSTOMER360_WRITE_ENABLED!=='1'"),'existing Customer360 write gate missing');
assert.ok(profile.includes("if('customer_id'in body)return json({ok:false,error:'customer_id_readonly'}"),'customer_id readonly contract missing');
assert.ok(media.includes('CRM_CUSTOMER360_WRITE_ENABLED'),'media write gate missing');
assert.ok(mediaUi.includes('window.__crmCustomerMediaUi20260908'),'media UI singleton missing');
assert.ok(!mediaUi.includes('document.documentElement'),'media UI must not observe document root');

console.log('CUSTOMER360_P0_CROSS_LAYER_AUDIT=PASS');
