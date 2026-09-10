import fs from 'node:fs';
import assert from 'node:assert/strict';

const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');

const principalCall='const effectiveRequest=await withOwnerPasswordPrincipal(request,env)';
const healthCall='const ownedHealth=await handleProductionHealthRequest(effectiveRequest,env)';
const mediaCall='const mediaApi=await handleCustomer360MediaRequest(effectiveRequest,env)';
const downstreamCall='let response=await app.fetch(effectiveRequest,env,ctx)';

assert.match(entry,/patchBrowserRootHealth/);
assert.match(entry,/patchReconciliationHealth/);
assert.match(entry,/handleProductionHealthRequest/);
assert.ok(entry.indexOf(principalCall)>=0,'owner principal normalization missing');
assert.ok(entry.indexOf(healthCall)>=0,'owned /health fast path missing');
assert.ok(entry.indexOf(mediaCall)>=0,'media API route missing');
assert.ok(entry.indexOf(downstreamCall)>=0,'downstream app.fetch missing');
assert.ok(entry.indexOf(principalCall)<entry.indexOf(healthCall),'principal normalization must happen before owned /health');
assert.ok(entry.indexOf(healthCall)<entry.indexOf(mediaCall),'owned /health must execute before media routing');
assert.ok(entry.indexOf(healthCall)<entry.indexOf(downstreamCall),'owned /health must execute before downstream app.fetch');
assert.match(entry,/customer360MediaHealth\(\)/);
assert.match(entry,/customer360MediaUiHealth\(\)/);
assert.match(entry,/ownerPasswordAuthHealth\(\)/);
assert.match(entry,/customer360_profile_media_schema_available/);
assert.match(entry,/customer360_delivery_links_schema_available/);
assert.match(entry,/url\.pathname==='\/api\/crm-health-check'/);
assert.doesNotMatch(entry,/url\.pathname==='\/health'\|\|url\.pathname==='\/api\/crm-health-check'/);

console.log('CUSTOMER360_MEDIA_PR52_HEALTH_FAST_PATH_INTEGRATION=PASS');
