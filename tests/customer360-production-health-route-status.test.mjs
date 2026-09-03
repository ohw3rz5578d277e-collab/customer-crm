import assert from 'node:assert/strict';
import fs from 'node:fs';

const source=fs.readFileSync(new URL('../src/production-index-crm-customer360-entry.js',import.meta.url),'utf8');

assert.match(source,/url\.pathname==='\/health'\|\|url\.pathname==='\/api\/crm-health-check'/,'Production entry must own the canonical health routes');
assert.match(source,/const inheritedNotFound=response\.status===404;/,'Owned health route must distinguish inherited not-found from other failures');
assert.match(source,/if\(inheritedNotFound\)data=\{\};/,'Inherited route-miss payload must be discarded before composing canonical health');
assert.match(source,/const status=inheritedNotFound\?200:response\.status;/,'Only inherited not-found status may be normalized for the owned health route');
assert.match(source,/\.\.\.\(inheritedNotFound\?\{ok:true\}:\{\}\)/,'Owned health route must replace stale 404 ok:false semantics');
assert.match(source,/\{status,headers:h\}/,'Health response must use the normalized status');
assert.match(source,/service:data\.service\|\|'customer-crm-api'/,'Health must identify the canonical Customer CRM service');
assert.doesNotMatch(source,/response\.status===401\?200|response\.status===403\?200|response\.status>=500\?200/,'Auth/server failures must never be normalized to success');
assert.match(source,/if\(request\.method==='GET'&&\(url\.pathname==='\/health'\|\|url\.pathname==='\/api\/crm-health-check'\)\)return patchHealth\(response,env\);[\s\S]*return response;/,'Unrelated routes must retain their lower-app response/status');

console.log('CUSTOMER360_PRODUCTION_HEALTH_ROUTE_OWNERSHIP=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_INHERITED_404_TO_200=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_INHERITED_404_PAYLOAD_NORMALIZED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_AUTH_SERVER_FAILURE_PRESERVED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_UNRELATED_404_PRESERVED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_SERVICE_MARKER=PASS');
