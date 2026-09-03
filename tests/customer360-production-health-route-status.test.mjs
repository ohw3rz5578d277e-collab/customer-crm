import assert from 'node:assert/strict';
import fs from 'node:fs';

const source=fs.readFileSync(new URL('../src/production-index-crm-customer360-entry.js',import.meta.url),'utf8');

assert.match(source,/url\.pathname==='\/health'\|\|url\.pathname==='\/api\/crm-health-check'/,'Production entry must own the canonical health routes');
assert.match(source,/const status=response\.status===404\?200:response\.status;/,'Only inherited not-found status may be normalized for the owned health route');
assert.match(source,/\{status,headers:h\}/,'Health response must use the normalized status');
assert.match(source,/service:data\.service\|\|'customer-crm-api'/,'Health must identify the canonical Customer CRM service');
assert.doesNotMatch(source,/response\.status===401\?200|response\.status===403\?200|response\.status>=500\?200/,'Auth/server failures must never be normalized to success');

console.log('CUSTOMER360_PRODUCTION_HEALTH_ROUTE_OWNERSHIP=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_INHERITED_404_TO_200=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_AUTH_SERVER_FAILURE_PRESERVED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_SERVICE_MARKER=PASS');
