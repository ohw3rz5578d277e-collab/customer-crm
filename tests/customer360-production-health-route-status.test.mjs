import assert from 'node:assert/strict';
import fs from 'node:fs';
import { patchHealth } from '../src/production-index-crm-customer360-entry.js';

const source=fs.readFileSync(new URL('../src/production-index-crm-customer360-entry.js',import.meta.url),'utf8');
assert.match(source,/url\.pathname==='\/health'\|\|url\.pathname==='\/api\/crm-health-check'/,'Production entry must own the canonical health routes');
assert.doesNotMatch(source,/if\(inheritedNotFound\)data=\{\};/,'Inherited 404 must not discard accumulated lower health markers');

async function run(status,payload){
  const response=new Response(JSON.stringify(payload),{status,headers:{'content-type':'application/json'}});
  const patched=await patchHealth(response,{});
  return {status:patched.status,data:await patched.json()};
}

const lowerMarkers={
  customer360_browser_marketing:true,
  customer360_customer_management:true,
  customer360_customer_detail:true,
  customer360_search:true,
  customer360_filter:true,
  customer360_refresh_profile:true,
  customer360_profile_auto_enrichment:true,
  custom_lower_marker:true
};

for(const payload of [
  {ok:false,message:'Not Found',...lowerMarkers},
  {ok:false,error:'route_not_found',message:'Not Found',...lowerMarkers}
]){
  const x=await run(404,payload);
  assert.equal(x.status,200);
  assert.equal(x.data.ok,true);
  assert.equal(x.data.service,'customer-crm-api');
  assert.equal('message' in x.data,false);
  assert.equal('error' in x.data,false);
  for(const [key,value] of Object.entries(lowerMarkers))assert.equal(x.data[key],value,`lower health marker lost: ${key}`);
  for(const key of ['customer360_build','customer360_identity_fallback','customer360_paid_ai_provider_active','customer360_profile_enrichment_schema_available'])assert.ok(key in x.data,`missing top-level health marker ${key}`);
}

for(const status of [401,403,429,500,503]){
  const x=await run(status,{ok:false,error:`status_${status}`,message:`failure_${status}`,custom_lower_marker:true});
  assert.equal(x.status,status);
  assert.equal(x.data.ok,false);
  assert.equal(x.data.error,`status_${status}`);
  assert.equal(x.data.message,`failure_${status}`);
  assert.equal(x.data.custom_lower_marker,true);
}

const success=await run(200,{ok:true,service:'lower-service',custom_lower_marker:true,another_lower_marker:'preserve-me'});
assert.equal(success.status,200);
assert.equal(success.data.ok,true);
assert.equal(success.data.service,'lower-service');
assert.equal(success.data.custom_lower_marker,true);
assert.equal(success.data.another_lower_marker,'preserve-me');

console.log('CUSTOMER360_PRODUCTION_HEALTH_ROUTE_OWNERSHIP=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_INHERITED_404_TO_200=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_INHERITED_404_STALE_SEMANTICS_REMOVED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_LOWER_MARKERS_PRESERVED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_TOP_LEVEL_MARKERS_PRESENT=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_401_403_429_500_503_PRESERVED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_NORMAL_SUCCESS_PRESERVED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_SERVICE_MARKER=PASS');
