import assert from 'node:assert/strict';
import fs from 'node:fs';
import { patchHealth } from '../src/production-index-crm-customer360-entry.js';

const source=fs.readFileSync(new URL('../src/production-index-crm-customer360-entry.js',import.meta.url),'utf8');
assert.match(source,/url\.pathname==='\/health'\|\|url\.pathname==='\/api\/crm-health-check'/,'Production entry must own the canonical health routes');

async function run(status,payload){
  const response=new Response(JSON.stringify(payload),{status,headers:{'content-type':'application/json'}});
  const patched=await patchHealth(response,{});
  return {status:patched.status,data:await patched.json()};
}

for(const payload of [
  {ok:false,message:'Not Found'},
  {ok:false,error:'route_not_found',message:'Not Found'}
]){
  const x=await run(404,payload);
  assert.equal(x.status,200);
  assert.equal(x.data.ok,true);
  assert.equal(x.data.service,'customer-crm-api');
  assert.equal('message' in x.data,false);
  assert.equal('error' in x.data,false);
  for(const key of ['customer360_build','customer360_identity_fallback','customer360_paid_ai_provider_active','customer360_profile_enrichment_schema_available'])assert.ok(key in x.data,`missing health marker ${key}`);
}

for(const status of [401,403,500,503]){
  const x=await run(status,{ok:false,error:`status_${status}`});
  assert.equal(x.status,status);
  assert.equal(x.data.ok,false);
  assert.equal(x.data.error,`status_${status}`);
}

const success=await run(200,{ok:true,service:'lower-service',lower_marker:'retained'});
assert.equal(success.status,200);
assert.equal(success.data.ok,true);
assert.equal(success.data.service,'lower-service');
assert.equal(success.data.lower_marker,'retained');

console.log('CUSTOMER360_PRODUCTION_HEALTH_ROUTE_OWNERSHIP=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_INHERITED_404_TO_200=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_INHERITED_404_PAYLOAD_NORMALIZED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_401_403_500_503_PRESERVED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_NORMAL_SUCCESS_PRESERVED=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_SERVICE_MARKER=PASS');
