import assert from 'node:assert/strict';
import fs from 'node:fs';
import { patchHealth, handleProductionHealthRequest } from '../src/production-index-crm-customer360-entry.js';

const source=fs.readFileSync(new URL('../src/production-index-crm-customer360-entry.js',import.meta.url),'utf8');
assert.match(source,/handleProductionHealthRequest\(request,env\)/,'Production entry must own /health before downstream app.fetch');
assert.match(source,/url\.pathname==='\/api\/crm-health-check'/,'Production entry must still own the legacy CRM health route');
assert.ok(source.indexOf('const ownedHealth=await handleProductionHealthRequest(request,env)')<source.indexOf('let response=await app.fetch(request,env,ctx)'),'Production /health fast path must execute before downstream app.fetch');
assert.doesNotMatch(source,/if\(inheritedNotFound\)data=\{\};/,'Inherited 404 must not discard accumulated lower health markers');

async function run(status,payload){
  const response=new Response(JSON.stringify(payload),{status,headers:{'content-type':'application/json'}});
  const patched=await patchHealth(response,{});
  return {status:patched.status,data:await patched.json()};
}

const readSql=[];
const readOnlyEnv={
  DB:{
    prepare(sql){
      readSql.push(sql);
      return{
        bind(){
          return{
            all:async()=>({results:[
              {name:'customer_profile_enrichment'},
              {name:'customer_family_member_metadata'},
              {name:'customer_field_evidence'},
              {name:'customer_notes_history'}
            ]})
          };
        }
      };
    }
  }
};
const owned=await handleProductionHealthRequest(new Request('https://customer-crm-api.example/health'),readOnlyEnv);
assert.equal(owned.status,200);
const ownedData=await owned.json();
for(const key of [
  'customer360_family_marketing_foundation',
  'customer360_profile_enrichment',
  'customer360_line_profile_extraction',
  'referrer_customer_id_exact_existing_customer_only',
  'customer360_profile_composed_into_detail',
  'customer360_profile_enrichment_schema_available',
  'customer360_family_metadata_available',
  'customer360_field_evidence_available',
  'customer360_notes_history_available'
])assert.equal(ownedData[key],true,`owned health marker missing: ${key}`);
assert.equal(ownedData.customer360_profile_initial_extra_request,false);
assert.equal(ownedData.customer_id_generation,false);
assert.equal(ownedData.customer_line_auto_apply,false);
assert.equal(ownedData.line_profile_auto_apply,false);
assert.equal(ownedData.customer_line_extraction_mode,'candidate-only');
assert.equal(ownedData.customer360_identity_fallback,false);
assert.equal(ownedData.customer360_paid_ai_provider_active,false);
assert.equal(ownedData.line_event_direction,'incoming');
assert.equal(ownedData.line_event_receive_status,'received');
assert.equal(readSql.length,1);
assert.match(readSql[0],/^SELECT /);
assert.doesNotMatch(readSql[0],/CREATE|ALTER|INSERT|UPDATE|DELETE/i);
assert.equal(await handleProductionHealthRequest(new Request('https://customer-crm-api.example/api/crm-health-check'),readOnlyEnv),null);

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

console.log('CUSTOMER360_PRODUCTION_HEALTH_READ_ONLY_FAST_PATH=PASS');
