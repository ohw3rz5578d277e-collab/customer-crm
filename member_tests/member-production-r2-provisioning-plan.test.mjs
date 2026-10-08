import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { createMemberPrivateMediaStorageAdapter } from '../src/member-production-storage-adapter.mjs';
import { handleMemberProductionRequest } from '../src/member-production-request-composition.mjs';
import { handleMemberAppHttpRequest } from '../src/member-app-http-composition.mjs';
import { handleMemberPrivateMediaHttpRequest } from '../src/member-private-media-http-router.mjs';

const DOC='docs/member-app/member-production-r2-provisioning-plan.md';
const WRANGLER='wrangler.jsonc';
const ENTRY='src/production-index-crm-customer360-entry.js';
const ADAPTER='src/member-production-storage-adapter.mjs';
const RUNTIME_PATHS=['src/member-production-request-composition.mjs','src/member-app-http-composition.mjs','src/member-private-media-http-router.mjs','src/member-private-media-content-adapter.mjs'];
const BUCKET='customer-crm-member-private-media';
const BINDING='MEMBER_PRIVATE_MEDIA_BUCKET';
const BUCKET_SHA256=crypto.createHash('sha256').update(BUCKET).digest('hex');

const doc=fs.readFileSync(DOC,'utf8');
const cfg=JSON.parse(fs.readFileSync(WRANGLER,'utf8'));
const entry=fs.readFileSync(ENTRY,'utf8');
const adapter=fs.readFileSync(ADAPTER,'utf8');
const runtimeSources=RUNTIME_PATHS.map(path=>[path,fs.readFileSync(path,'utf8')]);

assert.equal(BUCKET_SHA256,'6f1f7fa25143081a302fcd148a52ae660195b2a31cf93a55f9ed39cf2e25f388');
assert.ok(doc.includes(`Canonical bucket name: \`${BUCKET}\``));
assert.ok(doc.includes(`Canonical Worker binding: \`${BINDING}\``));
assert.ok(doc.includes('Verified jurisdiction: `default`'));
assert.ok(doc.includes('Public access: disabled'));
assert.ok(doc.includes('source-only runtime-wiring stage'));
assert.ok(doc.includes(`\`${BINDING} -> ${BUCKET}\``));
assert.ok(doc.includes('Source wiring and Production storage fetch remain separate gates.'));
assert.ok(doc.includes('adapter creation itself performs no R2 access'));
assert.ok(doc.includes('route OFF prevents the adapter from being used'));
assert.ok(doc.includes('Production storage fetch remains unapproved and off'));
assert.ok(doc.includes('Member Production route and private-media route remain unapproved and off'));
assert.ok(doc.includes('LINE Login Production activation remains unapproved'));
assert.ok(doc.includes('No step inherits authorization from a previous step.'));
for(const unrelated of ['ai-manga-publisher-assets','album-originals','album-previews','instagram-thumbs'])assert.ok(doc.includes(`\`${unrelated}\``));

assert.deepEqual(cfg.r2_buckets,[{binding:BINDING,bucket_name:BUCKET}],'default Wrangler scope must contain exactly one canonical private-media binding');
const wranglerScopes=[['default',cfg],...Object.entries(cfg?.env||{}).map(([name,value])=>[`env.${name}`,value])];
for(const [scopeName,scope] of wranglerScopes){
  assert.notEqual(String(scope?.vars?.MEMBER_PRODUCTION_ROUTE_MODE||'').trim().toLowerCase(),'enabled',`${scopeName} Member Production route must remain disabled`);
  assert.notEqual(String(scope?.vars?.MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE||'').trim().toLowerCase(),'enabled',`${scopeName} private-media route must remain disabled`);
}
for(const [scopeName,scope] of Object.entries(cfg?.env||{}))assert.equal(Array.isArray(scope?.r2_buckets)?scope.r2_buckets.length:0,0,`env.${scopeName} must not add another R2 binding`);

assert.ok(entry.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=false;'));
assert.ok(entry.includes("import { createMemberPrivateMediaStorageAdapter } from './member-production-storage-adapter.mjs';"));
assert.ok(entry.includes('createMemberPrivateMediaStorageAdapter(env?.MEMBER_PRIVATE_MEDIA_BUCKET)'));
assert.ok(entry.includes('private_media_storage_adapter:privateMediaStorageAdapter'));
assert.ok(entry.includes('public_asset_adapter:null'));
assert.ok(entry.includes('line_login_approved:false'));
assert.ok(!entry.includes(BUCKET),'Production entry must consume binding identifier, not hardcode bucket name');
const memberInvocationMatches=[...entry.matchAll(/handleMemberProductionRequest\s*\(/g)];
assert.equal(memberInvocationMatches.length,1,'Production entry must have exactly one Member Production dispatch invocation');

for(const [path,source] of runtimeSources){
  assert.ok(!source.includes(BUCKET),`${path} must not discover configured bucket by name`);
  assert.ok(!source.includes(BINDING),`${path} must not discover configured binding from env`);
}

assert.ok(adapter.includes('createMemberPrivateMediaStorageAdapter(binding)'));
assert.ok(adapter.includes('async get(key)'));
assert.ok(adapter.includes('binding.get(String(key))'));
assert.ok(adapter.includes('Object.freeze'));
assert.ok(adapter.includes('implicit_env_binding:false'));
assert.ok(adapter.includes('storage_write:false'));
assert.ok(adapter.includes('storage_delete:false'));
assert.doesNotMatch(adapter,/\.put\s*\(/);
assert.doesNotMatch(adapter,/\.delete\s*\(/);

let poisonCalls=0;
const poisonBinding=Object.freeze({get(){poisonCalls++;throw new Error('POISON_R2_BINDING_TOUCHED');}});
const wiredAdapter=createMemberPrivateMediaStorageAdapter(poisonBinding);
assert.ok(wiredAdapter&&typeof wiredAdapter.get==='function');
assert.equal(poisonCalls,0,'adapter construction must not touch R2');

const routeOff=await handleMemberProductionRequest(new Request('https://example.test/api/member/probe'),{MEMBER_PRODUCTION_ROUTE_MODE:'disabled'},{approved:false,private_media_storage_adapter:wiredAdapter,api_handler:async()=>{throw new Error('route-off must not dispatch');}});
assert.equal(routeOff,null);
assert.equal(poisonCalls,0,'route OFF must not fetch R2');

const poisonEnv={MEMBER_PRODUCTION_ROUTE_MODE:'enabled',MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE:'enabled',[BINDING]:poisonBinding,UNRELATED_R2_BINDING:poisonBinding};
let productionPrivateObserved=Symbol('unset');
await handleMemberProductionRequest(new Request('https://example.test/api/member/probe'),poisonEnv,{approved:true,private_media_storage_adapter:null,api_handler:async(_request,_env,options)=>{productionPrivateObserved=options.storage_adapter;return new Response('ok');}});
assert.equal(productionPrivateObserved,null,'composition must not discover private R2 binding from env');
assert.equal(poisonCalls,0);

let appObserved=Symbol('unset');
await handleMemberAppHttpRequest(new Request('https://example.test/api/member/probe'),poisonEnv,{storage_adapter:null,line_login_handler:async()=>null,private_media_handler:async(_request,_env,options)=>{appObserved=options.storage_adapter;return new Response('ok');}});
assert.equal(appObserved,null,'Member app composition must preserve explicitly supplied adapter only');
assert.equal(poisonCalls,0);

let routerContentCalled=false;
const routerUnavailable=await handleMemberPrivateMediaHttpRequest(new Request('https://example.test/api/member/media/content',{method:'POST',headers:{origin:'https://example.test'}}),poisonEnv,{storage_adapter:null,content_handler:async()=>{routerContentCalled=true;return new Response('unexpected');}});
assert.equal(routerContentCalled,false,'private-media router must not fall back to env binding');
assert.equal(routerUnavailable.status,503);
assert.equal((await routerUnavailable.json()).error,'private_media_storage_adapter_unavailable');
assert.equal(poisonCalls,0);

const explicitAdapter=Object.freeze({get(){return null;}});
let explicitProductionObserved=null;
await handleMemberProductionRequest(new Request('https://example.test/api/member/probe'),poisonEnv,{approved:true,private_media_storage_adapter:explicitAdapter,api_handler:async(_request,_env,options)=>{explicitProductionObserved=options.storage_adapter;return new Response('ok');}});
assert.equal(explicitProductionObserved,explicitAdapter,'Production composition passes only explicit adapter');

console.log('MEMBER_R2_PROVISIONING_PLAN_CONTRACT=PASS');
console.log(`PROPOSED_BUCKET_SHA256=${BUCKET_SHA256}`);
console.log(`PROPOSED_BINDING=${BINDING}`);
console.log(`WRANGLER_SCOPES_CHECKED=${wranglerScopes.length}`);
console.log('CANONICAL_R2_BINDING_WIRING=SOURCE_ONLY');
console.log('ADAPTER_CONSTRUCTION_R2_ACCESS=0');
console.log('ROUTE_OFF_R2_OBJECT_FETCH=0');
console.log('R2_WRITE=0');
console.log('MEMBER_ROUTE_ACTIVATION=0');
console.log('PRIVATE_MEDIA_ROUTE_ACTIVATION=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_WRITE=0');
