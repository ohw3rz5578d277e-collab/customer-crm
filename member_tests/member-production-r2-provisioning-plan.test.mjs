import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { handleMemberProductionRequest } from '../src/member-production-request-composition.mjs';
import { handleMemberAppHttpRequest } from '../src/member-app-http-composition.mjs';
import { handleMemberPrivateMediaHttpRequest } from '../src/member-private-media-http-router.mjs';

const DOC='docs/member-app/member-production-r2-provisioning-plan.md';
const WRANGLER='wrangler.jsonc';
const ENTRY='src/production-index-crm-customer360-entry.js';
const ADAPTER='src/member-production-storage-adapter.mjs';
const RUNTIME_PATHS=[
  'src/member-production-request-composition.mjs',
  'src/member-app-http-composition.mjs',
  'src/member-private-media-http-router.mjs',
  'src/member-private-media-content-adapter.mjs'
];

const BUCKET='customer-crm-member-private-media';
const BINDING='MEMBER_PRIVATE_MEDIA_BUCKET';
const BUCKET_SHA256=crypto.createHash('sha256').update(BUCKET).digest('hex');

const doc=fs.readFileSync(DOC,'utf8');
const wranglerText=fs.readFileSync(WRANGLER,'utf8');
const cfg=JSON.parse(wranglerText);
const entry=fs.readFileSync(ENTRY,'utf8');
const adapter=fs.readFileSync(ADAPTER,'utf8');
const runtimeSources=RUNTIME_PATHS.map(path=>[path,fs.readFileSync(path,'utf8')]);

assert.match(BUCKET,/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/);
assert.equal(BUCKET,'customer-crm-member-private-media');
assert.equal(BINDING,'MEMBER_PRIVATE_MEDIA_BUCKET');
assert.equal(BUCKET_SHA256,'6f1f7fa25143081a302fcd148a52ae660195b2a31cf93a55f9ed39cf2e25f388');

assert.ok(doc.includes(`Proposed bucket name: \`${BUCKET}\``));
assert.ok(doc.includes(`Proposed Worker binding: \`${BINDING}\``));
assert.ok(doc.includes('Authorized jurisdiction target after separate Owner approval: `default`'));
assert.ok(doc.includes('Public access: disabled'));
assert.ok(doc.includes('The first bucket is private-media-only.'));
assert.ok(doc.includes('No step inherits authorization from a previous step.'));
assert.ok(doc.includes('candidate verify run must prove exact-one-match'));
assert.ok(doc.includes('Adding the binding, Production deploy, private-media route activation, Member route activation, and Production storage fetch each remain separately Owner-gated.'));

for(const unrelated of [
  'ai-manga-publisher-assets',
  'album-originals',
  'album-previews',
  'instagram-thumbs'
]){
  assert.ok(doc.includes(`\`${unrelated}\``));
}

const wranglerScopes=[
  ['default',cfg],
  ...Object.entries(cfg?.env||{}).map(([name,value])=>[`env.${name}`,value])
];
for(const [scopeName,scope] of wranglerScopes){
  assert.ok(
    !Array.isArray(scope?.r2_buckets)||scope.r2_buckets.length===0,
    `${scopeName} must remain without R2 bindings`
  );
  assert.notEqual(
    String(scope?.vars?.MEMBER_PRODUCTION_ROUTE_MODE||'').trim().toLowerCase(),
    'enabled',
    `${scopeName} Member Production route must remain disabled`
  );
  assert.notEqual(
    String(scope?.vars?.MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE||'').trim().toLowerCase(),
    'enabled',
    `${scopeName} private-media route must remain disabled`
  );
  const serialized=JSON.stringify(scope);
  assert.ok(!serialized.includes(BUCKET),`${scopeName} must not configure proposed bucket`);
  assert.ok(!serialized.includes(BINDING),`${scopeName} must not configure proposed binding`);
}
assert.ok(!wranglerText.includes(BUCKET),'proposed bucket must not be configured before separate authorization');
assert.ok(!wranglerText.includes(BINDING),'proposed binding must not be configured before separate authorization');

assert.ok(entry.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=false;'));
assert.ok(!entry.includes(BUCKET));
assert.ok(!entry.includes(BINDING));
const memberInvocationMatches=[...entry.matchAll(/handleMemberProductionRequest\s*\(/g)];
assert.equal(memberInvocationMatches.length,1,'Production entry must have exactly one Member Production dispatch invocation');
assert.match(
  entry,
  /const memberResponse=await handleMemberProductionRequest\(request,env,\{\s*approved:MEMBER_PRODUCTION_OWNER_APPROVED,\s*line_login_approved:false,\s*public_asset_adapter:null,\s*private_media_storage_adapter:null\s*\}\);/,
  'Production entry must pass literal null storage adapters until separately authorized wiring is reviewed'
);

for(const [path,source] of runtimeSources){
  assert.ok(!source.includes(BUCKET),`${path} must not discover the proposed bucket by name`);
  assert.ok(!source.includes(BINDING),`${path} must not discover the proposed binding from env`);
}

assert.ok(adapter.includes('createMemberPrivateMediaStorageAdapter(binding)'));
assert.ok(adapter.includes('createMemberPublicAssetStorageAdapter(binding)'));
assert.ok(adapter.includes('async get(key)'));
assert.ok(adapter.includes('binding.get(String(key))'));
assert.ok(adapter.includes('Object.freeze'));
assert.ok(adapter.includes('implicit_env_binding:false'));
assert.ok(adapter.includes('storage_write:false'));
assert.ok(adapter.includes('storage_delete:false'));
assert.doesNotMatch(adapter,/\.put\s*\(/);
assert.doesNotMatch(adapter,/\.delete\s*\(/);

const poisonBinding=Object.freeze({
  get(){throw new Error('POISON_R2_BINDING_TOUCHED');}
});
const poisonEnv={
  MEMBER_PRODUCTION_ROUTE_MODE:'enabled',
  MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE:'enabled',
  [BINDING]:poisonBinding,
  UNRELATED_R2_BINDING:poisonBinding
};

let productionPrivateObserved=Symbol('unset');
await handleMemberProductionRequest(
  new Request('https://example.test/api/member/probe'),
  poisonEnv,
  {
    approved:true,
    private_media_storage_adapter:null,
    api_handler:async(_request,_env,options)=>{
      productionPrivateObserved=options.storage_adapter;
      return new Response('ok');
    }
  }
);
assert.equal(productionPrivateObserved,null,'Production composition must not discover private R2 bindings from env');

let productionPublicObserved=Symbol('unset');
await handleMemberProductionRequest(
  new Request('https://example.test/member-assets/probe.jpg'),
  poisonEnv,
  {
    approved:true,
    public_asset_adapter:null,
    asset_handler:async(_request,_env,options)=>{
      productionPublicObserved=options.asset_adapter;
      return new Response('ok');
    }
  }
);
assert.equal(productionPublicObserved,null,'Production composition must not discover public R2 bindings from env');

let appObserved=Symbol('unset');
await handleMemberAppHttpRequest(
  new Request('https://example.test/api/member/probe'),
  poisonEnv,
  {
    storage_adapter:null,
    line_login_handler:async()=>null,
    private_media_handler:async(_request,_env,options)=>{
      appObserved=options.storage_adapter;
      return new Response('ok');
    }
  }
);
assert.equal(appObserved,null,'Member app composition must preserve explicit null storage adapter');

let routerContentCalled=false;
const routerUnavailable=await handleMemberPrivateMediaHttpRequest(
  new Request('https://example.test/api/member/media/content',{
    method:'POST',
    headers:{origin:'https://example.test'}
  }),
  poisonEnv,
  {
    storage_adapter:null,
    content_handler:async()=>{
      routerContentCalled=true;
      return new Response('unexpected');
    }
  }
);
assert.equal(routerContentCalled,false,'private-media router must not fall back to an env binding');
assert.equal(routerUnavailable.status,503);
assert.equal((await routerUnavailable.json()).error,'private_media_storage_adapter_unavailable');

const explicitAdapter=Object.freeze({get(){return null;}});
let explicitProductionObserved=null;
await handleMemberProductionRequest(
  new Request('https://example.test/api/member/probe'),
  poisonEnv,
  {
    approved:true,
    private_media_storage_adapter:explicitAdapter,
    api_handler:async(_request,_env,options)=>{
      explicitProductionObserved=options.storage_adapter;
      return new Response('ok');
    }
  }
);
assert.equal(explicitProductionObserved,explicitAdapter,'Production composition must pass only the explicit private adapter');

let explicitAppObserved=null;
await handleMemberAppHttpRequest(
  new Request('https://example.test/api/member/probe'),
  poisonEnv,
  {
    storage_adapter:explicitAdapter,
    line_login_handler:async()=>null,
    private_media_handler:async(_request,_env,options)=>{
      explicitAppObserved=options.storage_adapter;
      return new Response('ok');
    }
  }
);
assert.equal(explicitAppObserved,explicitAdapter,'Member app composition must pass only the explicit private adapter');

let explicitRouterObserved=null;
const routerExplicit=await handleMemberPrivateMediaHttpRequest(
  new Request('https://example.test/api/member/media/content',{
    method:'POST',
    headers:{origin:'https://example.test'}
  }),
  poisonEnv,
  {
    storage_adapter:explicitAdapter,
    content_handler:async(_request,_env,storageAdapter)=>{
      explicitRouterObserved=storageAdapter;
      return new Response('ok');
    }
  }
);
assert.equal(routerExplicit.status,200);
assert.equal(explicitRouterObserved,explicitAdapter,'private-media router must pass only the explicit adapter');

console.log('MEMBER_R2_PROVISIONING_PLAN_CONTRACT=PASS');
console.log(`PROPOSED_BUCKET_SHA256=${BUCKET_SHA256}`);
console.log(`PROPOSED_BINDING=${BINDING}`);
console.log(`WRANGLER_SCOPES_CHECKED=${wranglerScopes.length}`);
console.log('POISON_ENV_IMPLICIT_BINDING=REJECTED');
console.log('CANONICAL_R2_BINDING_CONFIGURED=NO');
console.log('BUCKET_CREATE=0');
console.log('R2_OBJECT_READ=0');
console.log('R2_WRITE=0');
console.log('PRODUCTION_STORAGE_BINDING_CHANGE=0');
console.log('PRODUCTION_STORAGE_FETCH=0');
console.log('MEMBER_ROUTE_ACTIVATION=0');
console.log('PRIVATE_MEDIA_ROUTE_ACTIVATION=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_WRITE=0');
