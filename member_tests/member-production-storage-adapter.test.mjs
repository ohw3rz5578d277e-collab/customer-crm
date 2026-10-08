import assert from 'node:assert/strict';
import {
  createMemberPublicAssetStorageAdapter,
  createMemberPrivateMediaStorageAdapter,
  memberProductionStorageAdapterHealth,
  __test
} from '../src/member-production-storage-adapter.mjs';

function pass(name,condition){
  assert.equal(condition,true,name);
  console.log('PASS',name);
}

pass('invalid binding is rejected',createMemberPublicAssetStorageAdapter(null)===null);
pass('binding requires get function',createMemberPrivateMediaStorageAdapter({})===null);

let publicCalls=0;
const publicBinding={
  async get(key){
    publicCalls+=1;
    assert.equal(key,'/member-assets/creative/hero.webp');
    return {
      body:new Uint8Array([1,2,3]),
      size:3,
      httpMetadata:{contentType:'image/webp'},
      etag:'public-etag'
    };
  },
  async put(){throw new Error('must not be exposed');},
  async delete(){throw new Error('must not be exposed');}
};
const publicAdapter=createMemberPublicAssetStorageAdapter(publicBinding);
const publicObject=await publicAdapter.get('/member-assets/creative/hero.webp');
pass('public adapter performs one exact read',publicCalls===1);
pass('public adapter normalizes body size and mime',
  publicObject?.size===3
  && publicObject?.content_type==='image/webp'
  && publicObject?.body instanceof Uint8Array
);
pass('public adapter does not expose write methods',
  typeof publicAdapter.put==='undefined'&&typeof publicAdapter.delete==='undefined'
);

await publicAdapter.get('https://example.com/member-assets/x.webp');
await publicAdapter.get('/member-assets/../private/x.webp');
await publicAdapter.get('/other/x.webp');
pass('invalid public keys never reach binding',publicCalls===1);

let privateCalls=0;
const privateBinding={
  async get(key){
    privateCalls+=1;
    assert.equal(key,'member/fam_A/mem_A/cover.jpg');
    return {
      body:new Uint8Array([9,8,7,6]),
      size:4,
      httpMetadata:{contentType:'image/jpeg'},
      etag:'private-etag'
    };
  }
};
const privateAdapter=createMemberPrivateMediaStorageAdapter(privateBinding);
const privateObject=await privateAdapter.get('member/fam_A/mem_A/cover.jpg');
pass('private adapter performs one exact read',privateCalls===1);
pass('private adapter preserves normalized etag',
  privateObject?.size===4
  && privateObject?.content_type==='image/jpeg'
  && privateObject?.etag==='private-etag'
);

await privateAdapter.get('/member/fam_A/mem_A/cover.jpg');
await privateAdapter.get('member/fam_A/../fam_B/cover.jpg');
await privateAdapter.get('https://example.com/private.jpg');
pass('invalid private keys never reach binding',privateCalls===1);

pass('normalizer accepts direct content_type fallback',
  __test.normalizeObject({body:new Uint8Array([1]),size:1,content_type:'image/png'})?.content_type==='image/png'
);
pass('normalizer rejects missing body',__test.normalizeObject({size:1,content_type:'image/png'})===null);
pass('normalizer rejects invalid size',__test.normalizeObject({body:new Uint8Array([1]),size:-1,content_type:'image/png'})===null);

const health=memberProductionStorageAdapterHealth();
pass('health is source-only read-only',health.source_only===true&&health.read_only===true);
pass('health requires explicit binding argument',health.explicit_binding_argument_required===true&&health.implicit_env_binding===false);
pass('health exposes get only and no storage mutation',health.get_only===true&&health.storage_write===false&&health.storage_delete===false);
pass('health does not claim Production binding or activation',
  health.production_binding_configured===false
  && health.production_storage_fetch===false
  && health.production_route_activated===false
  && health.production_deploy===false
  && health.production_write===false
);

let sourceOnlyWiringCalls=0;
const sourceOnlyBinding={
  async get(){
    sourceOnlyWiringCalls+=1;
    throw new Error('route-off source wiring must not fetch R2');
  }
};
const sourceOnlyAdapter=createMemberPrivateMediaStorageAdapter(sourceOnlyBinding);
pass('private adapter creation does not access R2',sourceOnlyAdapter!==null&&sourceOnlyWiringCalls===0);

const {handleMemberProductionRequest}=await import('../src/member-production-request-composition.mjs');
const routeOffResult=await handleMemberProductionRequest(
  new Request('https://member.example.test/api/member/memories/mem_A/media'),
  {MEMBER_PRODUCTION_ROUTE_MODE:'disabled'},
  {
    approved:false,
    private_media_storage_adapter:sourceOnlyAdapter,
    api_handler:async()=>{throw new Error('route-off composition must not dispatch');}
  }
);
pass('route OFF with wired private adapter performs no R2 fetch',routeOffResult===null&&sourceOnlyWiringCalls===0);

console.log('MEMBER_PRODUCTION_STORAGE_ADAPTER_FOUNDATION=PASS');
console.log('PRODUCTION_BINDING_CHANGE=0');
console.log('PRODUCTION_STORAGE_FETCH=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_WRITE=0');
