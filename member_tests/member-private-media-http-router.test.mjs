import {
  handleMemberPrivateMediaHttpRequest,
  memberPrivateMediaHttpRouterHealth,
  __test
} from '../src/member-private-media-http-router.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const origin='https://member.example.test';

function request(path,body,options={}){
  const headers={
    origin:options.origin??origin,
    'content-type':'application/json',
    cookie:'__Host-mizuno_member_session=signed-session',
    ...(options.headers||{})
  };
  return new Request(origin+path,{
    method:options.method||'POST',
    headers,
    body:body===undefined?undefined:JSON.stringify(body)
  });
}

const unknown=await handleMemberPrivateMediaHttpRequest(
  request('/api/member/unknown',{media_id:'m1'}),
  {}
);
pass('unrelated Member path is not claimed',unknown===null);

let grantCalls=0;
const wrongMethod=await handleMemberPrivateMediaHttpRequest(
  request(__test.PUBLIC_GRANT_PATH,undefined,{method:'GET'}),
  {},
  {grant_handler:async()=>{grantCalls++;return new Response('must not run')}}
);
pass('private media public routes are POST-only',wrongMethod.status===405);
pass('wrong method does not reach internal handler',grantCalls===0);

const crossOrigin=await handleMemberPrivateMediaHttpRequest(
  request(__test.PUBLIC_GRANT_PATH,{media_id:'memory-media-1'},{origin:'https://evil.example'}),
  {},
  {grant_handler:async()=>{grantCalls++;return new Response('must not run')}}
);
pass('cross-origin request fails before internal handler',crossOrigin.status===403&&(await crossOrigin.clone().json()).error==='same_origin_required');
pass('cross-origin request never reaches grant handler',grantCalls===0);

let delegatedGrant=null;
const grant=await handleMemberPrivateMediaHttpRequest(
  request(__test.PUBLIC_GRANT_PATH,{media_id:'memory-media-1'}),
  {},
  {
    grant_handler:async delegated=>{
      grantCalls++;
      delegatedGrant=delegated;
      return new Response(JSON.stringify({
        ok:true,
        media:{media_id:'memory-media-1'},
        grant:'v1.1800000000.1800000120.signature',
        delivery_contract:{
          method:'POST',
          path:'/api/internal/member/media/content'
        }
      }),{
        status:200,
        headers:{'content-type':'application/json'}
      });
    }
  }
);
pass('same-origin grant reaches internal grant contract',grant.status===200&&grantCalls===1);
pass('grant path maps to internal namespace',new URL(delegatedGrant.url).pathname===__test.INTERNAL_GRANT_PATH);
pass('grant forwarding preserves request origin header',delegatedGrant.headers.get('origin')===origin);
pass('grant forwarding preserves Member session cookie',delegatedGrant.headers.get('cookie').includes('__Host-mizuno_member_session='));
const delegatedGrantBody=await delegatedGrant.clone().json();
pass('grant forwarding preserves exact JSON body',delegatedGrantBody.media_id==='memory-media-1'&&Object.keys(delegatedGrantBody).length===1);
pass('grant response is no-store and same-origin protected',grant.headers.get('cache-control').includes('no-store')&&grant.headers.get('cross-origin-resource-policy')==='same-origin');

let contentCalls=0;
const missingAdapter=await handleMemberPrivateMediaHttpRequest(
  request(__test.PUBLIC_CONTENT_PATH,{
    media_id:'memory-media-1',
    grant:'v1.1800000000.1800000120.signature'
  }),
  {},
  {
    content_handler:async()=>{contentCalls++;return new Response('must not run')}
  }
);
pass('content route requires explicit trusted storage adapter',missingAdapter.status===503&&(await missingAdapter.clone().json()).error==='private_media_storage_adapter_unavailable');
pass('missing storage adapter never reaches content handler',contentCalls===0);

const adapter={
  async get(key){
    return {body:new Uint8Array([1,2,3]),size:3,content_type:'image/jpeg',key};
  }
};

let delegatedContent=null;
let receivedAdapter=null;
const content=await handleMemberPrivateMediaHttpRequest(
  request(__test.PUBLIC_CONTENT_PATH,{
    media_id:'memory-media-1',
    grant:'v1.1800000000.1800000120.signature'
  }),
  {SHOULD_NOT_BE_USED_AS_STORAGE:{get(){throw new Error('implicit env binding forbidden')}}},
  {
    storage_adapter:adapter,
    content_handler:async(delegated,_env,storageAdapter)=>{
      contentCalls++;
      delegatedContent=delegated;
      receivedAdapter=storageAdapter;
      return new Response(new Uint8Array([255,216,255]),{
        status:200,
        headers:{
          'content-type':'image/jpeg',
          'content-length':'3'
        }
      });
    }
  }
);
pass('content path maps to internal namespace',content.status===200&&new URL(delegatedContent.url).pathname===__test.INTERNAL_CONTENT_PATH);
pass('content handler receives only explicitly injected storage adapter',receivedAdapter===adapter);
pass('content forwarding preserves signed Member session cookie',delegatedContent.headers.get('cookie').includes('__Host-mizuno_member_session='));
pass('content forwarding preserves same-origin evidence',delegatedContent.headers.get('origin')===origin);
const delegatedContentBody=await delegatedContent.clone().json();
pass('content forwarding preserves media_id and grant body',delegatedContentBody.media_id==='memory-media-1'&&delegatedContentBody.grant.startsWith('v1.'));
pass('binary content response preserves media type',content.headers.get('content-type')==='image/jpeg');
pass('binary content response is hardened for private same-origin use',content.headers.get('cache-control').includes('no-store')&&content.headers.get('cross-origin-resource-policy')==='same-origin'&&content.headers.get('x-content-type-options')==='nosniff');

const missingGrantHandler=await handleMemberPrivateMediaHttpRequest(
  request(__test.PUBLIC_GRANT_PATH,{media_id:'memory-media-1'}),
  {},
  {grant_handler:null}
);
pass('missing grant handler fails closed',missingGrantHandler.status===503&&(await missingGrantHandler.clone().json()).error==='private_media_grant_handler_unavailable');

const missingContentHandler=await handleMemberPrivateMediaHttpRequest(
  request(__test.PUBLIC_CONTENT_PATH,{
    media_id:'memory-media-1',
    grant:'v1.1800000000.1800000120.signature'
  }),
  {},
  {storage_adapter:adapter,content_handler:null}
);
pass('missing content handler fails closed',missingContentHandler.status===503&&(await missingContentHandler.clone().json()).error==='private_media_content_handler_unavailable');

const grantMismatch=await handleMemberPrivateMediaHttpRequest(
  request(__test.PUBLIC_GRANT_PATH,{media_id:'memory-media-1'}),
  {},
  {grant_handler:async()=>null}
);
pass('grant handler contract mismatch fails closed',grantMismatch.status===500&&(await grantMismatch.clone().json()).error==='private_media_grant_handler_contract_mismatch');

const contentMismatch=await handleMemberPrivateMediaHttpRequest(
  request(__test.PUBLIC_CONTENT_PATH,{
    media_id:'memory-media-1',
    grant:'v1.1800000000.1800000120.signature'
  }),
  {},
  {storage_adapter:adapter,content_handler:async()=>null}
);
pass('content handler contract mismatch fails closed',contentMismatch.status===500&&(await contentMismatch.clone().json()).error==='private_media_content_handler_contract_mismatch');

pass('route matcher recognizes only two exact public paths',
  __test.routeFor(__test.PUBLIC_GRANT_PATH)?.key==='grant'
  && __test.routeFor(__test.PUBLIC_CONTENT_PATH)?.key==='content'
  && __test.routeFor('/api/member/media/content/extra')===null
);

pass('storage adapter contract requires get()',__test.validStorageAdapter({get(){}})===true&&__test.validStorageAdapter({})===false&&__test.validStorageAdapter(null)===false);

const health=memberPrivateMediaHttpRouterHealth({});
pass('health reports source-only private media router',health.member_private_media_http_router===true&&health.source_only===true);
pass('health reports both reviewed private media source layers',health.grant_source_ready===true&&health.content_source_ready===true&&health.internal_contracts_reused===true);
pass('health requires session same-origin grant and reauthorization',health.signed_member_session_required===true&&health.same_origin_required===true&&health.grant_required_for_content===true&&health.private_media_reauthorization_required===true);
pass('health requires explicit adapter and forbids implicit env storage binding',health.trusted_storage_adapter_dependency_explicit===true&&health.implicit_env_storage_binding===false);
pass('health exposes no storage key or external redirect',health.storage_key_public_response===false&&health.signed_storage_url_exposed===false&&health.external_redirect===false);
pass('health keeps Production route storage fetch and write off',health.production_route_wired===false&&health.production_storage_binding===false&&health.production_storage_fetch===false&&health.production_write===false);
pass('health keeps CRM mutation ID generation and LINE send off',health.customer_create===false&&health.customer_id_generation===false&&health.canonical_crm_write===false&&health.line_send===false);

console.log(`MEMBER_PRIVATE_MEDIA_HTTP_ROUTER=${n}/${n} PASS`);
