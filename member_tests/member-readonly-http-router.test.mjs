import {
  handleMemberReadOnlyHttpRequest,
  memberReadOnlyHttpRouterHealth,
  __test
} from '../src/member-readonly-http-router.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const origin='https://member.example.test';
const session={
  customer_id:'26000123',
  family_id:'family-26000123',
  verified:true,
  source:'signed_member_session'
};

let verifyCalls=0;
const verifyOk=async()=>{
  verifyCalls++;
  return {status:'ok',verified:true,session,write_executed:false};
};

const calls=[];
const handlers={};
for(const key of __test.ROUTES.map(x=>x.key)){
  handlers[key]=async(request,_env,memberSession)=>{
    calls.push({
      key,
      path:new URL(request.url).pathname,
      search:new URL(request.url).search,
      memberSession
    });
    return new Response(JSON.stringify({ok:true,key}),{
      status:200,
      headers:{'content-type':'application/json'}
    });
  };
}

const unknown=await handleMemberReadOnlyHttpRequest(
  new Request(origin+'/api/member/unknown'),
  {},
  {verify_session:verifyOk,handlers}
);
pass('unknown Member path is not claimed',unknown===null);
pass('unknown path performs no session verification',verifyCalls===0);

const post=await handleMemberReadOnlyHttpRequest(
  new Request(origin+'/api/member/home',{method:'POST'}),
  {},
  {verify_session:verifyOk,handlers}
);
pass('known read route is GET-only',post.status===405&&(await post.clone().json()).error==='method_not_allowed');
pass('wrong method performs no session verification',verifyCalls===0);

let handlerCalls=0;
const missingSession=await handleMemberReadOnlyHttpRequest(
  new Request(origin+'/api/member/home'),
  {},
  {
    verify_session:async()=>({status:'member_session_required',verified:false}),
    handlers:{home:async()=>{handlerCalls++;return new Response('must not run')}}
  }
);
pass('missing signed Member session returns 401',missingSession.status===401&&(await missingSession.clone().json()).error==='member_session_required');
pass('missing session never reaches read model',handlerCalls===0);

const missingSecret=await handleMemberReadOnlyHttpRequest(
  new Request(origin+'/api/member/home'),
  {},
  {
    verify_session:async()=>({status:'member_session_secret_not_configured',verified:false}),
    handlers
  }
);
pass('missing Member session secret is service unavailable',missingSecret.status===503);

const fixtures=[
  ['/api/member/home','home','/api/internal/member/home'],
  ['/api/member/memories','memories','/api/internal/member/memories'],
  ['/api/member/my','my','/api/internal/member/my'],
  ['/api/member/shop/products','shop','/api/internal/member/shop/products'],
  ['/api/member/news','news','/api/internal/member/news'],
  ['/api/member/create/templates','create','/api/internal/member/creative/templates'],
  ['/api/member/favorites','favorites','/api/internal/member/favorites'],
  ['/api/member/family-pass','family_pass','/api/internal/member/family-pass'],
  ['/api/member/family-passport','family_passport','/api/internal/member/family-passport'],
  ['/api/member/next-memory','next_memory','/api/internal/member/next-memory'],
  ['/api/member/todays-memory','todays_memory','/api/internal/member/todays-memory']
];

for(const [publicPath,key,internalPath] of fixtures){
  const before=verifyCalls;
  const response=await handleMemberReadOnlyHttpRequest(
    new Request(origin+publicPath+'?limit=12'),
    {},
    {verify_session:verifyOk,handlers}
  );
  const body=await response.clone().json();
  const call=calls.at(-1);
  pass(key+' route returns handler response',response.status===200&&body.ok===true&&body.key===key);
  pass(key+' route verifies session exactly once',verifyCalls===before+1);
  pass(key+' route maps to internal read model path',call.key===key&&call.path===internalPath);
  pass(key+' route preserves query string',call.search==='?limit=12');
  pass(key+' route passes verified canonical session',call.memberSession===session);
  pass(key+' route hardens cache and indexing headers',response.headers.get('cache-control').includes('no-store')&&response.headers.get('x-robots-tag').includes('noindex'));
}

const detail=await handleMemberReadOnlyHttpRequest(
  new Request(origin+'/api/member/memories/memory-001?include=media'),
  {},
  {verify_session:verifyOk,handlers}
);
const detailCall=calls.at(-1);
pass('memory detail uses memories read model',detail.status===200&&detailCall.key==='memories');
pass('memory detail maps exact ID to internal detail path',detailCall.path==='/api/internal/member/memories/memory-001');
pass('memory detail preserves query string',detailCall.search==='?include=media');

const missingHandler=await handleMemberReadOnlyHttpRequest(
  new Request(origin+'/api/member/news'),
  {},
  {verify_session:verifyOk,handlers:{}}
);
pass('missing internal handler fails closed',missingHandler.status===503&&(await missingHandler.clone().json()).error==='member_read_handler_unavailable');

const mismatch=await handleMemberReadOnlyHttpRequest(
  new Request(origin+'/api/member/home'),
  {},
  {
    verify_session:verifyOk,
    handlers:{home:async()=>null}
  }
);
pass('internal handler contract mismatch fails closed',mismatch.status===500&&(await mismatch.clone().json()).error==='member_read_handler_contract_mismatch');

const health=memberReadOnlyHttpRouterHealth();
pass('health reports source-only router',health.member_readonly_http_router===true&&health.source_only===true);
pass('health requires signed session and single router verification',health.signed_member_session_required===true&&health.session_verified_once_at_router===true);
pass('health exposes read-only GET contract',health.get_only===true&&health.read_only===true&&health.route_count===12);
pass('health excludes all write and private-media mutation routes',health.favorite_mutation_route_included===false&&health.memory_write_route_included===false&&health.black_entitlement_write_route_included===false&&health.private_media_content_route_included===false&&health.private_media_grant_route_included===false);
pass('health keeps identity mutation and LINE send off',health.customer_create===false&&health.customer_id_generation===false&&health.family_create===false&&health.family_auto_link===false&&health.canonical_crm_write===false&&health.line_send===false);
pass('health keeps Production route and write off',health.production_route_wired===false&&health.production_write===false);

console.log(`MEMBER_READONLY_HTTP_ROUTER=${n}/${n} PASS`);
