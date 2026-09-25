import { verifyMemberSessionRequest } from './member-session-foundation.mjs';
import { handleMemberHomeReadRequest } from './member-home-read-model.mjs';
import { handleMemberMemoriesReadRequest } from './member-memories-read-model.mjs';
import { handleMemberMyReadRequest } from './member-my-read-model.mjs';
import { handleMemberShopCatalogReadRequest } from './member-shop-pickup-read-model.mjs';
import { handleMemberNewsReadRequest } from './member-news-read-model.mjs';
import { handleMemberCreativeCatalogReadRequest } from './member-creative-catalog-read-model.mjs';
import { handleMemberFavoritesReadRequest } from './member-favorites-read-model.mjs';
import { handleMemberFamilyPassReadRequest } from './member-family-pass-read-model.mjs';
import { handleMemberFamilyPassportReadRequest } from './member-family-passport-read-model.mjs';
import { handleMemberNextMemoryReadRequest } from './member-next-memory-read-model.mjs';
import { handleMemberTodaysMemoryReadRequest } from './member-todays-memory-read-model.mjs';

const BUILD='member-readonly-http-router-20260925-01';

const ROUTES=Object.freeze([
  {key:'home',public_path:'/api/member/home',internal_path:'/api/internal/member/home'},
  {key:'memories',public_path:'/api/member/memories',internal_path:'/api/internal/member/memories'},
  {key:'my',public_path:'/api/member/my',internal_path:'/api/internal/member/my'},
  {key:'shop',public_path:'/api/member/shop/products',internal_path:'/api/internal/member/shop/products'},
  {key:'news',public_path:'/api/member/news',internal_path:'/api/internal/member/news'},
  {key:'create',public_path:'/api/member/create/templates',internal_path:'/api/internal/member/creative/templates'},
  {key:'favorites',public_path:'/api/member/favorites',internal_path:'/api/internal/member/favorites'},
  {key:'family_pass',public_path:'/api/member/family-pass',internal_path:'/api/internal/member/family-pass'},
  {key:'family_passport',public_path:'/api/member/family-passport',internal_path:'/api/internal/member/family-passport'},
  {key:'next_memory',public_path:'/api/member/next-memory',internal_path:'/api/internal/member/next-memory'},
  {key:'todays_memory',public_path:'/api/member/todays-memory',internal_path:'/api/internal/member/todays-memory'}
]);

const DEFAULT_HANDLERS=Object.freeze({
  home:handleMemberHomeReadRequest,
  memories:handleMemberMemoriesReadRequest,
  my:handleMemberMyReadRequest,
  shop:handleMemberShopCatalogReadRequest,
  news:handleMemberNewsReadRequest,
  create:handleMemberCreativeCatalogReadRequest,
  favorites:handleMemberFavoritesReadRequest,
  family_pass:handleMemberFamilyPassReadRequest,
  family_passport:handleMemberFamilyPassportReadRequest,
  next_memory:handleMemberNextMemoryReadRequest,
  todays_memory:handleMemberTodaysMemoryReadRequest
});

const text=value=>value==null?'':String(value).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store, no-cache, must-revalidate, max-age=0',
      'pragma':'no-cache',
      'x-content-type-options':'nosniff',
      'x-frame-options':'DENY',
      'x-robots-tag':'noindex, nofollow, noarchive',
      'referrer-policy':'no-referrer',
      'cross-origin-opener-policy':'same-origin',
      'x-member-readonly-router-build':BUILD
    }
  });
}

function matchRoute(pathname){
  for(const route of ROUTES){
    if(pathname===route.public_path){
      return {...route,detail_id:null};
    }
  }

  const detail=pathname.match(/^\/api\/member\/memories\/([^/]{1,160})$/);
  if(detail){
    return {
      key:'memories',
      public_path:'/api/member/memories/:memory_id',
      internal_path:'/api/internal/member/memories/'+detail[1],
      detail_id:detail[1]
    };
  }

  return null;
}

function sessionHttpStatus(result){
  const status=text(result?.status);
  if(status==='member_session_secret_not_configured')return 503;
  return 401;
}

function internalRequest(request,internalPath){
  const url=new URL(request.url);
  url.pathname=internalPath;
  return new Request(url.toString(),{
    method:'GET',
    headers:request.headers
  });
}

function hardenResponse(response){
  if(!(response instanceof Response)){
    return json({ok:false,error:'member_read_handler_invalid_response'},500);
  }
  const headers=new Headers(response.headers);
  headers.set('cache-control','no-store, no-cache, must-revalidate, max-age=0');
  headers.set('pragma','no-cache');
  headers.set('x-content-type-options','nosniff');
  headers.set('x-frame-options','DENY');
  headers.set('x-robots-tag','noindex, nofollow, noarchive');
  headers.set('referrer-policy','no-referrer');
  headers.set('cross-origin-opener-policy','same-origin');
  headers.set('x-member-readonly-router-build',BUILD);
  return new Response(response.body,{
    status:response.status,
    statusText:response.statusText,
    headers
  });
}

export async function handleMemberReadOnlyHttpRequest(
  request,
  env,
  {
    verify_session=verifyMemberSessionRequest,
    handlers=DEFAULT_HANDLERS,
    now_seconds
  }={}
){
  const url=new URL(request.url);
  const route=matchRoute(url.pathname);
  if(!route)return null;

  if(request.method!=='GET'){
    return json({
      ok:false,
      error:'method_not_allowed',
      route:route.key,
      read_only:true,
      production_write:false
    },405);
  }

  if(typeof verify_session!=='function'){
    return json({
      ok:false,
      error:'member_session_verifier_unavailable',
      read_only:true,
      production_write:false
    },503);
  }

  const verified=await verify_session(request,env,{now_seconds});
  if(verified?.status!=='ok'||verified?.verified!==true||!verified?.session){
    return json({
      ok:false,
      error:text(verified?.status)||'member_session_required',
      read_only:true,
      production_write:false
    },sessionHttpStatus(verified));
  }

  const handler=handlers?.[route.key];
  if(typeof handler!=='function'){
    return json({
      ok:false,
      error:'member_read_handler_unavailable',
      route:route.key,
      read_only:true,
      production_write:false
    },503);
  }

  const response=await handler(
    internalRequest(request,route.internal_path),
    env,
    verified.session
  );

  if(response===null){
    return json({
      ok:false,
      error:'member_read_handler_contract_mismatch',
      route:route.key,
      read_only:true,
      production_write:false
    },500);
  }

  return hardenResponse(response);
}

export function memberReadOnlyHttpRouterHealth(){
  return {
    member_readonly_http_router:true,
    build:BUILD,
    source_only:true,
    public_prefix:'/api/member',
    internal_prefix:'/api/internal/member',
    route_count:ROUTES.length+1,
    routes:[
      ...ROUTES.map(route=>({key:route.key,path:route.public_path,method:'GET'})),
      {key:'memories_detail',path:'/api/member/memories/:memory_id',method:'GET'}
    ],
    signed_member_session_required:true,
    session_verified_once_at_router:true,
    internal_read_models_reused:true,
    get_only:true,
    read_only:true,
    favorite_mutation_route_included:false,
    memory_write_route_included:false,
    black_entitlement_write_route_included:false,
    private_media_content_route_included:false,
    private_media_grant_route_included:false,
    customer_create:false,
    customer_id_generation:false,
    family_create:false,
    family_auto_link:false,
    canonical_crm_write:false,
    line_send:false,
    production_route_wired:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  ROUTES,
  matchRoute,
  sessionHttpStatus,
  internalRequest,
  hardenResponse
};
