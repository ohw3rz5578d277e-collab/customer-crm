import {
  handleMemberPrivateMediaGrantRequest,
  memberPrivateMediaDeliveryGrantHealth
} from './member-private-media-delivery-grant.mjs';
import {
  handleMemberPrivateMediaContentRequest,
  memberPrivateMediaContentAdapterHealth
} from './member-private-media-content-adapter.mjs';

const BUILD='member-private-media-http-router-20260925-01';
const PUBLIC_GRANT_PATH='/api/member/media/grant';
const PUBLIC_CONTENT_PATH='/api/member/media/content';
const INTERNAL_GRANT_PATH='/api/internal/member/media/grant';
const INTERNAL_CONTENT_PATH='/api/internal/member/media/content';

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
      'cross-origin-resource-policy':'same-origin',
      'x-member-private-media-router-build':BUILD
    }
  });
}

function routeFor(pathname){
  if(pathname===PUBLIC_GRANT_PATH){
    return {key:'grant',internal_path:INTERNAL_GRANT_PATH};
  }
  if(pathname===PUBLIC_CONTENT_PATH){
    return {key:'content',internal_path:INTERNAL_CONTENT_PATH};
  }
  return null;
}

function sameOrigin(request){
  const raw=text(request?.headers?.get?.('origin'));
  if(!raw||raw==='null')return false;
  try{
    return new URL(raw).origin===new URL(request.url).origin;
  }catch{
    return false;
  }
}

function validStorageAdapter(adapter){
  return !!adapter&&typeof adapter==='object'&&typeof adapter.get==='function';
}

function internalRequest(request,internalPath){
  const url=new URL(request.url);
  url.pathname=internalPath;
  return new Request(url.toString(),request);
}

function hardenResponse(response){
  if(!(response instanceof Response)){
    return json({ok:false,error:'private_media_handler_invalid_response'},500);
  }

  const headers=new Headers(response.headers);
  headers.set('cache-control','no-store, no-cache, must-revalidate, max-age=0');
  headers.set('pragma','no-cache');
  headers.set('x-content-type-options','nosniff');
  headers.set('x-frame-options','DENY');
  headers.set('x-robots-tag','noindex, nofollow, noarchive');
  headers.set('referrer-policy','no-referrer');
  headers.set('cross-origin-opener-policy','same-origin');
  headers.set('cross-origin-resource-policy','same-origin');
  headers.set('x-member-private-media-router-build',BUILD);

  return new Response(response.body,{
    status:response.status,
    statusText:response.statusText,
    headers
  });
}

export async function handleMemberPrivateMediaHttpRequest(
  request,
  env,
  {
    grant_handler=handleMemberPrivateMediaGrantRequest,
    content_handler=handleMemberPrivateMediaContentRequest,
    storage_adapter=null
  }={}
){
  const route=routeFor(new URL(request.url).pathname);
  if(!route)return null;

  if(request.method!=='POST'){
    return json({
      ok:false,
      error:'method_not_allowed',
      private_media:true,
      production_write:false
    },405);
  }

  if(!sameOrigin(request)){
    return json({
      ok:false,
      error:'same_origin_required',
      private_media:true,
      production_write:false
    },403);
  }

  const delegated=internalRequest(request,route.internal_path);

  if(route.key==='grant'){
    if(typeof grant_handler!=='function'){
      return json({
        ok:false,
        error:'private_media_grant_handler_unavailable',
        production_write:false
      },503);
    }

    const response=await grant_handler(delegated,env);
    if(response===null){
      return json({
        ok:false,
        error:'private_media_grant_handler_contract_mismatch',
        production_write:false
      },500);
    }
    return hardenResponse(response);
  }

  if(!validStorageAdapter(storage_adapter)){
    return json({
      ok:false,
      error:'private_media_storage_adapter_unavailable',
      production_storage_binding:false,
      production_write:false
    },503);
  }

  if(typeof content_handler!=='function'){
    return json({
      ok:false,
      error:'private_media_content_handler_unavailable',
      production_write:false
    },503);
  }

  const response=await content_handler(
    delegated,
    env,
    storage_adapter
  );

  if(response===null){
    return json({
      ok:false,
      error:'private_media_content_handler_contract_mismatch',
      production_write:false
    },500);
  }

  return hardenResponse(response);
}

export function memberPrivateMediaHttpRouterHealth(env){
  const grant=memberPrivateMediaDeliveryGrantHealth(env);
  const content=memberPrivateMediaContentAdapterHealth(env);

  return {
    member_private_media_http_router:true,
    build:BUILD,
    source_only:true,
    routes:[
      {path:PUBLIC_GRANT_PATH,method:'POST',purpose:'short_lived_grant'},
      {path:PUBLIC_CONTENT_PATH,method:'POST',purpose:'authorized_binary_delivery'}
    ],
    internal_contracts_reused:true,
    grant_source_ready:grant.member_private_media_delivery_grant===true,
    content_source_ready:content.member_private_media_content_adapter===true,
    signed_member_session_required:true,
    same_origin_required:true,
    grant_required_for_content:true,
    private_media_reauthorization_required:true,
    trusted_storage_adapter_dependency_explicit:true,
    implicit_env_storage_binding:false,
    storage_key_public_response:false,
    signed_storage_url_exposed:false,
    external_redirect:false,
    range_requests_supported:false,
    private_no_store:true,
    production_route_wired:false,
    production_storage_binding:false,
    production_storage_fetch:false,
    customer_create:false,
    customer_id_generation:false,
    canonical_crm_write:false,
    line_send:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  PUBLIC_GRANT_PATH,
  PUBLIC_CONTENT_PATH,
  INTERNAL_GRANT_PATH,
  INTERNAL_CONTENT_PATH,
  routeFor,
  sameOrigin,
  validStorageAdapter,
  internalRequest,
  hardenResponse
};
