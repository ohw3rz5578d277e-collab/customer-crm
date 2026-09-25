import {
  handleMemberLineLoginHttpRequest,
  memberLineLoginHttpContractHealth
} from './member-line-login-http-contract.mjs';
import {
  handleMemberReadOnlyHttpRequest,
  memberReadOnlyHttpRouterHealth
} from './member-readonly-http-router.mjs';
import {
  handleMemberPrivateMediaHttpRequest,
  memberPrivateMediaHttpRouterHealth
} from './member-private-media-http-router.mjs';
import {
  memberAppSourceAcceptanceHealth
} from './member-app-source-integration.mjs';

const BUILD='member-app-http-composition-20260925-01';

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
      'x-member-app-composition-build':BUILD
    }
  });
}

function validHandler(handler){
  return typeof handler==='function';
}

export async function handleMemberAppHttpRequest(
  request,
  env,
  {
    line_login_approved=false,
    fetch_impl=globalThis.fetch,
    now_seconds,
    storage_adapter=null,
    line_login_handler=handleMemberLineLoginHttpRequest,
    private_media_handler=handleMemberPrivateMediaHttpRequest,
    read_only_handler=handleMemberReadOnlyHttpRequest
  }={}
){
  if(!request||typeof request.url!=='string'){
    return json({
      ok:false,
      error:'invalid_member_request',
      source_only:true,
      production_write:false
    },400);
  }

  const url=new URL(request.url);
  if(!url.pathname.startsWith('/api/member/'))return null;

  if(!validHandler(line_login_handler)){
    return json({
      ok:false,
      error:'member_line_login_handler_unavailable',
      source_only:true,
      production_write:false
    },503);
  }

  const loginResponse=await line_login_handler(
    request,
    env,
    {
      approved:line_login_approved===true,
      fetch_impl,
      now_seconds
    }
  );
  if(loginResponse!==null)return loginResponse;

  if(!validHandler(private_media_handler)){
    return json({
      ok:false,
      error:'member_private_media_handler_unavailable',
      source_only:true,
      production_write:false
    },503);
  }

  const mediaResponse=await private_media_handler(
    request,
    env,
    {storage_adapter}
  );
  if(mediaResponse!==null)return mediaResponse;

  if(!validHandler(read_only_handler)){
    return json({
      ok:false,
      error:'member_readonly_handler_unavailable',
      source_only:true,
      production_write:false
    },503);
  }

  const readResponse=await read_only_handler(
    request,
    env,
    {now_seconds}
  );
  if(readResponse!==null)return readResponse;

  return null;
}

export function memberAppHttpCompositionHealth(env){
  const ui=memberAppSourceAcceptanceHealth();
  const login=memberLineLoginHttpContractHealth(env);
  const read=memberReadOnlyHttpRouterHealth();
  const media=memberPrivateMediaHttpRouterHealth(env);

  const memberUiSourceReady=
    ui.member_app_source_integration===true
    && ui.all_five_ui_foundations_present===true;

  const lineLoginSourceReady=
    login.member_line_login_http_contract===true
    && login.source_only===true;

  const readOnlySourceReady=
    read.member_readonly_http_router===true
    && read.source_only===true
    && read.read_only===true;

  const privateMediaSourceReady=
    media.member_private_media_http_router===true
    && media.source_only===true;

  return {
    member_app_http_composition:true,
    build:BUILD,
    source_only:true,
    public_prefix:'/api/member/',
    router_order:[
      'line_login',
      'private_media',
      'read_only'
    ],
    member_ui_source_ready:memberUiSourceReady,
    line_login_source_ready:lineLoginSourceReady,
    read_only_source_ready:readOnlySourceReady,
    private_media_source_ready:privateMediaSourceReady,
    all_source_layers_present:
      memberUiSourceReady
      && lineLoginSourceReady
      && readOnlySourceReady
      && privateMediaSourceReady,
    line_login_default_approved:false,
    line_login_explicit_approval_passthrough_only:true,
    private_media_storage_adapter_explicit:true,
    implicit_env_storage_binding:false,
    unknown_member_api_falls_through:true,
    favorite_mutation_route_included:false,
    memory_write_route_included:false,
    black_entitlement_write_route_included:false,
    checkout_route_included:false,
    payment_route_included:false,
    customer_create:false,
    customer_id_generation:false,
    family_create:false,
    family_auto_link:false,
    canonical_crm_write:false,
    line_send:false,
    production_entry_wired:false,
    production_route_activation:false,
    production_storage_binding:false,
    production_storage_fetch:false,
    production_deploy:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  validHandler,
  text
};
