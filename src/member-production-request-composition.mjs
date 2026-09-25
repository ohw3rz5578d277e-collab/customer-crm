import { handleMemberAppHttpRequest } from './member-app-http-composition.mjs';
import { handleMemberBrowserPageRequest } from './member-browser-page.mjs';
import { handleMemberPublicAssetDeliveryRequest } from './member-public-asset-delivery.mjs';

const BUILD='member-production-request-composition-20260925-01';
const ROUTE_MODE_ENV='MEMBER_PRODUCTION_ROUTE_MODE';

const text=value=>value==null?'':String(value).trim();

export function memberProductionRouteModeEnabled(env){
  return text(env?.[ROUTE_MODE_ENV])==='enabled';
}

export function classifyMemberProductionRequest(request){
  if(!request||typeof request.url!=='string')return null;
  const pathname=new URL(request.url).pathname;
  if(pathname.startsWith('/member-assets/'))return 'assets';
  if(pathname==='/member'||pathname==='/member/'||/^\/member\/(home|memories|create|shop|my)\/?$/.test(pathname))return 'browser';
  if(pathname.startsWith('/api/member/'))return 'api';
  return null;
}

function unavailable(){
  return new Response(JSON.stringify({
    ok:false,
    error:'member_production_route_owner_authorization_required',
    production_write:false
  }),{
    status:503,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-content-type-options':'nosniff',
      'x-frame-options':'DENY',
      'x-robots-tag':'noindex, nofollow, noarchive',
      'referrer-policy':'no-referrer',
      'cross-origin-opener-policy':'same-origin',
      'cross-origin-resource-policy':'same-origin',
      'x-member-production-request-build':BUILD
    }
  });
}

export async function handleMemberProductionRequest(
  request,
  env,
  {
    approved=false,
    line_login_approved=false,
    public_asset_adapter=null,
    private_media_storage_adapter=null,
    now_seconds,
    asset_handler=handleMemberPublicAssetDeliveryRequest,
    browser_handler=handleMemberBrowserPageRequest,
    api_handler=handleMemberAppHttpRequest
  }={}
){
  const route=classifyMemberProductionRequest(request);
  if(!route)return null;

  if(!memberProductionRouteModeEnabled(env)){
    return null;
  }

  if(approved!==true){
    return unavailable();
  }

  if(route==='assets'){
    if(typeof asset_handler!=='function')return unavailable();
    return asset_handler(request,env,{asset_adapter:public_asset_adapter});
  }

  if(route==='browser'){
    if(typeof browser_handler!=='function')return unavailable();
    return browser_handler(request,env,{now_seconds});
  }

  if(typeof api_handler!=='function')return unavailable();
  return api_handler(request,env,{
    line_login_approved:line_login_approved===true,
    storage_adapter:private_media_storage_adapter,
    now_seconds
  });
}

export function memberProductionRequestCompositionHealth(env){
  return {
    member_production_request_composition:true,
    build:BUILD,
    source_only:true,
    route_mode_env:ROUTE_MODE_ENV,
    route_mode:memberProductionRouteModeEnabled(env)?'enabled':'disabled',
    route_mode_default_disabled:true,
    explicit_owner_approval_required:true,
    route_order:['assets','browser','api'],
    browser_routes:['/member','/member/home','/member/memories','/member/create','/member/shop','/member/my'],
    public_asset_prefix:'/member-assets/',
    api_prefix:'/api/member/',
    line_login_approval_separate:true,
    line_login_approval_default_false:true,
    public_asset_adapter_explicit:true,
    private_media_storage_adapter_explicit:true,
    implicit_env_asset_binding:false,
    implicit_env_private_media_binding:false,
    customer_create:false,
    customer_id_generation:false,
    family_create:false,
    family_auto_link:false,
    canonical_crm_write:false,
    line_send:false,
    production_entry_wired:false,
    production_route_activated:false,
    production_deploy:false,
    production_public_asset_fetch:false,
    production_private_media_fetch:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  ROUTE_MODE_ENV,
  text,
  unavailable
};
