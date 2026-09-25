const BUILD='member-production-route-wiring-preflight-20260925-02';
const CALLBACK_PATH='/api/member/login/line/callback';
const MEMBER_API_PREFIX='/api/member/';
const MEMBER_PAGE_PATH='/member';
const MEMBER_ASSET_PREFIX='/member-assets/';

const text=value=>value==null?'':String(value);

function includesAny(source,needles){
  return needles.some(needle=>source.includes(needle));
}

function exactCallbackMentioned(source){
  return source.includes(CALLBACK_PATH);
}

function hasMemberApiLiteral(source){
  return /['"`]\/api\/member(?:\/|['"`])/.test(source);
}

function hasMemberPageLiteral(source){
  return /['"`]\/member(?:\/|['"`])/.test(source);
}

function hasMemberAssetLiteral(source){
  return source.includes(MEMBER_ASSET_PREFIX)
    || (
      source.includes('member-production-request-composition.mjs')
      && source.includes('handleMemberProductionRequest')
    );
}

function hasCrossSiteApiBlanket(source){
  return /fetchSite\s*===\s*['"]cross-site['"]\s*&&\s*url\.pathname\.startsWith\(\s*['"]\/api\/['"]\s*\)/.test(source);
}

function hasCrossOriginBlanket(source){
  return /new URL\(origin\)\.origin\s*!==\s*url\.origin/.test(source)
    || /error:\s*['"]cross_origin_blocked['"]/.test(source);
}

function hasExactCallbackBoundaryException(source){
  if(!exactCallbackMentioned(source))return false;
  return includesAny(source,[
    'isMemberLineLoginCallback',
    'isMemberLoginCallback',
    'memberLineLoginCallback',
    'member_login_callback'
  ]);
}

function hasCompositionImport(source){
  const current=
    source.includes('member-production-request-composition.mjs')
    && source.includes('handleMemberProductionRequest');
  const legacyPlan=
    source.includes('member-app-http-composition.mjs')
    && source.includes('handleMemberAppHttpRequest');
  return current||legacyPlan;
}

function hasCompositionDispatch(source){
  return source.includes('handleMemberProductionRequest(')
    || source.includes('handleMemberAppHttpRequest(');
}

function hasGlobalBoundaryAnchor(source){
  const boundaryCall=
    source.includes('const blocked=enforceProductionRequestBoundary(request,env);')
    || source.includes('const blocked=enforceProductionRequestBoundary(request);');
  return boundaryCall
    && source.includes('if(blocked)return hardenProductionResponse(blocked,request);');
}

function hasCrmDispatchAnchor(source){
  return source.includes('const response=await handleCustomerCrmRequest(request,env,ctx);');
}

function hasResponseHardener(source){
  return source.includes('hardenProductionResponse(response,request)');
}

function compositionDispatchAfterBoundaryBeforeCrm(source){
  const currentBoundary=source.indexOf('const blocked=enforceProductionRequestBoundary(request,env);');
  const legacyBoundary=source.indexOf('const blocked=enforceProductionRequestBoundary(request);');
  const boundary=currentBoundary>=0?currentBoundary:legacyBoundary;
  const currentMember=source.indexOf('handleMemberProductionRequest(');
  const legacyMember=source.indexOf('handleMemberAppHttpRequest(');
  const member=currentMember>=0?currentMember:legacyMember;
  const crm=source.indexOf('const response=await handleCustomerCrmRequest(request,env,ctx);');
  return boundary>=0&&member>boundary&&crm>member;
}

function wranglerHasExplicitMemberPrivateMediaBinding(source){
  return /binding\s*["']?\s*:\s*["'][^"']*MEMBER[^"']*MEDIA[^"']*["']/i.test(source)
    || /MEMBER_PRIVATE_MEDIA_[A-Z0-9_]*BINDING/.test(source);
}

export function analyzeMemberProductionRouteWiring({
  production_entry_source='',
  wrangler_source='',
  observed={}
}={}){
  const production=text(production_entry_source);
  const wrangler=text(wrangler_source);

  const globalBoundary=hasGlobalBoundaryAnchor(production);
  const crmAnchor=hasCrmDispatchAnchor(production);
  const responseHardener=hasResponseHardener(production);
  const compositionImport=hasCompositionImport(production);
  const compositionDispatch=hasCompositionDispatch(production);
  const composedInSafeOrder=compositionDispatchAfterBoundaryBeforeCrm(production);

  const crossSiteApiBlanket=hasCrossSiteApiBlanket(production);
  const crossOriginBlanket=hasCrossOriginBlanket(production);
  const callbackException=hasExactCallbackBoundaryException(production);
  const lineCallbackBoundaryCompatible=
    (!crossSiteApiBlanket&&!crossOriginBlanket)
    || callbackException;

  const memberApiLiteral=hasMemberApiLiteral(production);
  const memberPageLiteral=hasMemberPageLiteral(production);
  const memberAssetLiteral=hasMemberAssetLiteral(production);

  const namespaceCollision=
    memberApiLiteral
    && !compositionImport
    && !compositionDispatch;

  const storageBindingDeclared=wranglerHasExplicitMemberPrivateMediaBinding(wrangler);
  const storageBindingVerified=
    observed?.private_media_storage_binding_ready===true;

  const sourcePlanReady=
    globalBoundary
    && crmAnchor
    && responseHardener
    && namespaceCollision===false;

  const blockers=[];
  if(!sourcePlanReady)blockers.push('PRODUCTION_ENTRY_SAFE_INSERTION_ANCHOR_NOT_VERIFIED');
  if(!compositionImport||!compositionDispatch)blockers.push('MEMBER_PRODUCTION_COMPOSITION_NOT_WIRED');
  if(compositionDispatch&&!composedInSafeOrder)blockers.push('MEMBER_PRODUCTION_COMPOSITION_ORDER_UNSAFE');
  if(!lineCallbackBoundaryCompatible)blockers.push('LINE_CALLBACK_CROSS_SITE_BOUNDARY_EXCEPTION_NOT_READY');
  if(!memberPageLiteral)blockers.push('MEMBER_BROWSER_PAGE_ROUTE_NOT_READY');
  if(!memberAssetLiteral)blockers.push('MEMBER_ASSET_ROUTE_NOT_READY');
  if(!storageBindingVerified)blockers.push('PRIVATE_MEDIA_PRODUCTION_STORAGE_BINDING_NOT_VERIFIED');
  if(observed?.production_route_owner_authorized!==true)blockers.push('OWNER_PRODUCTION_ROUTE_AUTHORIZATION_REQUIRED');

  return {
    status:'ok',
    build:BUILD,
    source_only:true,
    static_analysis_only:true,
    source_plan_ready:sourcePlanReady,
    production_activation_ready:false,
    blockers,
    current:{
      global_request_boundary_verified:globalBoundary,
      crm_dispatch_anchor_verified:crmAnchor,
      production_response_hardener_verified:responseHardener,
      member_composition_imported:compositionImport,
      member_composition_dispatched:compositionDispatch,
      member_composition_safe_order:composedInSafeOrder,
      member_api_namespace_present:memberApiLiteral,
      member_api_namespace_collision:namespaceCollision,
      cross_site_api_blanket:crossSiteApiBlanket,
      cross_origin_blanket:crossOriginBlanket,
      exact_line_callback_boundary_exception:callbackException,
      line_callback_boundary_compatible:lineCallbackBoundaryCompatible,
      member_browser_page_route_present:memberPageLiteral,
      member_asset_route_present:memberAssetLiteral,
      wrangler_member_private_media_binding_declared:storageBindingDeclared,
      private_media_storage_binding_verified:storageBindingVerified
    },
    required_wiring_order:[
      'enforce_global_production_request_boundary',
      'dispatch_member_composition_with_raw_member_request',
      'harden_member_response_with_existing_production_hardener',
      'fall_through_to_existing_customer_crm_request'
    ],
    required_boundary_change:{
      exact_path:CALLBACK_PATH,
      method:'GET',
      reason:'oauth_top_level_return_may_be_cross_site',
      scope:'exact_callback_only',
      state_nonce_pkce_verification_remains_required:true,
      url_token_forbidden_remains_required:true,
      blanket_cross_site_api_exception_for_other_paths:false
    },
    required_member_routes:{
      api_prefix:MEMBER_API_PREFIX,
      browser_page:MEMBER_PAGE_PATH,
      asset_prefix:MEMBER_ASSET_PREFIX
    },
    required_runtime_dependencies:{
      line_login_external_exchange:'separately_gated',
      member_session_secret:'separately_configured_secret',
      line_login_transaction_secret:'separately_configured_secret',
      line_login_channel_secret:'separately_configured_secret',
      private_media_delivery_secret:'separately_configured_secret',
      private_media_storage_adapter:'explicit_binding_only'
    },
    invariant:{
      production_entry_modified:false,
      production_route_activated:false,
      production_deploy_executed:false,
      production_storage_binding_changed:false,
      production_storage_fetch_executed:false,
      production_schema_apply_executed:false,
      production_write_executed:false,
      crm_write:false,
      line_send:false,
      customer_id_generation:false
    }
  };
}

export function memberProductionRouteWiringPreflightHealth(){
  return {
    member_production_route_wiring_preflight:true,
    build:BUILD,
    source_only:true,
    static_analysis_only:true,
    production_entry_modified:false,
    production_route_activated:false,
    production_deploy_executed:false,
    production_storage_binding_changed:false,
    production_storage_fetch_executed:false,
    production_schema_apply_executed:false,
    production_write_executed:false,
    crm_write:false,
    line_send:false,
    customer_id_generation:false
  };
}

export const __test={
  CALLBACK_PATH,
  MEMBER_API_PREFIX,
  MEMBER_PAGE_PATH,
  MEMBER_ASSET_PREFIX,
  hasMemberApiLiteral,
  hasMemberPageLiteral,
  hasMemberAssetLiteral,
  hasCrossSiteApiBlanket,
  hasCrossOriginBlanket,
  hasExactCallbackBoundaryException,
  hasCompositionImport,
  hasCompositionDispatch,
  hasGlobalBoundaryAnchor,
  hasCrmDispatchAnchor,
  hasResponseHardener,
  compositionDispatchAfterBoundaryBeforeCrm,
  wranglerHasExplicitMemberPrivateMediaBinding
};
