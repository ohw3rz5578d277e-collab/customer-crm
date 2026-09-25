import { memberProductionRequestCompositionHealth } from './member-production-request-composition.mjs';
import { memberProductionRouteWiringPreflightHealth } from './member-production-route-wiring-preflight.mjs';
import { memberBrowserPageHealth } from './member-browser-page.mjs';
import { memberPublicAssetDeliveryHealth } from './member-public-asset-delivery.mjs';
import { memberAppHttpCompositionHealth } from './member-app-http-composition.mjs';

const BUILD='member-production-integration-acceptance-20260925-01';
const OWNER_FLAG='MEMBER_PRODUCTION_OWNER_APPROVED';
const ROUTE_MODE_ENV='MEMBER_PRODUCTION_ROUTE_MODE';
const CALLBACK_PATH='/api/member/login/line/callback';

const IMPORT_ANCHOR="import app from './production-index-crm-browser-root-entry.js';";
const IMPORT_LINE="import { handleMemberProductionRequest, memberProductionRouteModeEnabled } from './member-production-request-composition.mjs';";
const SENSITIVE_OLD="return pathname==='/admin'||pathname.startsWith('/admin/')||pathname.startsWith('/api/')||pathname.startsWith('/__crm/');";
const SENSITIVE_NEW="return pathname==='/admin'||pathname.startsWith('/admin/')||pathname==='/member'||pathname.startsWith('/member/')||pathname.startsWith('/api/')||pathname.startsWith('/__crm/');";
const BOUNDARY_SIGNATURE_OLD='export function enforceProductionRequestBoundary(request){';
const BOUNDARY_SIGNATURE_NEW='export function enforceProductionRequestBoundary(request,env){';
const FETCH_BOUNDARY_OLD='const blocked=enforceProductionRequestBoundary(request);';
const FETCH_BOUNDARY_NEW='const blocked=enforceProductionRequestBoundary(request,env);';
const CRM_DISPATCH='const response=await handleCustomerCrmRequest(request,env,ctx);';

function count(source,needle){
  if(!needle)return 0;
  let n=0;
  let at=0;
  while((at=source.indexOf(needle,at))>=0){
    n++;
    at+=needle.length;
  }
  return n;
}

function replaceExactlyOnce(source,from,to){
  if(count(source,from)!==1)return {ok:false,source};
  return {ok:true,source:source.replace(from,to)};
}

export function buildMemberProductionDefaultOffWiringCandidate(productionEntrySource){
  let source=String(productionEntrySource||'');
  const failures=[];

  let step=replaceExactlyOnce(source,IMPORT_ANCHOR,IMPORT_ANCHOR+'\n'+IMPORT_LINE);
  if(!step.ok)failures.push('IMPORT_ANCHOR_MISMATCH');
  else source=step.source;

  const ownerGate=[
    '',
    'const '+OWNER_FLAG+'=false;',
    'function memberProductionBoundaryActive(env){',
    '  return '+OWNER_FLAG+'===true&&memberProductionRouteModeEnabled(env);',
    '}',
    'function isMemberLineLoginCallbackRequest(request,url){',
    "  const method=String(request.method||'GET').toUpperCase();",
    "  return method==='GET'&&url.pathname==='"+CALLBACK_PATH+"';",
    '}',
    ''
  ].join('\n');

  step=replaceExactlyOnce(
    source,
    'function isSensitiveProductionPath(pathname){',
    ownerGate+'function isSensitiveProductionPath(pathname){'
  );
  if(!step.ok)failures.push('SENSITIVE_FUNCTION_ANCHOR_MISMATCH');
  else source=step.source;

  step=replaceExactlyOnce(source,SENSITIVE_OLD,SENSITIVE_NEW);
  if(!step.ok)failures.push('SENSITIVE_PATH_BODY_MISMATCH');
  else source=step.source;

  step=replaceExactlyOnce(source,BOUNDARY_SIGNATURE_OLD,BOUNDARY_SIGNATURE_NEW);
  if(!step.ok)failures.push('BOUNDARY_SIGNATURE_MISMATCH');
  else source=step.source;

  step=replaceExactlyOnce(
    source,
    "  const origin=String(request.headers.get('origin')||'').trim();",
    "  const memberLineLoginCallback=memberProductionBoundaryActive(env)&&isMemberLineLoginCallbackRequest(request,url);\n  const origin=String(request.headers.get('origin')||'').trim();"
  );
  if(!step.ok)failures.push('BOUNDARY_CALLBACK_ANCHOR_MISMATCH');
  else source=step.source;

  step=replaceExactlyOnce(source,'  if(origin){','  if(origin&&!memberLineLoginCallback){');
  if(!step.ok)failures.push('BOUNDARY_ORIGIN_GUARD_MISMATCH');
  else source=step.source;

  step=replaceExactlyOnce(
    source,
    "  if(fetchSite==='cross-site'&&url.pathname.startsWith('/api/'))return securityJson({ok:false,error:'cross_site_api_blocked'},403);",
    "  if(fetchSite==='cross-site'&&url.pathname.startsWith('/api/')&&!memberLineLoginCallback)return securityJson({ok:false,error:'cross_site_api_blocked'},403);"
  );
  if(!step.ok)failures.push('BOUNDARY_FETCH_SITE_GUARD_MISMATCH');
  else source=step.source;

  step=replaceExactlyOnce(source,FETCH_BOUNDARY_OLD,FETCH_BOUNDARY_NEW);
  if(!step.ok)failures.push('FETCH_BOUNDARY_CALL_MISMATCH');
  else source=step.source;

  const memberDispatch=[
    'const memberResponse=await handleMemberProductionRequest(request,env,{',
    '      approved:'+OWNER_FLAG+',',
    '      line_login_approved:false,',
    '      public_asset_adapter:null,',
    '      private_media_storage_adapter:null',
    '    });',
    '    if(memberResponse)return hardenProductionResponse(memberResponse,request);',
    '    '+CRM_DISPATCH
  ].join('\n');

  step=replaceExactlyOnce(source,CRM_DISPATCH,memberDispatch);
  if(!step.ok)failures.push('CRM_DISPATCH_ANCHOR_MISMATCH');
  else source=step.source;

  return {
    status:failures.length?'anchor_mismatch':'ok',
    build:BUILD,
    candidate_source:failures.length?'':source,
    failures,
    source_only:true,
    default_off:true,
    owner_flag:OWNER_FLAG,
    route_mode_env:ROUTE_MODE_ENV,
    line_login_approved:false,
    public_asset_adapter:null,
    private_media_storage_adapter:null,
    production_write:false
  };
}

export function inspectMemberProductionCandidate(candidateSource){
  const source=String(candidateSource||'');
  const boundary=source.indexOf('const blocked=enforceProductionRequestBoundary(request,env);');
  const member=source.indexOf('const memberResponse=await handleMemberProductionRequest(request,env,{');
  const crm=source.indexOf(CRM_DISPATCH);

  return {
    import_present:count(source,IMPORT_LINE)===1,
    owner_flag_default_false:source.includes('const '+OWNER_FLAG+'=false;'),
    route_mode_checked:source.includes('memberProductionRouteModeEnabled(env)'),
    member_path_sensitive:source.includes("pathname==='/member'||pathname.startsWith('/member/')"),
    exact_callback_get_only:source.includes("method==='GET'&&url.pathname==='"+CALLBACK_PATH+"'"),
    callback_exception_owner_and_mode_gated:source.includes('memberProductionBoundaryActive(env)&&isMemberLineLoginCallbackRequest(request,url)'),
    callback_cross_origin_exception_narrow:source.includes('if(origin&&!memberLineLoginCallback){'),
    callback_cross_site_api_exception_narrow:source.includes("url.pathname.startsWith('/api/')&&!memberLineLoginCallback"),
    dispatch_after_boundary_before_crm:boundary>=0&&member>boundary&&crm>member,
    owner_flag_passed_to_handler:source.includes('approved:'+OWNER_FLAG),
    line_login_default_false:source.includes('line_login_approved:false'),
    public_asset_adapter_default_null:source.includes('public_asset_adapter:null'),
    private_media_adapter_default_null:source.includes('private_media_storage_adapter:null'),
    existing_crm_dispatch_preserved:count(source,CRM_DISPATCH)===1
  };
}

export function buildMemberProductionIntegrationAcceptance({
  production_entry_source='',
  env={},
  observed={}
}={}){
  const requestComposition=memberProductionRequestCompositionHealth(env);
  const wiring=memberProductionRouteWiringPreflightHealth();
  const browser=memberBrowserPageHealth();
  const assets=memberPublicAssetDeliveryHealth();
  const api=memberAppHttpCompositionHealth(env);

  const currentInspection=inspectMemberProductionCandidate(production_entry_source);
  const currentInspectionValues=Object.values(currentInspection);
  const currentDefaultOffApplied=
    currentInspectionValues.length>0
    && currentInspectionValues.every(value=>value===true);

  const generatedCandidate=currentDefaultOffApplied
    ?null
    :buildMemberProductionDefaultOffWiringCandidate(production_entry_source);

  const generatedInspection=
    !currentDefaultOffApplied&&generatedCandidate?.status==='ok'
      ?inspectMemberProductionCandidate(generatedCandidate.candidate_source)
      :{};

  const generatedValues=Object.values(generatedInspection);
  const generatedCandidateReady=
    generatedCandidate?.status==='ok'
    && generatedValues.length>0
    && generatedValues.every(value=>value===true);

  const defaultOffReady=currentDefaultOffApplied||generatedCandidateReady;
  const inspection=currentDefaultOffApplied?currentInspection:generatedInspection;

  const sourceReady=
    requestComposition.member_production_request_composition===true
    && wiring.member_production_route_wiring_preflight===true
    && browser.member_browser_page===true
    && assets.member_public_asset_delivery===true
    && api.member_app_http_composition===true
    && defaultOffReady;

  const blockers=[];
  if(!sourceReady)blockers.push('MEMBER_PRODUCTION_INTEGRATION_SOURCE_NOT_READY');
  if(!currentDefaultOffApplied&&observed?.production_entry_candidate_applied!==true)blockers.push('PRODUCTION_ENTRY_CANDIDATE_NOT_APPLIED');
  if(!currentDefaultOffApplied&&observed?.owner_production_entry_modification_authorized!==true)blockers.push('OWNER_PRODUCTION_ENTRY_MODIFICATION_AUTHORIZATION_REQUIRED');
  if(observed?.owner_production_route_activation_authorized!==true)blockers.push('OWNER_PRODUCTION_ROUTE_ACTIVATION_AUTHORIZATION_REQUIRED');
  if(observed?.production_route_mode_enabled!==true)blockers.push('MEMBER_PRODUCTION_ROUTE_MODE_NOT_ENABLED');
  if(observed?.member_runtime_dependencies_verified!==true)blockers.push('MEMBER_RUNTIME_DEPENDENCIES_NOT_VERIFIED');
  if(observed?.public_asset_binding_verified!==true)blockers.push('PUBLIC_ASSET_BINDING_NOT_VERIFIED');
  if(observed?.private_media_binding_verified!==true)blockers.push('PRIVATE_MEDIA_BINDING_NOT_VERIFIED');
  if(observed?.read_only_canary_passed!==true)blockers.push('MEMBER_READ_ONLY_CANARY_NOT_PASSED');

  const acceptanceSequence=currentDefaultOffApplied
    ?[
      'canonical_default_off_entry_source_already_applied',
      'verify_existing_crm_regressions_and_member_routes_still_inactive',
      'configure_and_verify_member_runtime_dependencies_without_route_activation',
      'fresh_owner_authorize_route_activation_exact_sha_and_scope',
      'activate_member_route_mode_and_owner_flag_in_exact_release',
      'run_read_only_member_canary',
      'activate_optional_private_media_only_under_separate_binding_gate',
      'keep_favorites_memory_black_and_commerce_write_gates_separate'
    ]
    :[
      'merge_source_only_acceptance',
      'fresh_owner_authorize_production_entry_modification_exact_sha',
      'apply_default_off_production_entry_candidate',
      'verify_existing_crm_regressions_and_member_routes_still_inactive',
      'configure_and_verify_member_runtime_dependencies_without_route_activation',
      'fresh_owner_authorize_route_activation_exact_sha_and_scope',
      'activate_member_route_mode_and_owner_flag_in_exact_release',
      'run_read_only_member_canary',
      'activate_optional_private_media_only_under_separate_binding_gate',
      'keep_favorites_memory_black_and_commerce_write_gates_separate'
    ];

  return {
    status:'ok',
    build:BUILD,
    source_only:true,
    static_acceptance_only:true,
    source_ready:sourceReady,
    canonical_source_entry_default_off_applied:currentDefaultOffApplied,
    default_off_candidate_ready:defaultOffReady,
    production_activation_ready:false,
    candidate:{
      status:currentDefaultOffApplied?'already_applied_default_off':generatedCandidate?.status||'not_ready',
      failures:currentDefaultOffApplied?[]:[...(generatedCandidate?.failures||[])],
      owner_flag_default:false,
      route_mode_env:ROUTE_MODE_ENV,
      line_login_approved:false,
      public_asset_adapter:null,
      private_media_storage_adapter:null,
      inspection
    },
    acceptance_sequence:acceptanceSequence,
    blockers,
    invariant:{
      production_entry_mutation_by_acceptance:false,
      production_route_activated:false,
      line_callback_boundary_exception_activated:false,
      production_deploy:false,
      production_schema_apply:false,
      production_d1_write:false,
      production_public_asset_binding:false,
      production_private_media_binding:false,
      production_public_asset_fetch:false,
      production_private_media_fetch:false,
      crm_write:false,
      line_send:false,
      customer_id_generation:false,
      checkout_payment:false,
      paid_spend:false
    }
  };
}

export function memberProductionIntegrationAcceptanceHealth(){
  return {
    member_production_integration_acceptance:true,
    build:BUILD,
    source_only:true,
    static_acceptance_only:true,
    exact_default_off_candidate_supported:true,
    canonical_source_entry_default_off_expected:true,
    production_activation_ready:false,
    production_entry_modified:false,
    production_route_activated:false,
    production_deploy:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  OWNER_FLAG,
  ROUTE_MODE_ENV,
  CALLBACK_PATH,
  IMPORT_ANCHOR,
  IMPORT_LINE,
  SENSITIVE_OLD,
  SENSITIVE_NEW,
  CRM_DISPATCH,
  count,
  replaceExactlyOnce
};
