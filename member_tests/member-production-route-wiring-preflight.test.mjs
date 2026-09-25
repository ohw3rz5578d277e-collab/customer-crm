import fs from 'node:fs';
import {
  analyzeMemberProductionRouteWiring,
  memberProductionRouteWiringPreflightHealth,
  __test
} from '../src/member-production-route-wiring-preflight.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const production=fs.readFileSync(
  new URL('../src/production-index-crm-customer360-entry.js',import.meta.url),
  'utf8'
);
const wrangler=fs.readFileSync(
  new URL('../wrangler.jsonc',import.meta.url),
  'utf8'
);

const current=analyzeMemberProductionRouteWiring({
  production_entry_source:production,
  wrangler_source:wrangler
});

pass('current Production entry has stable global request boundary anchor',
  current.current.global_request_boundary_verified===true
);
pass('current Production entry has stable CRM dispatch anchor',
  current.current.crm_dispatch_anchor_verified===true
);
pass('current Production response hardener is available',
  current.current.production_response_hardener_verified===true
);
pass('current Production entry has not imported Member composition',
  current.current.member_composition_imported===false
);
pass('current Production entry has not dispatched Member composition',
  current.current.member_composition_dispatched===false
);
pass('current Production entry has no pre-existing Member API namespace collision',
  current.current.member_api_namespace_collision===false
);
pass('current global boundary blocks cross-site API requests',
  current.current.cross_site_api_blanket===true
);
pass('current global boundary has cross-origin sensitive-path protection',
  current.current.cross_origin_blanket===true
);
pass('current Production entry has no exact LINE callback boundary exception',
  current.current.exact_line_callback_boundary_exception===false
);
pass('LINE callback is therefore not yet boundary-compatible',
  current.current.line_callback_boundary_compatible===false
);
pass('current Production entry has no Member browser page route',
  current.current.member_browser_page_route_present===false
);
pass('current Production entry has no Member asset route',
  current.current.member_asset_route_present===false
);
pass('current wrangler does not declare an implicit Member private-media binding',
  current.current.wrangler_member_private_media_binding_declared===false
);
pass('safe insertion anchors make the source wiring plan ready',
  current.source_plan_ready===true
);
pass('current state remains non-activating',
  current.production_activation_ready===false
);
pass('current blockers include unwired composition',
  current.blockers.includes('MEMBER_PRODUCTION_COMPOSITION_NOT_WIRED')
);
pass('current blockers include LINE callback boundary conflict',
  current.blockers.includes('LINE_CALLBACK_CROSS_SITE_BOUNDARY_EXCEPTION_NOT_READY')
);
pass('current blockers include Member browser page',
  current.blockers.includes('MEMBER_BROWSER_PAGE_ROUTE_NOT_READY')
);
pass('current blockers include Member assets',
  current.blockers.includes('MEMBER_ASSET_ROUTE_NOT_READY')
);
pass('current blockers include private-media Production binding evidence',
  current.blockers.includes('PRIVATE_MEDIA_PRODUCTION_STORAGE_BINDING_NOT_VERIFIED')
);
pass('current blockers include fresh Owner route authorization',
  current.blockers.includes('OWNER_PRODUCTION_ROUTE_AUTHORIZATION_REQUIRED')
);

const planned=production
  .replace(
    "import app from './production-index-crm-browser-root-entry.js';",
    "import app from './production-index-crm-browser-root-entry.js';\nimport { handleMemberAppHttpRequest } from './member-app-http-composition.mjs';"
  )
  .replace(
    "function isSensitiveProductionPath(pathname){",
    "function isMemberLineLoginCallbackGet(request,url){return request.method==='GET'&&url.pathname==='/api/member/login/line/callback'}\nfunction isSensitiveProductionPath(pathname){"
  )
  .replace(
    "const origin=String(request.headers.get('origin')||'').trim();",
    "const memberLineLoginCallback=isMemberLineLoginCallbackGet(request,url);\n  const origin=String(request.headers.get('origin')||'').trim();"
  )
  .replace(
    "if(origin){",
    "if(origin&&!memberLineLoginCallback){"
  )
  .replace(
    "if(fetchSite==='cross-site'&&url.pathname.startsWith('/api/'))",
    "if(fetchSite==='cross-site'&&url.pathname.startsWith('/api/')&&!memberLineLoginCallback)"
  )
  .replace(
    "const response=await handleCustomerCrmRequest(request,env,ctx);",
    "const memberResponse=await handleMemberAppHttpRequest(request,env,{line_login_approved:false,storage_adapter:null});\n    if(memberResponse)return hardenProductionResponse(memberResponse,request);\n    const response=await handleCustomerCrmRequest(request,env,ctx);"
  )
  +"\nconst MEMBER_PAGE='/member';\nconst MEMBER_ASSETS='/member-assets/';\n";

const plannedResult=analyzeMemberProductionRouteWiring({
  production_entry_source:planned,
  wrangler_source:wrangler,
  observed:{
    private_media_storage_binding_ready:true,
    production_route_owner_authorized:true
  }
});

pass('planned topology detects Member composition import',
  plannedResult.current.member_composition_imported===true
);
pass('planned topology detects Member composition dispatch',
  plannedResult.current.member_composition_dispatched===true
);
pass('planned topology places Member dispatch after boundary and before CRM',
  plannedResult.current.member_composition_safe_order===true
);
pass('planned topology detects exact LINE callback exception',
  plannedResult.current.exact_line_callback_boundary_exception===true
);
pass('planned topology makes LINE callback boundary-compatible',
  plannedResult.current.line_callback_boundary_compatible===true
);
pass('planned topology recognizes Member page and assets',
  plannedResult.current.member_browser_page_route_present===true
  && plannedResult.current.member_asset_route_present===true
);
pass('planned topology removes current structural blockers',
  !plannedResult.blockers.includes('MEMBER_PRODUCTION_COMPOSITION_NOT_WIRED')
  && !plannedResult.blockers.includes('MEMBER_PRODUCTION_COMPOSITION_ORDER_UNSAFE')
  && !plannedResult.blockers.includes('LINE_CALLBACK_CROSS_SITE_BOUNDARY_EXCEPTION_NOT_READY')
  && !plannedResult.blockers.includes('MEMBER_BROWSER_PAGE_ROUTE_NOT_READY')
  && !plannedResult.blockers.includes('MEMBER_ASSET_ROUTE_NOT_READY')
  && !plannedResult.blockers.includes('PRIVATE_MEDIA_PRODUCTION_STORAGE_BINDING_NOT_VERIFIED')
  && !plannedResult.blockers.includes('OWNER_PRODUCTION_ROUTE_AUTHORIZATION_REQUIRED')
);
pass('preflight never declares Production activation ready by itself',
  plannedResult.production_activation_ready===false
);
pass('required boundary exception stays exact callback GET only',
  plannedResult.required_boundary_change.exact_path===__test.CALLBACK_PATH
  && plannedResult.required_boundary_change.method==='GET'
  && plannedResult.required_boundary_change.blanket_cross_site_api_exception_for_other_paths===false
);
pass('required wiring order preserves existing CRM fallback',
  plannedResult.required_wiring_order.at(-1)==='fall_through_to_existing_customer_crm_request'
);
pass('preflight executes zero Production mutation',
  Object.values(plannedResult.invariant).every(value=>value===false)
);

const health=memberProductionRouteWiringPreflightHealth();
pass('health is source-only static analysis',
  health.member_production_route_wiring_preflight===true
  && health.source_only===true
  && health.static_analysis_only===true
);
pass('health records zero Production modification activation deploy storage write and LINE send',
  health.production_entry_modified===false
  && health.production_route_activated===false
  && health.production_deploy_executed===false
  && health.production_storage_binding_changed===false
  && health.production_storage_fetch_executed===false
  && health.production_schema_apply_executed===false
  && health.production_write_executed===false
  && health.crm_write===false
  && health.line_send===false
  && health.customer_id_generation===false
);

console.log(`MEMBER_PRODUCTION_ROUTE_WIRING_PREFLIGHT=${n}/${n} PASS`);
