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

pass('current Production entry has stable env-aware global request boundary anchor',
  current.current.global_request_boundary_verified===true
);
pass('current Production entry has stable CRM dispatch anchor',
  current.current.crm_dispatch_anchor_verified===true
);
pass('current Production response hardener is available',
  current.current.production_response_hardener_verified===true
);
pass('current canonical source imports Member Production request composition',
  current.current.member_composition_imported===true
);
pass('current canonical source dispatches Member Production request composition',
  current.current.member_composition_dispatched===true
);
pass('current canonical source dispatches Member after boundary and before CRM',
  current.current.member_composition_safe_order===true
);
pass('current Production entry has no Member API namespace collision',
  current.current.member_api_namespace_collision===false
);
pass('current global boundary retains cross-site API protection',
  current.current.cross_site_api_blanket===true
);
pass('current global boundary retains cross-origin sensitive-path protection',
  current.current.cross_origin_blanket===true
);
pass('current canonical source has exact LINE callback boundary exception',
  current.current.exact_line_callback_boundary_exception===true
);
pass('LINE callback boundary is compatible through exact narrow exception',
  current.current.line_callback_boundary_compatible===true
);
pass('current canonical source exposes Member browser page surface',
  current.current.member_browser_page_route_present===true
);
pass('current canonical request composition provides Member asset surface',
  current.current.member_asset_route_present===true
);
pass('current wrangler still does not declare implicit Member private-media binding',
  current.current.wrangler_member_private_media_binding_declared===false
);
pass('canonical default-off wiring keeps source plan ready',
  current.source_plan_ready===true
);
pass('preflight never declares Production activation ready by itself',
  current.production_activation_ready===false
);
pass('canonical structural wiring blockers are cleared',
  !current.blockers.includes('MEMBER_PRODUCTION_COMPOSITION_NOT_WIRED')
  && !current.blockers.includes('MEMBER_PRODUCTION_COMPOSITION_ORDER_UNSAFE')
  && !current.blockers.includes('LINE_CALLBACK_CROSS_SITE_BOUNDARY_EXCEPTION_NOT_READY')
  && !current.blockers.includes('MEMBER_BROWSER_PAGE_ROUTE_NOT_READY')
  && !current.blockers.includes('MEMBER_ASSET_ROUTE_NOT_READY')
);
pass('current blocker still requires private-media Production binding evidence',
  current.blockers.includes('PRIVATE_MEDIA_PRODUCTION_STORAGE_BINDING_NOT_VERIFIED')
);
pass('current blocker still requires fresh Owner route authorization',
  current.blockers.includes('OWNER_PRODUCTION_ROUTE_AUTHORIZATION_REQUIRED')
);

const observed=analyzeMemberProductionRouteWiring({
  production_entry_source:production,
  wrangler_source:wrangler,
  observed:{
    private_media_storage_binding_ready:true,
    production_route_owner_authorized:true
  }
});
pass('explicit observed binding and Owner evidence clears remaining preflight blockers',
  observed.blockers.length===0
);
pass('even complete observed evidence cannot self-authorize Production activation',
  observed.production_activation_ready===false
);
pass('required boundary exception remains exact callback GET only',
  observed.required_boundary_change.exact_path===__test.CALLBACK_PATH
  && observed.required_boundary_change.method==='GET'
  && observed.required_boundary_change.blanket_cross_site_api_exception_for_other_paths===false
);
pass('required wiring order preserves existing CRM fallback',
  observed.required_wiring_order.at(-1)==='fall_through_to_existing_customer_crm_request'
);
pass('preflight operation executes zero Production mutation',
  Object.values(observed.invariant).every(value=>value===false)
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
