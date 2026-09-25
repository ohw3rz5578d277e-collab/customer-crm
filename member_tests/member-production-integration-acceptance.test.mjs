import fs from 'node:fs';
import {
  buildMemberProductionDefaultOffWiringCandidate,
  inspectMemberProductionCandidate,
  buildMemberProductionIntegrationAcceptance,
  memberProductionIntegrationAcceptanceHealth,
  __test
} from '../src/member-production-integration-acceptance.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const production=fs.readFileSync(
  new URL('../src/production-index-crm-customer360-entry.js',import.meta.url),
  'utf8'
);

pass('current canonical Production entry imports Member Production composition exactly once',production.includes(__test.IMPORT_LINE)&&__test.count(production,__test.IMPORT_LINE)===1);
pass('current canonical Production entry preserves CRM dispatch anchor exactly once',__test.count(production,__test.CRM_DISPATCH)===1);
pass('current canonical Production boundary receives env',production.includes('export function enforceProductionRequestBoundary(request,env){'));

const currentInspection=inspectMemberProductionCandidate(production);
pass('current canonical source passes complete default-off inspection',Object.values(currentInspection).length>0&&Object.values(currentInspection).every(value=>value===true));
pass('current canonical source Owner flag defaults false',currentInspection.owner_flag_default_false===true);
pass('current canonical source keeps exact route mode gate',currentInspection.route_mode_checked===true);
pass('current canonical source keeps LINE approval false',currentInspection.line_login_default_false===true);
pass('current canonical source keeps public and private adapters null',currentInspection.public_asset_adapter_default_null===true&&currentInspection.private_media_adapter_default_null===true);
pass('current canonical source preserves callback and CRM ordering safety',currentInspection.exact_callback_get_only===true&&currentInspection.callback_exception_owner_and_mode_gated===true&&currentInspection.dispatch_after_boundary_before_crm===true&&currentInspection.existing_crm_dispatch_preserved===true);

const second=buildMemberProductionDefaultOffWiringCandidate(production);
pass('candidate builder fails closed if run against already-applied canonical source',second.status==='anchor_mismatch'&&second.failures.length>0);

const acceptance=buildMemberProductionIntegrationAcceptance({
  production_entry_source:production,
  env:{}
});
pass('integration acceptance is source-ready from current canonical source',acceptance.source_ready===true&&acceptance.default_off_candidate_ready===true);
pass('acceptance recognizes canonical default-off entry source as already applied',acceptance.canonical_source_entry_default_off_applied===true&&acceptance.candidate.status==='already_applied_default_off');
pass('static acceptance never declares Production activation ready',acceptance.production_activation_ready===false);
pass('applied canonical source no longer requires an entry patch blocker',!acceptance.blockers.includes('PRODUCTION_ENTRY_CANDIDATE_NOT_APPLIED')&&!acceptance.blockers.includes('OWNER_PRODUCTION_ENTRY_MODIFICATION_AUTHORIZATION_REQUIRED'));
pass('acceptance still requires separate route activation authorization',acceptance.blockers.includes('OWNER_PRODUCTION_ROUTE_ACTIVATION_AUTHORIZATION_REQUIRED'));
pass('acceptance still requires route mode runtime dependencies and both asset bindings',acceptance.blockers.includes('MEMBER_PRODUCTION_ROUTE_MODE_NOT_ENABLED')&&acceptance.blockers.includes('MEMBER_RUNTIME_DEPENDENCIES_NOT_VERIFIED')&&acceptance.blockers.includes('PUBLIC_ASSET_BINDING_NOT_VERIFIED')&&acceptance.blockers.includes('PRIVATE_MEDIA_BINDING_NOT_VERIFIED'));
pass('acceptance still requires a read-only Member canary',acceptance.blockers.includes('MEMBER_READ_ONLY_CANARY_NOT_PASSED'));
pass('post-merge sequence starts from canonical default-off source and keeps activation later',acceptance.acceptance_sequence[0]==='canonical_default_off_entry_source_already_applied'&&acceptance.acceptance_sequence.indexOf('fresh_owner_authorize_route_activation_exact_sha_and_scope')<acceptance.acceptance_sequence.indexOf('activate_member_route_mode_and_owner_flag_in_exact_release'));
pass('writes and commerce stay outside Member route activation',acceptance.acceptance_sequence.at(-1)==='keep_favorites_memory_black_and_commerce_write_gates_separate');
pass('acceptance operation performs zero Production mutation',Object.values(acceptance.invariant).every(value=>value===false));

const observed=buildMemberProductionIntegrationAcceptance({
  production_entry_source:production,
  env:{MEMBER_PRODUCTION_ROUTE_MODE:'enabled'},
  observed:{
    owner_production_route_activation_authorized:true,
    production_route_mode_enabled:true,
    member_runtime_dependencies_verified:true,
    public_asset_binding_verified:true,
    private_media_binding_verified:true,
    read_only_canary_passed:true
  }
});
pass('even complete observed evidence cannot self-authorize activation',observed.blockers.length===0&&observed.production_activation_ready===false);

const health=memberProductionIntegrationAcceptanceHealth();
pass('health is source-only static acceptance',health.member_production_integration_acceptance===true&&health.source_only===true&&health.static_acceptance_only===true);
pass('health expects canonical default-off source without claiming Production deployment',health.canonical_source_entry_default_off_expected===true&&health.production_route_activated===false&&health.production_deploy===false&&health.production_write===false);

console.log('MEMBER_PRODUCTION_INTEGRATION_ACCEPTANCE='+n+'/'+n+' PASS');
