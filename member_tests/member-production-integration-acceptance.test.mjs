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

pass('current Production entry has not imported Member Production composition',!production.includes(__test.IMPORT_LINE));
pass('current Production entry still has original CRM dispatch anchor',production.includes(__test.CRM_DISPATCH));
pass('current Production entry still has original boundary signature',production.includes('export function enforceProductionRequestBoundary(request){'));

const candidate=buildMemberProductionDefaultOffWiringCandidate(production);
pass('exact default-off wiring candidate builds from current canonical entry',candidate.status==='ok'&&candidate.failures.length===0&&candidate.candidate_source.length>production.length);
pass('candidate is explicitly source-only and default-off',candidate.source_only===true&&candidate.default_off===true&&candidate.production_write===false);
pass('candidate leaves LINE external exchange off',candidate.line_login_approved===false);
pass('candidate leaves both asset adapters unbound',candidate.public_asset_adapter===null&&candidate.private_media_storage_adapter===null);

const inspection=inspectMemberProductionCandidate(candidate.candidate_source);
pass('candidate imports one Production-facing Member handler',inspection.import_present===true);
pass('candidate Owner flag defaults false',inspection.owner_flag_default_false===true);
pass('candidate requires exact route mode env',inspection.route_mode_checked===true);
pass('candidate marks Member browser page sensitive without marking public assets sensitive',inspection.member_path_sensitive===true);
pass('candidate callback exception is exact GET only',inspection.exact_callback_get_only===true);
pass('candidate callback exception requires Owner flag plus route mode',inspection.callback_exception_owner_and_mode_gated===true);
pass('candidate narrows cross-origin exception to callback flag',inspection.callback_cross_origin_exception_narrow===true);
pass('candidate narrows cross-site API exception to callback flag',inspection.callback_cross_site_api_exception_narrow===true);
pass('candidate dispatches Member after global boundary and before existing CRM',inspection.dispatch_after_boundary_before_crm===true);
pass('candidate passes same Owner flag to Member handler',inspection.owner_flag_passed_to_handler===true);
pass('candidate keeps LINE approval false',inspection.line_login_default_false===true);
pass('candidate keeps public asset adapter null',inspection.public_asset_adapter_default_null===true);
pass('candidate keeps private media adapter null',inspection.private_media_adapter_default_null===true);
pass('candidate preserves existing CRM dispatch exactly once',inspection.existing_crm_dispatch_preserved===true);

const second=buildMemberProductionDefaultOffWiringCandidate(candidate.candidate_source);
pass('candidate builder fails closed if applied twice',second.status==='anchor_mismatch'&&second.failures.length>0);

const acceptance=buildMemberProductionIntegrationAcceptance({
  production_entry_source:production,
  env:{}
});
pass('integration acceptance is source-ready from current canonical source',acceptance.source_ready===true&&acceptance.default_off_candidate_ready===true);
pass('static acceptance never declares Production activation ready',acceptance.production_activation_ready===false);
pass('acceptance records unapplied Production entry candidate',acceptance.blockers.includes('PRODUCTION_ENTRY_CANDIDATE_NOT_APPLIED'));
pass('acceptance requires fresh Owner Production entry authorization',acceptance.blockers.includes('OWNER_PRODUCTION_ENTRY_MODIFICATION_AUTHORIZATION_REQUIRED'));
pass('acceptance requires separate route activation authorization',acceptance.blockers.includes('OWNER_PRODUCTION_ROUTE_ACTIVATION_AUTHORIZATION_REQUIRED'));
pass('acceptance requires runtime dependencies and both asset bindings',acceptance.blockers.includes('MEMBER_RUNTIME_DEPENDENCIES_NOT_VERIFIED')&&acceptance.blockers.includes('PUBLIC_ASSET_BINDING_NOT_VERIFIED')&&acceptance.blockers.includes('PRIVATE_MEDIA_BINDING_NOT_VERIFIED'));
pass('acceptance requires a read-only Member canary',acceptance.blockers.includes('MEMBER_READ_ONLY_CANARY_NOT_PASSED'));
pass('acceptance sequence separates default-off entry patch from later route activation',acceptance.acceptance_sequence.indexOf('apply_default_off_production_entry_candidate')<acceptance.acceptance_sequence.indexOf('activate_member_route_mode_and_owner_flag_in_exact_release'));
pass('writes and commerce stay outside Member route activation',acceptance.acceptance_sequence.at(-1)==='keep_favorites_memory_black_and_commerce_write_gates_separate');
pass('acceptance performs zero Production mutation',Object.values(acceptance.invariant).every(value=>value===false));

const observed=buildMemberProductionIntegrationAcceptance({
  production_entry_source:production,
  env:{MEMBER_PRODUCTION_ROUTE_MODE:'enabled'},
  observed:{
    production_entry_candidate_applied:true,
    owner_production_entry_modification_authorized:true,
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
pass('health keeps Production entry route deploy and write off',health.production_entry_modified===false&&health.production_route_activated===false&&health.production_deploy===false&&health.production_write===false);

console.log('MEMBER_PRODUCTION_INTEGRATION_ACCEPTANCE='+n+'/'+n+' PASS');
