import {
  buildMemberProductionDefaultOffWiringCandidate,
  inspectMemberProductionCandidate
} from './member-production-integration-acceptance.mjs';

const BUILD='member-production-entry-default-off-manifest-20260925-04';
const PRE_WIRING_ENTRY_BLOB_SHA='9791174a5be2aa8861134f6881e2cee451f50966';
const APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA='9b325cb61c3fe67006cef2ecc03d5769d7a693a4';
const ENTRY_PATH='src/production-index-crm-customer360-entry.js';
const SHA40_RE=/^[0-9a-f]{40}$/;

const text=value=>value==null?'':String(value).trim();

function normalizeSha(value){
  const normalized=text(value).toLowerCase();
  return SHA40_RE.test(normalized)?normalized:'';
}

function inspectionPass(inspection){
  const values=Object.values(inspection||{});
  return values.length>0&&values.every(value=>value===true);
}

function commonResult({observedMain,entryBlob,inspection,source,status}){
  return {
    status,
    build:BUILD,
    source_only:true,
    baseline_matches:true,
    exact_baseline:{
      observed_current_main_sha:observedMain,
      expected_current_main_sha:observedMain,
      exact_main_match:true,
      entry_blob_sha:entryBlob,
      entry_path:ENTRY_PATH
    },
    candidate:{
      source,
      inspection,
      default_off:true,
      owner_flag:'MEMBER_PRODUCTION_OWNER_APPROVED',
      route_mode_env:'MEMBER_PRODUCTION_ROUTE_MODE',
      line_login_approved:false,
      public_asset_adapter:null,
      private_media_storage_adapter:null
    },
    canonical_source_state:{
      default_off_wiring_applied:entryBlob===APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA,
      production_runtime_deployed:false,
      production_route_activated:false
    },
    future_patch_contract:{
      allowed_primary_path:ENTRY_PATH,
      fresh_current_main_sha_required:true,
      expected_main_sha_must_match_fresh_observation:true,
      generated_from_exact_entry_blob:true,
      apply_exact_candidate_only:true,
      manual_edit_after_generation:false,
      entry_patch_required:entryBlob!==APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA,
      production_deploy_separate_gate:true,
      route_activation_separate_gate:true,
      line_login_exchange_separate_gate:true,
      public_asset_binding_separate_gate:true,
      private_media_binding_separate_gate:true
    },
    existing_release_gate:{
      workflow:'.github/workflows/deploy-cloudflare.yml',
      production_entry_path_triggers_full_regression:true,
      all_local_tests:true,
      production_entry_import:true,
      wrangler_dry_run:true,
      deploy_on_pull_request:false
    },
    invariant:{
      production_route_activated:false,
      production_deploy:false,
      production_schema_apply:false,
      production_d1_write:false,
      public_asset_binding:false,
      private_media_binding:false,
      public_asset_fetch:false,
      private_media_fetch:false,
      crm_write:false,
      line_send:false,
      customer_id_generation:false,
      checkout_payment:false,
      paid_spend:false
    }
  };
}

export function buildMemberProductionEntryDefaultOffManifest({
  production_entry_source='',
  observed_current_main_sha='',
  expected_current_main_sha='',
  base_entry_blob_sha=APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA
}={}){
  const source=String(production_entry_source||'');
  const observedMain=normalizeSha(observed_current_main_sha);
  const expectedMain=normalizeSha(expected_current_main_sha);
  const entryBlob=text(base_entry_blob_sha).toLowerCase();

  if(!observedMain||!expectedMain){
    return {
      status:'current_main_sha_required',
      build:BUILD,
      source_only:true,
      baseline_matches:false,
      expected:{
        main_sha_strategy:'fresh_exact_match_required',
        pre_wiring_entry_blob_sha:PRE_WIRING_ENTRY_BLOB_SHA,
        applied_default_off_entry_blob_sha:APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA,
        entry_path:ENTRY_PATH
      },
      production_write:false
    };
  }

  if(observedMain!==expectedMain){
    return {
      status:'current_main_sha_mismatch',
      build:BUILD,
      source_only:true,
      baseline_matches:false,
      observed_current_main_sha:observedMain,
      expected_current_main_sha:expectedMain,
      production_write:false
    };
  }

  if(entryBlob===APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA){
    const inspection=inspectMemberProductionCandidate(source);
    if(!inspectionPass(inspection)){
      return {
        status:'applied_source_inspection_failed',
        build:BUILD,
        source_only:true,
        baseline_matches:false,
        exact_main_sha:observedMain,
        entry_blob_sha:entryBlob,
        inspection,
        production_write:false
      };
    }
    return commonResult({
      observedMain,
      entryBlob,
      inspection,
      source,
      status:'already_applied_default_off'
    });
  }

  if(entryBlob!==PRE_WIRING_ENTRY_BLOB_SHA){
    return {
      status:'entry_blob_mismatch',
      build:BUILD,
      source_only:true,
      baseline_matches:false,
      exact_main_sha:observedMain,
      expected:{
        pre_wiring_entry_blob_sha:PRE_WIRING_ENTRY_BLOB_SHA,
        applied_default_off_entry_blob_sha:APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA,
        entry_path:ENTRY_PATH
      },
      production_write:false
    };
  }

  const candidate=buildMemberProductionDefaultOffWiringCandidate(source);
  if(candidate.status!=='ok'){
    return {
      status:'candidate_generation_failed',
      build:BUILD,
      source_only:true,
      baseline_matches:true,
      exact_main_sha:observedMain,
      candidate_status:candidate.status,
      candidate_failures:[...(candidate.failures||[])],
      production_write:false
    };
  }

  const inspection=inspectMemberProductionCandidate(candidate.candidate_source);
  if(!inspectionPass(inspection)){
    return {
      status:'candidate_inspection_failed',
      build:BUILD,
      source_only:true,
      baseline_matches:true,
      exact_main_sha:observedMain,
      inspection,
      production_write:false
    };
  }

  return commonResult({
    observedMain,
    entryBlob,
    inspection,
    source:candidate.candidate_source,
    status:'candidate_ready'
  });
}

export function memberProductionEntryDefaultOffManifestHealth(){
  return {
    member_production_entry_default_off_manifest:true,
    build:BUILD,
    source_only:true,
    main_sha_strategy:'fresh_exact_match_required',
    static_main_sha_pinning:false,
    fresh_main_gate_required:true,
    pre_wiring_entry_blob_sha:PRE_WIRING_ENTRY_BLOB_SHA,
    applied_default_off_entry_blob_sha:APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA,
    baseline_entry_blob_sha:APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA,
    entry_path:ENTRY_PATH,
    exact_candidate_only:true,
    canonical_source_entry_default_off_applied:true,
    production_runtime_deployed:false,
    production_route_activated:false,
    production_deploy:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  PRE_WIRING_ENTRY_BLOB_SHA,
  APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA,
  BASE_ENTRY_BLOB_SHA:APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA,
  ENTRY_PATH,
  SHA40_RE,
  normalizeSha,
  inspectionPass
};
