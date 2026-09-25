import {
  buildMemberProductionDefaultOffWiringCandidate,
  inspectMemberProductionCandidate
} from './member-production-integration-acceptance.mjs';

const BUILD='member-production-entry-default-off-manifest-20260925-03';
const BASE_ENTRY_BLOB_SHA='9791174a5be2aa8861134f6881e2cee451f50966';
const ENTRY_PATH='src/production-index-crm-customer360-entry.js';
const SHA40_RE=/^[0-9a-f]{40}$/;

const text=value=>value==null?'':String(value).trim();

function normalizeSha(value){
  const normalized=text(value).toLowerCase();
  return SHA40_RE.test(normalized)?normalized:'';
}

export function buildMemberProductionEntryDefaultOffManifest({
  production_entry_source='',
  observed_current_main_sha='',
  expected_current_main_sha='',
  base_entry_blob_sha=BASE_ENTRY_BLOB_SHA
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
        entry_blob_sha:BASE_ENTRY_BLOB_SHA,
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
      expected:{
        main_sha_strategy:'fresh_exact_match_required',
        entry_blob_sha:BASE_ENTRY_BLOB_SHA,
        entry_path:ENTRY_PATH
      },
      production_write:false
    };
  }

  if(entryBlob!==BASE_ENTRY_BLOB_SHA){
    return {
      status:'entry_blob_mismatch',
      build:BUILD,
      source_only:true,
      baseline_matches:false,
      exact_main_sha:observedMain,
      expected:{
        main_sha_strategy:'fresh_exact_match_required',
        entry_blob_sha:BASE_ENTRY_BLOB_SHA,
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
      expected:{
        main_sha_strategy:'fresh_exact_match_required',
        entry_blob_sha:BASE_ENTRY_BLOB_SHA,
        entry_path:ENTRY_PATH
      },
      production_write:false
    };
  }

  const inspection=inspectMemberProductionCandidate(candidate.candidate_source);
  const inspectionValues=Object.values(inspection);
  const inspectionPass=
    inspectionValues.length>0
    && inspectionValues.every(value=>value===true);

  return {
    status:inspectionPass?'ok':'candidate_inspection_failed',
    build:BUILD,
    source_only:true,
    baseline_matches:true,
    exact_baseline:{
      observed_current_main_sha:observedMain,
      expected_current_main_sha:expectedMain,
      exact_main_match:true,
      entry_blob_sha:BASE_ENTRY_BLOB_SHA,
      entry_path:ENTRY_PATH
    },
    candidate:{
      source:inspectionPass?candidate.candidate_source:'',
      inspection,
      default_off:candidate.default_off===true,
      owner_flag:candidate.owner_flag,
      route_mode_env:candidate.route_mode_env,
      line_login_approved:candidate.line_login_approved,
      public_asset_adapter:candidate.public_asset_adapter,
      private_media_storage_adapter:candidate.private_media_storage_adapter
    },
    future_patch_contract:{
      allowed_primary_path:ENTRY_PATH,
      fresh_current_main_sha_required:true,
      expected_main_sha_must_match_fresh_observation:true,
      generated_from_exact_entry_blob:true,
      apply_exact_candidate_only:true,
      manual_edit_after_generation:false,
      owner_authorization_required_before_apply:true,
      owner_authorization_required_before_merge:true,
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
      production_entry_modified:false,
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

export function memberProductionEntryDefaultOffManifestHealth(){
  return {
    member_production_entry_default_off_manifest:true,
    build:BUILD,
    source_only:true,
    main_sha_strategy:'fresh_exact_match_required',
    static_main_sha_pinning:false,
    fresh_main_gate_required:true,
    baseline_entry_blob_sha:BASE_ENTRY_BLOB_SHA,
    entry_path:ENTRY_PATH,
    exact_candidate_only:true,
    production_entry_modified:false,
    production_route_activated:false,
    production_deploy:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  BASE_ENTRY_BLOB_SHA,
  ENTRY_PATH,
  SHA40_RE,
  normalizeSha
};
