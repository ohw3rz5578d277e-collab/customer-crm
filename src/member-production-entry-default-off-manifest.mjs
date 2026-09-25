import {
  buildMemberProductionDefaultOffWiringCandidate,
  inspectMemberProductionCandidate
} from './member-production-integration-acceptance.mjs';

const BUILD='member-production-entry-default-off-manifest-20260925-01';
const BASE_MAIN_SHA='bd10e2056918c59a1bf66d13ec4537bd3427c7a4';
const BASE_ENTRY_BLOB_SHA='9791174a5be2aa8861134f6881e2cee451f50966';
const ENTRY_PATH='src/production-index-crm-customer360-entry.js';

const text=value=>value==null?'':String(value).trim();

export function buildMemberProductionEntryDefaultOffManifest({
  production_entry_source='',
  base_main_sha=BASE_MAIN_SHA,
  base_entry_blob_sha=BASE_ENTRY_BLOB_SHA
}={}){
  const source=String(production_entry_source||'');
  const baselineMatches=
    text(base_main_sha)===BASE_MAIN_SHA
    && text(base_entry_blob_sha)===BASE_ENTRY_BLOB_SHA;

  if(!baselineMatches){
    return {
      status:'baseline_mismatch',
      build:BUILD,
      source_only:true,
      baseline_matches:false,
      expected:{
        main_sha:BASE_MAIN_SHA,
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
      candidate_status:candidate.status,
      candidate_failures:[...(candidate.failures||[])],
      expected:{
        main_sha:BASE_MAIN_SHA,
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
      main_sha:BASE_MAIN_SHA,
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
      generated_from_exact_baseline:true,
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
    baseline_main_sha:BASE_MAIN_SHA,
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
  BASE_MAIN_SHA,
  BASE_ENTRY_BLOB_SHA,
  ENTRY_PATH
};
