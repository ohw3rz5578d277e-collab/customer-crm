import { buildMemberLineLoginPromotionReadiness, __test as readinessTest } from './member-production-line-login-promotion-readiness.mjs';

const PACKET='member_line_login_promotion_blocked_handoff_v1';
const EXECUTABLE_WORKFLOW_PATH='.github/workflows/member-production-line-login-promotion.yml';
const ALLOWED_NON_RUNTIME_DIFFS=Object.freeze([
  'docs/member-app/member-production-line-login-promotion-readiness.md',
  'member_tests/member-production-line-login-promotion-readiness.test.mjs',
  'src/member-production-line-login-promotion-readiness.mjs'
]);
const AUTHORIZED_CANDIDATE=readinessTest.AUTHORIZED_CANDIDATE;
const SHA_RE=/^[0-9a-f]{40}$/i;

function text(v){return v==null?'':String(v).trim();}
function bool(v){return v===true;}
function sortedStrings(values){return Array.isArray(values)?values.map(text).filter(Boolean).sort():[];}

export function buildMemberLineLoginPromotionBlockedHandoff(input={}){
  const currentMainSha=text(input.current_main_sha).toLowerCase();
  const candidateSourceSha=text(input.candidate_source_sha).toLowerCase();
  const stagedVersionId=text(input.staged_version_id).toLowerCase();
  const candidateSha256=text(input.candidate_sha256).toLowerCase();
  const stagingRunId=text(input.staging_run_id);
  const expectedActiveVersionId=text(input.expected_active_version_id).toLowerCase();
  const blockers=[];

  if(!SHA_RE.test(currentMainSha))blockers.push('CURRENT_MAIN_SHA_INVALID');
  if(candidateSourceSha!==AUTHORIZED_CANDIDATE.expected_sha)blockers.push('CANDIDATE_SOURCE_SHA_MISMATCH');
  if(stagedVersionId!==AUTHORIZED_CANDIDATE.staged_version_id)blockers.push('STAGED_VERSION_ID_MISMATCH');
  if(candidateSha256!==AUTHORIZED_CANDIDATE.candidate_sha256)blockers.push('CANDIDATE_SHA256_MISMATCH');
  if(stagingRunId!==AUTHORIZED_CANDIDATE.staging_run_id)blockers.push('STAGING_RUN_ID_MISMATCH');
  if(expectedActiveVersionId!==AUTHORIZED_CANDIDATE.expected_active_version_id)blockers.push('EXPECTED_ACTIVE_VERSION_ID_MISMATCH');

  try{
    const readiness=buildMemberLineLoginPromotionReadiness({
      expected_sha:candidateSourceSha,
      staged_version_id:stagedVersionId,
      candidate_sha256:candidateSha256,
      staging_run_id:stagingRunId,
      expected_active_version_id:expectedActiveVersionId,
      stage_receipt:input.stage_receipt,
      active_deployment:input.active_deployment,
      staged_version:input.staged_version
    });
    if(readiness.readiness!=='PASS')blockers.push('PROMOTION_READINESS_NOT_PASS');
  }catch(error){
    blockers.push('PROMOTION_READINESS_INVALID:'+text(error?.message||error));
  }

  const drift=input.runtime_drift_evidence||{};
  if(!bool(drift.checked))blockers.push('RUNTIME_DRIFT_NOT_CHECKED');
  if(text(drift.base_sha).toLowerCase()!==AUTHORIZED_CANDIDATE.expected_sha)blockers.push('RUNTIME_DRIFT_BASE_SHA_MISMATCH');
  if(text(drift.head_sha).toLowerCase()!==currentMainSha)blockers.push('RUNTIME_DRIFT_HEAD_SHA_MISMATCH');
  if(drift.production_entry_changed!==false)blockers.push('PRODUCTION_ENTRY_DRIFT_NOT_EXCLUDED');
  if(drift.wrangler_changed!==false)blockers.push('WRANGLER_DRIFT_NOT_EXCLUDED');
  if(drift.runtime_workflow_changed!==false)blockers.push('RUNTIME_WORKFLOW_DRIFT_NOT_EXCLUDED');
  const changedFiles=sortedStrings(drift.changed_files);
  const allowed=sortedStrings(ALLOWED_NON_RUNTIME_DIFFS);
  if(JSON.stringify(changedFiles)!==JSON.stringify(allowed))blockers.push('NON_RUNTIME_DIFF_SET_MISMATCH');

  const safety=input.repository_safety_evidence||{};
  if(!bool(safety.executable_workflow_write_blocked))blockers.push('EXECUTABLE_WORKFLOW_BLOCK_NOT_CONFIRMED');
  if(text(safety.blocked_path)!==EXECUTABLE_WORKFLOW_PATH)blockers.push('BLOCKED_PATH_MISMATCH');
  if(safety.bypass_attempted!==false)blockers.push('BYPASS_STATE_NOT_FALSE');
  if(safety.executable_promotion_path_available!==false)blockers.push('EXECUTABLE_PROMOTION_PATH_NOT_FALSE');

  const packetReady=blockers.length===0;
  return {
    packet:PACKET,
    packet_ready:packetReady,
    authorization_granted:false,
    executable_promotion_available:false,
    repository_safety_blocked:true,
    blocked_path:EXECUTABLE_WORKFLOW_PATH,
    current_main_sha:currentMainSha,
    candidate_source_sha:AUTHORIZED_CANDIDATE.expected_sha,
    staged_version_id:AUTHORIZED_CANDIDATE.staged_version_id,
    candidate_sha256:AUTHORIZED_CANDIDATE.candidate_sha256,
    staging_run_id:AUTHORIZED_CANDIDATE.staging_run_id,
    expected_active_version_id:AUTHORIZED_CANDIDATE.expected_active_version_id,
    allowed_non_runtime_diffs:[...ALLOWED_NON_RUNTIME_DIFFS],
    blockers,
    approval_text:'',
    next_boundary:'REPOSITORY_SAFE_EXECUTABLE_PROMOTION_PATH_REQUIRED',
    safety:{
      production_traffic_change:0,
      production_deploy:0,
      worker_version_upload:0,
      production_d1_read:0,
      production_d1_write:0,
      migration_apply:0,
      r2_object_read:0,
      r2_object_write:0,
      secret_change:0,
      line_send:0,
      customer_prospect_mutation:0,
      customer_id_generation:0,
      private_media_route_activation:0,
      favorites_write_activation:0,
      memory_write_activation:0,
      family_pass_write_activation:0,
      commerce_activation:0,
      black_automatic_award:0,
      paid_spend:0,
      issue_26_command_post:0
    }
  };
}

export function memberLineLoginPromotionBlockedHandoffHealth(){
  return {
    member_line_login_promotion_blocked_handoff:true,
    packet:PACKET,
    source_only:true,
    executable_promotion_available:false,
    authorization_granted:false,
    production_mutation:false,
    next_boundary:'REPOSITORY_SAFE_EXECUTABLE_PROMOTION_PATH_REQUIRED'
  };
}

export const __test={
  PACKET,
  EXECUTABLE_WORKFLOW_PATH,
  ALLOWED_NON_RUNTIME_DIFFS,
  AUTHORIZED_CANDIDATE,
  SHA_RE
};
