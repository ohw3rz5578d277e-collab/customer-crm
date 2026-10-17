import { buildMemberLineLoginPromotionReadiness } from './member-production-line-login-promotion-readiness.mjs';

const PACKET='member_line_login_promotion_blocked_handoff_v1';
const EXECUTABLE_WORKFLOW_PATH='.github/workflows/member-production-line-login-promotion.yml';
const REQUIRED_READINESS_DIFFS=Object.freeze([
  'docs/member-app/member-production-line-login-promotion-readiness.md',
  'member_tests/member-production-line-login-promotion-readiness.test.mjs',
  'src/member-production-line-login-promotion-readiness.mjs'
]);
const REQUIRED_READINESS_BLOBS=Object.freeze({
  'docs/member-app/member-production-line-login-promotion-readiness.md':'ec1ed3fdc1fac59f70447eadfc9df7db6a972287',
  'member_tests/member-production-line-login-promotion-readiness.test.mjs':'a26ba0302d9e315e642c747dc8498df6cda4790e',
  'src/member-production-line-login-promotion-readiness.mjs':'8be93701a8fb5e3a2e9fd72776a7a2ce698aa140'
});
const REQUIRED_INHERITED_DIFFS=Object.freeze([
  'src/crm-customer-list-daily-operations.mjs',
  'tests/customer-list-daily-operations.mjs'
]);
const REQUIRED_INHERITED_BLOBS=Object.freeze({
  'src/crm-customer-list-daily-operations.mjs':'39f8abcb8671f8dd2d2f33c6c63e60228a29c992',
  'tests/customer-list-daily-operations.mjs':'e853fc46f8c2fd4ff84726eba99f82942ffe9b68'
});
const OPTIONAL_HANDOFF_DIFFS=Object.freeze([
  'docs/member-app/member-production-line-login-promotion-blocked-handoff.md',
  'member_tests/member-production-line-login-promotion-blocked-handoff.test.mjs',
  'src/member-production-line-login-promotion-blocked-handoff.mjs'
]);
const ALLOWED_NON_RUNTIME_DIFFS=Object.freeze([
  ...REQUIRED_READINESS_DIFFS,
  ...REQUIRED_INHERITED_DIFFS,
  ...OPTIONAL_HANDOFF_DIFFS
]);
const AUTHORIZED_CANDIDATE=Object.freeze({
  expected_sha:'fdfa65fa797c3ee89885ee99244e7a9ad691b124',
  staged_version_id:'be711f54-8716-4178-9949-d5ced183d9dc',
  candidate_sha256:'af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8',
  staging_run_id:'38233408939',
  expected_active_version_id:'6d34db85-9a52-45e9-8f7d-094f9fc7957a'
});
const SHA_RE=/^[0-9a-f]{40}$/i;

function text(v){return v==null?'':String(v).trim();}
function bool(v){return v===true;}
function sortedStrings(values){return Array.isArray(values)?values.map(text).filter(Boolean).sort():[];}
function plainObject(value){return value&&typeof value==='object'&&!Array.isArray(value)?value:{};}

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
    for(const [key,expected] of Object.entries(AUTHORIZED_CANDIDATE)){
      if(text(readiness[key]).toLowerCase()!==expected)blockers.push('PROMOTION_READINESS_IDENTITY_MISMATCH:'+key);
    }
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
  const changedSet=new Set(changedFiles);
  const allowedSet=new Set(ALLOWED_NON_RUNTIME_DIFFS);
  if(changedSet.size!==changedFiles.length)blockers.push('NON_RUNTIME_DIFF_DUPLICATE');
  if(changedFiles.some(path=>!allowedSet.has(path)))blockers.push('NON_RUNTIME_DIFF_NOT_ALLOWED');
  if(REQUIRED_READINESS_DIFFS.some(path=>!changedSet.has(path)))blockers.push('REQUIRED_READINESS_DIFF_MISSING');
  if(REQUIRED_INHERITED_DIFFS.some(path=>!changedSet.has(path)))blockers.push('REQUIRED_INHERITED_DIFF_MISSING');
  const handoffCount=OPTIONAL_HANDOFF_DIFFS.filter(path=>changedSet.has(path)).length;
  if(handoffCount!==0&&handoffCount!==OPTIONAL_HANDOFF_DIFFS.length)blockers.push('HANDOFF_DIFF_SET_PARTIAL');

  if(!bool(drift.readiness_blob_evidence_checked))blockers.push('READINESS_BLOB_EVIDENCE_NOT_CHECKED');
  const readinessBlobs=plainObject(drift.readiness_file_blobs);
  const blobPaths=Object.keys(readinessBlobs).sort();
  const requiredBlobPaths=Object.keys(REQUIRED_READINESS_BLOBS).sort();
  if(JSON.stringify(blobPaths)!==JSON.stringify(requiredBlobPaths))blockers.push('READINESS_BLOB_EVIDENCE_PATH_SET_MISMATCH');
  for(const [path,expectedBlob] of Object.entries(REQUIRED_READINESS_BLOBS)){
    if(text(readinessBlobs[path]).toLowerCase()!==expectedBlob)blockers.push('READINESS_BLOB_MISMATCH:'+path);
  }

  if(!bool(drift.inherited_blob_evidence_checked))blockers.push('INHERITED_BLOB_EVIDENCE_NOT_CHECKED');
  const inheritedBlobs=plainObject(drift.inherited_file_blobs);
  const inheritedBlobPaths=Object.keys(inheritedBlobs).sort();
  const requiredInheritedBlobPaths=Object.keys(REQUIRED_INHERITED_BLOBS).sort();
  if(JSON.stringify(inheritedBlobPaths)!==JSON.stringify(requiredInheritedBlobPaths))blockers.push('INHERITED_BLOB_EVIDENCE_PATH_SET_MISMATCH');
  for(const [path,expectedBlob] of Object.entries(REQUIRED_INHERITED_BLOBS)){
    if(text(inheritedBlobs[path]).toLowerCase()!==expectedBlob)blockers.push('INHERITED_BLOB_MISMATCH:'+path);
  }

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
    required_readiness_diffs:[...REQUIRED_READINESS_DIFFS],
    required_readiness_blobs:{...REQUIRED_READINESS_BLOBS},
    required_inherited_diffs:[...REQUIRED_INHERITED_DIFFS],
    required_inherited_blobs:{...REQUIRED_INHERITED_BLOBS},
    optional_handoff_diffs:[...OPTIONAL_HANDOFF_DIFFS],
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
    readiness_blob_binding:'IMMUTABLE_GIT_BLOBS',
    inherited_drift_blob_binding:'IMMUTABLE_GIT_BLOBS',
    executable_promotion_available:false,
    authorization_granted:false,
    production_mutation:false,
    next_boundary:'REPOSITORY_SAFE_EXECUTABLE_PROMOTION_PATH_REQUIRED'
  };
}

export const __test={
  PACKET,
  EXECUTABLE_WORKFLOW_PATH,
  REQUIRED_READINESS_DIFFS,
  REQUIRED_READINESS_BLOBS,
  REQUIRED_INHERITED_DIFFS,
  REQUIRED_INHERITED_BLOBS,
  OPTIONAL_HANDOFF_DIFFS,
  ALLOWED_NON_RUNTIME_DIFFS,
  AUTHORIZED_CANDIDATE,
  SHA_RE
};
