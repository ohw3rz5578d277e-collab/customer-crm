const BUILD='member-production-line-login-promotion-readiness-20261012-05';

const UUID_RE=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SHA_RE=/^[0-9a-f]{40}$/i;
const SHA256_RE=/^[0-9a-f]{64}$/i;

const AUTHORIZED_CANDIDATE=Object.freeze({
  expected_sha:'fdfa65fa797c3ee89885ee99244e7a9ad691b124',
  staged_version_id:'be711f54-8716-4178-9949-d5ced183d9dc',
  candidate_sha256:'af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8',
  staging_run_id:'38233408939',
  expected_active_version_id:'6d34db85-9a52-45e9-8f7d-094f9fc7957a'
});

const REQUIRED_DISABLED=Object.freeze([
  'private_media_content_route',
  'favorites_write',
  'memory_write',
  'family_pass_entitlement_write'
]);

function exactString(value){return value==null?'':String(value).trim();}
function bool(value){return value===true;}
function zero(value){return value===0||value==='0';}

export function buildMemberLineLoginPromotionReadiness(input={}){
  const expectedSha=exactString(input.expected_sha).toLowerCase();
  const stagedVersionId=exactString(input.staged_version_id).toLowerCase();
  const candidateSha256=exactString(input.candidate_sha256).toLowerCase();
  const stagingRunId=exactString(input.staging_run_id);
  const expectedActiveVersionId=exactString(input.expected_active_version_id).toLowerCase();

  if(!SHA_RE.test(expectedSha))throw new Error('INVALID_EXPECTED_SHA');
  if(!UUID_RE.test(stagedVersionId))throw new Error('INVALID_STAGED_VERSION_ID');
  if(!SHA256_RE.test(candidateSha256))throw new Error('INVALID_CANDIDATE_SHA256');
  if(!/^\d+$/.test(stagingRunId))throw new Error('INVALID_STAGING_RUN_ID');
  if(!UUID_RE.test(expectedActiveVersionId))throw new Error('INVALID_EXPECTED_ACTIVE_VERSION_ID');

  if(expectedSha!==AUTHORIZED_CANDIDATE.expected_sha)throw new Error('UNAUTHORIZED_EXPECTED_SHA');
  if(stagedVersionId!==AUTHORIZED_CANDIDATE.staged_version_id)throw new Error('UNAUTHORIZED_STAGED_VERSION_ID');
  if(candidateSha256!==AUTHORIZED_CANDIDATE.candidate_sha256)throw new Error('UNAUTHORIZED_CANDIDATE_SHA256');
  if(stagingRunId!==AUTHORIZED_CANDIDATE.staging_run_id)throw new Error('UNAUTHORIZED_STAGING_RUN_ID');
  if(expectedActiveVersionId!==AUTHORIZED_CANDIDATE.expected_active_version_id)throw new Error('UNAUTHORIZED_ACTIVE_VERSION_ID');
  if(stagedVersionId===expectedActiveVersionId)throw new Error('STAGED_VERSION_ALREADY_ACTIVE');

  const receipt=input.stage_receipt||{};
  const receiptChecks={
    success:bool(receipt.success),
    exact_sha:exactString(receipt.exact_sha).toLowerCase()===AUTHORIZED_CANDIDATE.expected_sha,
    staged_version:exactString(receipt.staged_version_id).toLowerCase()===AUTHORIZED_CANDIDATE.staged_version_id,
    candidate_digest:exactString(receipt.candidate_sha256).toLowerCase()===AUTHORIZED_CANDIDATE.candidate_sha256,
    unchanged_active:exactString(receipt.unchanged_active_version_id).toLowerCase()===AUTHORIZED_CANDIDATE.expected_active_version_id,
    staging_run:exactString(receipt.staging_run_id)===AUTHORIZED_CANDIDATE.staging_run_id,
    owner_approval:bool(receipt.owner_approved),
    route:bool(receipt.member_route_enabled),
    line_approval:bool(receipt.line_login_approved),
    line_exchange:bool(receipt.line_login_external_exchange_enabled),
    traffic:zero(receipt.production_traffic_change),
    deploy:zero(receipt.production_deploy),
    d1_read:zero(receipt.production_d1_read),
    d1_write:zero(receipt.production_d1_write),
    r2_read:zero(receipt.r2_object_read),
    r2_write:zero(receipt.r2_object_write),
    secret:zero(receipt.secret_change),
    line_send:zero(receipt.line_send),
    customer:zero(receipt.customer_prospect_mutation),
    customer_id:zero(receipt.customer_id_generation),
    commerce:zero(receipt.commerce_activation),
    black:zero(receipt.black_automatic_award)
  };
  for(const key of REQUIRED_DISABLED){receiptChecks[key]=receipt[key]===false;}
  const failedReceipt=Object.entries(receiptChecks).filter(([,ok])=>!ok).map(([name])=>name);
  if(failedReceipt.length)throw new Error('LINE_LOGIN_STAGE_RECEIPT_INVALID:'+failedReceipt.join(','));

  const deployment=input.active_deployment||{};
  const versions=Array.isArray(deployment.versions)?deployment.versions:[];
  const full=versions.filter(v=>Number(v?.percentage)===100);
  if(versions.length!==1)throw new Error('PRODUCTION_BASELINE_NOT_SINGLE_VERSION');
  if(full.length!==1||exactString(full[0]?.version_id).toLowerCase()!==AUTHORIZED_CANDIDATE.expected_active_version_id){
    throw new Error('ACTIVE_VERSION_NOT_OWNER_EXPECTED');
  }
  if(versions.some(v=>exactString(v?.version_id).toLowerCase()===AUTHORIZED_CANDIDATE.staged_version_id&&Number(v?.percentage)>0)){
    throw new Error('STAGED_VERSION_ALREADY_RECEIVES_TRAFFIC');
  }

  const staged=input.staged_version||{};
  if(exactString(staged.version_id).toLowerCase()!==AUTHORIZED_CANDIDATE.staged_version_id)throw new Error('STAGED_VERSION_VIEW_ID_MISMATCH');
  if(exactString(staged.candidate_sha256).toLowerCase()!==AUTHORIZED_CANDIDATE.candidate_sha256)throw new Error('STAGED_VERSION_DIGEST_MISMATCH');
  if(staged.owner_approved!==true)throw new Error('STAGED_OWNER_APPROVAL_NOT_ENABLED');
  if(staged.member_route_enabled!==true)throw new Error('STAGED_MEMBER_ROUTE_NOT_ENABLED');
  if(staged.line_login_approved!==true)throw new Error('STAGED_LINE_LOGIN_APPROVAL_NOT_ENABLED');
  if(staged.line_login_external_exchange_enabled!==true)throw new Error('STAGED_LINE_LOGIN_EXCHANGE_NOT_ENABLED');
  for(const key of REQUIRED_DISABLED){
    if(staged[key]!==false)throw new Error('STAGED_PROHIBITED_MODE_NOT_DISABLED:'+key);
  }
  if(staged.resource_bindings_unchanged!==true)throw new Error('STAGED_RESOURCE_BINDINGS_CHANGED');

  return {
    build:BUILD,
    source_only:true,
    readiness:'PASS',
    expected_sha:AUTHORIZED_CANDIDATE.expected_sha,
    staged_version_id:AUTHORIZED_CANDIDATE.staged_version_id,
    candidate_sha256:AUTHORIZED_CANDIDATE.candidate_sha256,
    staging_run_id:AUTHORIZED_CANDIDATE.staging_run_id,
    expected_active_version_id:AUTHORIZED_CANDIDATE.expected_active_version_id,
    candidate_binding:'IMMUTABLE_SOURCE_CONSTANTS',
    owner_approval_required:true,
    production_traffic_current_percent:0,
    production_mutation:false,
    production_traffic_change:false,
    production_deploy:false,
    production_d1_read:false,
    production_d1_write:false,
    r2_object_read:false,
    r2_object_write:false,
    secret_change:false,
    line_send:false,
    customer_prospect_mutation:false,
    customer_id_generation:false,
    private_media_route_activation:false,
    favorites_write_activation:false,
    memory_write_activation:false,
    family_pass_write_activation:false,
    commerce_activation:false,
    black_automatic_award:false,
    next_boundary:'FRESH_OWNER_GATED_LINE_LOGIN_PROMOTION_REQUIRED'
  };
}

export function memberLineLoginPromotionReadinessHealth(){
  return {
    member_line_login_promotion_readiness:true,
    build:BUILD,
    source_only:true,
    candidate_binding:'IMMUTABLE_SOURCE_CONSTANTS',
    owner_approval_required:true,
    production_mutation:false,
    production_traffic_change:false,
    production_deploy:false,
    production_d1_read:false,
    production_d1_write:false,
    r2_object_read:false,
    r2_object_write:false,
    secret_change:false,
    line_send:false,
    customer_prospect_mutation:false,
    next_boundary:'FRESH_OWNER_GATED_LINE_LOGIN_PROMOTION_REQUIRED'
  };
}

export const __test={BUILD,UUID_RE,SHA_RE,SHA256_RE,AUTHORIZED_CANDIDATE,REQUIRED_DISABLED};
