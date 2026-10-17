import assert from 'node:assert/strict';
import {
  buildMemberLineLoginPromotionBlockedHandoff,
  memberLineLoginPromotionBlockedHandoffHealth,
  __test
} from '../src/member-production-line-login-promotion-blocked-handoff.mjs';

const AUDITED_MAIN='fe9bbcf8c650941b9c0788f633cd4796bbe6cb41';
const readinessDiffs=[
  'src/member-production-line-login-promotion-readiness.mjs',
  'docs/member-app/member-production-line-login-promotion-readiness.md',
  'member_tests/member-production-line-login-promotion-readiness.test.mjs'
];
const readinessBlobs={
  'src/member-production-line-login-promotion-readiness.mjs':'8be93701a8fb5e3a2e9fd72776a7a2ce698aa140',
  'docs/member-app/member-production-line-login-promotion-readiness.md':'ec1ed3fdc1fac59f70447eadfc9df7db6a972287',
  'member_tests/member-production-line-login-promotion-readiness.test.mjs':'a26ba0302d9e315e642c747dc8498df6cda4790e'
};
const inheritedDiffs=[
  'src/crm-customer-list-daily-operations.mjs',
  'tests/customer-list-daily-operations.mjs'
];
const inheritedBlobs={
  'src/crm-customer-list-daily-operations.mjs':'06bd614f97bcb94992dcedbe47f48990f0a56b1f',
  'tests/customer-list-daily-operations.mjs':'ca1ec71457b8ea5a5507d33ff85d1f686d4261a4'
};
const auditedDiffs=[...readinessDiffs,...inheritedDiffs];
const handoffDiffs=[
  'src/member-production-line-login-promotion-blocked-handoff.mjs',
  'docs/member-app/member-production-line-login-promotion-blocked-handoff.md',
  'member_tests/member-production-line-login-promotion-blocked-handoff.test.mjs'
];

const candidate={
  current_main_sha:AUDITED_MAIN,
  candidate_source_sha:'fdfa65fa797c3ee89885ee99244e7a9ad691b124',
  staged_version_id:'be711f54-8716-4178-9949-d5ced183d9dc',
  candidate_sha256:'af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8',
  staging_run_id:'38233408939',
  expected_active_version_id:'6d34db85-9a52-45e9-8f7d-094f9fc7957a',
  stage_receipt:{
    success:true,
    exact_sha:'fdfa65fa797c3ee89885ee99244e7a9ad691b124',
    staged_version_id:'be711f54-8716-4178-9949-d5ced183d9dc',
    candidate_sha256:'af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8',
    unchanged_active_version_id:'6d34db85-9a52-45e9-8f7d-094f9fc7957a',
    staging_run_id:'38233408939',
    owner_approved:true,
    member_route_enabled:true,
    line_login_approved:true,
    line_login_external_exchange_enabled:true,
    private_media_content_route:false,
    favorites_write:false,
    memory_write:false,
    family_pass_entitlement_write:false,
    production_traffic_change:0,
    production_deploy:0,
    production_d1_read:0,
    production_d1_write:0,
    r2_object_read:0,
    r2_object_write:0,
    secret_change:0,
    line_send:0,
    customer_prospect_mutation:0,
    customer_id_generation:0,
    commerce_activation:0,
    black_automatic_award:0
  },
  active_deployment:{versions:[{version_id:'6d34db85-9a52-45e9-8f7d-094f9fc7957a',percentage:100}]},
  staged_version:{
    version_id:'be711f54-8716-4178-9949-d5ced183d9dc',
    candidate_sha256:'af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8',
    owner_approved:true,
    member_route_enabled:true,
    line_login_approved:true,
    line_login_external_exchange_enabled:true,
    private_media_content_route:false,
    favorites_write:false,
    memory_write:false,
    family_pass_entitlement_write:false,
    resource_bindings_unchanged:true
  },
  runtime_drift_evidence:{
    checked:true,
    base_sha:'fdfa65fa797c3ee89885ee99244e7a9ad691b124',
    head_sha:AUDITED_MAIN,
    production_entry_changed:false,
    wrangler_changed:false,
    runtime_workflow_changed:false,
    changed_files:[...auditedDiffs],
    readiness_blob_evidence_checked:true,
    readiness_file_blobs:{...readinessBlobs},
    inherited_blob_evidence_checked:true,
    inherited_file_blobs:{...inheritedBlobs}
  },
  repository_safety_evidence:{
    executable_workflow_write_blocked:true,
    blocked_path:'.github/workflows/member-production-line-login-promotion.yml',
    bypass_attempted:false,
    executable_promotion_path_available:false
  }
};

function clone(v){return structuredClone(v);}
function packet(input=candidate){return buildMemberLineLoginPromotionBlockedHandoff(input);}
function hasBlocker(result,prefix){return result.blockers.some(v=>v===prefix||v.startsWith(prefix));}
function fails(input,blocker){const result=packet(input);assert.equal(result.packet_ready,false);assert.equal(hasBlocker(result,blocker),true,`${blocker} not found: ${result.blockers.join(',')}`);return result;}

{
  const result=packet();
  assert.equal(result.packet_ready,true);
  assert.deepEqual(result.blockers,[]);
  assert.equal(result.audited_current_main_sha,AUDITED_MAIN);
  assert.equal(result.current_main_sha,AUDITED_MAIN);
  assert.equal(result.authorization_granted,false);
  assert.equal(result.executable_promotion_available,false);
  assert.equal(result.repository_safety_blocked,true);
  assert.equal(result.approval_text,'');
  assert.equal(result.next_boundary,'REPOSITORY_SAFE_EXECUTABLE_PROMOTION_PATH_REQUIRED');
  assert.deepEqual([...result.required_audited_diffs].sort(),[...auditedDiffs].sort());
  assert.deepEqual(result.required_readiness_blobs,readinessBlobs);
  assert.deepEqual(result.required_inherited_blobs,inheritedBlobs);
  for(const value of Object.values(result.safety))assert.equal(value,0);
  console.log('PASS exact audited-main blocked handoff packet is ready but grants no authorization');
}

{
  const input=clone(candidate);
  input.current_main_sha='a'.repeat(40);
  input.runtime_drift_evidence.head_sha=input.current_main_sha;
  fails(input,'CURRENT_MAIN_SHA_MISMATCH');
  console.log('PASS stale or fabricated current main fails closed even when drift head copies it');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.head_sha='b'.repeat(40);
  fails(input,'RUNTIME_DRIFT_HEAD_SHA_MISMATCH');
  console.log('PASS drift head must equal the pinned audited main');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files.push(...handoffDiffs);
  fails(input,'AUDITED_DIFF_SET_MISMATCH');
  console.log('PASS complete handoff extras are rejected because the pinned candidate-to-main audit is exactly five files');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files.push('unexpected.txt');
  fails(input,'AUDITED_DIFF_SET_MISMATCH');
  console.log('PASS arbitrary extra changed file fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files.push('');
  const result=fails(input,'NON_RUNTIME_DIFF_INVALID_ENTRY');
  assert.equal(hasBlocker(result,'AUDITED_DIFF_SET_MISMATCH'),true);
  console.log('PASS blank changed-file evidence entry fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files.push('   ');
  const result=fails(input,'NON_RUNTIME_DIFF_INVALID_ENTRY');
  assert.equal(hasBlocker(result,'AUDITED_DIFF_SET_MISMATCH'),true);
  console.log('PASS whitespace-only changed-file evidence entry fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files[0]=` ${input.runtime_drift_evidence.changed_files[0]} `;
  fails(input,'NON_RUNTIME_DIFF_INVALID_ENTRY');
  console.log('PASS whitespace-padded changed-file evidence entry fails closed even when normalization would match');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files.push(42);
  const result=fails(input,'NON_RUNTIME_DIFF_INVALID_ENTRY');
  assert.equal(hasBlocker(result,'AUDITED_DIFF_SET_MISMATCH'),true);
  console.log('PASS non-string changed-file evidence entry fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files=input.runtime_drift_evidence.changed_files.filter(v=>v!==readinessDiffs[0]);
  const result=fails(input,'AUDITED_DIFF_SET_MISMATCH');
  assert.equal(hasBlocker(result,'REQUIRED_READINESS_DIFF_MISSING'),true);
  console.log('PASS missing readiness provenance fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files=input.runtime_drift_evidence.changed_files.filter(v=>v!==inheritedDiffs[0]);
  const result=fails(input,'AUDITED_DIFF_SET_MISMATCH');
  assert.equal(hasBlocker(result,'REQUIRED_INHERITED_DIFF_MISSING'),true);
  console.log('PASS missing inherited current-main drift fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files.push(readinessDiffs[0]);
  fails(input,'NON_RUNTIME_DIFF_DUPLICATE');
  console.log('PASS duplicate changed-file evidence fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.readiness_file_blobs['src/member-production-line-login-promotion-readiness.mjs']='1'.repeat(40);
  fails(input,'READINESS_BLOB_MISMATCH:src/member-production-line-login-promotion-readiness.mjs');
  console.log('PASS changed readiness blob fails closed');
}

{
  const input=clone(candidate);
  delete input.runtime_drift_evidence.readiness_file_blobs['docs/member-app/member-production-line-login-promotion-readiness.md'];
  fails(input,'READINESS_BLOB_EVIDENCE_PATH_SET_MISMATCH');
  console.log('PASS missing readiness blob evidence fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.readiness_file_blobs['unexpected.txt']='2'.repeat(40);
  fails(input,'READINESS_BLOB_EVIDENCE_PATH_SET_MISMATCH');
  console.log('PASS extra readiness blob evidence fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.readiness_blob_evidence_checked=false;
  fails(input,'READINESS_BLOB_EVIDENCE_NOT_CHECKED');
  console.log('PASS unchecked readiness blob evidence fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.inherited_file_blobs['src/crm-customer-list-daily-operations.mjs']='3'.repeat(40);
  fails(input,'INHERITED_BLOB_MISMATCH:src/crm-customer-list-daily-operations.mjs');
  console.log('PASS stale inherited current-main blob fails closed');
}

{
  const input=clone(candidate);
  delete input.runtime_drift_evidence.inherited_file_blobs['tests/customer-list-daily-operations.mjs'];
  fails(input,'INHERITED_BLOB_EVIDENCE_PATH_SET_MISMATCH');
  console.log('PASS missing inherited blob evidence fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.inherited_file_blobs['unexpected.txt']='4'.repeat(40);
  fails(input,'INHERITED_BLOB_EVIDENCE_PATH_SET_MISMATCH');
  console.log('PASS extra inherited blob evidence fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.inherited_blob_evidence_checked=false;
  fails(input,'INHERITED_BLOB_EVIDENCE_NOT_CHECKED');
  console.log('PASS unchecked inherited blob evidence fails closed');
}

for(const [field,blocker] of [
  ['production_entry_changed','PRODUCTION_ENTRY_DRIFT_NOT_EXCLUDED'],
  ['wrangler_changed','WRANGLER_DRIFT_NOT_EXCLUDED'],
  ['runtime_workflow_changed','RUNTIME_WORKFLOW_DRIFT_NOT_EXCLUDED']
]){
  const input=clone(candidate);
  input.runtime_drift_evidence[field]=true;
  fails(input,blocker);
}
console.log('PASS Production entry, wrangler, and runtime workflow drift each fail closed');

{
  const input=clone(candidate);
  input.repository_safety_evidence.bypass_attempted=true;
  fails(input,'BYPASS_STATE_NOT_FALSE');
  console.log('PASS repository safety bypass attempt fails closed');
}

{
  const input=clone(candidate);
  input.repository_safety_evidence.executable_promotion_path_available=true;
  fails(input,'EXECUTABLE_PROMOTION_PATH_NOT_FALSE');
  console.log('PASS executable promotion path cannot be claimed while blocked');
}

{
  const input=clone(candidate);
  input.staged_version_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  const result=fails(input,'STAGED_VERSION_ID_MISMATCH');
  assert.equal(hasBlocker(result,'PROMOTION_READINESS_INVALID:'),true);
  console.log('PASS alternate staged candidate cannot inherit handoff authority');
}

{
  const input=clone(candidate);
  input.stage_receipt.owner_approved=false;
  fails(input,'PROMOTION_READINESS_INVALID:');
  console.log('PASS missing staged Owner approval fails closed');
}

{
  const input=clone(candidate);
  input.active_deployment.versions[0].version_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  fails(input,'PROMOTION_READINESS_INVALID:');
  console.log('PASS active Production version drift fails closed');
}

{
  const health=memberLineLoginPromotionBlockedHandoffHealth();
  assert.equal(health.member_line_login_promotion_blocked_handoff,true);
  assert.equal(health.source_only,true);
  assert.equal(health.audited_current_main_sha,AUDITED_MAIN);
  assert.equal(health.readiness_blob_binding,'IMMUTABLE_GIT_BLOBS');
  assert.equal(health.inherited_drift_blob_binding,'IMMUTABLE_GIT_BLOBS');
  assert.equal(health.executable_promotion_available,false);
  assert.equal(health.authorization_granted,false);
  assert.equal(health.production_mutation,false);
  assert.equal(health.next_boundary,'REPOSITORY_SAFE_EXECUTABLE_PROMOTION_PATH_REQUIRED');
  console.log('PASS health exposes pinned audited main and source-only non-executable boundary');
}

assert.equal(__test.EXECUTABLE_WORKFLOW_PATH,'.github/workflows/member-production-line-login-promotion.yml');
assert.equal(__test.AUDITED_CURRENT_MAIN_SHA,AUDITED_MAIN);
assert.deepEqual([...__test.REQUIRED_READINESS_DIFFS].sort(),[...readinessDiffs].sort());
assert.deepEqual(__test.REQUIRED_READINESS_BLOBS,readinessBlobs);
assert.deepEqual([...__test.REQUIRED_INHERITED_DIFFS].sort(),[...inheritedDiffs].sort());
assert.deepEqual(__test.REQUIRED_INHERITED_BLOBS,inheritedBlobs);
assert.deepEqual([...__test.REQUIRED_AUDITED_DIFFS].sort(),[...auditedDiffs].sort());
assert.deepEqual(__test.AUTHORIZED_CANDIDATE,{
  expected_sha:'fdfa65fa797c3ee89885ee99244e7a9ad691b124',
  staged_version_id:'be711f54-8716-4178-9949-d5ced183d9dc',
  candidate_sha256:'af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8',
  staging_run_id:'38233408939',
  expected_active_version_id:'6d34db85-9a52-45e9-8f7d-094f9fc7957a'
});

console.log('MEMBER_PRODUCTION_LINE_LOGIN_PROMOTION_BLOCKED_HANDOFF=PASS');
console.log('AUDITED_CURRENT_MAIN_SHA='+AUDITED_MAIN);
console.log('PRODUCTION_TRAFFIC_CHANGE=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('WORKER_VERSION_UPLOAD=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('R2_OBJECT_READ=0');
console.log('R2_OBJECT_WRITE=0');
console.log('SECRET_CHANGE=0');
console.log('LINE_SEND=0');
console.log('CUSTOMER_PROSPECT_MUTATION=0');
console.log('ISSUE_26_COMMAND_POST=0');
