import assert from 'node:assert/strict';
import {
  buildMemberLineLoginPromotionBlockedHandoff,
  memberLineLoginPromotionBlockedHandoffHealth,
  __test
} from '../src/member-production-line-login-promotion-blocked-handoff.mjs';

const readinessDiffs=[
  'src/member-production-line-login-promotion-readiness.mjs',
  'docs/member-app/member-production-line-login-promotion-readiness.md',
  'member_tests/member-production-line-login-promotion-readiness.test.mjs'
];
const handoffDiffs=[
  'src/member-production-line-login-promotion-blocked-handoff.mjs',
  'docs/member-app/member-production-line-login-promotion-blocked-handoff.md',
  'member_tests/member-production-line-login-promotion-blocked-handoff.test.mjs'
];

const candidate={
  current_main_sha:'990e5e768615afb8dc51f2b5ed27ba4140382d9a',
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
  active_deployment:{
    versions:[{version_id:'6d34db85-9a52-45e9-8f7d-094f9fc7957a',percentage:100}]
  },
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
    head_sha:'990e5e768615afb8dc51f2b5ed27ba4140382d9a',
    production_entry_changed:false,
    wrangler_changed:false,
    runtime_workflow_changed:false,
    changed_files:[...readinessDiffs]
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

{
  const result=packet();
  assert.equal(result.packet_ready,true);
  assert.equal(result.authorization_granted,false);
  assert.equal(result.executable_promotion_available,false);
  assert.equal(result.repository_safety_blocked,true);
  assert.equal(result.approval_text,'');
  assert.equal(result.next_boundary,'REPOSITORY_SAFE_EXECUTABLE_PROMOTION_PATH_REQUIRED');
  assert.deepEqual(result.blockers,[]);
  assert.equal(result.current_main_sha,candidate.current_main_sha);
  assert.equal(result.candidate_source_sha,candidate.candidate_source_sha);
  assert.equal(result.staged_version_id,candidate.staged_version_id);
  assert.equal(result.candidate_sha256,candidate.candidate_sha256);
  assert.equal(result.staging_run_id,candidate.staging_run_id);
  assert.equal(result.expected_active_version_id,candidate.expected_active_version_id);
  for(const value of Object.values(result.safety))assert.equal(value,0);
  console.log('PASS pre-merge blocked handoff packet binds exact ready candidate while granting no authorization');
}

{
  const input=clone(candidate);
  input.current_main_sha='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
  input.runtime_drift_evidence.head_sha=input.current_main_sha;
  input.runtime_drift_evidence.changed_files=[...readinessDiffs,...handoffDiffs];
  const result=packet(input);
  assert.equal(result.packet_ready,true);
  assert.deepEqual(result.blockers,[]);
  assert.equal(result.authorization_granted,false);
  assert.equal(result.executable_promotion_available,false);
  console.log('PASS post-merge handoff files remain valid non-runtime evidence without granting authorization');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.production_entry_changed=true;
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'PRODUCTION_ENTRY_DRIFT_NOT_EXCLUDED'),true);
  console.log('PASS production entry drift fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.wrangler_changed=true;
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'WRANGLER_DRIFT_NOT_EXCLUDED'),true);
  console.log('PASS wrangler drift fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files.push('src/production-index-crm-customer360-entry.js');
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'NON_RUNTIME_DIFF_NOT_ALLOWED'),true);
  console.log('PASS unexpected runtime changed file fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files.push(handoffDiffs[0]);
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'HANDOFF_DIFF_SET_PARTIAL'),true);
  console.log('PASS partial handoff diff set fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files=input.runtime_drift_evidence.changed_files.filter(v=>v!==readinessDiffs[0]);
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'REQUIRED_READINESS_DIFF_MISSING'),true);
  console.log('PASS missing readiness provenance fails closed');
}

{
  const input=clone(candidate);
  input.runtime_drift_evidence.changed_files.push(readinessDiffs[0]);
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'NON_RUNTIME_DIFF_DUPLICATE'),true);
  console.log('PASS duplicate drift evidence fails closed');
}

{
  const input=clone(candidate);
  input.repository_safety_evidence.bypass_attempted=true;
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'BYPASS_STATE_NOT_FALSE'),true);
  console.log('PASS bypass attempt fails closed');
}

{
  const input=clone(candidate);
  input.repository_safety_evidence.executable_promotion_path_available=true;
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'EXECUTABLE_PROMOTION_PATH_NOT_FALSE'),true);
  console.log('PASS executable path claim fails closed while safety block is canonical');
}

{
  const input=clone(candidate);
  input.staged_version_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'STAGED_VERSION_ID_MISMATCH'),true);
  assert.equal(hasBlocker(result,'PROMOTION_READINESS_INVALID:'),true);
  console.log('PASS alternate candidate cannot inherit handoff authority');
}

{
  const input=clone(candidate);
  input.stage_receipt.owner_approved=false;
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'PROMOTION_READINESS_INVALID:'),true);
  console.log('PASS missing staged Owner approval fails closed');
}

{
  const input=clone(candidate);
  input.active_deployment.versions[0].version_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  const result=packet(input);
  assert.equal(result.packet_ready,false);
  assert.equal(hasBlocker(result,'PROMOTION_READINESS_INVALID:'),true);
  console.log('PASS active Production version drift fails closed');
}

{
  const health=memberLineLoginPromotionBlockedHandoffHealth();
  assert.equal(health.member_line_login_promotion_blocked_handoff,true);
  assert.equal(health.source_only,true);
  assert.equal(health.executable_promotion_available,false);
  assert.equal(health.authorization_granted,false);
  assert.equal(health.production_mutation,false);
  assert.equal(health.next_boundary,'REPOSITORY_SAFE_EXECUTABLE_PROMOTION_PATH_REQUIRED');
  console.log('PASS health exposes source-only non-executable boundary');
}

assert.equal(__test.EXECUTABLE_WORKFLOW_PATH,'.github/workflows/member-production-line-login-promotion.yml');
assert.deepEqual([...__test.REQUIRED_READINESS_DIFFS].sort(),[...readinessDiffs].sort());
assert.deepEqual([...__test.OPTIONAL_HANDOFF_DIFFS].sort(),[...handoffDiffs].sort());
assert.deepEqual([...__test.ALLOWED_NON_RUNTIME_DIFFS].sort(),[...readinessDiffs,...handoffDiffs].sort());

console.log('MEMBER_PRODUCTION_LINE_LOGIN_PROMOTION_BLOCKED_HANDOFF=PASS');
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
