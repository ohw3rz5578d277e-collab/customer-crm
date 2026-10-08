import assert from 'node:assert/strict';
import {buildMemberProductionActivationReadinessAssembly,memberProductionActivationReadinessAssemblyHealth} from '../src/member-production-activation-readiness-assembly.mjs';
import {__test as readinessTest} from '../src/member-production-readiness-preflight.mjs';

const RELEASE='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
const backendEvidence={
  release_sha:RELEASE,
  step9_verified_sha:RELEASE,
  step9_final_gate_passed:true,
  step9_node_major:22,
  exact_head_checkout_verified:true,
  workflow_exact_head_contract_verified:true,
  all_member_tests_workflow_contract_verified:true,
  steps_1_8_matrix_verified:true,
  cross_contract_security_verified:true,
  production_default_off_verified:true
};

const env={
  MEMBER_SESSION_SECRET:'member-session-secret-12345678901234567890',
  MEMBER_LINE_LOGIN_TRANSACTION_SECRET:'line-login-transaction-secret-123456789012345',
  MEMBER_LINE_LOGIN_CHANNEL_ID:'1234567890',
  MEMBER_LINE_LOGIN_REDIRECT_URI:'https://example.test/member/line/callback',
  MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:'private-media-delivery-secret-123456789012345',
  MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE:'enabled'
};

const observed={
  applied_migrations:readinessTest.MIGRATIONS.map(item=>item.name),
  line_external_token_exchange_ready:true,
  line_external_id_token_verification_ready:true,
  login_routes_wired:true,
  member_read_routes_wired:true,
  production_member_route_entry_ready:true,
  private_media_storage_adapter_ready:true,
  private_media_storage_binding_ready:true,
  private_media_routes_wired:true,
  private_media_owner_authorized:true
};

const empty=buildMemberProductionActivationReadinessAssembly();
assert.equal(empty.source_only,true);
assert.equal(empty.technical_activation_ready,false);
assert.equal(empty.activation_allowed,false);
assert.equal(empty.production_activation_authorized,false);
assert.equal(empty.owner_authorization_required,true);
assert.ok(empty.blockers.technical.includes('MEMBER_EXISTING_RUNTIME_READINESS_NOT_READY'));
assert.ok(empty.blockers.authorization.includes('OWNER_MEMBER_PRODUCTION_ACTIVATION_AUTHORIZATION_REQUIRED'));
assert.ok(Object.values(empty.invariant).every(value=>value===false));

const stale=buildMemberProductionActivationReadinessAssembly({
  env,
  observed,
  backend_evidence:{...backendEvidence,step9_verified_sha:'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'}
});
assert.equal(stale.gates.existing_runtime.main_activation_ready,true);
assert.equal(stale.gates.backend_sequence.activation_ready,false);
assert.equal(stale.technical_activation_ready,false);
assert.ok(stale.blockers.technical.includes('MEMBER_BACKEND_STEP9_SHA_MISMATCH'));
assert.equal(stale.activation_allowed,false);

const technicallyReady=buildMemberProductionActivationReadinessAssembly({env,observed,backend_evidence:backendEvidence});
assert.equal(technicallyReady.gates.existing_runtime.main_activation_ready,true);
assert.equal(technicallyReady.gates.backend_sequence.activation_ready,true);
assert.equal(technicallyReady.technical_activation_ready,true);
assert.deepEqual(technicallyReady.blockers.technical,[]);
assert.equal(technicallyReady.activation_allowed,false);
assert.equal(technicallyReady.production_activation_authorized,false);
assert.equal(technicallyReady.owner_authorization_required,true);
assert.deepEqual(technicallyReady.blockers.authorization,['OWNER_MEMBER_PRODUCTION_ACTIVATION_AUTHORIZATION_REQUIRED']);
assert.ok(Object.values(technicallyReady.invariant).every(value=>value===false));

const health=memberProductionActivationReadinessAssemblyHealth();
assert.equal(health.member_production_activation_readiness_assembly,true);
assert.equal(health.source_only,true);
assert.equal(health.existing_runtime_gate_required,true);
assert.equal(health.backend_steps_1_9_gate_required,true);
assert.equal(health.exact_release_sha_evidence_required,true);
assert.equal(health.owner_authorization_required,true);
assert.equal(health.activation_allowed,false);

console.log('MEMBER_PRODUCTION_ACTIVATION_READINESS_ASSEMBLY=PASS');
