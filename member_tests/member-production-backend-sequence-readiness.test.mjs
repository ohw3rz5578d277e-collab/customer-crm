import assert from 'node:assert/strict';
import {
  buildMemberProductionBackendSequenceReadiness,
  memberProductionBackendSequenceReadinessHealth
} from '../src/member-production-backend-sequence-readiness.mjs';

const RELEASE='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

const complete={
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

const empty=buildMemberProductionBackendSequenceReadiness();
assert.equal(empty.activation_ready,false);
assert.ok(empty.blockers.includes('MEMBER_BACKEND_RELEASE_SHA_INVALID'));
assert.ok(empty.blockers.includes('MEMBER_BACKEND_STEP9_FINAL_GATE_NOT_VERIFIED'));
assert.equal(empty.authorization.owner_activation_authorized,false);
assert.equal(empty.authorization.production_action_allowed,false);
assert.ok(Object.values(empty.invariant).every(value=>value===false));

const stale=buildMemberProductionBackendSequenceReadiness({
  ...complete,
  step9_verified_sha:'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
});
assert.equal(stale.activation_ready,false);
assert.ok(stale.blockers.includes('MEMBER_BACKEND_STEP9_SHA_MISMATCH'));

const coercedNode=buildMemberProductionBackendSequenceReadiness({...complete,step9_node_major:'22'});
assert.equal(coercedNode.activation_ready,false);
assert.ok(coercedNode.blockers.includes('MEMBER_BACKEND_STEP9_NODE22_NOT_VERIFIED'));

const uppercase=buildMemberProductionBackendSequenceReadiness({...complete,release_sha:RELEASE.toUpperCase()});
assert.equal(uppercase.activation_ready,false);
assert.ok(uppercase.blockers.includes('MEMBER_BACKEND_RELEASE_SHA_INVALID'));

const missingMatrix=buildMemberProductionBackendSequenceReadiness({...complete,steps_1_8_matrix_verified:false});
assert.equal(missingMatrix.activation_ready,false);
assert.ok(missingMatrix.blockers.includes('MEMBER_BACKEND_STEPS_1_8_MATRIX_NOT_VERIFIED'));

const ready=buildMemberProductionBackendSequenceReadiness(complete);
assert.equal(ready.activation_ready,true);
assert.deepEqual(ready.blockers,[]);
assert.equal(ready.evidence.exact_sha_match,true);
assert.equal(ready.evidence.step9_node_major,22);
assert.equal(ready.authorization.owner_activation_authorized,false);
assert.equal(ready.authorization.production_action_allowed,false);

const health=memberProductionBackendSequenceReadinessHealth();
assert.equal(health.member_production_backend_sequence_readiness,true);
assert.equal(health.source_only,true);
assert.equal(health.exact_sha_evidence_required,true);
assert.equal(health.node22_required,true);
assert.equal(health.steps_1_9_required,true);
assert.equal(health.production_action_allowed,false);

console.log('MEMBER_PRODUCTION_BACKEND_SEQUENCE_READINESS=PASS');
