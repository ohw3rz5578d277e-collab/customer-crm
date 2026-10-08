const BUILD='member-production-backend-sequence-readiness-20261009-01';
const SHA_RE=/^[0-9a-f]{40}$/;

const strictSha=value=>typeof value==='string'&&SHA_RE.test(value)?value:'';
const strictTrue=value=>value===true;

export function buildMemberProductionBackendSequenceReadiness(evidence={}){
  const releaseSha=strictSha(evidence?.release_sha);
  const verifiedSha=strictSha(evidence?.step9_verified_sha);
  const nodeMajor=Number.isInteger(evidence?.step9_node_major)?evidence.step9_node_major:null;

  const conditions=[
    {ok:releaseSha!=='' ,blocker:'MEMBER_BACKEND_RELEASE_SHA_INVALID'},
    {ok:verifiedSha!=='' ,blocker:'MEMBER_BACKEND_STEP9_VERIFIED_SHA_INVALID'},
    {ok:releaseSha!==''&&verifiedSha!==''&&releaseSha===verifiedSha,blocker:'MEMBER_BACKEND_STEP9_SHA_MISMATCH'},
    {ok:strictTrue(evidence?.step9_final_gate_passed),blocker:'MEMBER_BACKEND_STEP9_FINAL_GATE_NOT_VERIFIED'},
    {ok:nodeMajor===22,blocker:'MEMBER_BACKEND_STEP9_NODE22_NOT_VERIFIED'},
    {ok:strictTrue(evidence?.exact_head_checkout_verified),blocker:'MEMBER_BACKEND_EXACT_HEAD_CHECKOUT_NOT_VERIFIED'},
    {ok:strictTrue(evidence?.workflow_exact_head_contract_verified),blocker:'MEMBER_BACKEND_WORKFLOW_EXACT_HEAD_CONTRACT_NOT_VERIFIED'},
    {ok:strictTrue(evidence?.all_member_tests_workflow_contract_verified),blocker:'MEMBER_BACKEND_ALL_MEMBER_TESTS_CONTRACT_NOT_VERIFIED'},
    {ok:strictTrue(evidence?.steps_1_8_matrix_verified),blocker:'MEMBER_BACKEND_STEPS_1_8_MATRIX_NOT_VERIFIED'},
    {ok:strictTrue(evidence?.cross_contract_security_verified),blocker:'MEMBER_BACKEND_CROSS_CONTRACT_SECURITY_NOT_VERIFIED'},
    {ok:strictTrue(evidence?.production_default_off_verified),blocker:'MEMBER_BACKEND_PRODUCTION_DEFAULT_OFF_NOT_VERIFIED'}
  ];
  const blockers=conditions.filter(item=>!item.ok).map(item=>item.blocker);

  return {
    status:'ok',
    build:BUILD,
    source_only:true,
    required_for_production_activation:true,
    activation_ready:blockers.length===0,
    blockers,
    evidence:{
      release_sha:releaseSha||null,
      step9_verified_sha:verifiedSha||null,
      exact_sha_match:releaseSha!==''&&releaseSha===verifiedSha,
      step9_final_gate_passed:strictTrue(evidence?.step9_final_gate_passed),
      step9_node_major:nodeMajor,
      exact_head_checkout_verified:strictTrue(evidence?.exact_head_checkout_verified),
      workflow_exact_head_contract_verified:strictTrue(evidence?.workflow_exact_head_contract_verified),
      all_member_tests_workflow_contract_verified:strictTrue(evidence?.all_member_tests_workflow_contract_verified),
      steps_1_8_matrix_verified:strictTrue(evidence?.steps_1_8_matrix_verified),
      cross_contract_security_verified:strictTrue(evidence?.cross_contract_security_verified),
      production_default_off_verified:strictTrue(evidence?.production_default_off_verified)
    },
    authorization:{
      owner_activation_authorized:false,
      production_action_allowed:false,
      policy:'technical_readiness_never_implies_owner_authorization'
    },
    invariant:{
      production_deploy:false,
      production_worker_activation:false,
      production_route_activation:false,
      production_d1_read:false,
      production_d1_write:false,
      production_d1_delete:false,
      migration_apply:false,
      crm_write:false,
      line_send:false,
      google_network_send:false,
      r2_access:false,
      customer_id_generation:false,
      family_id_generation:false,
      black_write:false,
      memory_write:false,
      paid_spend:false
    }
  };
}

export function memberProductionBackendSequenceReadinessHealth(){
  return {
    member_production_backend_sequence_readiness:true,
    build:BUILD,
    source_only:true,
    exact_sha_evidence_required:true,
    node22_required:true,
    steps_1_9_required:true,
    production_default_off_required:true,
    owner_activation_authorized:false,
    production_action_allowed:false,
    production_deploy:false,
    production_d1_read:false,
    production_d1_write:false,
    route_activation:false
  };
}

export const __test={SHA_RE,strictSha};
