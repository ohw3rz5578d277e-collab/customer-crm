import {buildMemberProductionReadinessPreflight} from './member-production-readiness-preflight.mjs';
import {buildMemberProductionBackendSequenceReadiness} from './member-production-backend-sequence-readiness.mjs';

const BUILD='member-production-activation-readiness-assembly-20261009-01';

export function buildMemberProductionActivationReadinessAssembly({env={},observed={},backend_evidence={}}={}){
  const runtime=buildMemberProductionReadinessPreflight({env,observed});
  const backend=buildMemberProductionBackendSequenceReadiness(backend_evidence);
  const technicalBlockers=[];

  if(runtime.main_activation_ready!==true){
    technicalBlockers.push('MEMBER_EXISTING_RUNTIME_READINESS_NOT_READY');
  }
  if(backend.activation_ready!==true){
    technicalBlockers.push(...backend.blockers);
  }

  const technicalReady=technicalBlockers.length===0;

  return {
    status:'ok',
    build:BUILD,
    source_only:true,
    technical_activation_ready:technicalReady,
    activation_allowed:false,
    production_activation_authorized:false,
    owner_authorization_required:true,
    authorization_policy:'fresh_explicit_exact_sha_owner_authorization_required_after_technical_readiness',
    blockers:{
      technical:[...new Set(technicalBlockers)],
      authorization:['OWNER_MEMBER_PRODUCTION_ACTIVATION_AUTHORIZATION_REQUIRED']
    },
    gates:{
      existing_runtime:runtime,
      backend_sequence:backend
    },
    invariant:{
      production_deploy:false,
      production_worker_activation:false,
      production_route_activation:false,
      production_d1_read:false,
      production_d1_write:false,
      production_d1_delete:false,
      migration_apply:false,
      customer_mutation:false,
      family_mutation:false,
      prospect_mutation:false,
      member_mutation:false,
      crm_write:false,
      line_send:false,
      google_network_send:false,
      r2_access:false,
      secret_change:false,
      security_policy_change:false,
      commerce_activation:false,
      black_write:false,
      memory_write:false,
      paid_spend:false
    }
  };
}

export function memberProductionActivationReadinessAssemblyHealth(){
  return {
    member_production_activation_readiness_assembly:true,
    build:BUILD,
    source_only:true,
    existing_runtime_gate_required:true,
    backend_steps_1_9_gate_required:true,
    exact_release_sha_evidence_required:true,
    owner_authorization_required:true,
    activation_allowed:false,
    production_deploy:false,
    production_d1_read:false,
    production_d1_write:false,
    route_activation:false
  };
}
