import { resolveCanonicalCustomerByVerifiedLineUserId } from './crm-member-line-identity.mjs';
import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';
import { buildMemberMemoryPlanForFamily } from './crm-member-memory-plan.mjs';
import { memberSessionFoundationHealth } from './member-session-foundation.mjs';

const BUILD='member-bootstrap-plan-20260924-01';
const MAX_LINE_USER_ID=256;

const text=v=>v==null?'':String(v).trim();

function publicFamilyView(family){
  return {
    family_id:text(family?.family_id),
    display_name:text(family?.display_name),
    relation:text(family?.relation),
    access_role:text(family?.access_role)
  };
}

function memoryPlanSummary(memoryResult){
  const plan=memoryResult?.plan||{};
  return {
    schema_applied:memoryResult?.member_memory_schema_applied===true,
    source_eligible:Number(memoryResult?.source_diagnostics?.eligible_memories||0),
    to_create:Array.isArray(plan.to_create)?plan.to_create.length:0,
    already_synced:Array.isArray(plan.already_synced)?plan.already_synced.length:0,
    skipped:Array.isArray(plan.skipped)?plan.skipped.length:0,
    conflicts:Array.isArray(plan.conflicts)?plan.conflicts.length:0
  };
}

function failure(stage,status,extra={}){
  return {
    status,
    ready:false,
    stage,
    write_executed:false,
    session_issued:false,
    memory_write_executed:false,
    ...extra
  };
}

export async function buildMemberBootstrapPlanFromVerifiedLineIdentity(env,{
  verified_line_user_id
}={}){
  const lineUserId=text(verified_line_user_id);
  if(!lineUserId||lineUserId.length>MAX_LINE_USER_ID){
    return failure('line_identity','invalid_line_user_id');
  }

  const line=await resolveCanonicalCustomerByVerifiedLineUserId(env,lineUserId);
  if(line.status!=='linked'){
    return failure('line_identity',line.status,{
      identity_source:'verified_line_user_id_exact',
      fallback_used:false,
      review_required:line.status==='ambiguous_line_identity'||line.status==='noncanonical_customer_id'
    });
  }

  const customerId=text(line.customer_id);
  const family=await readMemberFamilyByCustomer(env,customerId);
  if(family.status!=='linked'){
    return failure('family_identity',family.status,{
      customer_id:customerId,
      identity_source:'verified_line_user_id_exact',
      fallback_used:false,
      review_required:family.status==='ambiguous_family_identity'
    });
  }

  const familyId=text(family.family?.family_id);
  const memory=await buildMemberMemoryPlanForFamily(env,{
    family_id:familyId,
    customer_id:customerId
  });

  if(memory.status!=='ok'){
    return failure('memory_plan',memory.status,{
      customer_id:customerId,
      family:publicFamilyView(family.family),
      identity_source:'verified_line_user_id_exact',
      fallback_used:false,
      memory:memoryPlanSummary(memory),
      review_required:memory.status==='memory_sync_conflict'||memory.status==='ambiguous_family_identity'
    });
  }

  const sessionHealth=memberSessionFoundationHealth(env);
  const sessionReady=sessionHealth.configured===true;

  return {
    status:'ready',
    ready:true,
    identity_source:'verified_line_user_id_exact',
    fallback_used:false,
    customer_id:customerId,
    family:publicFamilyView(family.family),
    session:{
      ready_to_issue:sessionReady,
      issued:false,
      secret_configured:sessionReady,
      cookie_name:sessionHealth.cookie_name,
      max_age_seconds:sessionHealth.max_age_seconds,
      signature:sessionHealth.signature
    },
    memory:memoryPlanSummary(memory),
    next_steps:{
      issue_session:sessionReady,
      historical_memory_write_required:memory.plan.to_create.length>0,
      historical_memory_write_count:memory.plan.to_create.length,
      open_member_home_after_writes:sessionReady&&memory.plan.conflicts.length===0
    },
    write_executed:false,
    session_issued:false,
    memory_write_executed:false
  };
}

export function memberBootstrapPlanHealth(){
  return {
    member_bootstrap_plan:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    verified_line_identity_required_upstream:true,
    exact_line_user_id_only:true,
    exact_customer_id_only:true,
    explicit_family_link_only:true,
    family_auto_inference:false,
    session_issue_in_plan:false,
    historical_memory_write_in_plan:false,
    line_send:false,
    production_write:false
  };
}
