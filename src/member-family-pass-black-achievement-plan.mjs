import { readMemberFamilyPassForSession } from './member-family-pass-read-model.mjs';

const BUILD='member-family-pass-black-achievement-plan-20260924-01';

export async function buildMemberFamilyPassBlackAchievementPlan(env,session){
  const current=await readMemberFamilyPassForSession(env,session);
  if(current.status!=='ok'){
    return {
      status:current.status,
      ready:false,
      write_executed:false
    };
  }

  const pass=current.family_pass;
  const entitlement=current.entitlement;

  if(entitlement.durable_black){
    return {
      status:'ok',
      ready:true,
      family_id:current.family_id,
      customer_id:current.customer_id,
      action:'none',
      reason:'black_already_persisted',
      current_memory_count:pass.memory_count,
      black_currently_qualified:pass.black_currently_qualified,
      black_lifetime_entitled:true,
      entitlement_schema_applied:entitlement.schema_applied,
      write_required:false,
      write_executed:false
    };
  }

  if(!pass.black_currently_qualified){
    return {
      status:'ok',
      ready:true,
      family_id:current.family_id,
      customer_id:current.customer_id,
      action:'none',
      reason:'black_threshold_not_reached',
      current_memory_count:pass.memory_count,
      black_currently_qualified:false,
      black_lifetime_entitled:false,
      entitlement_schema_applied:entitlement.schema_applied,
      write_required:false,
      write_executed:false
    };
  }

  if(!entitlement.schema_applied){
    return {
      status:'black_entitlement_schema_not_applied',
      ready:false,
      family_id:current.family_id,
      customer_id:current.customer_id,
      action:'blocked',
      reason:'schema_required_before_black_persistence',
      current_memory_count:pass.memory_count,
      black_currently_qualified:true,
      black_lifetime_entitled:false,
      entitlement_schema_applied:false,
      write_required:false,
      write_executed:false
    };
  }

  return {
    status:'ok',
    ready:true,
    family_id:current.family_id,
    customer_id:current.customer_id,
    action:'award_black_lifetime',
    reason:'black_threshold_reached_without_durable_entitlement',
    current_memory_count:pass.memory_count,
    black_currently_qualified:true,
    black_lifetime_entitled:false,
    entitlement_schema_applied:true,
    proposed_record:{
      family_id:current.family_id,
      black_lifetime:1,
      qualifying_memory_count:pass.memory_count,
      achievement_source:'published-member-memories'
    },
    write_required:true,
    write_executed:false
  };
}

export function memberFamilyPassBlackAchievementPlanHealth(){
  return {
    member_family_pass_black_achievement_plan:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    threshold:10,
    requires_published_non_deleted_member_memories:true,
    exact_family_scope:true,
    duplicate_black_award_noop:true,
    schema_required_before_award:true,
    automatic_backfill:false,
    entitlement_write_executor:false,
    discount_enforcement:false,
    production_write:false
  };
}
