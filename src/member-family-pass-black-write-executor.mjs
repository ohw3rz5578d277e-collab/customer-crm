import { buildMemberFamilyPassBlackAchievementPlan } from './member-family-pass-black-achievement-plan.mjs';
import { readMemberFamilyPassForSession } from './member-family-pass-read-model.mjs';

const BUILD='member-family-pass-black-write-executor-20260924-01';
const WRITE_MODE='enabled';

const text=v=>v==null?'':String(v).trim();

function writeEnabled(env){
  return text(env?.MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE).toLowerCase()===WRITE_MODE;
}

function nowIso(nowOverride){
  if(nowOverride instanceof Date&&!Number.isNaN(nowOverride.getTime()))return nowOverride.toISOString();
  if(Number.isFinite(Number(nowOverride))){
    const n=Number(nowOverride);
    const ms=n>10_000_000_000?n:n*1000;
    const d=new Date(ms);
    if(!Number.isNaN(d.getTime()))return d.toISOString();
  }
  return new Date().toISOString();
}

function fail(status,extra={}){
  return {
    status,
    write_executed:false,
    black_entitlement_created:false,
    ...extra
  };
}

function validProposedRecord(plan){
  const row=plan?.proposed_record;
  return !!(
    plan?.action==='award_black_lifetime'
    && plan?.write_required===true
    && text(row?.family_id)
    && Number(row?.black_lifetime)===1
    && Number.isInteger(Number(row?.qualifying_memory_count))
    && Number(row.qualifying_memory_count)>=10
    && text(row?.achievement_source)==='published-member-memories'
    && text(row.family_id)===text(plan.family_id)
  );
}

export async function executeMemberFamilyPassBlackAward(env,session,{
  approved=false,
  now
}={}){
  if(approved!==true){
    return fail('write_approval_required');
  }
  if(!writeEnabled(env)){
    return fail('black_entitlement_write_disabled');
  }
  if(typeof env?.DB?.prepare!=='function'){
    return fail('database_unavailable');
  }

  // Always rebuild the authoritative Family-scoped plan immediately before write.
  const before=await buildMemberFamilyPassBlackAchievementPlan(env,session);
  if(before.status!=='ok'){
    return fail(before.status,{
      review_required:before.status!=='black_entitlement_schema_not_applied'
    });
  }

  if(before.action==='none'){
    return {
      status:'ok',
      write_executed:false,
      black_entitlement_created:false,
      idempotent_noop:true,
      reason:before.reason,
      family_id:before.family_id,
      customer_id:before.customer_id,
      current_memory_count:before.current_memory_count,
      black_lifetime_entitled:before.black_lifetime_entitled===true
    };
  }

  if(!validProposedRecord(before)){
    return fail('invalid_black_award_plan',{review_required:true});
  }

  const row=before.proposed_record;
  const achievedAt=nowIso(now);
  const statement=env.DB.prepare(
    `INSERT INTO member_family_pass_entitlements (
      family_id,
      black_lifetime,
      black_achieved_at,
      qualifying_memory_count,
      achievement_source
    ) VALUES (?,?,?,?,?)`
  ).bind(
    text(row.family_id),
    1,
    achievedAt,
    Number(row.qualifying_memory_count),
    'published-member-memories'
  );

  try{
    const result=await statement.run();
    if(result?.success===false){
      throw Object.assign(new Error('BLACK entitlement insert failed'),{name:'D1InsertError'});
    }
  }catch(error){
    // Do not blind retry. Re-read the authoritative state in case another writer
    // safely inserted the same Family lifetime entitlement first.
    const afterFailure=await readMemberFamilyPassForSession(env,session);
    if(
      afterFailure.status==='ok'
      && afterFailure.entitlement?.durable_black===true
      && text(afterFailure.family_id)===text(before.family_id)
    ){
      return {
        status:'ok',
        write_executed:false,
        black_entitlement_created:false,
        idempotent_noop:true,
        concurrent_writer_won:true,
        family_id:before.family_id,
        customer_id:before.customer_id,
        black_lifetime_entitled:true
      };
    }
    return fail('black_entitlement_write_failed',{
      review_required:true,
      error_class:text(error?.name)||'Error'
    });
  }

  const after=await readMemberFamilyPassForSession(env,session);
  if(
    after.status!=='ok'
    || after.entitlement?.durable_black!==true
    || after.family_pass?.current_tier!=='BLACK'
    || text(after.family_id)!==text(before.family_id)
  ){
    return {
      status:'black_entitlement_postwrite_verification_failed',
      write_executed:true,
      black_entitlement_created:true,
      family_id:before.family_id,
      customer_id:before.customer_id,
      review_required:true
    };
  }

  return {
    status:'ok',
    write_executed:true,
    black_entitlement_created:true,
    idempotent_noop:false,
    family_id:after.family_id,
    customer_id:after.customer_id,
    black_achieved_at:after.entitlement.black_achieved_at,
    qualifying_memory_count:after.entitlement.qualifying_memory_count,
    current_memory_count:after.family_pass.memory_count,
    black_lifetime_entitled:true
  };
}

export function memberFamilyPassBlackWriteExecutorHealth(env){
  return {
    member_family_pass_black_write_executor:true,
    build:BUILD,
    write_mode:writeEnabled(env)?'enabled':'disabled',
    write_default_disabled:true,
    explicit_approved_flag_required:true,
    prewrite_plan_rebuild:true,
    insert_only:true,
    update_supported:false,
    delete_supported:false,
    downgrade_supported:false,
    family_primary_key_idempotency:true,
    concurrent_writer_reread:true,
    postwrite_verification:true,
    automatic_backfill:false,
    discount_enforcement:false,
    shooting_fee_discount:false,
    canonical_crm_write:false,
    production_route_wired:false,
    production_write_enabled:false,
    line_send:false
  };
}

export const __test={
  writeEnabled,
  nowIso,
  validProposedRecord
};
