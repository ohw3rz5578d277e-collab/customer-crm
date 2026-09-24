import { buildMemberFavoriteMutationPlan } from './member-favorites-mutation-plan.mjs';

const BUILD='member-favorites-write-executor-20260925-01';
const WRITE_MODE='enabled';

const text=v=>v==null?'':String(v).trim();

function writeEnabled(env){
  return text(env?.MEMBER_FAVORITES_WRITE_MODE).toLowerCase()===WRITE_MODE;
}

function fail(status,extra={}){
  return {
    status,
    write_executed:false,
    favorite_changed:false,
    ...extra
  };
}

export async function executeMemberFavoriteMutation(
  env,
  session,
  {
    memory_id,
    desired_favorite,
    approved=false
  }={}
){
  if(approved!==true){
    return fail('write_approval_required');
  }
  if(!writeEnabled(env)){
    return fail('favorites_write_disabled');
  }
  if(typeof env?.DB?.prepare!=='function'){
    return fail('database_unavailable');
  }

  const before=await buildMemberFavoriteMutationPlan(
    env,
    session,
    {memory_id,desired_favorite}
  );

  if(before.status!=='ok'){
    return fail(before.status,{
      review_required:[
        'ambiguous_family_identity',
        'family_identity_incomplete'
      ].includes(before.status)
    });
  }

  if(before.action==='none'){
    return {
      status:'ok',
      write_executed:false,
      favorite_changed:false,
      idempotent_noop:true,
      family_id:before.family_id,
      customer_id:before.customer_id,
      memory_id:before.memory_id,
      favorite:before.current_favorite
    };
  }

  const statement=before.action==='add'
    ?env.DB.prepare(
      `INSERT INTO member_memory_favorites (
        family_id,
        customer_id,
        memory_id
      ) VALUES (?,?,?)`
    ).bind(
      before.family_id,
      before.customer_id,
      before.memory_id
    )
    :env.DB.prepare(
      `DELETE FROM member_memory_favorites
       WHERE family_id=?
         AND customer_id=?
         AND memory_id=?`
    ).bind(
      before.family_id,
      before.customer_id,
      before.memory_id
    );

  try{
    const result=await statement.run();
    if(result?.success===false){
      throw Object.assign(new Error('Favorite mutation failed'),{name:'D1MutationError'});
    }
  }catch(error){
    const afterFailure=await buildMemberFavoriteMutationPlan(
      env,
      session,
      {memory_id:before.memory_id,desired_favorite:before.desired_favorite}
    );

    if(
      afterFailure.status==='ok'
      && afterFailure.action==='none'
      && afterFailure.current_favorite===before.desired_favorite
    ){
      return {
        status:'ok',
        write_executed:false,
        favorite_changed:false,
        idempotent_noop:true,
        concurrent_writer_won:true,
        family_id:before.family_id,
        customer_id:before.customer_id,
        memory_id:before.memory_id,
        favorite:before.desired_favorite
      };
    }

    return fail('favorite_write_failed',{
      review_required:true,
      family_id:before.family_id,
      customer_id:before.customer_id,
      memory_id:before.memory_id,
      error_class:text(error?.name)||'Error'
    });
  }

  const after=await buildMemberFavoriteMutationPlan(
    env,
    session,
    {memory_id:before.memory_id,desired_favorite:before.desired_favorite}
  );

  if(
    after.status!=='ok'
    || after.action!=='none'
    || after.current_favorite!==before.desired_favorite
  ){
    return {
      status:'favorite_postwrite_verification_failed',
      write_executed:true,
      favorite_changed:true,
      family_id:before.family_id,
      customer_id:before.customer_id,
      memory_id:before.memory_id,
      review_required:true
    };
  }

  return {
    status:'ok',
    write_executed:true,
    favorite_changed:true,
    idempotent_noop:false,
    action:before.action,
    family_id:after.family_id,
    customer_id:after.customer_id,
    memory_id:after.memory_id,
    favorite:after.current_favorite
  };
}

export function memberFavoritesWriteExecutorHealth(env){
  return {
    member_favorites_write_executor:true,
    build:BUILD,
    write_mode:writeEnabled(env)?'enabled':'disabled',
    write_default_disabled:true,
    explicit_approved_flag_required:true,
    declarative_desired_state:true,
    prewrite_plan_rebuild:true,
    postwrite_plan_verification:true,
    insert_supported:true,
    delete_supported:true,
    update_supported:false,
    exact_family_customer_memory:true,
    published_memory_required:true,
    idempotent_add:true,
    idempotent_remove:true,
    concurrent_writer_reread:true,
    canonical_crm_write:false,
    line_send:false,
    production_route_wired:false,
    production_write_enabled:false
  };
}

export const __test={
  writeEnabled
};
