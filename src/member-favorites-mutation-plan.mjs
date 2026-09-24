import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';

const BUILD='member-favorites-mutation-plan-20260925-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const MAX_MEMORY_ID=160;

const text=v=>v==null?'':String(v).trim();

async function first(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  return (await q.first())||null;
}

async function tableExists(env,name){
  return !!(await first(
    env,
    "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
    [name]
  ));
}

function validSession(session){
  const familyId=text(session?.family_id);
  const customerId=text(session?.customer_id);
  if(!familyId||familyId.length>MAX_FAMILY_ID)return {ok:false,error:'invalid_member_session'};
  if(!CUSTOMER_ID_RE.test(customerId))return {ok:false,error:'invalid_member_session'};
  return {ok:true,family_id:familyId,customer_id:customerId};
}

function validMemoryId(memoryId){
  const id=text(memoryId);
  return !!id
    && id.length<=MAX_MEMORY_ID
    && !/[\u0000-\u001f\u007f]/.test(id);
}

async function authorizeSession(env,session){
  const normalized=validSession(session);
  if(!normalized.ok)return normalized;

  const family=await readMemberFamilyByCustomer(env,normalized.customer_id);
  if(family.status!=='linked')return {ok:false,error:family.status};
  if(text(family.family?.family_id)!==normalized.family_id){
    return {ok:false,error:'family_access_denied'};
  }

  return {ok:true,...normalized};
}

export async function buildMemberFavoriteMutationPlan(
  env,
  session,
  {
    memory_id,
    desired_favorite
  }={}
){
  const memoryId=text(memory_id);
  if(!validMemoryId(memoryId)){
    return {
      status:'invalid_memory_id',
      write_required:false,
      read_only:true
    };
  }
  if(typeof desired_favorite!=='boolean'){
    return {
      status:'invalid_desired_favorite',
      write_required:false,
      read_only:true
    };
  }

  const auth=await authorizeSession(env,session);
  if(!auth.ok){
    return {
      status:auth.error,
      write_required:false,
      read_only:true
    };
  }

  const favoriteSchema=await tableExists(env,'member_memory_favorites');
  if(!favoriteSchema){
    return {
      status:'favorites_schema_not_applied',
      write_required:false,
      read_only:true
    };
  }

  const memorySchema=await tableExists(env,'member_memories');
  if(!memorySchema){
    return {
      status:'member_memory_schema_not_applied',
      write_required:false,
      read_only:true
    };
  }

  const memory=await first(
    env,
    `SELECT memory_id,family_id
       FROM member_memories
       WHERE memory_id=?
         AND family_id=?
         AND published=1
         AND COALESCE(deleted_at,'')=''
       LIMIT 1`,
    [memoryId,auth.family_id]
  );

  if(!memory){
    return {
      status:'memory_not_found',
      write_required:false,
      read_only:true
    };
  }

  const favorite=await first(
    env,
    `SELECT family_id,customer_id,memory_id,created_at
       FROM member_memory_favorites
       WHERE family_id=?
         AND customer_id=?
         AND memory_id=?
       LIMIT 1`,
    [auth.family_id,auth.customer_id,memoryId]
  );

  const currentFavorite=!!favorite;
  const action=currentFavorite===desired_favorite
    ?'none'
    :desired_favorite
      ?'add'
      :'remove';

  return {
    status:'ok',
    family_id:auth.family_id,
    customer_id:auth.customer_id,
    memory_id:memoryId,
    current_favorite:currentFavorite,
    desired_favorite,
    action,
    write_required:action!=='none',
    idempotent_noop:action==='none',
    ownership_scope:'family_customer_memory',
    published_memory_required:true,
    read_only:true
  };
}

export function memberFavoriteMutationPlanHealth(){
  return {
    member_favorite_mutation_plan:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    exact_family_customer_memory:true,
    server_verified_member_session_required:true,
    published_memory_required:true,
    deleted_memory_hidden:true,
    desired_state_boolean_required:true,
    actions:['add','remove','none'],
    fuzzy_identity:false,
    automatic_write:false,
    production_write:false
  };
}

export const __test={
  validSession,
  validMemoryId
};
