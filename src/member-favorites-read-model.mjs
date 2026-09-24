import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';

const BUILD='member-favorites-read-model-20260925-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const FAVORITE_LIMIT=200;

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-favorites-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer'
    }
  });
}

async function first(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  return (await q.first())||null;
}

async function all(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  const result=await q.all();
  return result.results||[];
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

async function authorizeSession(env,session){
  const normalized=validSession(session);
  if(!normalized.ok)return normalized;

  const family=await readMemberFamilyByCustomer(env,normalized.customer_id);
  if(family.status!=='linked'){
    return {ok:false,error:family.status};
  }
  if(text(family.family?.family_id)!==normalized.family_id){
    return {ok:false,error:'family_access_denied'};
  }

  return {ok:true,...normalized};
}

export async function readFavoriteRowsForAuthorizedMember(env,session){
  const normalized=validSession(session);
  if(!normalized.ok){
    return {
      status:'invalid_member_session',
      favorites:[],
      read_only:true
    };
  }

  if(!(await tableExists(env,'member_memory_favorites'))){
    return {
      status:'favorites_schema_not_applied',
      favorites:[],
      read_only:true
    };
  }

  if(!(await tableExists(env,'member_memories'))){
    return {
      status:'member_memory_schema_not_applied',
      favorites:[],
      read_only:true
    };
  }

  const rows=await all(
    env,
    `SELECT
       f.memory_id,
       f.created_at
       FROM member_memory_favorites f
       INNER JOIN member_memories m
         ON m.memory_id=f.memory_id
        AND m.family_id=f.family_id
       WHERE f.family_id=?
         AND f.customer_id=?
         AND m.published=1
         AND COALESCE(m.deleted_at,'')=''
       ORDER BY f.created_at DESC, f.memory_id
       LIMIT ${FAVORITE_LIMIT}`,
    [normalized.family_id,normalized.customer_id]
  );

  return {
    status:'ok',
    family_id:normalized.family_id,
    customer_id:normalized.customer_id,
    favorites:rows.map(row=>({
      memory_id:text(row.memory_id),
      created_at:text(row.created_at)||null
    })).filter(row=>!!row.memory_id),
    read_only:true
  };
}

export async function readMemberFavoritesForSession(env,session){
  const auth=await authorizeSession(env,session);
  if(!auth.ok){
    return {
      status:auth.error,
      favorites:[],
      read_only:true
    };
  }

  const result=await readFavoriteRowsForAuthorizedMember(env,auth);
  if(result.status!=='ok')return result;

  return {
    ...result,
    favorite_count:result.favorites.length,
    mutable:false,
    source:{
      table:'member_memory_favorites',
      ownership_scope:'family_customer_memory',
      published_memory_only:true,
      deleted_memory_hidden:true
    }
  };
}

export async function handleMemberFavoritesReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/favorites')return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  const result=await readMemberFavoritesForSession(env,session);

  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='family_access_denied')return json({ok:false,error:'family_access_denied'},403);
  if(result.status==='favorites_schema_not_applied'){
    return json({ok:false,error:'favorites_schema_not_applied'},409);
  }
  if(result.status==='member_memory_schema_not_applied'){
    return json({ok:false,error:'member_memory_schema_not_applied'},409);
  }
  if(result.status==='schema_not_applied'){
    return json({ok:false,error:'member_family_schema_not_applied'},409);
  }
  if(result.status==='ambiguous_family_identity'){
    return json({ok:false,error:'ambiguous_family_identity',review_required:true},409);
  }
  if(result.status==='unlinked'||result.status==='family_inactive_or_missing'){
    return json({ok:false,error:result.status},403);
  }

  return json({ok:false,error:result.status||'favorites_unavailable'},409);
}

export function memberFavoritesReadHealth(){
  return {
    member_favorites_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    production_schema_applied:false,
    session_identity_source:'server_verified_member_session',
    explicit_family_link_required:true,
    ownership_scope:'family_customer_memory',
    published_memory_only:true,
    deleted_memory_hidden:true,
    cross_family_fail_closed:true,
    per_customer_preferences:true,
    shared_family_preference:false,
    favorite_mutation_ready:false,
    automatic_write:false,
    line_send:false,
    production_write:false
  };
}

export const __test={
  validSession,
  FAVORITE_LIMIT
};
