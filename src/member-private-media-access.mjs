import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';

const BUILD='member-private-media-access-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const MAX_MEDIA_ID=160;
const MAX_STORAGE_KEY=512;

const text=v=>v==null?'':String(v).trim();

async function first(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  return (await q.first())||null;
}

async function tableExists(env,name){
  return !!(await first(env,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",[name]));
}

function validSession(session){
  const familyId=text(session?.family_id);
  const customerId=text(session?.customer_id);
  if(!familyId||familyId.length>MAX_FAMILY_ID)return {ok:false,error:'invalid_member_session'};
  if(!CUSTOMER_ID_RE.test(customerId))return {ok:false,error:'invalid_member_session'};
  return {ok:true,family_id:familyId,customer_id:customerId};
}

function validMediaId(mediaId){
  const raw=mediaId==null?'':String(mediaId);
  if(!raw||raw.length>MAX_MEDIA_ID)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  return true;
}

function validPrivateStorageKey(value){
  const raw=value==null?'':String(value);
  if(!raw||raw.length>MAX_STORAGE_KEY)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  const key=raw;
  if(/^https?:\/\//i.test(key))return false;
  if(key.startsWith('/'))return false;
  if(key.split('/').some(part=>part==='..'))return false;
  return true;
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

export async function authorizeMemberPrivateMediaAccess(env,session,mediaId){
  if(!validMediaId(mediaId)){
    return {status:'invalid_media_id',authorized:false,read_only:true};
  }

  const auth=await authorizeSession(env,session);
  if(!auth.ok){
    return {status:auth.error,authorized:false,read_only:true};
  }

  if(!(await tableExists(env,'member_memories'))||!(await tableExists(env,'member_memory_media'))){
    return {status:'member_memory_schema_not_applied',authorized:false,read_only:true};
  }

  const row=await first(
    env,
    `SELECT
       mm.media_id,
       mm.memory_id,
       mm.family_id,
       mm.storage_key,
       mm.media_type,
       mm.role,
       mm.width,
       mm.height
     FROM member_memory_media mm
     INNER JOIN member_memories m
       ON m.memory_id=mm.memory_id
      AND m.family_id=mm.family_id
     WHERE mm.media_id=?
       AND mm.family_id=?
       AND COALESCE(mm.deleted_at,'')=''
       AND m.published=1
       AND COALESCE(m.deleted_at,'')=''
     LIMIT 1`,
    [text(mediaId),auth.family_id]
  );

  if(!row){
    return {status:'media_not_found',authorized:false,read_only:true};
  }

  if(!validPrivateStorageKey(row.storage_key)){
    return {status:'invalid_private_storage_key',authorized:false,review_required:true,read_only:true};
  }

  return {
    status:'ok',
    authorized:true,
    family_id:auth.family_id,
    customer_id:auth.customer_id,
    media:{
      media_id:text(row.media_id),
      memory_id:text(row.memory_id),
      storage_key:text(row.storage_key),
      media_type:text(row.media_type)||'image',
      role:text(row.role)||'preview',
      width:row.width==null?null:Number(row.width),
      height:row.height==null?null:Number(row.height)
    },
    read_only:true
  };
}

export function memberPrivateMediaAccessHealth(){
  return {
    member_private_media_access:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    production_storage_fetch:false,
    server_verified_member_session_required:true,
    explicit_family_link_required:true,
    published_memory_required:true,
    deleted_media_hidden:true,
    cross_family_fail_closed:true,
    storage_key_internal_only:true,
    storage_key_public_response:false,
    absolute_url_storage_key_allowed:false,
    production_write:false
  };
}

export const __test={validSession,validMediaId,validPrivateStorageKey};
