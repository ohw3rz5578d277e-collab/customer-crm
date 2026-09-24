import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';

const BUILD='member-memories-read-model-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const MAX_MEMORY_ID=160;
const LIST_LIMIT=200;
const MEDIA_LIMIT=1000;

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-memories-read-build':BUILD,
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
  const r=await q.all();
  return r.results||[];
}

async function tableExists(env,name){
  return !!(await first(env,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",[name]));
}

function safeHttpsUrl(value){
  try{
    const u=new URL(text(value));
    return u.protocol==='https:'?u.toString():'';
  }catch{
    return '';
  }
}

function safeDecode(value){
  try{
    return decodeURIComponent(value);
  }catch{
    return '';
  }
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
    return {ok:false,error:family.status,family_id:normalized.family_id,customer_id:normalized.customer_id};
  }
  if(text(family.family?.family_id)!==normalized.family_id){
    return {ok:false,error:'family_access_denied',family_id:normalized.family_id,customer_id:normalized.customer_id};
  }
  return {
    ok:true,
    family_id:normalized.family_id,
    customer_id:normalized.customer_id,
    access_role:text(family.family?.access_role),
    relation:text(family.family?.relation)
  };
}

async function ensureMemorySchema(env){
  const memories=await tableExists(env,'member_memories');
  const media=await tableExists(env,'member_memory_media');
  return memories&&media;
}

function mediaView(row){
  return {
    media_id:text(row.media_id),
    media_type:text(row.media_type)||'image',
    role:text(row.role)||'preview',
    sort_order:Number.isFinite(Number(row.sort_order))?Number(row.sort_order):0,
    width:row.width==null?null:Number(row.width),
    height:row.height==null?null:Number(row.height),
    private_delivery_required:true
  };
}

function sortMedia(a,b){
  const roleA=text(a.role)==='cover'?0:1;
  const roleB=text(b.role)==='cover'?0:1;
  return roleA-roleB
    || Number(a.sort_order||0)-Number(b.sort_order||0)
    || text(a.media_id).localeCompare(text(b.media_id));
}

function memoryListItem(row,mediaRows=[]){
  const media=mediaRows.slice().sort(sortMedia);
  const cover=media.length?mediaView(media[0]):null;
  const amazon=safeHttpsUrl(row.amazon_photos_url);
  return {
    memory_id:text(row.memory_id),
    shoot_date:text(row.shoot_date),
    genre:text(row.genre),
    title:text(row.title)||'MEMORY',
    cover,
    preview_count:media.length,
    amazon_photos_available:!!amazon,
    favorite:false,
    favorite_mutable:false,
    create_available:false,
    shop_available:false
  };
}

function memoryDetail(row,mediaRows=[]){
  const amazon=safeHttpsUrl(row.amazon_photos_url);
  return {
    memory:{
      memory_id:text(row.memory_id),
      shoot_date:text(row.shoot_date),
      genre:text(row.genre),
      title:text(row.title)||'MEMORY'
    },
    media:mediaRows.slice().sort(sortMedia).map(mediaView),
    amazon_link:amazon?{provider:'amazon_photos',url:amazon}:null,
    favorite:false,
    favorite_mutable:false,
    next_memory:null,
    create_available:false,
    create_actions:[],
    shop_available:false
  };
}

async function readFamilyMedia(env,familyId){
  return all(
    env,
    `SELECT media_id,memory_id,family_id,media_type,role,sort_order,width,height
       FROM member_memory_media
      WHERE family_id=?
        AND COALESCE(deleted_at,'')=''
      ORDER BY memory_id,
               CASE WHEN role='cover' THEN 0 ELSE 1 END,
               sort_order,
               media_id
      LIMIT ${MEDIA_LIMIT}`,
    [familyId]
  );
}

export async function readMemberMemoriesForSession(env,session){
  const auth=await authorizeSession(env,session);
  if(!auth.ok)return {status:auth.error,memories:[],read_only:true};

  if(!(await ensureMemorySchema(env))){
    return {status:'member_memory_schema_not_applied',memories:[],read_only:true};
  }

  const rows=await all(
    env,
    `SELECT memory_id,family_id,shoot_date,genre,title,amazon_photos_url,published,created_at,updated_at
       FROM member_memories
      WHERE family_id=?
        AND published=1
        AND COALESCE(deleted_at,'')=''
      ORDER BY COALESCE(shoot_date,'') DESC, created_at DESC, memory_id DESC
      LIMIT ${LIST_LIMIT}`,
    [auth.family_id]
  );

  const mediaRows=await readFamilyMedia(env,auth.family_id);
  const mediaByMemory=new Map();
  for(const media of mediaRows){
    const memoryId=text(media.memory_id);
    if(!mediaByMemory.has(memoryId))mediaByMemory.set(memoryId,[]);
    mediaByMemory.get(memoryId).push(media);
  }

  return {
    status:'ok',
    family_id:auth.family_id,
    memories:rows.map(row=>memoryListItem(row,mediaByMemory.get(text(row.memory_id))||[])),
    read_only:true
  };
}

export async function readMemberMemoryDetailForSession(env,session,memoryId){
  const id=text(memoryId);
  if(!id||id.length>MAX_MEMORY_ID)return {status:'invalid_memory_id',read_only:true};

  const auth=await authorizeSession(env,session);
  if(!auth.ok)return {status:auth.error,read_only:true};

  if(!(await ensureMemorySchema(env))){
    return {status:'member_memory_schema_not_applied',read_only:true};
  }

  const row=await first(
    env,
    `SELECT memory_id,family_id,shoot_date,genre,title,amazon_photos_url,published,created_at,updated_at
       FROM member_memories
      WHERE memory_id=?
        AND family_id=?
        AND published=1
        AND COALESCE(deleted_at,'')=''
      LIMIT 1`,
    [id,auth.family_id]
  );

  if(!row)return {status:'memory_not_found',read_only:true};

  const mediaRows=await all(
    env,
    `SELECT media_id,memory_id,family_id,media_type,role,sort_order,width,height
       FROM member_memory_media
      WHERE memory_id=?
        AND family_id=?
        AND COALESCE(deleted_at,'')=''
      ORDER BY CASE WHEN role='cover' THEN 0 ELSE 1 END, sort_order, media_id
      LIMIT 100`,
    [id,auth.family_id]
  );

  return {
    status:'ok',
    family_id:auth.family_id,
    ...memoryDetail(row,mediaRows),
    read_only:true
  };
}

export async function handleMemberMemoriesReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  const listPath=url.pathname==='/api/internal/member/memories';
  const detailMatch=url.pathname.match(/^\/api\/internal\/member\/memories\/([^/]{1,160})$/);
  if(!listPath&&!detailMatch)return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  const decodedMemoryId=detailMatch?safeDecode(detailMatch[1]):'';
  if(detailMatch&&!decodedMemoryId)return json({ok:false,error:'invalid_memory_id'},400);

  const result=listPath
    ?await readMemberMemoriesForSession(env,session)
    :await readMemberMemoryDetailForSession(env,session,decodedMemoryId);

  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='family_access_denied')return json({ok:false,error:'family_access_denied'},403);
  if(result.status==='memory_not_found')return json({ok:false,error:'memory_not_found'},404);
  if(result.status==='member_memory_schema_not_applied'){
    return json({ok:false,error:'member_memory_schema_not_applied'},409);
  }
  if(result.status==='schema_not_applied'){
    return json({ok:false,error:'member_family_schema_not_applied'},409);
  }
  if(result.status==='ambiguous_family_identity')return json({ok:false,error:'ambiguous_family_identity',review_required:true},409);
  if(result.status==='unlinked'||result.status==='family_inactive_or_missing')return json({ok:false,error:result.status},403);
  return json({ok:false,error:result.status||'invalid_request'},400);
}

export function memberMemoriesReadHealth(){
  return {
    member_memories_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    explicit_family_link_required:true,
    published_only:true,
    deleted_hidden:true,
    cross_family_fail_closed:true,
    malformed_memory_id_fail_closed:true,
    private_storage_key_exposed:false,
    production_write:false
  };
}

export const __test={safeHttpsUrl,safeDecode,validSession,memoryListItem,memoryDetail};
