import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';

const BUILD='member-family-passport-read-model-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const MEMORY_LIMIT=500;

const MILESTONES=[
  {
    code:'OMIYAMAIRI',
    label:'お宮参り',
    aliases:['お宮参り']
  },
  {
    code:'FIRST_BIRTHDAY',
    label:'1st Birthday',
    aliases:[
      '1歳',
      '1歳バースデー',
      '1歳誕生日',
      'ファーストバースデー',
      '1st birthday',
      'first birthday'
    ]
  },
  {
    code:'SHICHIGOSAN',
    label:'七五三',
    aliases:['七五三']
  },
  {
    code:'SCHOOL_ENTRANCE',
    label:'入学',
    aliases:[
      '入学',
      '入学撮影',
      '入学記念'
    ]
  }
];

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-family-passport-read-build':BUILD,
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
    return {
      ok:false,
      error:family.status,
      family_id:normalized.family_id,
      customer_id:normalized.customer_id
    };
  }
  if(text(family.family?.family_id)!==normalized.family_id){
    return {
      ok:false,
      error:'family_access_denied',
      family_id:normalized.family_id,
      customer_id:normalized.customer_id
    };
  }

  return {
    ok:true,
    family_id:normalized.family_id,
    customer_id:normalized.customer_id
  };
}

function normalizeGenre(value){
  return text(value)
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[\s　]+/g,' ');
}

const NORMALIZED_MILESTONES=MILESTONES.map(m=>({
  ...m,
  normalized_aliases:new Set(m.aliases.map(normalizeGenre))
}));

function milestoneForGenre(genre){
  const normalized=normalizeGenre(genre);
  if(!normalized)return null;
  for(const milestone of NORMALIZED_MILESTONES){
    if(milestone.normalized_aliases.has(normalized))return milestone.code;
  }
  return null;
}

function compareDateText(a,b){
  return text(a).localeCompare(text(b));
}

function buildPassport(rows=[]){
  const evidence=new Map(MILESTONES.map(m=>[m.code,[]]));
  const unmatchedGenres=new Set();

  for(const row of rows){
    const genre=text(row?.genre);
    const code=milestoneForGenre(genre);
    if(!code){
      if(genre)unmatchedGenres.add(genre);
      continue;
    }
    evidence.get(code).push({
      memory_id:text(row?.memory_id),
      shoot_date:text(row?.shoot_date),
      genre
    });
  }

  const milestones=MILESTONES.map(def=>{
    const items=(evidence.get(def.code)||[])
      .filter(x=>x.memory_id)
      .sort((a,b)=>compareDateText(a.shoot_date,b.shoot_date)||a.memory_id.localeCompare(b.memory_id));
    const first=items[0]||null;
    const latest=items[items.length-1]||null;
    return {
      code:def.code,
      label:def.label,
      achieved:items.length>0,
      evidence_count:items.length,
      first_achieved_date:first?.shoot_date||null,
      latest_achieved_date:latest?.shoot_date||null,
      evidence_memory_ids:items.map(x=>x.memory_id)
    };
  });

  const achievedCount=milestones.filter(x=>x.achieved).length;

  return {
    milestones,
    achieved_count:achievedCount,
    total_milestones:milestones.length,
    completion_ratio:milestones.length?achievedCount/milestones.length:0,
    all_achieved:achievedCount===milestones.length,
    unmatched_genres:[...unmatchedGenres].sort((a,b)=>a.localeCompare(b,'ja')),
    genre_matching:'explicit_alias_only',
    child_specific:false
  };
}

export async function readMemberFamilyPassportForSession(env,session){
  const auth=await authorizeSession(env,session);
  if(!auth.ok){
    return {
      status:auth.error,
      passport:null,
      read_only:true
    };
  }

  if(!(await tableExists(env,'member_memories'))){
    return {
      status:'member_memory_schema_not_applied',
      passport:null,
      read_only:true
    };
  }

  const rows=await all(
    env,
    `SELECT memory_id,family_id,shoot_date,genre
       FROM member_memories
      WHERE family_id=?
        AND published=1
        AND COALESCE(deleted_at,'')=''
      ORDER BY COALESCE(shoot_date,''),memory_id
      LIMIT ${MEMORY_LIMIT}`,
    [auth.family_id]
  );

  return {
    status:'ok',
    family_id:auth.family_id,
    customer_id:auth.customer_id,
    passport:buildPassport(rows),
    source:{
      table:'member_memories',
      family_scoped:true,
      published_only:true,
      deleted_hidden:true,
      explicit_genre_aliases_only:true
    },
    read_only:true
  };
}

export async function handleMemberFamilyPassportReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/family-passport')return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  const result=await readMemberFamilyPassportForSession(env,session);
  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='family_access_denied')return json({ok:false,error:'family_access_denied'},403);
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
  return json({ok:false,error:result.status||'invalid_request'},400);
}

export function memberFamilyPassportReadHealth(){
  return {
    member_family_passport_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    explicit_family_link_required:true,
    count_source:'published_non_deleted_member_memories',
    genre_matching:'explicit_alias_only',
    substring_genre_inference:false,
    generic_birthday_implies_first_birthday:false,
    combined_school_genre_implies_entrance:false,
    child_specific:false,
    milestones:MILESTONES.map(x=>({code:x.code,label:x.label})),
    production_write:false
  };
}

export const __test={
  MILESTONES,
  normalizeGenre,
  milestoneForGenre,
  buildPassport,
  validSession
};
