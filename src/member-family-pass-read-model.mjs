import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';

const BUILD='member-family-pass-read-model-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;

const TIERS=[
  {code:'FAMILY',threshold:1},
  {code:'WELCOME_BACK',threshold:2},
  {code:'SILVER',threshold:3},
  {code:'GOLD',threshold:5},
  {code:'BLACK',threshold:10}
];

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-family-pass-read-build':BUILD,
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
    customer_id:normalized.customer_id,
    relation:text(family.family?.relation),
    access_role:text(family.family?.access_role)
  };
}

export function computeCurrentFamilyPass(memoryCount){
  const count=Math.max(0,Math.floor(Number(memoryCount)||0));
  let current={code:'UNRANKED',threshold:0};
  for(const tier of TIERS){
    if(count>=tier.threshold)current=tier;
  }

  const next=TIERS.find(tier=>tier.threshold>count)||null;
  const previousThreshold=current.threshold;
  const nextThreshold=next?.threshold??current.threshold;
  const span=Math.max(1,nextThreshold-previousThreshold);
  const progress=next
    ?Math.max(0,Math.min(1,(count-previousThreshold)/span))
    :1;

  return {
    memory_count:count,
    current_tier:current.code,
    current_threshold:current.threshold,
    next_tier:next?.code||null,
    next_threshold:next?.threshold??null,
    memories_to_next:next?Math.max(0,next.threshold-count):0,
    progress_ratio:progress,
    milestones:TIERS.map(tier=>({
      tier:tier.code,
      threshold:tier.threshold,
      achieved:count>=tier.threshold
    })),
    black_currently_qualified:count>=10,
    black_lifetime_persistence_supported:false,
    entitlement_enforcement_ready:false
  };
}

export async function readMemberFamilyPassForSession(env,session){
  const auth=await authorizeSession(env,session);
  if(!auth.ok){
    return {
      status:auth.error,
      read_only:true
    };
  }

  if(!(await tableExists(env,'member_memories'))){
    return {
      status:'member_memory_schema_not_applied',
      read_only:true
    };
  }

  const row=await first(
    env,
    `SELECT COUNT(*) AS memory_count
       FROM member_memories
      WHERE family_id=?
        AND published=1
        AND COALESCE(deleted_at,'')=''`,
    [auth.family_id]
  );

  const memoryCount=Number(row?.memory_count||0);
  const pass=computeCurrentFamilyPass(memoryCount);

  return {
    status:'ok',
    family_id:auth.family_id,
    customer_id:auth.customer_id,
    family_pass:pass,
    benefit_contract:{
      black_photo_goods_discount_percent:10,
      applies_to_shooting_fee:false,
      enforcement_ready:false
    },
    count_source:{
      table:'member_memories',
      family_scoped:true,
      published_only:true,
      deleted_hidden:true,
      one_memory_equals_one_shoot:true
    },
    read_only:true
  };
}

export async function handleMemberFamilyPassReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/family-pass')return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  const result=await readMemberFamilyPassForSession(env,session);
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

export function memberFamilyPassReadHealth(){
  return {
    member_family_pass_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    explicit_family_link_required:true,
    count_source:'published_non_deleted_member_memories',
    one_memory_equals_one_shoot:true,
    thresholds:{
      family:1,
      welcome_back:2,
      silver:3,
      gold:5,
      black:10
    },
    black_lifetime_persistence_supported:false,
    black_benefit_enforcement_ready:false,
    black_goods_discount_percent:10,
    black_shooting_fee_discount:false,
    production_write:false
  };
}

export const __test={
  TIERS,
  validSession,
  computeCurrentFamilyPass
};
