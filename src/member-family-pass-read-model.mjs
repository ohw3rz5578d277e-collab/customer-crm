import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';

const BUILD='member-family-pass-read-model-20261009-step8';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const BLACK_ACHIEVEMENT_SOURCE='published-member-memories';
const UTC_INSTANT_RE=/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

const TIERS=[
  {code:'FAMILY',threshold:1},
  {code:'WELCOME_BACK',threshold:2},
  {code:'SILVER',threshold:3},
  {code:'GOLD',threshold:5},
  {code:'BLACK',threshold:10}
];

const text=v=>v==null?'':String(v).trim();
const strictText=v=>typeof v==='string'?v.trim():'';
const strictNonNegativeInteger=v=>typeof v==='number'&&Number.isSafeInteger(v)&&v>=0?v:null;

function canonicalUtcInstant(value){
  if(typeof value!=='string'||!UTC_INSTANT_RE.test(value))return false;
  const ms=Date.parse(value);
  return Number.isFinite(ms)&&new Date(ms).toISOString()===value;
}

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

function normalizeBlackEntitlement(row,expectedFamilyId){
  const expected=strictText(expectedFamilyId);
  if(!row||!expected)return null;
  if(strictText(row.family_id)!==expected)return null;
  if(row.black_lifetime!==1)return null;
  const achievedAt=strictText(row.black_achieved_at);
  const count=strictNonNegativeInteger(row.qualifying_memory_count);
  if(!canonicalUtcInstant(achievedAt)||count===null||count<10)return null;
  if(strictText(row.achievement_source)!==BLACK_ACHIEVEMENT_SOURCE)return null;
  return {
    family_id:expected,
    black_lifetime:true,
    black_achieved_at:achievedAt,
    qualifying_memory_count:count,
    achievement_source:BLACK_ACHIEVEMENT_SOURCE
  };
}

export function computeCurrentFamilyPass(memoryCount,{
  durable_black_entitlement=false,
  entitlement_schema_applied=false,
  black_achieved_at=''
}={}){
  const count=Math.max(0,Math.floor(Number(memoryCount)||0));
  const durableBlack=durable_black_entitlement===true;
  const effectiveBlack=durableBlack||count>=10;

  let current={code:'UNRANKED',threshold:0};
  for(const tier of TIERS){
    if(count>=tier.threshold)current=tier;
  }
  if(effectiveBlack){
    current=TIERS[TIERS.length-1];
  }

  const next=effectiveBlack?null:(TIERS.find(tier=>tier.threshold>count)||null);
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
      achieved:durableBlack?true:count>=tier.threshold
    })),
    black_currently_qualified:count>=10,
    black_lifetime_entitled:durableBlack,
    black_achieved_at:durableBlack?text(black_achieved_at):'',
    effective_black:effectiveBlack,
    tier_basis:durableBlack&&count<10?'durable_black_entitlement':'current_memory_count',
    black_lifetime_persistence_supported:entitlement_schema_applied===true,
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
  const memoryCount=strictNonNegativeInteger(row?.memory_count);
  if(memoryCount===null){
    return {
      status:'invalid_memory_count_evidence',
      family_id:auth.family_id,
      customer_id:auth.customer_id,
      read_only:true
    };
  }

  const entitlementSchemaApplied=await tableExists(env,'member_family_pass_entitlements');
  let entitlement=null;
  let entitlementRecordCount=0;
  if(entitlementSchemaApplied){
    const entitlementRow=await first(
      env,
      `SELECT family_id,black_lifetime,black_achieved_at,qualifying_memory_count,achievement_source
         FROM member_family_pass_entitlements
        WHERE family_id=?
          AND black_lifetime=1
        LIMIT 1`,
      [auth.family_id]
    );
    entitlementRecordCount=entitlementRow?1:0;
    entitlement=normalizeBlackEntitlement(entitlementRow,auth.family_id);
    if(entitlementRow&&!entitlement){
      return {
        status:'invalid_durable_black_evidence',
        family_id:auth.family_id,
        customer_id:auth.customer_id,
        read_only:true
      };
    }
  }

  const pass=computeCurrentFamilyPass(memoryCount,{
    durable_black_entitlement:!!entitlement,
    entitlement_schema_applied:entitlementSchemaApplied,
    black_achieved_at:entitlement?.black_achieved_at||''
  });

  return {
    status:'ok',
    family_id:auth.family_id,
    customer_id:auth.customer_id,
    family_pass:pass,
    entitlement:{
      schema_applied:entitlementSchemaApplied,
      schema_verified:true,
      read_verified:entitlementSchemaApplied,
      query_family_id:auth.family_id,
      record_count:entitlementSchemaApplied?entitlementRecordCount:null,
      durable_black:!!entitlement,
      black_achieved_at:entitlement?.black_achieved_at||null,
      qualifying_memory_count:entitlement?.qualifying_memory_count??null,
      achievement_source:entitlement?.achievement_source||null
    },
    benefit_contract:{
      black_photo_goods_discount_percent:10,
      applies_to_shooting_fee:false,
      enforcement_ready:false
    },
    count_source:{
      table:'member_memories',
      family_scoped:true,
      evidence_family_id:auth.family_id,
      count_verified:true,
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
  if(result.status==='invalid_memory_count_evidence'||result.status==='invalid_durable_black_evidence'){
    return json({ok:false,error:result.status,review_required:true},409);
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
    memory_count_evidence_strict:true,
    memory_count_exact_family_binding:true,
    one_memory_equals_one_shoot:true,
    thresholds:{
      family:1,
      welcome_back:2,
      silver:3,
      gold:5,
      black:10
    },
    durable_black_entitlement_source_supported:true,
    durable_black_exact_family_binding:true,
    durable_black_canonical_utc_required:true,
    durable_black_achievement_source:BLACK_ACHIEVEMENT_SOURCE,
    black_lifetime_persistence_supported:true,
    entitlement_table:'member_family_pass_entitlements',
    entitlement_schema_optional_for_backward_compatibility:true,
    production_entitlement_schema_apply_executed:false,
    black_benefit_enforcement_ready:false,
    black_goods_discount_percent:10,
    black_shooting_fee_discount:false,
    production_write:false
  };
}

export const __test={
  TIERS,
  BLACK_ACHIEVEMENT_SOURCE,
  validSession,
  strictNonNegativeInteger,
  canonicalUtcInstant,
  normalizeBlackEntitlement,
  computeCurrentFamilyPass
};
