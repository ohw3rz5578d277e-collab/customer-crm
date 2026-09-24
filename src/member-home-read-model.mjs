import { readMemberMemoriesForSession } from './member-memories-read-model.mjs';
import { readMemberFamilyPassForSession } from './member-family-pass-read-model.mjs';
import { readMemberFamilyPassportForSession } from './member-family-passport-read-model.mjs';
import { readMemberNextMemoryForSession } from './member-next-memory-read-model.mjs';

const BUILD='member-home-read-model-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const RECENT_MEMORY_LIMIT=3;
const NEXT_MEMORY_CANDIDATE_LIMIT=3;

const FATAL_SECURITY_STATUSES=new Set([
  'invalid_member_session',
  'family_access_denied',
  'schema_not_applied',
  'ambiguous_family_identity',
  'unlinked',
  'family_inactive_or_missing',
  'family_identity_incomplete',
  'ambiguous_child_identity'
]);

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-home-read-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer'
    }
  });
}

function validSession(session){
  const familyId=text(session?.family_id);
  const customerId=text(session?.customer_id);
  if(!familyId||familyId.length>MAX_FAMILY_ID)return {ok:false,error:'invalid_member_session'};
  if(!CUSTOMER_ID_RE.test(customerId))return {ok:false,error:'invalid_member_session'};
  return {ok:true,family_id:familyId,customer_id:customerId};
}

function fatalComponentStatus(results=[]){
  for(const result of results){
    const status=text(result?.status);
    if(FATAL_SECURITY_STATUSES.has(status))return status;
  }
  return '';
}

function componentIdentityMatches(result,session){
  if(result?.status!=='ok')return true;
  const familyId=text(result.family_id);
  const customerId=text(result.customer_id);
  if(familyId&&familyId!==session.family_id)return false;
  if(customerId&&customerId!==session.customer_id)return false;
  return true;
}

function sectionUnavailable(error){
  return {
    available:false,
    error:text(error)||'unavailable'
  };
}

function sectionAvailable(data){
  return {
    available:true,
    data
  };
}

export function composeMemberHomeModel({
  session,
  memories,
  familyPass,
  passport,
  nextMemory
}){
  const normalized=validSession(session);
  if(!normalized.ok){
    return {
      status:'invalid_member_session',
      read_only:true
    };
  }

  const results=[memories,familyPass,passport,nextMemory];

  const fatal=fatalComponentStatus(results);
  if(fatal){
    return {
      status:fatal,
      review_required:['ambiguous_family_identity','family_identity_incomplete','ambiguous_child_identity'].includes(fatal),
      read_only:true
    };
  }

  if(!results.every(result=>componentIdentityMatches(result,normalized))){
    return {
      status:'component_identity_mismatch',
      review_required:true,
      read_only:true
    };
  }

  // MEMORIES is the Home core. If its schema is unavailable, do not compose
  // a partial Home that could present an incomplete Family history as normal.
  if(memories?.status!=='ok'){
    return {
      status:text(memories?.status)||'memories_unavailable',
      read_only:true
    };
  }

  const recentMemories=(memories.memories||[])
    .slice(0,RECENT_MEMORY_LIMIT);

  const familyPassSection=familyPass?.status==='ok'
    ?sectionAvailable({
        family_pass:familyPass.family_pass,
        entitlement:familyPass.entitlement,
        benefit_contract:familyPass.benefit_contract
      })
    :sectionUnavailable(familyPass?.status);

  const passportSection=passport?.status==='ok'
    ?sectionAvailable(passport.passport)
    :sectionUnavailable(passport?.status);

  // NEXT MEMORY depends on managed child profile data that may not yet be
  // available in every environment. Its schema absence is optional/degraded,
  // while identity ambiguity has already failed closed above.
  const nextMemorySection=nextMemory?.status==='ok'
    ?sectionAvailable({
        next_memory:nextMemory.next_memory||null,
        candidates:(nextMemory.candidates||[]).slice(0,NEXT_MEMORY_CANDIDATE_LIMIT),
        consultation_cta:nextMemory.consultation_cta||{
          channel:'line',
          intent:'consultation',
          automatic_send:false
        },
        family_history_is_child_specific:nextMemory.family_history_is_child_specific===true,
        child_memory_link_available:nextMemory.child_memory_link_available===true
      })
    :sectionUnavailable(nextMemory?.status);

  const sections={
    memories:sectionAvailable({
      recent:recentMemories,
      visible_memory_count:(memories.memories||[]).length
    }),
    family_pass:familyPassSection,
    family_passport:passportSection,
    next_memory:nextMemorySection
  };

  const unavailableSections=Object.entries(sections)
    .filter(([,value])=>value.available!==true)
    .map(([key,value])=>({section:key,error:value.error}));

  return {
    status:'ok',
    family_id:normalized.family_id,
    customer_id:normalized.customer_id,
    home:{
      recent_memories:recentMemories,
      visible_memory_count:(memories.memories||[]).length,
      family_pass:familyPassSection.available?familyPassSection.data:null,
      family_passport:passportSection.available?passportSection.data:null,
      next_memory:nextMemorySection.available?nextMemorySection.data:null,
      sections,
      partial:unavailableSections.length>0,
      unavailable_sections:unavailableSections,
      future_modules:{
        today_memory:false,
        creative:false,
        shop_pickup:false,
        news:false
      }
    },
    read_only:true
  };
}

export async function readMemberHomeForSession(env,session,{as_of}={}){
  const normalized=validSession(session);
  if(!normalized.ok){
    return {
      status:'invalid_member_session',
      read_only:true
    };
  }

  const [memories,familyPass,passport,nextMemory]=await Promise.all([
    readMemberMemoriesForSession(env,normalized),
    readMemberFamilyPassForSession(env,normalized),
    readMemberFamilyPassportForSession(env,normalized),
    readMemberNextMemoryForSession(env,normalized,{as_of})
  ]);

  return composeMemberHomeModel({
    session:normalized,
    memories,
    familyPass,
    passport,
    nextMemory
  });
}

export async function handleMemberHomeReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/home')return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  // Query parameters never supply identity or deterministic clock controls.
  const result=await readMemberHomeForSession(env,session);

  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='family_access_denied')return json({ok:false,error:'family_access_denied'},403);
  if(result.status==='unlinked'||result.status==='family_inactive_or_missing'){
    return json({ok:false,error:result.status},403);
  }
  if(result.status==='schema_not_applied'){
    return json({ok:false,error:'member_family_schema_not_applied'},409);
  }
  if(result.status==='member_memory_schema_not_applied'){
    return json({ok:false,error:'member_memory_schema_not_applied'},409);
  }
  if(result.status==='ambiguous_family_identity'
    || result.status==='family_identity_incomplete'
    || result.status==='ambiguous_child_identity'
    || result.status==='component_identity_mismatch'){
    return json({ok:false,error:result.status,review_required:true},409);
  }
  return json({ok:false,error:result.status||'home_unavailable'},409);
}

export function memberHomeReadHealth(){
  return {
    member_home_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    request_as_of_input:false,
    components:[
      'member_memories',
      'family_pass',
      'family_passport',
      'next_memory'
    ],
    component_identity_consistency_required:true,
    identity_security_failures_fail_closed:true,
    memories_core_required:true,
    next_memory_optional_schema_degradation:true,
    recent_memory_limit:RECENT_MEMORY_LIMIT,
    next_memory_candidate_limit:NEXT_MEMORY_CANDIDATE_LIMIT,
    today_memory_active:false,
    creative_active:false,
    shop_pickup_active:false,
    news_active:false,
    automatic_contact:false,
    line_send:false,
    reservation_creation:false,
    production_write:false
  };
}

export const __test={
  validSession,
  fatalComponentStatus,
  componentIdentityMatches,
  composeMemberHomeModel
};
