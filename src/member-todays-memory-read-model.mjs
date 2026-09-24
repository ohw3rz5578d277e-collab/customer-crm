import { readMemberMemoriesForSession } from './member-memories-read-model.mjs';
import { jstToday } from './crm-customer360-marketing-engine.mjs';

const BUILD='member-todays-memory-read-model-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const EXACT_LIMIT=3;
const SEASONAL_WINDOW_DAYS=7;

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-todays-memory-read-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer'
    }
  });
}

function validDateOnly(value){
  const m=text(value).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if(!m)return '';
  const y=Number(m[1]);
  const month=Number(m[2]);
  const day=Number(m[3]);
  const d=new Date(Date.UTC(y,month-1,day));
  if(
    d.getUTCFullYear()!==y
    || d.getUTCMonth()+1!==month
    || d.getUTCDate()!==day
  )return '';
  return `${m[1]}-${m[2]}-${m[3]}`;
}

function validSession(session){
  const familyId=text(session?.family_id);
  const customerId=text(session?.customer_id);
  if(!familyId||familyId.length>MAX_FAMILY_ID)return {ok:false,error:'invalid_member_session'};
  if(!CUSTOMER_ID_RE.test(customerId))return {ok:false,error:'invalid_member_session'};
  return {ok:true,family_id:familyId,customer_id:customerId};
}

function memoryDateParts(memory){
  const date=validDateOnly(text(memory?.shoot_date).slice(0,10));
  if(!date)return null;
  const [year,month,day]=date.split('-').map(Number);
  return {date,year,month,day};
}

function decorateMemory(memory,asOfParts,mode,distanceDays=0){
  const parts=memoryDateParts(memory);
  if(!parts)return null;
  return {
    ...memory,
    anniversary:{
      mode,
      years_ago:asOfParts.year-parts.year,
      calendar_distance_days:distanceDays
    }
  };
}

export function buildTodaysMemory(memories=[],asOf=jstToday()){
  const safeAsOf=validDateOnly(asOf);
  if(!safeAsOf){
    return {
      status:'invalid_as_of',
      today_memory:null,
      read_only:true
    };
  }

  const [year,month,day]=safeAsOf.split('-').map(Number);
  const asOfParts={year,month,day};

  const eligible=(memories||[])
    .map(memory=>({memory,parts:memoryDateParts(memory)}))
    .filter(x=>x.parts&&x.parts.year<year);

  const exact=eligible
    .filter(x=>x.parts.month===month&&x.parts.day===day)
    .sort((a,b)=>b.parts.date.localeCompare(a.parts.date))
    .slice(0,EXACT_LIMIT)
    .map(x=>decorateMemory(x.memory,asOfParts,'exact_anniversary',0))
    .filter(Boolean);

  if(exact.length){
    return {
      status:'ok',
      today_memory:{
        as_of:safeAsOf,
        mode:'exact_anniversary',
        headline:'この日の思い出',
        primary:exact[0],
        memories:exact,
        exact_match_count:exact.length,
        seasonal_fallback_used:false
      },
      source:{
        reader:'member_memories_read_model',
        published_only:true,
        deleted_hidden:true,
        family_scoped:true,
        bounded_source_window:true
      },
      read_only:true
    };
  }

  const seasonal=eligible
    .filter(x=>x.parts.month===month)
    .map(x=>({
      ...x,
      distance:Math.abs(x.parts.day-day)
    }))
    .filter(x=>x.distance>0&&x.distance<=SEASONAL_WINDOW_DAYS)
    .sort((a,b)=>
      a.distance-b.distance
      || b.parts.date.localeCompare(a.parts.date)
      || text(a.memory?.memory_id).localeCompare(text(b.memory?.memory_id))
    );

  const fallback=seasonal.length
    ?decorateMemory(seasonal[0].memory,asOfParts,'seasonal_nearby',seasonal[0].distance)
    :null;

  return {
    status:'ok',
    today_memory:fallback?{
      as_of:safeAsOf,
      mode:'seasonal_nearby',
      headline:'この頃の思い出',
      primary:fallback,
      memories:[fallback],
      exact_match_count:0,
      seasonal_fallback_used:true
    }:null,
    source:{
      reader:'member_memories_read_model',
      published_only:true,
      deleted_hidden:true,
      family_scoped:true,
      bounded_source_window:true
    },
    read_only:true
  };
}

export async function readMemberTodaysMemoryForSession(env,session,{as_of}={}){
  const normalized=validSession(session);
  if(!normalized.ok){
    return {
      status:'invalid_member_session',
      today_memory:null,
      read_only:true
    };
  }

  const memories=await readMemberMemoriesForSession(env,normalized);
  if(memories.status!=='ok'){
    return {
      status:memories.status,
      today_memory:null,
      read_only:true
    };
  }

  if(text(memories.family_id)!==normalized.family_id){
    return {
      status:'component_identity_mismatch',
      today_memory:null,
      review_required:true,
      read_only:true
    };
  }

  const safeAsOf=validDateOnly(as_of)||jstToday();
  const built=buildTodaysMemory(memories.memories,safeAsOf);

  return {
    ...built,
    family_id:normalized.family_id,
    customer_id:normalized.customer_id,
    visible_memory_count:memories.memories.length
  };
}

export async function handleMemberTodaysMemoryReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/todays-memory')return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  // Request query parameters never control identity or the deterministic clock.
  const result=await readMemberTodaysMemoryForSession(env,session);

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
    || result.status==='component_identity_mismatch'){
    return json({ok:false,error:result.status,review_required:true},409);
  }
  return json({ok:false,error:result.status||'todays_memory_unavailable'},409);
}

export function memberTodaysMemoryReadHealth(){
  return {
    member_todays_memory_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    request_as_of_input:false,
    source_reader:'member_memories_read_model',
    published_only:true,
    deleted_hidden:true,
    exact_anniversary_priority:true,
    exact_match_limit:EXACT_LIMIT,
    seasonal_fallback_same_month_only:true,
    seasonal_fallback_window_days:SEASONAL_WINDOW_DAYS,
    seasonal_fallback_honest_label:true,
    current_year_excluded:true,
    leap_day_exact_only_on_feb29:true,
    bounded_source_window:true,
    database_total_claim:false,
    home_activation:false,
    automatic_contact:false,
    line_send:false,
    production_write:false
  };
}

export const __test={
  validDateOnly,
  validSession,
  memoryDateParts,
  decorateMemory,
  buildTodaysMemory
};
