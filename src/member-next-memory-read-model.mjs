import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';
import { buildOpportunities, jstToday } from './crm-customer360-marketing-engine.mjs';

const BUILD='member-next-memory-read-model-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const CHILD_LIMIT_PER_CUSTOMER=50;
const MEMORY_LIMIT=500;
const ALLOWED_TYPES=new Set([
  'birthday',
  'half_birthday',
  'first_birthday',
  'shichigosan',
  'school_entry_candidate',
  'graduation_candidate',
  'coming_of_age_candidate'
]);

const SPECIFIC_PRIORITY={
  first_birthday:0,
  half_birthday:1,
  shichigosan:2,
  school_entry_candidate:3,
  graduation_candidate:4,
  coming_of_age_candidate:5,
  birthday:9
};

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-next-memory-read-build':BUILD,
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

function dateOnly(value){
  const m=text(value).match(/^(\d{4}-\d{2}-\d{2})/);
  return m?m[1]:'';
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

  const familyCustomerIds=[...new Set(
    (family.family?.member_customer_ids||[])
      .map(text)
      .filter(CUSTOMER_ID_RE.test.bind(CUSTOMER_ID_RE))
  )].sort();

  if(!familyCustomerIds.includes(normalized.customer_id)){
    return {
      ok:false,
      error:'family_identity_incomplete',
      family_id:normalized.family_id,
      customer_id:normalized.customer_id
    };
  }

  return {
    ok:true,
    family_id:normalized.family_id,
    customer_id:normalized.customer_id,
    family_customer_ids:familyCustomerIds
  };
}

async function readCanonicalChildren(env,customerIds){
  if(!(await tableExists(env,'customer_family_members'))){
    return {status:'child_profile_schema_not_applied',children:[]};
  }

  const children=[];
  const seenChildIds=new Map();

  for(const customerId of customerIds){
    const rows=await all(
      env,
      `SELECT id,customer_id,relation,name,birthdate,school_stage,deleted_at
         FROM customer_family_members
        WHERE CAST(customer_id AS TEXT)=?
          AND relation='child'
          AND COALESCE(deleted_at,'')=''
        ORDER BY created_at,id
        LIMIT ${CHILD_LIMIT_PER_CUSTOMER}`,
      [customerId]
    );

    for(const row of rows){
      if(text(row.customer_id)!==customerId)continue;
      const childId=text(row.id);
      if(!childId)continue;

      const normalized={
        id:childId,
        relation:'child',
        name:text(row.name),
        birthdate:dateOnly(row.birthdate),
        school_stage:text(row.school_stage),
        source_customer_id:customerId
      };

      if(seenChildIds.has(childId)){
        const previous=seenChildIds.get(childId);
        if(
          previous.source_customer_id!==normalized.source_customer_id
          || previous.birthdate!==normalized.birthdate
          || previous.school_stage!==normalized.school_stage
          || previous.name!==normalized.name
        ){
          return {
            status:'ambiguous_child_identity',
            children:[],
            review_required:true
          };
        }
        continue;
      }

      seenChildIds.set(childId,normalized);
      children.push(normalized);
    }
  }

  return {
    status:'ok',
    children:children.sort((a,b)=>
      a.source_customer_id.localeCompare(b.source_customer_id)
      || a.id.localeCompare(b.id)
    )
  };
}

async function readFamilyMemoryGenres(env,familyId){
  if(!(await tableExists(env,'member_memories'))){
    return {status:'member_memory_schema_not_applied',genres:[]};
  }

  const rows=await all(
    env,
    `SELECT memory_id,shoot_date,genre
       FROM member_memories
      WHERE family_id=?
        AND published=1
        AND COALESCE(deleted_at,'')=''
      ORDER BY COALESCE(shoot_date,''),memory_id
      LIMIT ${MEMORY_LIMIT}`,
    [familyId]
  );

  const genres=[...new Set(rows.map(x=>text(x.genre)).filter(Boolean))]
    .sort((a,b)=>a.localeCompare(b,'ja'));

  return {
    status:'ok',
    genres,
    memory_count:rows.length
  };
}

function genreEvidenceForType(type,genres=[]){
  const normalized=genres.map(x=>text(x).normalize('NFKC').toLowerCase());
  const exact=values=>values.some(v=>normalized.includes(v.normalize('NFKC').toLowerCase()));

  if(type==='half_birthday')return exact(['ハーフバースデー']);
  if(type==='first_birthday')return exact(['1歳','1歳バースデー','1歳誕生日','ファーストバースデー','1st birthday','first birthday']);
  if(type==='shichigosan')return exact(['七五三']);
  if(type==='school_entry_candidate')return exact(['入学','入学撮影','入学記念']);
  if(type==='graduation_candidate')return exact(['卒業','卒業撮影','卒業記念']);
  if(type==='coming_of_age_candidate')return exact(['成人','成人式','成人記念']);
  if(type==='birthday')return exact(['バースデー','誕生日']);
  return false;
}

function dedupeGenericBirthdays(opportunities=[]){
  const specificKeys=new Set(
    opportunities
      .filter(o=>o.type!=='birthday')
      .map(o=>[text(o.member_id),text(o.date)].join('|'))
  );

  return opportunities.filter(o=>{
    if(o.type!=='birthday')return true;
    return !specificKeys.has([text(o.member_id),text(o.date)].join('|'));
  });
}

function normalizeCandidate(opportunity,familyGenres){
  const days=opportunity.days==null?null:Number(opportunity.days);
  return {
    type:text(opportunity.type),
    label:text(opportunity.label),
    target_date:dateOnly(opportunity.date)||null,
    days_until:Number.isFinite(days)?days:null,
    child:{
      child_id:text(opportunity.member_id),
      display_name:text(opportunity.member_name)||null,
      age:opportunity.age==null?null:Number(opportunity.age)
    },
    source:text(opportunity.source)||'birthdate',
    family_level_history_match:genreEvidenceForType(opportunity.type,familyGenres),
    history_scope:'family_not_child',
    candidate_only:true,
    automatic_contact:false
  };
}

function rankCandidates(candidates=[]){
  return [...candidates].sort((a,b)=>{
    const ad=a.days_until==null?99999:a.days_until;
    const bd=b.days_until==null?99999:b.days_until;
    return ad-bd
      || (SPECIFIC_PRIORITY[a.type]??99)-(SPECIFIC_PRIORITY[b.type]??99)
      || text(a.child?.child_id).localeCompare(text(b.child?.child_id))
      || a.type.localeCompare(b.type);
  });
}

export function buildNextMemoryCandidates(children=[],familyGenres=[],asOf=jstToday()){
  const raw=buildOpportunities({},children,asOf)
    .filter(o=>ALLOWED_TYPES.has(text(o.type)))
    .filter(o=>o.days==null||Number(o.days)>=0);

  const deduped=dedupeGenericBirthdays(raw);
  const candidates=rankCandidates(deduped.map(o=>normalizeCandidate(o,familyGenres)));

  return {
    as_of:asOf,
    next_memory:candidates[0]||null,
    candidates,
    family_genre_history:[...familyGenres],
    family_history_is_child_specific:false,
    child_memory_link_available:false,
    recommendation_basis:'canonical_child_birthdate_school_stage_plus_family_memory_genres',
    automatic_contact:false
  };
}

export async function readMemberNextMemoryForSession(env,session,{as_of}={}){
  const auth=await authorizeSession(env,session);
  if(!auth.ok){
    return {
      status:auth.error,
      next_memory:null,
      candidates:[],
      read_only:true
    };
  }

  const childrenResult=await readCanonicalChildren(env,auth.family_customer_ids);
  if(childrenResult.status!=='ok'){
    return {
      status:childrenResult.status,
      review_required:childrenResult.review_required===true,
      next_memory:null,
      candidates:[],
      read_only:true
    };
  }

  const memoryResult=await readFamilyMemoryGenres(env,auth.family_id);
  if(memoryResult.status!=='ok'){
    return {
      status:memoryResult.status,
      next_memory:null,
      candidates:[],
      read_only:true
    };
  }

  const safeAsOf=dateOnly(as_of)||jstToday();
  const model=buildNextMemoryCandidates(
    childrenResult.children,
    memoryResult.genres,
    safeAsOf
  );

  return {
    status:'ok',
    family_id:auth.family_id,
    customer_id:auth.customer_id,
    family_customer_count:auth.family_customer_ids.length,
    canonical_child_count:childrenResult.children.length,
    published_memory_count:memoryResult.memory_count,
    ...model,
    read_only:true
  };
}

export async function handleMemberNextMemoryReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/next-memory')return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  const result=await readMemberNextMemoryForSession(env,session);
  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='family_access_denied')return json({ok:false,error:'family_access_denied'},403);
  if(result.status==='family_identity_incomplete'){
    return json({ok:false,error:'family_identity_incomplete',review_required:true},409);
  }
  if(result.status==='ambiguous_child_identity'){
    return json({ok:false,error:'ambiguous_child_identity',review_required:true},409);
  }
  if(result.status==='child_profile_schema_not_applied'){
    return json({ok:false,error:'child_profile_schema_not_applied'},409);
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
  return json({ok:false,error:result.status||'invalid_request'},400);
}

export function memberNextMemoryReadHealth(){
  return {
    member_next_memory_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    request_as_of_input:false,
    explicit_family_link_required:true,
    family_linked_customer_ids_only:true,
    child_source:'customer_family_members',
    child_identity_by_exact_id_only:true,
    child_name_identity_inference:false,
    child_memory_link_available:false,
    family_history_is_child_specific:false,
    memory_source:'published_non_deleted_member_memories',
    shared_opportunity_logic:'crm-customer360-marketing-engine',
    marketing_priority_used:false,
    line_draft_used:false,
    automatic_contact:false,
    line_send:false,
    production_write:false
  };
}

export const __test={
  dateOnly,
  genreEvidenceForType,
  dedupeGenericBirthdays,
  normalizeCandidate,
  rankCandidates,
  buildNextMemoryCandidates,
  validSession
};
