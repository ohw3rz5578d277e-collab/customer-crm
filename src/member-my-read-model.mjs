import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';
import { readMemberFamilyPassForSession } from './member-family-pass-read-model.mjs';
import { readMemberFamilyPassportForSession } from './member-family-passport-read-model.mjs';
import { readMemberFavoritesForSession } from './member-favorites-read-model.mjs';
import { readMemberNextMemoryForSession } from './member-next-memory-read-model.mjs';

const BUILD='member-my-read-model-20260925-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;

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
      'x-member-my-read-build':BUILD,
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

function componentIdentityMatches(result,session){
  if(result?.status!=='ok')return true;
  const familyId=text(result.family_id);
  const customerId=text(result.customer_id);
  if(familyId&&familyId!==session.family_id)return false;
  if(customerId&&customerId!==session.customer_id)return false;
  return true;
}

function fatalComponentStatus(results=[]){
  for(const result of results){
    const status=text(result?.status);
    if(FATAL_SECURITY_STATUSES.has(status))return status;
  }
  return '';
}

function sectionAvailable(data){
  return {available:true,data};
}

function sectionUnavailable(error){
  return {available:false,error:text(error)||'unavailable'};
}

function safeFamilySummary(familyResult){
  const family=familyResult?.family||{};
  return {
    display_name:text(family.display_name)||null,
    relation:text(family.relation)||null,
    access_role:text(family.access_role)||null,
    linked_member_count:Array.isArray(family.member_customer_ids)
      ?family.member_customer_ids.filter(id=>CUSTOMER_ID_RE.test(text(id))).length
      :0,
    family_profile_edit_ready:false,
    member_identity_edit_ready:false
  };
}

export function composeMemberMyModel({
  session,
  family,
  familyPass,
  passport,
  favorites,
  nextMemory
}){
  const normalized=validSession(session);
  if(!normalized.ok){
    return {status:'invalid_member_session',read_only:true};
  }

  if(family?.status!=='linked'){
    const status=text(family?.status)||'family_unavailable';
    return {
      status,
      review_required:status==='ambiguous_family_identity',
      read_only:true
    };
  }

  if(text(family.family?.family_id)!==normalized.family_id){
    return {
      status:'family_access_denied',
      read_only:true
    };
  }

  const components=[familyPass,passport,favorites,nextMemory];
  const fatal=fatalComponentStatus(components);
  if(fatal){
    return {
      status:fatal,
      review_required:[
        'ambiguous_family_identity',
        'family_identity_incomplete',
        'ambiguous_child_identity'
      ].includes(fatal),
      read_only:true
    };
  }

  if(!components.every(result=>componentIdentityMatches(result,normalized))){
    return {
      status:'component_identity_mismatch',
      review_required:true,
      read_only:true
    };
  }

  // Family Pass is the MY core because it provides the authoritative
  // published MEMORY count and current membership tier.
  if(familyPass?.status!=='ok'){
    return {
      status:text(familyPass?.status)||'family_pass_unavailable',
      read_only:true
    };
  }

  const passportSection=passport?.status==='ok'
    ?sectionAvailable(passport.passport)
    :sectionUnavailable(passport?.status);

  const favoritesSection=favorites?.status==='ok'
    ?sectionAvailable({
        favorite_count:Number(favorites.favorite_count||0),
        mutable:favorites.mutable===true
      })
    :sectionUnavailable(favorites?.status);

  const nextMemorySection=nextMemory?.status==='ok'
    ?sectionAvailable({
        next_memory:nextMemory.next_memory||null,
        canonical_child_count:Number(nextMemory.canonical_child_count||0),
        consultation_cta:nextMemory.consultation_cta||{
          channel:'line',
          intent:'consultation',
          automatic_send:false
        }
      })
    :sectionUnavailable(nextMemory?.status);

  const sections={
    family:sectionAvailable(safeFamilySummary(family)),
    family_pass:sectionAvailable({
      family_pass:familyPass.family_pass,
      entitlement:familyPass.entitlement,
      benefit_contract:familyPass.benefit_contract
    }),
    family_passport:passportSection,
    favorites:favoritesSection,
    next_memory:nextMemorySection,
    settings:sectionAvailable({
      family_profile_edit_ready:false,
      member_profile_edit_ready:false,
      notification_settings_ready:false,
      line_link_settings_ready:false,
      account_deletion_ready:false
    })
  };

  const unavailableSections=Object.entries(sections)
    .filter(([,value])=>value.available!==true)
    .map(([section,value])=>({section,error:value.error}));

  return {
    status:'ok',
    identity:{
      family_id:normalized.family_id,
      customer_id:normalized.customer_id
    },
    my:{
      family:sections.family.data,
      memory_count:Number(familyPass.family_pass?.memory_count||0),
      current_tier:text(familyPass.family_pass?.current_tier)||'UNRANKED',
      effective_black:familyPass.family_pass?.effective_black===true,
      favorite_count:favoritesSection.available
        ?favoritesSection.data.favorite_count
        :null,
      family_pass:sections.family_pass.data,
      family_passport:passportSection.available?passportSection.data:null,
      next_memory:nextMemorySection.available?nextMemorySection.data:null,
      settings:sections.settings.data,
      sections,
      partial:unavailableSections.length>0,
      unavailable_sections:unavailableSections,
      capabilities:{
        family_profile_edit:false,
        member_profile_edit:false,
        favorites_mutation:false,
        notification_settings_mutation:false,
        reservation_creation:false,
        automatic_contact:false,
        line_send:false
      }
    },
    read_only:true
  };
}

export async function readMemberMyForSession(env,session,{as_of}={}){
  const normalized=validSession(session);
  if(!normalized.ok){
    return {status:'invalid_member_session',read_only:true};
  }

  const family=await readMemberFamilyByCustomer(env,normalized.customer_id);
  if(family.status!=='linked'){
    return {
      status:family.status,
      review_required:family.status==='ambiguous_family_identity',
      read_only:true
    };
  }
  if(text(family.family?.family_id)!==normalized.family_id){
    return {status:'family_access_denied',read_only:true};
  }

  const [familyPass,passport,favorites,nextMemory]=await Promise.all([
    readMemberFamilyPassForSession(env,normalized),
    readMemberFamilyPassportForSession(env,normalized),
    readMemberFavoritesForSession(env,normalized),
    readMemberNextMemoryForSession(env,normalized,{as_of})
  ]);

  return composeMemberMyModel({
    session:normalized,
    family,
    familyPass,
    passport,
    favorites,
    nextMemory
  });
}

export async function handleMemberMyReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/my')return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  // Query parameters never supply identity or clock controls.
  const result=await readMemberMyForSession(env,session);

  if(result.status==='ok'){
    return json({
      ok:true,
      status:'ok',
      my:result.my,
      read_only:true
    });
  }

  if(result.status==='family_access_denied'){
    return json({ok:false,error:'family_access_denied'},403);
  }
  if(result.status==='unlinked'||result.status==='family_inactive_or_missing'){
    return json({ok:false,error:result.status},403);
  }
  if(result.status==='schema_not_applied'){
    return json({ok:false,error:'member_family_schema_not_applied'},409);
  }
  if(result.status==='member_memory_schema_not_applied'){
    return json({ok:false,error:'member_memory_schema_not_applied'},409);
  }
  if(
    result.status==='ambiguous_family_identity'
    || result.status==='family_identity_incomplete'
    || result.status==='ambiguous_child_identity'
    || result.status==='component_identity_mismatch'
  ){
    return json({ok:false,error:result.status,review_required:true},409);
  }

  return json({ok:false,error:result.status||'my_unavailable'},409);
}

export function memberMyReadHealth(){
  return {
    member_my_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    request_as_of_input:false,
    family_identity_core_required:true,
    family_pass_core_required:true,
    memory_count_source:'family_pass_published_member_memories',
    family_passport_optional_degradation:true,
    favorites_optional_degradation:true,
    next_memory_optional_schema_degradation:true,
    component_identity_consistency_required:true,
    identity_security_failures_fail_closed:true,
    customer_id_public_response:false,
    family_id_public_response:false,
    family_member_customer_ids_public_response:false,
    family_profile_edit_ready:false,
    member_profile_edit_ready:false,
    favorites_mutation_ready:false,
    notification_settings_mutation_ready:false,
    reservation_creation:false,
    automatic_contact:false,
    line_send:false,
    production_write:false
  };
}

export const __test={
  validSession,
  componentIdentityMatches,
  fatalComponentStatus,
  safeFamilySummary,
  composeMemberMyModel
};
