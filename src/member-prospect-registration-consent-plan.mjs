import crypto from 'node:crypto';
import {buildProspectRegistrationPlan} from './member-identity-prospect-foundation.mjs';
import {planConsentRecord} from './member-consent-foundation.mjs';

const MEMBER_ID_RE=/^MID_[A-Za-z0-9_-]{22,}$/;
const PROSPECT_ID_RE=/^PID_[A-Za-z0-9_-]{22,}$/;
const IDEMPOTENCY_KEY_RE=/^[A-Za-z0-9._:-]{16,128}$/;
const EMAIL_RE=/^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const hasOwn=(obj,key)=>Object.prototype.hasOwnProperty.call(obj,key);
const strictText=value=>typeof value==='string'&&value.trim()===value?value:null;

function sha256(value){
  return crypto.createHash('sha256').update(value).digest('hex');
}

function parseCount(value){
  if(typeof value==='number') return Number.isSafeInteger(value)&&value>=0?value:null;
  if(typeof value==='string'&&/^(?:0|[1-9]\d*)$/.test(value)){
    const parsed=Number(value);
    return Number.isSafeInteger(parsed)?parsed:null;
  }
  return null;
}

function blocked(status,{review_required=false}={}){
  return {
    status,
    ready:false,
    review_required,
    write_allowed:false,
    execute:false,
    production_write_authorized:false,
    customer_id_generation:false,
    customer_id_from_client:false,
    raw_profile_output:false
  };
}

function validateProfile(profile){
  if(!profile||typeof profile!=='object'||Array.isArray(profile)) return {ok:false,status:'invalid_profile'};
  const limits={name:120,phone:40,address:500,email:254};
  const out={};
  for(const key of Object.keys(limits)){
    const value=strictText(profile[key]);
    if(value===null||value.length===0||value.length>limits[key]) return {ok:false,status:`invalid_profile_${key}`};
    out[key]=value;
  }
  if(!EMAIL_RE.test(out.email)) return {ok:false,status:'invalid_profile_email'};
  return {ok:true,profile:out};
}

function inspectScopedCount(evidence,{count_key,scope_key,expected_scope}){
  const count=parseCount(evidence[count_key]);
  if(count===null) return {ok:false,status:`invalid_${count_key}`};
  const scope=strictText(evidence[scope_key]);
  if(scope===null) return {ok:false,status:`invalid_${scope_key}`};
  if(scope!==expected_scope) return {ok:false,status:`${scope_key}_mismatch`};
  return {ok:true,count};
}

export function planProspectRegistrationConsent(evidence={}){
  if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)) return blocked('invalid_registration_evidence');

  const memberId=strictText(evidence.member_identity_id);
  const prospectId=strictText(evidence.prospect_id);
  if(memberId===null||!MEMBER_ID_RE.test(memberId)) return blocked('invalid_member_identity');
  if(prospectId===null||!PROSPECT_ID_RE.test(prospectId)) return blocked('invalid_prospect_id');
  if(evidence.server_generated_identity_verified!==true) return blocked('server_generated_identity_not_verified');
  if(evidence.member_identity_source!=='server_generated'||evidence.prospect_id_source!=='server_generated'){
    return blocked('invalid_identity_source');
  }

  if(hasOwn(evidence,'canonical_customer_id')&&evidence.canonical_customer_id!==null){
    return blocked('prospect_customer_scope_forbidden',{review_required:true});
  }
  if(hasOwn(evidence,'family_id')&&evidence.family_id!==null){
    return blocked('prospect_family_scope_forbidden',{review_required:true});
  }

  const identityPlan=buildProspectRegistrationPlan({member_identity_id:memberId,prospect_id:prospectId});
  if(identityPlan.status!=='ready'||identityPlan.customer_id!==null||identityPlan.customer_id_generation!==false){
    return blocked('identity_plan_not_ready',{review_required:true});
  }

  const profileCheck=validateProfile(evidence.profile);
  if(!profileCheck.ok) return blocked(profileCheck.status);
  const profileDigest=sha256(JSON.stringify([
    profileCheck.profile.name,
    profileCheck.profile.phone,
    profileCheck.profile.address,
    profileCheck.profile.email
  ]));

  const consentPlan=planConsentRecord({
    member_identity_id:memberId,
    terms_version:evidence.terms_version,
    terms_sha256:evidence.terms_sha256,
    privacy_version:evidence.privacy_version,
    privacy_sha256:evidence.privacy_sha256,
    terms_accepted:evidence.terms_accepted,
    privacy_accepted:evidence.privacy_accepted,
    accepted_at:evidence.accepted_at
  });
  if(consentPlan.status!=='ready'||consentPlan.append_only!==true||consentPlan.replace_prior_consent!==false){
    return blocked(`consent_${consentPlan.status}`);
  }

  const idempotencyKey=strictText(evidence.registration_idempotency_key);
  if(idempotencyKey===null||!IDEMPOTENCY_KEY_RE.test(idempotencyKey)) return blocked('invalid_registration_idempotency_key');

  const registrationEventId=`REG_${sha256(JSON.stringify([
    idempotencyKey,memberId,prospectId,profileDigest,
    consentPlan.terms_version,consentPlan.terms_sha256,
    consentPlan.privacy_version,consentPlan.privacy_sha256,consentPlan.accepted_at
  ]))}`;
  const consentEventId=`CONS_${sha256(JSON.stringify([
    memberId,prospectId,registrationEventId,
    consentPlan.terms_version,consentPlan.terms_sha256,
    consentPlan.privacy_version,consentPlan.privacy_sha256,consentPlan.accepted_at
  ]))}`;

  const memberCollision=inspectScopedCount(evidence,{
    count_key:'existing_member_identity_count',
    scope_key:'existing_member_identity_count_member_identity_id',
    expected_scope:memberId
  });
  if(!memberCollision.ok) return blocked(memberCollision.status,{review_required:true});

  const prospectCollision=inspectScopedCount(evidence,{
    count_key:'existing_prospect_id_count',
    scope_key:'existing_prospect_id_count_prospect_id',
    expected_scope:prospectId
  });
  if(!prospectCollision.ok) return blocked(prospectCollision.status,{review_required:true});

  const registrationCount=inspectScopedCount(evidence,{
    count_key:'existing_registration_event_count',
    scope_key:'existing_registration_event_count_event_id',
    expected_scope:registrationEventId
  });
  if(!registrationCount.ok) return blocked(registrationCount.status,{review_required:true});

  const consentCount=inspectScopedCount(evidence,{
    count_key:'existing_consent_event_count',
    scope_key:'existing_consent_event_count_event_id',
    expected_scope:consentEventId
  });
  if(!consentCount.ok) return blocked(consentCount.status,{review_required:true});

  const counts=[memberCollision.count,prospectCollision.count,registrationCount.count,consentCount.count];
  if(counts.every(count=>count===0)){
    return {
      status:'ready',
      ready:true,
      review_required:false,
      member_identity_id:memberId,
      prospect_id:prospectId,
      canonical_customer_id:null,
      family_id:null,
      profile_digest_sha256:profileDigest,
      registration_event_id:registrationEventId,
      consent_event_id:consentEventId,
      consent:{
        member_identity_id:memberId,
        terms_version:consentPlan.terms_version,
        terms_sha256:consentPlan.terms_sha256,
        privacy_version:consentPlan.privacy_version,
        privacy_sha256:consentPlan.privacy_sha256,
        accepted_at:consentPlan.accepted_at,
        append_only:true,
        replace_prior_consent:false
      },
      transaction_required:true,
      transaction_operations:[
        'create_member_identity',
        'create_prospect',
        'append_registration_event',
        'append_consent_event'
      ],
      partial_commit_allowed:false,
      registration_completed:false,
      completion_requires_atomic_execution:true,
      server_generated_identity_verified:true,
      customer_id_generation:false,
      customer_id_from_client:false,
      family_id_generation:false,
      fuzzy_identity_linking:false,
      raw_profile_output:false,
      profile_pii_long_term_storage_authorized:false,
      write_allowed:false,
      execute:false,
      production_write_authorized:false,
      execution_requires_separate_gate:true
    };
  }

  if(counts.every(count=>count===1)){
    return {
      ...blocked('registration_already_recorded'),
      member_identity_id:memberId,
      prospect_id:prospectId,
      registration_event_id:registrationEventId,
      consent_event_id:consentEventId,
      idempotent_replay:true
    };
  }

  return blocked('registration_state_conflict',{review_required:true});
}

export const __test={MEMBER_ID_RE,PROSPECT_ID_RE,IDEMPOTENCY_KEY_RE,EMAIL_RE,parseCount};
