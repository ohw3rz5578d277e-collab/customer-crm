import crypto from 'node:crypto';
import {buildProspectRegistrationPlan} from './member-identity-prospect-foundation.mjs';
import {planConsentRecord} from './member-consent-foundation.mjs';

const MEMBER_ID_RE=/^MID_[A-Za-z0-9_-]{22,}$/;
const PROSPECT_ID_RE=/^PID_[A-Za-z0-9_-]{22,}$/;
const IDEMPOTENCY_KEY_RE=/^[A-Za-z0-9._:-]{16,128}$/;
const VERSION_RE=/^[A-Za-z0-9._-]{1,64}$/;
const SHA256_HEX_RE=/^[0-9a-f]{64}$/;
const EMAIL_RE=/^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const hasOwn=(obj,key)=>Object.prototype.hasOwnProperty.call(obj,key);
const strictText=value=>typeof value==='string'&&value.trim()===value?value:null;

function sha256(value){ return crypto.createHash('sha256').update(value).digest('hex'); }
function parseCount(value){
  if(typeof value==='number') return Number.isSafeInteger(value)&&value>=0?value:null;
  if(typeof value==='string'&&/^(?:0|[1-9]\d*)$/.test(value)){
    const parsed=Number(value);
    return Number.isSafeInteger(parsed)?parsed:null;
  }
  return null;
}
function blocked(status,{review_required=false}={}){
  return {status,ready:false,review_required,write_allowed:false,execute:false,production_write_authorized:false,customer_id_generation:false,customer_id_from_client:false,raw_profile_output:false};
}
function validateProfile(profile){
  if(!profile||typeof profile!=='object'||Array.isArray(profile)) return {ok:false,status:'invalid_profile'};
  const limits={name:120,phone:40,address:500,email:254},out={};
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
function exactText(evidence,key,expected){
  const value=strictText(evidence[key]);
  return value!==null&&value===expected;
}
function inspectRegistrationEventCount(evidence,{memberId,prospectId,idempotencyKey}){
  const count=parseCount(evidence.existing_registration_event_count);
  if(count===null) return {ok:false,status:'invalid_existing_registration_event_count'};
  if(!exactText(evidence,'existing_registration_event_count_member_identity_id',memberId)) return {ok:false,status:'registration_event_count_member_scope_mismatch'};
  if(!exactText(evidence,'existing_registration_event_count_prospect_id',prospectId)) return {ok:false,status:'registration_event_count_prospect_scope_mismatch'};
  if(!exactText(evidence,'existing_registration_event_count_idempotency_key',idempotencyKey)) return {ok:false,status:'registration_event_count_idempotency_scope_mismatch'};
  return {ok:true,count};
}
function inspectConsentEventCount(evidence,{memberId,termsHash,privacyHash,acceptedAt}){
  const count=parseCount(evidence.existing_consent_event_count);
  if(count===null) return {ok:false,status:'invalid_existing_consent_event_count'};
  if(!exactText(evidence,'existing_consent_event_count_member_identity_id',memberId)) return {ok:false,status:'consent_event_count_member_scope_mismatch'};
  if(!exactText(evidence,'existing_consent_event_count_terms_sha256',termsHash)) return {ok:false,status:'consent_event_count_terms_scope_mismatch'};
  if(!exactText(evidence,'existing_consent_event_count_privacy_sha256',privacyHash)) return {ok:false,status:'consent_event_count_privacy_scope_mismatch'};
  if(!exactText(evidence,'existing_consent_event_count_accepted_at',acceptedAt)) return {ok:false,status:'consent_event_count_time_scope_mismatch'};
  return {ok:true,count};
}

export function planProspectRegistrationConsent(evidence={}){
  if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)) return blocked('invalid_registration_evidence');

  const memberId=strictText(evidence.member_identity_id),prospectId=strictText(evidence.prospect_id);
  if(memberId===null||!MEMBER_ID_RE.test(memberId)) return blocked('invalid_member_identity');
  if(prospectId===null||!PROSPECT_ID_RE.test(prospectId)) return blocked('invalid_prospect_id');
  if(evidence.server_generated_identity_verified!==true) return blocked('server_generated_identity_not_verified');
  if(evidence.member_identity_source!=='server_generated'||evidence.prospect_id_source!=='server_generated') return blocked('invalid_identity_source');
  if(hasOwn(evidence,'canonical_customer_id')&&evidence.canonical_customer_id!==null) return blocked('prospect_customer_scope_forbidden',{review_required:true});
  if(hasOwn(evidence,'family_id')&&evidence.family_id!==null) return blocked('prospect_family_scope_forbidden',{review_required:true});

  const identityPlan=buildProspectRegistrationPlan({member_identity_id:memberId,prospect_id:prospectId});
  if(identityPlan.status!=='ready'||identityPlan.customer_id!==null||identityPlan.customer_id_generation!==false) return blocked('identity_plan_not_ready',{review_required:true});

  const profileCheck=validateProfile(evidence.profile);
  if(!profileCheck.ok) return blocked(profileCheck.status);
  const profileDigest=sha256(JSON.stringify([profileCheck.profile.name,profileCheck.profile.phone,profileCheck.profile.address,profileCheck.profile.email]));

  const termsVersion=strictText(evidence.terms_version),privacyVersion=strictText(evidence.privacy_version);
  const termsHash=strictText(evidence.terms_sha256),privacyHash=strictText(evidence.privacy_sha256),acceptedAt=strictText(evidence.accepted_at);
  if(termsVersion===null||privacyVersion===null||!VERSION_RE.test(termsVersion)||!VERSION_RE.test(privacyVersion)) return blocked('consent_invalid_document_identity');
  if(termsHash===null||privacyHash===null||!SHA256_HEX_RE.test(termsHash)||!SHA256_HEX_RE.test(privacyHash)) return blocked('consent_invalid_document_identity');
  if(acceptedAt===null) return blocked('consent_invalid_accepted_at');
  if(evidence.server_accepted_at_verified!==true||evidence.accepted_at_source!=='server') return blocked('consent_server_timestamp_not_verified');
  if(evidence.current_consent_documents_verified!==true) return blocked('consent_current_documents_not_verified');

  const latestTermsVersion=strictText(evidence.latest_terms_version),latestPrivacyVersion=strictText(evidence.latest_privacy_version);
  const latestTermsHash=strictText(evidence.latest_terms_sha256),latestPrivacyHash=strictText(evidence.latest_privacy_sha256);
  if(latestTermsVersion===null||latestPrivacyVersion===null||!VERSION_RE.test(latestTermsVersion)||!VERSION_RE.test(latestPrivacyVersion)) return blocked('consent_invalid_latest_document_identity');
  if(latestTermsHash===null||latestPrivacyHash===null||!SHA256_HEX_RE.test(latestTermsHash)||!SHA256_HEX_RE.test(latestPrivacyHash)) return blocked('consent_invalid_latest_document_identity');
  if(termsVersion!==latestTermsVersion||privacyVersion!==latestPrivacyVersion||termsHash!==latestTermsHash||privacyHash!==latestPrivacyHash) return blocked('consent_document_not_current');

  const consentPlan=planConsentRecord({member_identity_id:memberId,terms_version:termsVersion,terms_sha256:termsHash,privacy_version:privacyVersion,privacy_sha256:privacyHash,terms_accepted:evidence.terms_accepted,privacy_accepted:evidence.privacy_accepted,accepted_at:acceptedAt});
  if(consentPlan.status!=='ready'||consentPlan.append_only!==true||consentPlan.replace_prior_consent!==false) return blocked(`consent_${consentPlan.status}`);

  const idempotencyKey=strictText(evidence.registration_idempotency_key);
  if(idempotencyKey===null||!IDEMPOTENCY_KEY_RE.test(idempotencyKey)) return blocked('invalid_registration_idempotency_key');
  const registrationEventId=`REG_${sha256(JSON.stringify([idempotencyKey,memberId,prospectId,profileDigest,consentPlan.terms_version,consentPlan.terms_sha256,consentPlan.privacy_version,consentPlan.privacy_sha256,consentPlan.accepted_at]))}`;
  const consentEventId=`CONS_${sha256(JSON.stringify([memberId,prospectId,registrationEventId,consentPlan.terms_version,consentPlan.terms_sha256,consentPlan.privacy_version,consentPlan.privacy_sha256,consentPlan.accepted_at]))}`;

  const memberCollision=inspectScopedCount(evidence,{count_key:'existing_member_identity_count',scope_key:'existing_member_identity_count_member_identity_id',expected_scope:memberId});
  if(!memberCollision.ok) return blocked(memberCollision.status,{review_required:true});
  const prospectCollision=inspectScopedCount(evidence,{count_key:'existing_prospect_id_count',scope_key:'existing_prospect_id_count_prospect_id',expected_scope:prospectId});
  if(!prospectCollision.ok) return blocked(prospectCollision.status,{review_required:true});
  const registrationCount=inspectRegistrationEventCount(evidence,{memberId,prospectId,idempotencyKey});
  if(!registrationCount.ok) return blocked(registrationCount.status,{review_required:true});
  const consentCount=inspectConsentEventCount(evidence,{memberId,termsHash:consentPlan.terms_sha256,privacyHash:consentPlan.privacy_sha256,acceptedAt:consentPlan.accepted_at});
  if(!consentCount.ok) return blocked(consentCount.status,{review_required:true});

  const counts=[memberCollision.count,prospectCollision.count,registrationCount.count,consentCount.count];
  if(counts.every(count=>count===0)){
    return {
      status:'ready',ready:true,review_required:false,member_identity_id:memberId,prospect_id:prospectId,canonical_customer_id:null,family_id:null,
      profile_digest_sha256:profileDigest,registration_event_id:registrationEventId,consent_event_id:consentEventId,
      consent:{member_identity_id:memberId,terms_version:consentPlan.terms_version,terms_sha256:consentPlan.terms_sha256,privacy_version:consentPlan.privacy_version,privacy_sha256:consentPlan.privacy_sha256,accepted_at:consentPlan.accepted_at,accepted_at_source:'server',current_documents_verified:true,append_only:true,replace_prior_consent:false},
      transaction_required:true,transaction_operations:['create_member_identity','create_prospect','append_registration_event','append_consent_event'],partial_commit_allowed:false,
      registration_completed:false,completion_requires_atomic_execution:true,registration_completion_requires_profile_persistence:true,
      profile_persistence_stage:'deferred_to_google_bounded_cache_gate',profile_pii_long_term_storage_authorized:false,
      server_generated_identity_verified:true,customer_id_generation:false,customer_id_from_client:false,family_id_generation:false,fuzzy_identity_linking:false,
      raw_profile_output:false,write_allowed:false,execute:false,production_write_authorized:false,execution_requires_separate_gate:true
    };
  }

  if(counts.every(count=>count===1)){
    const registrationReplayVerified=evidence.existing_registration_event_verified===true&&exactText(evidence,'persisted_registration_event_id',registrationEventId)&&exactText(evidence,'persisted_registration_member_identity_id',memberId)&&exactText(evidence,'persisted_registration_prospect_id',prospectId)&&exactText(evidence,'persisted_registration_profile_digest_sha256',profileDigest);
    const consentReplayVerified=evidence.existing_consent_event_verified===true&&exactText(evidence,'persisted_consent_event_id',consentEventId)&&exactText(evidence,'persisted_consent_member_identity_id',memberId)&&exactText(evidence,'persisted_consent_registration_event_id',registrationEventId)&&exactText(evidence,'persisted_consent_terms_sha256',consentPlan.terms_sha256)&&exactText(evidence,'persisted_consent_privacy_sha256',consentPlan.privacy_sha256)&&exactText(evidence,'persisted_consent_accepted_at',consentPlan.accepted_at);
    if(!registrationReplayVerified||!consentReplayVerified) return blocked('registration_replay_evidence_mismatch',{review_required:true});
    return {...blocked('registration_already_recorded'),member_identity_id:memberId,prospect_id:prospectId,registration_event_id:registrationEventId,consent_event_id:consentEventId,idempotent_replay:true};
  }

  return blocked('registration_state_conflict',{review_required:true});
}

export const __test={MEMBER_ID_RE,PROSPECT_ID_RE,IDEMPOTENCY_KEY_RE,VERSION_RE,SHA256_HEX_RE,EMAIL_RE,parseCount};
