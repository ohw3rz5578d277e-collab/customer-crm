import {planConsentRecord} from './member-consent-foundation.mjs';

const MEMBER_ID_RE=/^MID_[A-Za-z0-9_-]{22,}$/;
const PROSPECT_ID_RE=/^PID_[A-Za-z0-9_-]{22,}$/;
const CONSENT_EVENT_ID_RE=/^CE_[A-Za-z0-9_-]{22,}$/;
const VERSION_RE=/^[A-Za-z0-9._-]{1,64}$/;
const SHA256_HEX_RE=/^[0-9a-f]{64}$/;
const ACQUISITION_SOURCE_RE=/^[A-Za-z0-9._:/-]{1,128}$/;
const UTC_INSTANT_RE=/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const hasOwn=(obj,key)=>Object.prototype.hasOwnProperty.call(obj,key);
const strictText=value=>typeof value==='string'?value:null;

function blocked(status,{review_required=false}={}){
  return {
    status,
    review_required,
    ready:false,
    write_allowed:false,
    execute:false,
    production_write_authorized:false
  };
}

function parseCount(value){
  if(typeof value==='number'){
    return Number.isSafeInteger(value)&&value>=0?value:null;
  }
  if(typeof value==='string'&&/^(?:0|[1-9]\d*)$/.test(value)){
    const parsed=Number(value);
    return Number.isSafeInteger(parsed)?parsed:null;
  }
  return null;
}

function parseUtcInstant(value){
  if(typeof value!=='string'||!UTC_INSTANT_RE.test(value)) return null;
  const epoch=Date.parse(value);
  if(!Number.isFinite(epoch)) return null;
  return new Date(epoch).toISOString()===value?epoch:null;
}

function exactZeroCount(evidence,{count_key,scope_key,expected_scope,invalid_status,scope_status,collision_status}){
  const count=parseCount(evidence[count_key]);
  if(count===null) return blocked(invalid_status,{review_required:true});
  const scope=strictText(evidence[scope_key]);
  if(scope===null||scope!==expected_scope){
    return blocked(scope_status,{review_required:true});
  }
  if(count!==0) return blocked(collision_status,{review_required:true});
  return null;
}

export function planProspectRegistrationWithConsent(evidence={}){
  if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)){
    return blocked('invalid_registration_evidence');
  }

  const memberId=strictText(evidence.member_identity_id);
  const prospectId=strictText(evidence.prospect_id);
  const consentEventId=strictText(evidence.consent_event_id);
  const acquisitionSource=strictText(evidence.acquisition_source);

  if(memberId===null||!MEMBER_ID_RE.test(memberId)) return blocked('invalid_member_identity');
  if(prospectId===null||!PROSPECT_ID_RE.test(prospectId)) return blocked('invalid_prospect_id');
  if(consentEventId===null||!CONSENT_EVENT_ID_RE.test(consentEventId)) return blocked('invalid_consent_event_id');
  if(acquisitionSource===null||!ACQUISITION_SOURCE_RE.test(acquisitionSource)) return blocked('invalid_acquisition_source');

  if(evidence.ids_server_generated_verified!==true){
    return blocked('server_generated_identity_not_verified');
  }
  if(evidence.registration_request_server_verified!==true){
    return blocked('registration_request_not_verified');
  }

  for(const key of ['canonical_customer_id','family_id','promoted_customer_id']){
    if(hasOwn(evidence,key)&&evidence[key]!==null){
      return blocked('prospect_scope_violation',{review_required:true});
    }
  }

  const memberCollision=exactZeroCount(evidence,{
    count_key:'member_identity_collision_count',
    scope_key:'member_identity_count_member_identity_id',
    expected_scope:memberId,
    invalid_status:'invalid_member_identity_collision_count',
    scope_status:'member_identity_count_scope_mismatch',
    collision_status:'member_identity_collision'
  });
  if(memberCollision) return memberCollision;

  const prospectCollision=exactZeroCount(evidence,{
    count_key:'prospect_collision_count',
    scope_key:'prospect_count_prospect_id',
    expected_scope:prospectId,
    invalid_status:'invalid_prospect_collision_count',
    scope_status:'prospect_count_scope_mismatch',
    collision_status:'prospect_collision'
  });
  if(prospectCollision) return prospectCollision;

  const consentCollision=exactZeroCount(evidence,{
    count_key:'consent_event_collision_count',
    scope_key:'consent_event_count_consent_event_id',
    expected_scope:consentEventId,
    invalid_status:'invalid_consent_event_collision_count',
    scope_status:'consent_event_count_scope_mismatch',
    collision_status:'consent_event_collision'
  });
  if(consentCollision) return consentCollision;

  const termsVersion=strictText(evidence.terms_version);
  const termsSha=strictText(evidence.terms_sha256);
  const privacyVersion=strictText(evidence.privacy_version);
  const privacySha=strictText(evidence.privacy_sha256);
  const acceptedAt=strictText(evidence.accepted_at);

  if(termsVersion===null||privacyVersion===null||!VERSION_RE.test(termsVersion)||!VERSION_RE.test(privacyVersion)){
    return blocked('invalid_consent_document_version');
  }
  if(termsSha===null||privacySha===null||!SHA256_HEX_RE.test(termsSha)||!SHA256_HEX_RE.test(privacySha)){
    return blocked('invalid_consent_document_digest');
  }
  if(evidence.terms_document_verified!==true||evidence.privacy_document_verified!==true){
    return blocked('consent_document_not_verified');
  }
  if(evidence.terms_accepted!==true||evidence.privacy_accepted!==true){
    return blocked('consent_incomplete');
  }
  if(evidence.accepted_at_server_recorded!==true){
    return blocked('accepted_at_not_server_recorded');
  }
  if(parseUtcInstant(acceptedAt)===null){
    return blocked('invalid_accepted_at');
  }

  const consentPlan=planConsentRecord({
    member_identity_id:memberId,
    terms_version:termsVersion,
    terms_sha256:termsSha,
    privacy_version:privacyVersion,
    privacy_sha256:privacySha,
    terms_accepted:true,
    privacy_accepted:true,
    accepted_at:acceptedAt
  });
  if(consentPlan.status!=='ready'||consentPlan.append_only!==true||consentPlan.replace_prior_consent!==false){
    return blocked('consent_foundation_rejected',{review_required:true});
  }

  const memberStatement=[
    'INSERT INTO member_identities (member_identity_id, canonical_customer_id, prospect_id, status)',
    "VALUES (?, NULL, ?, 'active')"
  ].join('\n');
  const prospectStatement=[
    'INSERT INTO member_prospects (prospect_id, member_identity_id, acquisition_source, status, promoted_customer_id)',
    "VALUES (?, ?, ?, 'prospect', NULL)"
  ].join('\n');
  const consentStatement=[
    'INSERT INTO member_consent_evidence',
    '  (consent_event_id, member_identity_id, prospect_id, terms_version, terms_sha256, privacy_version, privacy_sha256, accepted_at, evidence_source)',
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'prospect_registration')"
  ].join('\n');

  return {
    status:'ready',
    review_required:false,
    ready:true,
    member_identity_id:memberId,
    prospect_id:prospectId,
    consent_event_id:consentEventId,
    acquisition_source:acquisitionSource,
    canonical_customer_id:null,
    family_id:null,
    promoted_customer_id:null,
    customer_id_generation:false,
    family_id_generation:false,
    fuzzy_identity_linking:false,
    consent:{
      terms_version:termsVersion,
      terms_sha256:termsSha,
      privacy_version:privacyVersion,
      privacy_sha256:privacySha,
      accepted_at:acceptedAt,
      append_only:true,
      replace_prior_consent:false,
      update_allowed:false,
      delete_allowed:false
    },
    transaction:{
      mode:'all_or_nothing',
      statements:[
        {name:'insert_member_identity',sql:memberStatement,binds:[memberId,prospectId],success_requires_affected_rows:1},
        {name:'insert_prospect',sql:prospectStatement,binds:[prospectId,memberId,acquisitionSource],success_requires_affected_rows:1},
        {name:'append_consent_evidence',sql:consentStatement,binds:[consentEventId,memberId,prospectId,termsVersion,termsSha,privacyVersion,privacySha,acceptedAt],success_requires_affected_rows:1}
      ],
      rollback_on_any_failure:true,
      retry_without_revalidation:false
    },
    write_allowed:false,
    execute:false,
    production_write_authorized:false,
    execution_requires_separate_gate:true,
    crm_customer_mutation:false,
    customer_master_mutation:false,
    line_send:false,
    google_network_send:false
  };
}

export const __test={
  MEMBER_ID_RE,
  PROSPECT_ID_RE,
  CONSENT_EVENT_ID_RE,
  VERSION_RE,
  SHA256_HEX_RE,
  ACQUISITION_SOURCE_RE,
  UTC_INSTANT_RE,
  parseCount,
  parseUtcInstant
};
