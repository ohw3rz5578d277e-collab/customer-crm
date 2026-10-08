import crypto from 'node:crypto';

const ALLOWED_REASONS=new Set(['identity_mismatch','version_conflict','promotion_collision','binding_mismatch']);
const MEMBER_ID_RE=/^MID_[A-Za-z0-9_-]{22,}$/;
const CUSTOMER_ID_RE=/^\d{8}$/;
const PROSPECT_ID_RE=/^PID_[A-Za-z0-9_-]{22,}$/;
const IDEMPOTENCY_KEY_RE=/^[A-Za-z0-9._:-]{16,128}$/;
const REVIEW_ID_RE=/^RV_[0-9a-f]{64}$/;
const DECISION_ID_RE=/^RVD_[0-9a-f]{64}$/;
const DIGEST_RE=/^[0-9a-f]{64}$/;
const strictText=value=>typeof value==='string'&&value.trim()===value?value:null;

function blocked(status,{review_required=false}={}){
 return {
  status,
  ready:false,
  review_required,
  queue_write_allowed:false,
  master_write_allowed:false,
  google_send_allowed:false,
  execute:false,
  production_write_authorized:false
 };
}

function isCanonicalJsonValue(value,seen=new Set()){
 if(value===null||typeof value==='string'||typeof value==='boolean') return true;
 if(typeof value==='number') return Number.isFinite(value);
 if(Array.isArray(value)){
  if(seen.has(value)) return false;
  seen.add(value);
  const ok=value.every(item=>isCanonicalJsonValue(item,seen));
  seen.delete(value);
  return ok;
 }
 if(typeof value==='object'){
  const proto=Object.getPrototypeOf(value);
  if(proto!==Object.prototype&&proto!==null) return false;
  if(seen.has(value)) return false;
  seen.add(value);
  const ok=Object.keys(value).every(key=>isCanonicalJsonValue(value[key],seen));
  seen.delete(value);
  return ok;
 }
 return false;
}

function canonicalize(value){
 if(Array.isArray(value)) return value.map(canonicalize);
 if(value&&typeof value==='object') return Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonicalize(value[key])]));
 return value;
}

export function reviewPayloadDigest(payload){
 if(!isCanonicalJsonValue(payload)) throw new Error('invalid_review_payload');
 const canonical=JSON.stringify(canonicalize(payload));
 return crypto.createHash('sha256').update(canonical).digest('hex');
}

export function createReviewId({queue_idempotency_key,member_identity_id,reason_code,subject_type,subject_id,payload_digest_sha256}={}){
 const key=strictText(queue_idempotency_key),member=strictText(member_identity_id),reason=strictText(reason_code),type=strictText(subject_type),subject=strictText(subject_id),digest=strictText(payload_digest_sha256);
 if(key===null||!IDEMPOTENCY_KEY_RE.test(key)||member===null||!MEMBER_ID_RE.test(member)||!ALLOWED_REASONS.has(reason)||!['customer','prospect'].includes(type)||subject===null||digest===null||!DIGEST_RE.test(digest)) throw new Error('invalid_review_identity_evidence');
 return `RV_${crypto.createHash('sha256').update(JSON.stringify([key,member,reason,type,subject,digest])).digest('hex')}`;
}

function parseCount(value){
 if(typeof value==='number') return Number.isSafeInteger(value)&&value>=0?value:null;
 if(typeof value==='string'&&/^(?:0|[1-9]\d*)$/.test(value)){
  const parsed=Number(value);
  return Number.isSafeInteger(parsed)?parsed:null;
 }
 return null;
}

function subjectFromEvidence({claimed_customer_id,prospect_id}){
 const customer=strictText(claimed_customer_id),prospect=strictText(prospect_id);
 const hasCustomer=customer!==null&&customer!=='';
 const hasProspect=prospect!==null&&prospect!=='';
 if(hasCustomer===hasProspect) return {ok:false,status:'invalid_review_subject'};
 if(hasCustomer){
  if(!CUSTOMER_ID_RE.test(customer)) return {ok:false,status:'invalid_customer_id'};
  return {ok:true,subject_type:'customer',subject_id:customer,claimed_customer_id:customer,prospect_id:null};
 }
 if(!PROSPECT_ID_RE.test(prospect)) return {ok:false,status:'invalid_prospect_id'};
 return {ok:true,subject_type:'prospect',subject_id:prospect,claimed_customer_id:null,prospect_id:prospect};
}

function exactText(evidence,key,expected){
 const value=strictText(evidence[key]);
 return value!==null&&value===expected;
}

export function planProfileReview(evidence={}){
 if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)) return blocked('invalid_review_evidence');
 const reason=strictText(evidence.reason_code);
 if(reason===null||!ALLOWED_REASONS.has(reason)) return blocked('invalid_reason');
 const member=strictText(evidence.member_identity_id);
 if(member===null||!MEMBER_ID_RE.test(member)) return blocked('invalid_member_identity');
 if(evidence.member_identity_authenticated!==true||!exactText(evidence,'authenticated_member_identity_id',member)) return blocked('member_identity_not_authenticated',{review_required:true});

 const subject=subjectFromEvidence(evidence);
 if(!subject.ok) return blocked(subject.status,{review_required:subject.status==='invalid_review_subject'});
 if(!evidence.submitted_profile||typeof evidence.submitted_profile!=='object'||Array.isArray(evidence.submitted_profile)||!isCanonicalJsonValue(evidence.submitted_profile)) return blocked('invalid_submitted_profile');
 const payloadDigest=reviewPayloadDigest(evidence.submitted_profile);

 const key=strictText(evidence.queue_idempotency_key);
 if(key===null||!IDEMPOTENCY_KEY_RE.test(key)) return blocked('invalid_queue_idempotency_key');
 const reviewId=createReviewId({queue_idempotency_key:key,member_identity_id:member,reason_code:reason,subject_type:subject.subject_type,subject_id:subject.subject_id,payload_digest_sha256:payloadDigest});

 const count=parseCount(evidence.existing_review_count);
 if(count===null) return blocked('invalid_existing_review_count',{review_required:true});
 const countScopeVerified=exactText(evidence,'existing_review_count_member_identity_id',member)&&
  exactText(evidence,'existing_review_count_idempotency_key',key)&&
  exactText(evidence,'existing_review_count_reason_code',reason)&&
  exactText(evidence,'existing_review_count_subject_type',subject.subject_type)&&
  exactText(evidence,'existing_review_count_subject_id',subject.subject_id)&&
  exactText(evidence,'existing_review_count_payload_digest_sha256',payloadDigest);
 if(!countScopeVerified) return blocked('review_count_scope_mismatch',{review_required:true});

 if(count===0){
  return {
   status:'review_required',ready:true,review_required:true,review_id:reviewId,reason_code:reason,member_identity_id:member,
   subject_type:subject.subject_type,subject_id:subject.subject_id,claimed_customer_id:subject.claimed_customer_id,prospect_id:subject.prospect_id,
   payload_digest_sha256:payloadDigest,submitted_profile_ephemeral:true,raw_profile_durable_storage_authorized:false,
   customer_message:'変更内容を受け付けました',admin_decision_required:true,queue_persistence_requires_separate_gate:true,
   master_update_blocked_until_review_decision:true,master_write_allowed:false,queue_write_allowed:false,google_send_allowed:false,
   execute:false,production_write_authorized:false
  };
 }

 if(count===1){
  const persistedVerified=evidence.persisted_review_verified===true&&
   exactText(evidence,'persisted_review_id',reviewId)&&
   exactText(evidence,'persisted_review_member_identity_id',member)&&
   exactText(evidence,'persisted_review_reason_code',reason)&&
   exactText(evidence,'persisted_review_subject_type',subject.subject_type)&&
   exactText(evidence,'persisted_review_subject_id',subject.subject_id)&&
   exactText(evidence,'persisted_review_payload_digest_sha256',payloadDigest)&&
   exactText(evidence,'persisted_review_status','pending');
  if(!persistedVerified) return blocked('review_replay_evidence_mismatch',{review_required:true});
  return {
   ...blocked('review_already_pending',{review_required:true}),review_id:reviewId,member_identity_id:member,subject_type:subject.subject_type,
   subject_id:subject.subject_id,payload_digest_sha256:payloadDigest,idempotent_replay:true,admin_decision_required:true
  };
 }
 return blocked('review_state_conflict',{review_required:true});
}

export function planReviewDecision(evidence={}){
 if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)) return blocked('invalid_decision_evidence');
 const reviewId=strictText(evidence.review_id),member=strictText(evidence.member_identity_id),reason=strictText(evidence.reason_code),type=strictText(evidence.subject_type),subject=strictText(evidence.subject_id),digest=strictText(evidence.payload_digest_sha256);
 if(reviewId===null||!REVIEW_ID_RE.test(reviewId)||member===null||!MEMBER_ID_RE.test(member)||reason===null||!ALLOWED_REASONS.has(reason)||!['customer','prospect'].includes(type)||subject===null||digest===null||!DIGEST_RE.test(digest)) return blocked('invalid_review_identity');
 if(type==='customer'&&!CUSTOMER_ID_RE.test(subject)) return blocked('invalid_customer_id');
 if(type==='prospect'&&!PROSPECT_ID_RE.test(subject)) return blocked('invalid_prospect_id');
 const persistedReviewBound=evidence.persisted_review_verified===true&&
  exactText(evidence,'persisted_review_id',reviewId)&&
  exactText(evidence,'persisted_review_member_identity_id',member)&&
  exactText(evidence,'persisted_review_reason_code',reason)&&
  exactText(evidence,'persisted_review_subject_type',type)&&
  exactText(evidence,'persisted_review_subject_id',subject)&&
  exactText(evidence,'persisted_review_payload_digest_sha256',digest);
 if(!persistedReviewBound) return blocked('persisted_review_binding_mismatch',{review_required:true});

 if(evidence.admin_actor_verified!==true) return blocked('admin_actor_not_verified');
 const adminActor=strictText(evidence.admin_actor_id);
 if(adminActor===null||adminActor.length<3||adminActor.length>128) return blocked('invalid_admin_actor');
 const decision=strictText(evidence.decision);
 if(!['approve','reject'].includes(decision)) return blocked('invalid_decision');
 const key=strictText(evidence.decision_idempotency_key);
 if(key===null||!IDEMPOTENCY_KEY_RE.test(key)) return blocked('invalid_decision_idempotency_key');
 const decisionEventId=`RVD_${crypto.createHash('sha256').update(JSON.stringify([key,reviewId,decision,adminActor,digest])).digest('hex')}`;

 const decisionCount=parseCount(evidence.existing_decision_count);
 if(decisionCount===null) return blocked('invalid_existing_decision_count',{review_required:true});
 const decisionScopeVerified=exactText(evidence,'existing_decision_count_review_id',reviewId)&&
  exactText(evidence,'existing_decision_count_idempotency_key',key)&&
  exactText(evidence,'existing_decision_count_decision',decision)&&
  exactText(evidence,'existing_decision_count_admin_actor_id',adminActor);
 if(!decisionScopeVerified) return blocked('decision_count_scope_mismatch',{review_required:true});

 if(decisionCount===1){
  const finalStatus=decision==='approve'?'approved':'rejected';
  const persistedDecisionVerified=evidence.persisted_decision_verified===true&&
   exactText(evidence,'persisted_decision_event_id',decisionEventId)&&
   exactText(evidence,'persisted_decision_review_id',reviewId)&&
   exactText(evidence,'persisted_decision_decision',decision)&&
   exactText(evidence,'persisted_decision_admin_actor_id',adminActor)&&
   exactText(evidence,'persisted_decision_payload_digest_sha256',digest)&&
   exactText(evidence,'persisted_review_status',finalStatus);
  if(!persistedDecisionVerified) return blocked('decision_replay_evidence_mismatch',{review_required:true});
  return {...blocked('decision_already_recorded'),review_id:reviewId,decision_event_id:decisionEventId,decision,idempotent_replay:true};
 }
 if(decisionCount>1) return blocked('decision_state_conflict',{review_required:true});
 if(!exactText(evidence,'persisted_review_status','pending')) return blocked('not_pending',{review_required:true});

 if(decision==='reject'){
  return {
   status:'reject_ready',ready:true,review_id:reviewId,decision_event_id:decisionEventId,decision:'reject',audit_required:true,
   master_write_allowed:false,queue_write_allowed:false,google_send_allowed:false,execute:false,production_write_authorized:false,
   execution_requires_separate_gate:true
  };
 }

 if(evidence.identity_reverified!==true||!exactText(evidence,'identity_reverified_member_identity_id',member)) return blocked('reverification_required',{review_required:true});
 if(evidence.latest_version_verified!==true||!exactText(evidence,'latest_version_verified_subject_type',type)||!exactText(evidence,'latest_version_verified_subject_id',subject)) return blocked('latest_version_reverification_required',{review_required:true});
 const version=parseCount(evidence.verified_current_profile_version);
 if(version===null) return blocked('invalid_verified_profile_version',{review_required:true});
 return {
  status:'approve_ready',ready:true,review_id:reviewId,decision_event_id:decisionEventId,decision:'approve',member_identity_id:member,subject_type:type,subject_id:subject,
  payload_digest_sha256:digest,verified_current_profile_version:version,audit_required:true,new_sync_event_required:true,step5_google_sync_required:true,
  master_write_allowed:false,queue_write_allowed:false,google_send_allowed:false,execute:false,production_write_authorized:false,
  execution_requires_separate_gate:true
 };
}

export const __test={MEMBER_ID_RE,CUSTOMER_ID_RE,PROSPECT_ID_RE,IDEMPOTENCY_KEY_RE,REVIEW_ID_RE,DECISION_ID_RE,DIGEST_RE,parseCount,isCanonicalJsonValue};
