import crypto from 'node:crypto';

const MEMBER_ID_RE=/^MID_[A-Za-z0-9_-]{22,}$/;
const CUSTOMER_ID_RE=/^\d{8}$/;
const PROSPECT_ID_RE=/^PID_[A-Za-z0-9_-]{22,}$/;
const DECISION_EVENT_RE=/^RVD_[0-9a-f]{64}$/;
const SYNC_EVENT_RE=/^SE_[0-9a-f]{64}$/;
const DIGEST_RE=/^[0-9a-f]{64}$/;
const DESTINATION_CONTRACT='member_google_master_v1';
const MAX_ATTEMPTS=4;
const RETRY_DELAYS_MS=[60_000,300_000,1_800_000];
const RETRYABLE_RESULTS=new Set(['timeout','network_error','google_429','google_5xx']);
const strictText=value=>typeof value==='string'&&value.trim()===value?value:null;

function blocked(status,{review_required=false}={}){
 return {
  status,
  ready:false,
  review_required,
  send_allowed:false,
  execute:false,
  google_write_allowed:false,
  production_write_authorized:false
 };
}

function canonicalize(value){
 if(value===null||typeof value==='string'||typeof value==='boolean') return value;
 if(typeof value==='number'){
  if(!Number.isFinite(value)) throw new Error('invalid_profile_payload');
  return value;
 }
 if(Array.isArray(value)) return value.map(canonicalize);
 if(value&&typeof value==='object'){
  const out={};
  for(const key of Object.keys(value).sort()){
   const item=value[key];
   if(item===undefined||typeof item==='function'||typeof item==='symbol'||typeof item==='bigint') throw new Error('invalid_profile_payload');
   out[key]=canonicalize(item);
  }
  return out;
 }
 throw new Error('invalid_profile_payload');
}

export function profilePayloadDigest(profile){
 const canonical=JSON.stringify(canonicalize(profile));
 return crypto.createHash('sha256').update(canonical).digest('hex');
}

function parseVersion(value,{min=0}={}){
 return typeof value==='number'&&Number.isSafeInteger(value)&&value>=min?value:null;
}

function parseCount(value,{max=Number.MAX_SAFE_INTEGER}={}){
 return typeof value==='number'&&Number.isSafeInteger(value)&&value>=0&&value<=max?value:null;
}

function exactText(evidence,key,expected){
 const value=strictText(evidence[key]);
 return value!==null&&value===expected;
}

function validateSubject(typeValue,idValue){
 const type=strictText(typeValue),id=strictText(idValue);
 if(type==='customer'&&id!==null&&CUSTOMER_ID_RE.test(id)) return {ok:true,type,id};
 if(type==='prospect'&&id!==null&&PROSPECT_ID_RE.test(id)) return {ok:true,type,id};
 return {ok:false};
}

export function deriveSyncEventId({decision_event_id,member_identity_id,subject_type,subject_id,profile_version,payload_digest_sha256}={}){
 const decision=strictText(decision_event_id),member=strictText(member_identity_id),digest=strictText(payload_digest_sha256);
 const subject=validateSubject(subject_type,subject_id);
 const version=parseVersion(profile_version,{min:1});
 if(decision===null||!DECISION_EVENT_RE.test(decision)||member===null||!MEMBER_ID_RE.test(member)||!subject.ok||version===null||digest===null||!DIGEST_RE.test(digest)){
  throw new Error('invalid_sync_event_identity');
 }
 return `SE_${crypto.createHash('sha256').update(JSON.stringify([decision,member,subject.type,subject.id,version,digest])).digest('hex')}`;
}

export function planGoogleSyncDispatch(evidence={}){
 if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)) return blocked('invalid_dispatch_evidence');
 if(evidence.server_execution_context_verified!==true) return blocked('server_context_not_verified');
 if(strictText(evidence.destination_contract)!==DESTINATION_CONTRACT) return blocked('invalid_destination_contract');
 if(strictText(evidence.review_decision_status)!=='approve_ready') return blocked('review_not_approved',{review_required:true});

 const decision=strictText(evidence.decision_event_id),member=strictText(evidence.member_identity_id),digest=strictText(evidence.payload_digest_sha256);
 const subject=validateSubject(evidence.subject_type,evidence.subject_id);
 const currentVersion=parseVersion(evidence.verified_current_profile_version,{min:0});
 const nextVersion=parseVersion(evidence.next_profile_version,{min:1});
 if(decision===null||!DECISION_EVENT_RE.test(decision)||member===null||!MEMBER_ID_RE.test(member)||digest===null||!DIGEST_RE.test(digest)||!subject.ok||currentVersion===null||nextVersion===null){
  return blocked('invalid_dispatch_identity',{review_required:true});
 }
 if(nextVersion!==currentVersion+1) return blocked('profile_version_not_next',{review_required:true});
 if(evidence.review_decision_verified!==true||!exactText(evidence,'persisted_decision_event_id',decision)||!exactText(evidence,'persisted_decision_member_identity_id',member)||!exactText(evidence,'persisted_decision_subject_type',subject.type)||!exactText(evidence,'persisted_decision_subject_id',subject.id)||!exactText(evidence,'persisted_decision_payload_digest_sha256',digest)||evidence.persisted_decision_profile_version!==currentVersion){
  return blocked('review_decision_binding_mismatch',{review_required:true});
 }
 let profileDigest;
 try{ profileDigest=profilePayloadDigest(evidence.profile); }catch{ return blocked('invalid_profile_payload',{review_required:true}); }
 if(profileDigest!==digest) return blocked('profile_digest_mismatch',{review_required:true});

 const syncEventId=deriveSyncEventId({decision_event_id:decision,member_identity_id:member,subject_type:subject.type,subject_id:subject.id,profile_version:nextVersion,payload_digest_sha256:digest});
 const count=parseCount(evidence.existing_sync_event_count);
 if(count===null) return blocked('invalid_sync_event_count',{review_required:true});
 if(!exactText(evidence,'existing_sync_event_count_member_identity_id',member)||!exactText(evidence,'existing_sync_event_count_subject_type',subject.type)||!exactText(evidence,'existing_sync_event_count_subject_id',subject.id)||evidence.existing_sync_event_count_profile_version!==nextVersion||!exactText(evidence,'existing_sync_event_count_payload_digest_sha256',digest)){
  return blocked('sync_event_count_scope_mismatch',{review_required:true});
 }
 if(count===0){
  return {
   status:'dispatch_ready',ready:true,sync_event_id:syncEventId,decision_event_id:decision,member_identity_id:member,
   subject_type:subject.type,subject_id:subject.id,profile_version:nextVersion,payload_digest_sha256:digest,
   destination_contract:DESTINATION_CONTRACT,max_attempts:MAX_ATTEMPTS,retry_delays_ms:[...RETRY_DELAYS_MS],
   idempotency_required:true,history_append_required:true,reconciliation_required:true,browser_direct_google_access:false,
   send_allowed:false,execute:false,google_write_allowed:false,production_write_authorized:false,execution_requires_separate_gate:true
  };
 }
 if(count===1){
  const persisted=evidence.persisted_sync_event_verified===true&&
   exactText(evidence,'persisted_sync_event_id',syncEventId)&&
   exactText(evidence,'persisted_sync_member_identity_id',member)&&
   exactText(evidence,'persisted_sync_subject_type',subject.type)&&
   exactText(evidence,'persisted_sync_subject_id',subject.id)&&
   evidence.persisted_sync_profile_version===nextVersion&&
   exactText(evidence,'persisted_sync_payload_digest_sha256',digest);
  if(!persisted) return blocked('sync_event_replay_evidence_mismatch',{review_required:true});
  return {...blocked('sync_event_already_planned'),sync_event_id:syncEventId,idempotent_replay:true,reconciliation_required:true};
 }
 return blocked('sync_event_state_conflict',{review_required:true});
}

export function planGoogleSyncAttemptResult(evidence={}){
 if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)) return blocked('invalid_attempt_evidence');
 const event=strictText(evidence.sync_event_id),result=strictText(evidence.result);
 const attempt=parseCount(evidence.attempt_number,{max:MAX_ATTEMPTS});
 if(event===null||!SYNC_EVENT_RE.test(event)||attempt===null||attempt<1) return blocked('invalid_attempt_identity',{review_required:true});
 if(evidence.persisted_sync_event_verified!==true||!exactText(evidence,'persisted_sync_event_id',event)||evidence.persisted_attempt_number!==attempt){
  return blocked('attempt_binding_mismatch',{review_required:true});
 }
 if(result==='success'){
  if(evidence.remote_commit_verified!==true) return blocked('remote_commit_not_verified',{review_required:true});
  return {
   status:'sync_confirmed',ready:true,sync_event_id:event,attempt_number:attempt,reconciliation_required:true,
   send_allowed:false,execute:false,google_write_allowed:false,production_write_authorized:false
  };
 }
 if(RETRYABLE_RESULTS.has(result)){
  if(attempt>=MAX_ATTEMPTS) return blocked('retry_budget_exhausted',{review_required:true});
  return {
   status:'retry_scheduled',ready:true,sync_event_id:event,attempt_number:attempt,next_attempt_number:attempt+1,
   retry_delay_ms:RETRY_DELAYS_MS[attempt-1],reconciliation_required:true,
   send_allowed:false,execute:false,google_write_allowed:false,production_write_authorized:false,execution_requires_separate_gate:true
  };
 }
 if(['invalid_request','auth_failure','binding_conflict','version_conflict','digest_conflict'].includes(result)) return blocked('permanent_sync_failure',{review_required:true});
 return blocked('invalid_attempt_result',{review_required:true});
}

export function planGoogleSyncReconciliation(evidence={}){
 if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)) return blocked('invalid_reconciliation_evidence');
 const event=strictText(evidence.sync_event_id),digest=strictText(evidence.expected_payload_digest_sha256);
 const subject=validateSubject(evidence.expected_subject_type,evidence.expected_subject_id);
 const expectedVersion=parseVersion(evidence.expected_profile_version,{min:1});
 const attempts=parseCount(evidence.completed_attempt_count,{max:MAX_ATTEMPTS});
 if(event===null||!SYNC_EVENT_RE.test(event)||digest===null||!DIGEST_RE.test(digest)||!subject.ok||expectedVersion===null||attempts===null){
  return blocked('invalid_reconciliation_identity',{review_required:true});
 }
 if(evidence.persisted_sync_event_verified!==true||!exactText(evidence,'persisted_sync_event_id',event)||!exactText(evidence,'persisted_sync_subject_type',subject.type)||!exactText(evidence,'persisted_sync_subject_id',subject.id)||evidence.persisted_sync_profile_version!==expectedVersion||!exactText(evidence,'persisted_sync_payload_digest_sha256',digest)){
  return blocked('reconciliation_binding_mismatch',{review_required:true});
 }
 if(evidence.remote_observation_available!==true){
  if(attempts>=MAX_ATTEMPTS) return blocked('reconciliation_retry_budget_exhausted',{review_required:true});
  return {...blocked('reconciliation_retry_required'),sync_event_id:event,next_attempt_number:attempts+1,execution_requires_separate_gate:true};
 }
 const remoteSubject=validateSubject(evidence.remote_subject_type,evidence.remote_subject_id);
 const remoteVersion=parseVersion(evidence.remote_profile_version,{min:0});
 const remoteDigest=strictText(evidence.remote_payload_digest_sha256);
 if(!remoteSubject.ok||remoteVersion===null||remoteDigest===null||!DIGEST_RE.test(remoteDigest)) return blocked('invalid_remote_evidence',{review_required:true});
 if(remoteSubject.type!==subject.type||remoteSubject.id!==subject.id) return blocked('remote_subject_mismatch',{review_required:true});
 if(remoteVersion===expectedVersion&&remoteDigest===digest){
  return {status:'in_sync',ready:true,sync_event_id:event,reconciliation_complete:true,send_allowed:false,execute:false,google_write_allowed:false,production_write_authorized:false};
 }
 if(remoteVersion>expectedVersion) return blocked('remote_version_ahead',{review_required:true});
 if(remoteVersion===expectedVersion&&remoteDigest!==digest) return blocked('remote_digest_conflict',{review_required:true});
 if(attempts>=MAX_ATTEMPTS) return blocked('reconciliation_retry_budget_exhausted',{review_required:true});
 return {...blocked('reconciliation_retry_required'),sync_event_id:event,next_attempt_number:attempts+1,execution_requires_separate_gate:true};
}

export const __test={MEMBER_ID_RE,CUSTOMER_ID_RE,PROSPECT_ID_RE,DECISION_EVENT_RE,SYNC_EVENT_RE,DIGEST_RE,DESTINATION_CONTRACT,MAX_ATTEMPTS,RETRY_DELAYS_MS,RETRYABLE_RESULTS,parseVersion,parseCount};
