import crypto from 'node:crypto';

const CUSTOMER_ID_RE=/^\d{8}$/;
const PROSPECT_ID_RE=/^PID_[A-Za-z0-9_-]{22,}$/;
const EVENT_ID_RE=/^SE_[A-Za-z0-9_-]{22,}$/;
const DIGEST_RE=/^[0-9a-f]{64}$/i;
const VERSION_RE=/^(0|[1-9]\d*)$/;
const text=v=>v==null?'':String(v).trim();

export function createSyncEventId(){
  return 'SE_'+crypto.randomBytes(24).toString('base64url');
}

function canonicalize(value){
 if(Array.isArray(value)) return value.map(canonicalize);
 if(value && typeof value==='object'){
   return Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonicalize(value[key])]));
 }
 return value;
}

function parseVersionEvidence(value,{min}){
 let n;
 if(typeof value==='number') n=value;
 else if(typeof value==='string'&&VERSION_RE.test(value.trim())) n=Number(value.trim());
 else return null;
 return Number.isInteger(n)&&n>=min?n:null;
}

export function profilePayloadDigest(profile){
  const canonical=JSON.stringify(canonicalize(profile||{}));
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

export function planProfileSync({
 subject_type,
 customer_id='',
 prospect_id='',
 member_identity_verified=false,
 profile_version,
 previous_profile_version,
 sync_event_id,
 profile={}
}={}){
 const type=text(subject_type);
 const eventId=text(sync_event_id);
 const version=Number(profile_version);
 const previous=Number(previous_profile_version);

 if(member_identity_verified!==true) return {status:'identity_not_verified',review_required:true,send_allowed:false};
 if(!EVENT_ID_RE.test(eventId)) return {status:'invalid_sync_event_id',send_allowed:false};
 if(!Number.isInteger(version)||version<1||!Number.isInteger(previous)||previous<0||version!==previous+1){
   return {status:'invalid_profile_version',review_required:true,send_allowed:false};
 }

 let subjectId='';
 if(type==='customer'){
   subjectId=text(customer_id);
   if(!CUSTOMER_ID_RE.test(subjectId)) return {status:'invalid_customer_id',send_allowed:false};
 } else if(type==='prospect'){
   subjectId=text(prospect_id);
   if(!PROSPECT_ID_RE.test(subjectId)) return {status:'invalid_prospect_id',send_allowed:false};
 } else {
   return {status:'invalid_subject_type',send_allowed:false};
 }

 return {
   status:'ready',
   subject_type:type,
   subject_id:subjectId,
   sync_event_id:eventId,
   profile_version:version,
   payload_digest_sha256:profilePayloadDigest(profile),
   idempotency_required:true,
   history_append_required:true,
   browser_direct_google_access:false,
   send_allowed:false
 };
}

export function planSyncReplay({incoming_event_id,incoming_version,incoming_digest,last_event_id,last_version,last_digest}={}){
 const ie=text(incoming_event_id), le=text(last_event_id);
 const iv=parseVersionEvidence(incoming_version,{min:1});
 const lv=parseVersionEvidence(last_version,{min:0});
 const id=text(incoming_digest), ld=text(last_digest);
 if(iv===null||lv===null){
   return {status:'invalid_version_evidence',review_required:true,master_write:false,history_append:false};
 }
 const initialBoundary=lv===0&&!le&&!ld;
 if(!DIGEST_RE.test(id)||(!initialBoundary&&!DIGEST_RE.test(ld))){
   return {status:'invalid_replay_evidence',review_required:true,master_write:false,history_append:false};
 }
 if(ie&&ie===le){
   if(iv===lv&&id===ld) return {status:'idempotent_replay',master_write:false,history_append:false};
   return {status:'event_replay_conflict',review_required:true,master_write:false,history_append:false};
 }
 if(iv<lv) return {status:'stale_version',master_write:false,history_append:false};
 if(iv===lv&&id!==ld) return {status:'version_digest_conflict',review_required:true,master_write:false,history_append:false};
 if(iv!==lv+1) return {status:'version_gap',review_required:true,master_write:false,history_append:false};
 return {status:'accept_next_version',master_write:false,history_append:false,execution_requires_separate_gate:true};
}
