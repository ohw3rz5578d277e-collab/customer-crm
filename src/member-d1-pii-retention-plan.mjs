const STATES=Object.freeze({
 PENDING_SYNC:'PENDING_SYNC',
 SYNCED:'SYNCED',
 VERIFIED:'VERIFIED',
 PURGE_ELIGIBLE:'PURGE_ELIGIBLE',
 PII_PURGED:'PII_PURGED',
 DEADLINE_RECOVERY:'DEADLINE_RECOVERY'
});

const dayMs=24*60*60*1000;
const cacheTtlMs=7*dayMs;
const hardRetentionMs=30*dayMs;
const isSafeTime=value=>typeof value==='number'&&Number.isSafeInteger(value)&&value>=0;
const isBoolean=value=>typeof value==='boolean';

export function planPiiRetention({
 now_ms,
 pii_written_at_ms,
 google_synced=false,
 identity_verified=false,
 version_verified=false,
 digest_verified=false,
 pii_present=true
}={}){
 if(!isSafeTime(now_ms)||!isSafeTime(pii_written_at_ms)||now_ms<pii_written_at_ms||pii_written_at_ms>Number.MAX_SAFE_INTEGER-hardRetentionMs){
   return {status:'invalid_time',pii_access_allowed:false,purge_required:false,write_allowed:false};
 }
 const ageMs=now_ms-pii_written_at_ms;
 const ageDays=Math.floor(ageMs/dayMs);
 const retentionDeadline=pii_written_at_ms+hardRetentionMs;

 // Only explicit false proves PII is already gone. Ambiguous presence evidence is
 // treated as potentially present so the hard deadline can never be bypassed.
 if(pii_present===false){
   return {status:STATES.PII_PURGED,age_days:ageDays,retention_deadline_ms:retentionDeadline,pii_access_allowed:false,purge_required:false,write_allowed:false};
 }

 const googleVerified=google_synced===true;
 const identityVerified=identity_verified===true;
 const versionVerified=version_verified===true;
 const digestVerified=digest_verified===true;
 const fullyVerified=googleVerified&&identityVerified&&versionVerified&&digestVerified;

 // Absolute deadline outranks missing/corrupt verification metadata. Once reached,
 // PII must be inaccessible and purge-required even if sync evidence is unavailable.
 if(ageMs>=hardRetentionMs){
   return {
     status:fullyVerified?STATES.PURGE_ELIGIBLE:STATES.DEADLINE_RECOVERY,
     age_days:ageDays,
     retention_deadline_ms:retentionDeadline,
     pii_access_allowed:false,
     purge_required:true,
     retain_retry_audit_only:!fullyVerified,
     write_allowed:false
   };
 }

 // Before the deadline, malformed verification evidence cannot authorize reads or
 // early purge. Exact booleans are required for all verification dimensions.
 if(!isBoolean(google_synced)||!isBoolean(identity_verified)||!isBoolean(version_verified)||!isBoolean(digest_verified)){
   return {status:'invalid_verification_evidence',age_days:ageDays,retention_deadline_ms:retentionDeadline,pii_access_allowed:false,purge_required:false,write_allowed:false};
 }
 if(!googleVerified) return {status:STATES.PENDING_SYNC,age_days:ageDays,retention_deadline_ms:retentionDeadline,pii_access_allowed:true,purge_required:false,write_allowed:false};
 if(!identityVerified||!versionVerified||!digestVerified) return {status:STATES.SYNCED,age_days:ageDays,retention_deadline_ms:retentionDeadline,pii_access_allowed:true,purge_required:false,write_allowed:false};
 return {status:STATES.PURGE_ELIGIBLE,verified_state:STATES.VERIFIED,age_days:ageDays,retention_deadline_ms:retentionDeadline,pii_access_allowed:true,purge_required:true,write_allowed:false};
}

export const __test={STATES,dayMs,cacheTtlMs,hardRetentionMs};
