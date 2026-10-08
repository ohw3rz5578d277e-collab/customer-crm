const STATES=Object.freeze({
 PENDING_SYNC:'PENDING_SYNC',
 SYNCED:'SYNCED',
 VERIFIED:'VERIFIED',
 PURGE_ELIGIBLE:'PURGE_ELIGIBLE',
 PII_PURGED:'PII_PURGED',
 DEADLINE_RECOVERY:'DEADLINE_RECOVERY'
});

const dayMs=24*60*60*1000;
const cacheTtlDays=7;
const hardRetentionDays=30;
const hardRetentionMs=hardRetentionDays*dayMs;

function validTimestamp(value){
 return typeof value==='number'&&Number.isSafeInteger(value)&&value>=0;
}

function safeDeadline(start,delta){
 if(!validTimestamp(start)||!Number.isSafeInteger(delta)||delta<0) return null;
 const deadline=start+delta;
 return Number.isSafeInteger(deadline)?deadline:null;
}

export function planPiiRetention({
 now_ms,
 pii_written_at_ms,
 google_synced=false,
 identity_verified=false,
 version_verified=false,
 pii_present=true
}={}){
 if(!validTimestamp(now_ms)||!validTimestamp(pii_written_at_ms)||now_ms<pii_written_at_ms){
   return {status:'invalid_time',write_allowed:false};
 }
 const hardPurgeDeadline=safeDeadline(pii_written_at_ms,hardRetentionMs);
 if(hardPurgeDeadline===null) return {status:'invalid_time',write_allowed:false};
 const ageDays=Math.floor((now_ms-pii_written_at_ms)/dayMs);
 if(pii_present===false) return {
   status:STATES.PII_PURGED,
   age_days:ageDays,
   pii_access_allowed:false,
   purge_required:false,
   write_allowed:false,
   hard_purge_deadline_ms:hardPurgeDeadline,
   retention_deadline_reset:false
 };

 const googleVerified=google_synced===true;
 const identityVerified=identity_verified===true;
 const versionVerified=version_verified===true;
 const fullyVerified=googleVerified&&identityVerified&&versionVerified;
 if(now_ms>=hardPurgeDeadline){
   return {
     status:fullyVerified?STATES.PURGE_ELIGIBLE:STATES.DEADLINE_RECOVERY,
     age_days:ageDays,
     pii_access_allowed:false,
     purge_required:true,
     retain_retry_audit_only:!fullyVerified,
     write_allowed:false,
     hard_purge_deadline_ms:hardPurgeDeadline,
     retention_deadline_reset:false
   };
 }
 if(!googleVerified) return {
   status:STATES.PENDING_SYNC,
   age_days:ageDays,
   pii_access_allowed:true,
   purge_required:false,
   write_allowed:false,
   hard_purge_deadline_ms:hardPurgeDeadline,
   retention_deadline_reset:false
 };
 if(!identityVerified||!versionVerified) return {
   status:STATES.SYNCED,
   age_days:ageDays,
   pii_access_allowed:true,
   purge_required:false,
   write_allowed:false,
   hard_purge_deadline_ms:hardPurgeDeadline,
   retention_deadline_reset:false
 };
 return {
   status:STATES.PURGE_ELIGIBLE,
   verified_state:STATES.VERIFIED,
   age_days:ageDays,
   pii_access_allowed:true,
   purge_required:true,
   write_allowed:false,
   hard_purge_deadline_ms:hardPurgeDeadline,
   retention_deadline_reset:false
 };
}

export const __test={STATES,dayMs,cacheTtlDays,hardRetentionDays,hardRetentionMs,validTimestamp,safeDeadline};
