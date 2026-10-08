const STATES=Object.freeze({
 PENDING_SYNC:'PENDING_SYNC',
 SYNCED:'SYNCED',
 VERIFIED:'VERIFIED',
 PURGE_ELIGIBLE:'PURGE_ELIGIBLE',
 PII_PURGED:'PII_PURGED',
 DEADLINE_RECOVERY:'DEADLINE_RECOVERY'
});

const dayMs=24*60*60*1000;

export function planPiiRetention({
 now_ms,
 pii_written_at_ms,
 google_synced=false,
 identity_verified=false,
 version_verified=false,
 pii_present=true
}={}){
 const now=Number(now_ms);
 const written=Number(pii_written_at_ms);
 if(!Number.isFinite(now)||!Number.isFinite(written)||now<written){
   return {status:'invalid_time',write_allowed:false};
 }
 const ageDays=Math.floor((now-written)/dayMs);
 if(!pii_present) return {status:STATES.PII_PURGED,age_days:ageDays,pii_access_allowed:false,purge_required:false,write_allowed:false};

 const googleVerified=google_synced===true;
 const identityVerified=identity_verified===true;
 const versionVerified=version_verified===true;
 const fullyVerified=googleVerified&&identityVerified&&versionVerified;
 if(ageDays>=30){
   return {
     status:fullyVerified?STATES.PURGE_ELIGIBLE:STATES.DEADLINE_RECOVERY,
     age_days:ageDays,
     pii_access_allowed:false,
     purge_required:true,
     retain_retry_audit_only:!fullyVerified,
     write_allowed:false
   };
 }
 if(!googleVerified) return {status:STATES.PENDING_SYNC,age_days:ageDays,pii_access_allowed:true,purge_required:false,write_allowed:false};
 if(!identityVerified||!versionVerified) return {status:STATES.SYNCED,age_days:ageDays,pii_access_allowed:true,purge_required:false,write_allowed:false};
 return {status:STATES.PURGE_ELIGIBLE,verified_state:STATES.VERIFIED,age_days:ageDays,pii_access_allowed:true,purge_required:true,write_allowed:false};
}

export const __test={STATES,dayMs};
