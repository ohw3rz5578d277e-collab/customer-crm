const DAY_MS=86400000;
const CACHE_TTL_MS=7*DAY_MS;
const HARD_RETENTION_MS=30*DAY_MS;

function validTimestamp(value){
 return typeof value==='number'&&Number.isSafeInteger(value)&&value>=0;
}

function safeDeadline(start,delta){
 if(!validTimestamp(start)||!Number.isSafeInteger(delta)||delta<0) return null;
 const deadline=start+delta;
 return Number.isSafeInteger(deadline)?deadline:null;
}

// Pure cache decision; never reads storage, mutates timestamps, or returns profile PII.
export function evaluateMemberProfileCache({now_ms,cached_at_ms,pii_written_at_ms,identity_verified=false,version_verified=false,google_available=false}={}){
 if(!validTimestamp(now_ms)) return {status:'invalid_time',read_allowed:false};
 if(!validTimestamp(pii_written_at_ms)||pii_written_at_ms>now_ms) return {status:'invalid_time',read_allowed:false};
 const hardPurgeDeadline=safeDeadline(pii_written_at_ms,HARD_RETENTION_MS);
 if(hardPurgeDeadline===null) return {status:'invalid_time',read_allowed:false};
 // Absolute retention deadline is based only on write-time evidence and must
 // outrank missing/corrupt cache evidence, Google outages, and softer gates.
 if(now_ms>=hardPurgeDeadline) return {
   status:'pii_retention_expired',
   read_allowed:false,
   purge_required:true,
   hard_purge_deadline_ms:hardPurgeDeadline,
   retention_deadline_reset:false
 };
 if(!validTimestamp(cached_at_ms)||cached_at_ms>now_ms) return {status:'invalid_time',read_allowed:false};
 if(cached_at_ms<pii_written_at_ms) return {status:'stale_cache_verification',read_allowed:false};
 if(identity_verified!==true||version_verified!==true) return {status:'verification_required',read_allowed:false};
 const ttlDeadline=safeDeadline(cached_at_ms,CACHE_TTL_MS);
 if(ttlDeadline===null) return {status:'invalid_time',read_allowed:false};
 const cacheExpiresAt=Math.min(ttlDeadline,hardPurgeDeadline);
 if(now_ms>=cacheExpiresAt) return {
   status:'cache_expired',
   read_allowed:false,
   refresh_required:true,
   cache_expires_at_ms:cacheExpiresAt,
   hard_purge_deadline_ms:hardPurgeDeadline,
   retention_deadline_reset:false
 };
 // A verified, unexpired cache is permitted during a temporary Google outage.
 return {
   status:'ready',
   read_allowed:true,
   source:'verified_cache',
   google_refresh_available:google_available===true,
   cache_expires_at_ms:cacheExpiresAt,
   hard_purge_deadline_ms:hardPurgeDeadline,
   retention_deadline_reset:false
 };
}

export const __test={DAY_MS,CACHE_TTL_MS,HARD_RETENTION_MS,validTimestamp,safeDeadline};
