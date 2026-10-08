import {evaluateMemberProfileCache} from './member-profile-cache-read-gate.mjs';

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

// Source-only decision: no Google, D1, or network access.
export function planMemberProfileCacheRefresh({now_ms,pii_written_at_ms,google_synced_at_ms,identity_verified=false,version_verified=false,google_available=false,google_version_matches=false}={}){
 if(!validTimestamp(now_ms)) return {status:'invalid_time',refresh_allowed:false};
 if(!validTimestamp(pii_written_at_ms)||pii_written_at_ms>now_ms) return {status:'invalid_time',refresh_allowed:false};
 const hardPurgeDeadline=safeDeadline(pii_written_at_ms,HARD_RETENTION_MS);
 if(hardPurgeDeadline===null) return {status:'invalid_time',refresh_allowed:false};
 // The absolute 30-day PII retention deadline is based on the original local
 // write time and outranks missing Google sync evidence and all refresh gates.
 if(now_ms>=hardPurgeDeadline) return {
   status:'pii_retention_expired',
   refresh_allowed:false,
   purge_required:true,
   hard_purge_deadline_ms:hardPurgeDeadline,
   retention_deadline_reset:false
 };
 if(!validTimestamp(google_synced_at_ms)||google_synced_at_ms>now_ms) return {status:'invalid_time',refresh_allowed:false};
 if(google_available!==true) return {status:'google_unavailable',refresh_allowed:false};
 if(identity_verified!==true||version_verified!==true||google_version_matches!==true) return {status:'verification_required',refresh_allowed:false};
 if(google_synced_at_ms<pii_written_at_ms) return {status:'stale_google_sync',refresh_allowed:false};
 // A successful verified Google read may refresh cache freshness now, but it
 // never rewrites pii_written_at_ms or moves the absolute 30-day deadline.
 const decision=evaluateMemberProfileCache({now_ms,cached_at_ms:now_ms,pii_written_at_ms,identity_verified,version_verified,google_available});
 if(decision.status!=='ready') return {status:decision.status,refresh_allowed:false,...(decision.purge_required===true?{purge_required:true}:{})};
 const ttlDeadline=safeDeadline(now_ms,CACHE_TTL_MS);
 if(ttlDeadline===null) return {status:'invalid_time',refresh_allowed:false};
 return {
   status:'refresh_ready',
   refresh_allowed:false,
   requires_separate_write_gate:true,
   cache_expires_at_ms:Math.min(ttlDeadline,hardPurgeDeadline),
   hard_purge_deadline_ms:hardPurgeDeadline,
   pii_written_at_preserved:true,
   retention_deadline_reset:false
 };
}

export const __test={DAY_MS,CACHE_TTL_MS,HARD_RETENTION_MS,validTimestamp,safeDeadline};
