import {evaluateMemberProfileCache} from './member-profile-cache-read-gate.mjs';

// Source-only decision: no Google, D1, or network access.
export function planMemberProfileCacheRefresh({now_ms,pii_written_at_ms,google_synced_at_ms,identity_verified=false,version_verified=false,google_available=false,google_version_matches=false}={}){
 const validNow=typeof now_ms==='number'&&Number.isSafeInteger(now_ms)&&now_ms>=0;
 if(!validNow) return {status:'invalid_time',refresh_allowed:false};
 const validAt=t=>typeof t==='number'&&Number.isSafeInteger(t)&&t>=0&&t<=now_ms;
 if(!validAt(pii_written_at_ms)) return {status:'invalid_time',refresh_allowed:false};
 // The absolute 30-day PII retention deadline is based on valid write-time evidence
 // and outranks missing Google sync evidence and all refresh/outage gates.
 if(now_ms-pii_written_at_ms>=30*86400000) return {status:'pii_retention_expired',refresh_allowed:false,purge_required:true};
 if(!validAt(google_synced_at_ms)) return {status:'invalid_time',refresh_allowed:false};
 if(google_available!==true) return {status:'google_unavailable',refresh_allowed:false};
 if(identity_verified!==true||version_verified!==true||google_version_matches!==true) return {status:'verification_required',refresh_allowed:false};
 if(google_synced_at_ms<pii_written_at_ms) return {status:'stale_google_sync',refresh_allowed:false};
 // A successful verified Google read refreshes cache freshness now. The prior
 // Google sync timestamp remains evidence that the upstream snapshot is not
 // older than the local PII write, while pii_written_at_ms alone controls the
 // absolute 30-day retention deadline.
 const decision=evaluateMemberProfileCache({now_ms,cached_at_ms:now_ms,pii_written_at_ms,identity_verified,version_verified,google_available});
 if(decision.status!=='ready') return {status:decision.status,refresh_allowed:false,...(decision.purge_required===true?{purge_required:true}:{})};
 return {status:'refresh_ready',refresh_allowed:false,requires_separate_write_gate:true,cache_expires_at_ms:Math.min(now_ms+7*86400000,pii_written_at_ms+30*86400000)};
}
