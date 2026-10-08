const DAY_MS=86400000;

// Pure cache decision; never reads storage or returns profile PII.
export function evaluateMemberProfileCache({now_ms,cached_at_ms,pii_written_at_ms,identity_verified=false,version_verified=false,google_available=false}={}){
 const valid=t=>typeof t==='number'&&Number.isSafeInteger(t)&&t>=0&&t<=now_ms;
 if(!valid(now_ms)||!valid(cached_at_ms)||!valid(pii_written_at_ms)) return {status:'invalid_time',read_allowed:false};
 // Absolute retention deadline takes precedence over outage and verification gates.
 if(now_ms-pii_written_at_ms>=30*DAY_MS) return {status:'pii_retention_expired',read_allowed:false,purge_required:true};
 if(identity_verified!==true||version_verified!==true) return {status:'verification_required',read_allowed:false};
 if(now_ms-cached_at_ms>=7*DAY_MS) return {status:'cache_expired',read_allowed:false};
 // A verified, unexpired cache is permitted during a temporary Google outage.
 return {status:'ready',read_allowed:true,source:'verified_cache',google_refresh_available:google_available===true};
}
