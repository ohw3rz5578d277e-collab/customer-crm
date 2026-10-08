import assert from 'node:assert/strict';
import {evaluateMemberProfileCache,__test} from '../src/member-profile-cache-read-gate.mjs';
const day=86400000,now=Date.UTC(2026,9,8);
const base={now_ms:now,cached_at_ms:now-day,pii_written_at_ms:now-2*day,identity_verified:true,version_verified:true,google_available:true};

assert.equal(__test.CACHE_TTL_MS,7*day);
assert.equal(__test.HARD_RETENTION_MS,30*day);

const ready=evaluateMemberProfileCache(base);
assert.equal(ready.read_allowed,true);
assert.equal(ready.status,'ready');
assert.equal(ready.cache_expires_at_ms,now+6*day);
assert.equal(ready.hard_purge_deadline_ms,now+28*day);
assert.equal(ready.retention_deadline_reset,false);
assert.equal(evaluateMemberProfileCache({...base,google_available:false}).status,'ready');
assert.equal(evaluateMemberProfileCache({...base,identity_verified:false}).status,'verification_required');

// Exact TTL boundary is expired; one millisecond before it remains readable.
const ttlBase={...base,cached_at_ms:now-7*day,pii_written_at_ms:now-8*day};
assert.equal(evaluateMemberProfileCache(ttlBase).status,'cache_expired');
assert.equal(evaluateMemberProfileCache({...ttlBase,now_ms:now-1}).status,'ready');

assert.equal(evaluateMemberProfileCache({...base,pii_written_at_ms:now-30*day}).purge_required,true);
assert.equal(evaluateMemberProfileCache({...base,cached_at_ms:now+1}).status,'invalid_time');
assert.equal(evaluateMemberProfileCache({...base,now_ms:NaN}).status,'invalid_time');

// Hard retention deadline must still request purge during outages, verification failures,
// and even when cache freshness evidence is absent or malformed.
assert.equal(evaluateMemberProfileCache({...base,pii_written_at_ms:now-30*day,google_available:false}).purge_required,true);
assert.equal(evaluateMemberProfileCache({...base,pii_written_at_ms:now-30*day,identity_verified:false}).purge_required,true);
assert.equal(evaluateMemberProfileCache({...base,pii_written_at_ms:now-30*day,cached_at_ms:undefined}).status,'pii_retention_expired');
assert.equal(evaluateMemberProfileCache({...base,pii_written_at_ms:now-30*day,cached_at_ms:undefined}).purge_required,true);
assert.equal(evaluateMemberProfileCache({...base,pii_written_at_ms:now-30*day,cached_at_ms:'corrupt'}).purge_required,true);
// Before the hard deadline, malformed cache time still fails closed as invalid_time.
assert.equal(evaluateMemberProfileCache({...base,cached_at_ms:undefined}).status,'invalid_time');
// A cache verification older than the PII write cannot prove the current profile version.
assert.equal(evaluateMemberProfileCache({...base,cached_at_ms:now-3*day,pii_written_at_ms:now-2*day}).status,'stale_cache_verification');
assert.equal(evaluateMemberProfileCache({...base,cached_at_ms:now-3*day,pii_written_at_ms:now-2*day,google_available:false}).read_allowed,false);
// Verified, fresh cached data remains readable during a temporary Google outage.
assert.equal(evaluateMemberProfileCache({...base,google_available:false}).read_allowed,true);
assert.equal(evaluateMemberProfileCache({...base,google_available:false}).google_refresh_available,false);
assert.equal(evaluateMemberProfileCache({...base,google_available:false,cached_at_ms:now-7*day,pii_written_at_ms:now-8*day}).read_allowed,false);

// A 7-day cache window may never extend beyond the original 30-day PII deadline.
const nearHardDeadline=evaluateMemberProfileCache({...base,cached_at_ms:now-day,pii_written_at_ms:now-29*day});
assert.equal(nearHardDeadline.status,'ready');
assert.equal(nearHardDeadline.cache_expires_at_ms,now+day);
assert.equal(nearHardDeadline.hard_purge_deadline_ms,now+day);

// Time evidence is strict: strings and unsafe additions do not coerce.
assert.equal(evaluateMemberProfileCache({...base,now_ms:String(now)}).status,'invalid_time');
assert.equal(evaluateMemberProfileCache({...base,cached_at_ms:String(now-day)}).status,'invalid_time');
assert.equal(evaluateMemberProfileCache({...base,pii_written_at_ms:String(now-2*day)}).status,'invalid_time');
assert.equal(evaluateMemberProfileCache({now_ms:Number.MAX_SAFE_INTEGER,cached_at_ms:Number.MAX_SAFE_INTEGER,pii_written_at_ms:Number.MAX_SAFE_INTEGER,identity_verified:true,version_verified:true}).status,'invalid_time');

console.log('MEMBER_PROFILE_CACHE_READ_GATE=PASS');
console.log('PROFILE_CACHE_TTL_DAYS=7');
console.log('D1_PII_HARD_RETENTION_DAYS=30');
console.log('READ_EXTENDS_RETENTION=NO');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
