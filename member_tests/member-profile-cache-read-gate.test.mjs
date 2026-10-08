import assert from 'node:assert/strict';
import {evaluateMemberProfileCache} from '../src/member-profile-cache-read-gate.mjs';
const day=86400000,now=Date.UTC(2026,9,8);
const base={now_ms:now,cached_at_ms:now-day,pii_written_at_ms:now-2*day,identity_verified:true,version_verified:true,google_available:true};
assert.equal(evaluateMemberProfileCache(base).read_allowed,true);
assert.equal(evaluateMemberProfileCache({...base,google_available:false}).status,'ready');
assert.equal(evaluateMemberProfileCache({...base,identity_verified:false}).status,'verification_required');
assert.equal(evaluateMemberProfileCache({...base,cached_at_ms:now-7*day,pii_written_at_ms:now-8*day}).status,'cache_expired');
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
