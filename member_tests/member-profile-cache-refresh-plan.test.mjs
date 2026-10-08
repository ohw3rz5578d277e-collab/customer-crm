import assert from 'node:assert/strict';
import {planMemberProfileCacheRefresh} from '../src/member-profile-cache-refresh-plan.mjs';
const day=86400000,now=Date.UTC(2026,9,8);
const base={now_ms:now,pii_written_at_ms:now-2*day,google_synced_at_ms:now-day,identity_verified:true,version_verified:true,google_available:true,google_version_matches:true,google_digest_matches:true};

const ready=planMemberProfileCacheRefresh(base);
assert.equal(ready.status,'refresh_ready');
assert.equal(ready.refresh_allowed,false);
assert.equal(ready.requires_separate_write_gate,true);
assert.equal(ready.cache_expires_at_ms,now+7*day);
assert.equal(ready.retention_deadline_ms,now+28*day);
assert.equal(ready.google_version_and_digest_verified,true);

assert.equal(planMemberProfileCacheRefresh({...base,google_available:false}).status,'google_unavailable');
assert.equal(planMemberProfileCacheRefresh({...base,google_version_matches:false}).status,'verification_required');
assert.equal(planMemberProfileCacheRefresh({...base,google_digest_matches:false}).status,'verification_required');
assert.equal(planMemberProfileCacheRefresh({...base,google_digest_matches:'true'}).status,'verification_required');
assert.equal(planMemberProfileCacheRefresh({...base,google_synced_at_ms:now-3*day}).status,'stale_google_sync');

// The hard deadline outranks outage, verification and malformed sync timestamp evidence.
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day}).status,'pii_retention_expired');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,google_available:false}).status,'pii_retention_expired');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,google_available:false}).purge_required,true);
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,identity_verified:false}).status,'pii_retention_expired');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,google_digest_matches:false}).status,'pii_retention_expired');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,google_synced_at_ms:undefined}).status,'pii_retention_expired');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,google_synced_at_ms:'corrupt'}).purge_required,true);

assert.equal(planMemberProfileCacheRefresh({...base,google_synced_at_ms:undefined}).status,'invalid_time');
const oldButVerified=planMemberProfileCacheRefresh({...base,google_synced_at_ms:now-7*day,pii_written_at_ms:now-8*day});
assert.equal(oldButVerified.status,'refresh_ready');
assert.equal(oldButVerified.cache_expires_at_ms,now+7*day);
const nearRetentionDeadline=planMemberProfileCacheRefresh({...base,google_synced_at_ms:now-day,pii_written_at_ms:now-29*day});
assert.equal(nearRetentionDeadline.status,'refresh_ready');
assert.equal(nearRetentionDeadline.cache_expires_at_ms,now+day);
assert.equal(nearRetentionDeadline.retention_deadline_ms,now+day);
assert.equal(planMemberProfileCacheRefresh({...base,google_synced_at_ms:now+1}).status,'invalid_time');

console.log('PROFILE_CACHE_REFRESH_PLAN=PASS');
console.log('CACHE_TTL_DAYS=7');
console.log('HARD_RETENTION_DAYS=30');
console.log('VERSION_AND_DIGEST_MATCH_REQUIRED=YES');
console.log('REFRESH_RESETS_HARD_DEADLINE=NO');
console.log('PRODUCTION_D1_WRITE=0');
