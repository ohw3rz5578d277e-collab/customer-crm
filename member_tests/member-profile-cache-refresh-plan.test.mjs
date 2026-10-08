import assert from 'node:assert/strict';
import {planMemberProfileCacheRefresh,__test} from '../src/member-profile-cache-refresh-plan.mjs';
const day=86400000,now=Date.UTC(2026,9,8);
const base={now_ms:now,pii_written_at_ms:now-2*day,google_synced_at_ms:now-day,identity_verified:true,version_verified:true,google_available:true,google_version_matches:true};

assert.equal(__test.CACHE_TTL_MS,7*day);
assert.equal(__test.HARD_RETENTION_MS,30*day);

const ready=planMemberProfileCacheRefresh(base);
assert.equal(ready.status,'refresh_ready');
assert.equal(ready.refresh_allowed,false);
assert.equal(ready.requires_separate_write_gate,true);
assert.equal(ready.cache_expires_at_ms,now+7*day);
assert.equal(ready.hard_purge_deadline_ms,now+28*day);
assert.equal(ready.pii_written_at_preserved,true);
assert.equal(ready.retention_deadline_reset,false);
assert.equal(planMemberProfileCacheRefresh({...base,google_available:false}).status,'google_unavailable');
assert.equal(planMemberProfileCacheRefresh({...base,google_version_matches:false}).status,'verification_required');
assert.equal(planMemberProfileCacheRefresh({...base,google_synced_at_ms:now-3*day}).status,'stale_google_sync');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day}).status,'pii_retention_expired');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,google_available:false}).status,'pii_retention_expired');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,google_available:false}).purge_required,true);
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,identity_verified:false}).status,'pii_retention_expired');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,google_synced_at_ms:undefined}).status,'pii_retention_expired');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,google_synced_at_ms:undefined}).purge_required,true);
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day,google_synced_at_ms:'corrupt'}).purge_required,true);
assert.equal(planMemberProfileCacheRefresh({...base,google_synced_at_ms:undefined}).status,'invalid_time');
const oldButVerified=planMemberProfileCacheRefresh({...base,google_synced_at_ms:now-7*day,pii_written_at_ms:now-8*day});
assert.equal(oldButVerified.status,'refresh_ready');
assert.equal(oldButVerified.cache_expires_at_ms,now+7*day);
const nearRetentionDeadline=planMemberProfileCacheRefresh({...base,google_synced_at_ms:now-day,pii_written_at_ms:now-29*day});
assert.equal(nearRetentionDeadline.status,'refresh_ready');
assert.equal(nearRetentionDeadline.cache_expires_at_ms,now+day);
assert.equal(nearRetentionDeadline.hard_purge_deadline_ms,now+day);
assert.equal(nearRetentionDeadline.retention_deadline_reset,false);
assert.equal(planMemberProfileCacheRefresh({...base,google_synced_at_ms:now+1}).status,'invalid_time');

// Exact hard-retention boundary always wins over a refresh opportunity.
const oneMsBefore=planMemberProfileCacheRefresh({...base,now_ms:now-1,pii_written_at_ms:now-30*day,google_synced_at_ms:now-day});
assert.equal(oneMsBefore.status,'refresh_ready');
assert.equal(oneMsBefore.cache_expires_at_ms,now);
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:now-30*day}).purge_required,true);

// Time evidence must be strict safe-integer numbers; numeric strings are rejected.
assert.equal(planMemberProfileCacheRefresh({...base,now_ms:String(now)}).status,'invalid_time');
assert.equal(planMemberProfileCacheRefresh({...base,pii_written_at_ms:String(now-2*day)}).status,'invalid_time');
assert.equal(planMemberProfileCacheRefresh({...base,google_synced_at_ms:String(now-day)}).status,'invalid_time');
assert.equal(planMemberProfileCacheRefresh({now_ms:Number.MAX_SAFE_INTEGER,pii_written_at_ms:Number.MAX_SAFE_INTEGER,google_synced_at_ms:Number.MAX_SAFE_INTEGER,identity_verified:true,version_verified:true,google_available:true,google_version_matches:true}).status,'invalid_time');

console.log('MEMBER_PROFILE_CACHE_REFRESH_PLAN=PASS');
console.log('PROFILE_CACHE_TTL_DAYS=7');
console.log('D1_PII_HARD_RETENTION_DAYS=30');
console.log('REFRESH_RESETS_PII_WRITE_CLOCK=NO');
console.log('PRODUCTION_GOOGLE_REQUEST=0');
console.log('PRODUCTION_D1_WRITE=0');
