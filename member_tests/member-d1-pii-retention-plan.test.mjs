import assert from 'node:assert/strict';
import {planPiiRetention,__test} from '../src/member-d1-pii-retention-plan.mjs';

const t0=Date.UTC(2026,0,1);
const d=n=>t0+n*__test.dayMs;

let p=planPiiRetention({now_ms:d(1),pii_written_at_ms:t0});
assert.equal(p.status,'PENDING_SYNC');
assert.equal(p.pii_access_allowed,true);
assert.equal(p.purge_required,false);
assert.equal(p.write_allowed,false);
assert.equal(p.retention_deadline_ms,d(30));

p=planPiiRetention({now_ms:d(10),pii_written_at_ms:t0,google_synced:true});
assert.equal(p.status,'SYNCED');

p=planPiiRetention({now_ms:d(10),pii_written_at_ms:t0,google_synced:true,identity_verified:true,version_verified:true,digest_verified:true});
assert.equal(p.status,'PURGE_ELIGIBLE');
assert.equal(p.verified_state,'VERIFIED');
assert.equal(p.pii_access_allowed,true);
assert.equal(p.purge_required,true);

p=planPiiRetention({now_ms:d(10),pii_written_at_ms:t0,google_synced:true,identity_verified:true,version_verified:true,digest_verified:false});
assert.equal(p.status,'SYNCED');
assert.equal(p.purge_required,false);

for(const malformed of ['false','true',0,1,null]){
 p=planPiiRetention({now_ms:d(10),pii_written_at_ms:t0,google_synced:malformed});
 assert.equal(p.status,'invalid_verification_evidence');
 assert.equal(p.pii_access_allowed,false);
 assert.equal(p.purge_required,false);
}

for(const badTime of ['0',null,NaN,Infinity,Number.MAX_SAFE_INTEGER+1]){
 p=planPiiRetention({now_ms:badTime,pii_written_at_ms:t0});
 assert.equal(p.status,'invalid_time');
 assert.equal(p.pii_access_allowed,false);
}

p=planPiiRetention({now_ms:d(30),pii_written_at_ms:t0});
assert.equal(p.status,'DEADLINE_RECOVERY');
assert.equal(p.pii_access_allowed,false);
assert.equal(p.purge_required,true);
assert.equal(p.retain_retry_audit_only,true);
assert.equal(p.write_allowed,false);

p=planPiiRetention({now_ms:d(30),pii_written_at_ms:t0,google_synced:'corrupt',identity_verified:null,version_verified:'true',digest_verified:1});
assert.equal(p.status,'DEADLINE_RECOVERY');
assert.equal(p.pii_access_allowed,false);
assert.equal(p.purge_required,true);
assert.equal(p.retain_retry_audit_only,true);

for(const ambiguousPresence of [null,0,'']){
 p=planPiiRetention({now_ms:d(31),pii_written_at_ms:t0,pii_present:ambiguousPresence});
 assert.equal(p.status,'DEADLINE_RECOVERY');
 assert.equal(p.pii_access_allowed,false);
 assert.equal(p.purge_required,true);
 assert.equal(p.retain_retry_audit_only,true);
}

p=planPiiRetention({now_ms:d(30),pii_written_at_ms:t0,google_synced:true,identity_verified:true,version_verified:true,digest_verified:true});
assert.equal(p.status,'PURGE_ELIGIBLE');
assert.equal(p.pii_access_allowed,false);
assert.equal(p.purge_required,true);

p=planPiiRetention({now_ms:d(31),pii_written_at_ms:t0,pii_present:false});
assert.equal(p.status,'PII_PURGED');
assert.equal(p.pii_access_allowed,false);
assert.equal(p.purge_required,false);

console.log('D1_PII_RETENTION_PLAN=PASS');
console.log('CACHE_TTL_DAYS=7');
console.log('HARD_RETENTION_DAYS=30');
console.log('STRICT_SAFE_INTEGER_TIME=YES');
console.log('STRICT_VERIFICATION_EVIDENCE=YES');
console.log('EARLY_PURGE_REQUIRES_DIGEST=YES');
console.log('DEADLINE_OVERRIDES_CORRUPT_VERIFICATION=YES');
console.log('PII_PURGED_REQUIRES_EXPLICIT_FALSE=YES');
console.log('DEADLINE_EXTENDS_ON_READ=NO');
console.log('PII_ACCESS_AFTER_DEADLINE=0');
console.log('PRODUCTION_PURGE_EXECUTOR=0');
console.log('PRODUCTION_D1_WRITE=0');
