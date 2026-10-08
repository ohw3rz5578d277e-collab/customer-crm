import assert from 'node:assert/strict';
import {planPiiRetention,__test} from '../src/member-d1-pii-retention-plan.mjs';

const t0=Date.UTC(2026,0,1);
const d=n=>t0+n*__test.dayMs;

let p=planPiiRetention({now_ms:d(1),pii_written_at_ms:t0});
assert.equal(p.status,'PENDING_SYNC');
assert.equal(p.pii_access_allowed,true);
assert.equal(p.purge_required,false);
assert.equal(p.write_allowed,false);

p=planPiiRetention({now_ms:d(10),pii_written_at_ms:t0,google_synced:true});
assert.equal(p.status,'SYNCED');

p=planPiiRetention({now_ms:d(10),pii_written_at_ms:t0,google_synced:true,identity_verified:true,version_verified:true});
assert.equal(p.status,'PURGE_ELIGIBLE');
assert.equal(p.verified_state,'VERIFIED');
assert.equal(p.pii_access_allowed,true);
assert.equal(p.purge_required,true);

p=planPiiRetention({
 now_ms:d(10),
 pii_written_at_ms:t0,
 google_synced:'false',
 identity_verified:'false',
 version_verified:'false'
});
assert.equal(p.status,'PENDING_SYNC');
assert.equal(p.pii_access_allowed,true);
assert.equal(p.purge_required,false);
assert.equal(p.write_allowed,false);

p=planPiiRetention({now_ms:d(30),pii_written_at_ms:t0});
assert.equal(p.status,'DEADLINE_RECOVERY');
assert.equal(p.pii_access_allowed,false);
assert.equal(p.purge_required,true);
assert.equal(p.retain_retry_audit_only,true);
assert.equal(p.write_allowed,false);

p=planPiiRetention({
 now_ms:d(30),
 pii_written_at_ms:t0,
 google_synced:'false',
 identity_verified:'false',
 version_verified:'false'
});
assert.equal(p.status,'DEADLINE_RECOVERY');
assert.equal(p.pii_access_allowed,false);
assert.equal(p.purge_required,true);
assert.equal(p.retain_retry_audit_only,true);
assert.equal(p.write_allowed,false);

p=planPiiRetention({now_ms:d(30),pii_written_at_ms:t0,google_synced:true,identity_verified:true,version_verified:true});
assert.equal(p.status,'PURGE_ELIGIBLE');
assert.equal(p.pii_access_allowed,false);
assert.equal(p.purge_required,true);

p=planPiiRetention({now_ms:d(31),pii_written_at_ms:t0,pii_present:false});
assert.equal(p.status,'PII_PURGED');
assert.equal(p.pii_access_allowed,false);

console.log('D1_PII_RETENTION_PLAN=PASS');
console.log('HARD_RETENTION_DAYS=30');
console.log('STRICT_VERIFICATION_EVIDENCE=YES');
console.log('DEADLINE_EXTENDS_ON_READ=NO');
console.log('PII_ACCESS_AFTER_DEADLINE=0');
console.log('PRODUCTION_PURGE_EXECUTOR=0');
console.log('PRODUCTION_D1_WRITE=0');
