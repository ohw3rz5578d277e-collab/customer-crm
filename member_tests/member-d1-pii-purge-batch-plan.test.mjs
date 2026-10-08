import assert from 'node:assert/strict';
import {planMemberPiiPurgeBatch} from '../src/member-d1-pii-purge-batch-plan.mjs';
const day=86400000,now=Date.UTC(2026,9,8);
const row=(id,age,extra={})=>({record_id:id,pii_written_at_ms:now-age*day,pii_present:true,google_synced:false,identity_verified:false,version_verified:false,digest_verified:false,...extra});

let result=planMemberPiiPurgeBatch({now_ms:now,rows:[row('expired',30),row('fresh',1)]});
assert.equal(result.status,'ready');
assert.equal(result.execution_allowed,false);
assert.equal(result.operations.length,1);
assert.equal(result.operations[0].record_id,'expired');
assert.equal(result.operations[0].deadline_recovery,true);
assert.equal(result.operations[0].retention_deadline_ms,now);
assert.equal(result.contains_pii,false);

result=planMemberPiiPurgeBatch({now_ms:now,rows:[row('verified',1,{google_synced:true,identity_verified:true,version_verified:true,digest_verified:true})]});
assert.equal(result.operations.length,1);
assert.equal(result.operations[0].deadline_recovery,false);

result=planMemberPiiPurgeBatch({now_ms:now,rows:[row('digest-missing',1,{google_synced:true,identity_verified:true,version_verified:true,digest_verified:false})]});
assert.equal(result.operations.length,0);

// The 30-day hard deadline cannot be blocked by missing/corrupt verification evidence.
for(const expiredRow of [
 {record_id:'missing-meta',pii_written_at_ms:now-30*day,pii_present:true},
 row('corrupt-meta',31,{google_synced:'true',identity_verified:null,version_verified:'yes',digest_verified:1}),
 row('unknown-presence',31,{pii_present:'unknown'})
]){
 result=planMemberPiiPurgeBatch({now_ms:now,rows:[expiredRow]});
 assert.equal(result.status,'ready');
 assert.equal(result.operations.length,1);
 assert.equal(result.operations[0].deadline_recovery,true);
 assert.equal(result.operations[0].retain_retry_audit_only,true);
}

// Explicit false proves PII is already absent, even after the deadline.
result=planMemberPiiPurgeBatch({now_ms:now,rows:[row('already-purged',31,{pii_present:false})]});
assert.equal(result.status,'ready');
assert.equal(result.operations.length,0);

// Before day 30, malformed verification evidence must not authorize early purge.
assert.equal(planMemberPiiPurgeBatch({now_ms:now,rows:[row('wrong',1,{google_synced:'true'})]}).status,'invalid_record_evidence');
assert.equal(planMemberPiiPurgeBatch({now_ms:now,rows:[row('missing-digest',1,{digest_verified:undefined})]}).status,'invalid_record_evidence');

assert.equal(planMemberPiiPurgeBatch({now_ms:now,rows:[row('same',30),row('same',30)]}).status,'invalid_record');
assert.equal(planMemberPiiPurgeBatch({now_ms:now,rows:[row('a',30),row('b',30)],max_batch_size:1}).status,'batch_limit_exceeded');
assert.equal(planMemberPiiPurgeBatch({now_ms:now,rows:[row('bad',-1)]}).status,'invalid_record_time');
assert.equal(planMemberPiiPurgeBatch({now_ms:now,rows:[row('badtime',31,{pii_written_at_ms:'0'})]}).status,'invalid_record_time');
assert.equal(planMemberPiiPurgeBatch({now_ms:-1,rows:[]}).status,'invalid_time');
assert.equal(planMemberPiiPurgeBatch({now_ms:Number.MAX_SAFE_INTEGER+1,rows:[]}).status,'invalid_time');
assert.equal(planMemberPiiPurgeBatch({now_ms:now,rows:[row('negative',31,{pii_written_at_ms:-1})]}).status,'invalid_record_time');
assert.equal(planMemberPiiPurgeBatch({now_ms:now,rows:[row('overflow',31,{pii_written_at_ms:Number.MAX_SAFE_INTEGER+1})]}).status,'invalid_record_time');

console.log('D1_PII_PURGE_BATCH_PLAN=PASS');
console.log('EARLY_PURGE_REQUIRES_DIGEST=YES');
console.log('HARD_DEADLINE_METADATA_INDEPENDENT=YES');
console.log('EXPLICIT_PII_ABSENT_SKIPS_PURGE=YES');
console.log('EXECUTION_ALLOWED=0');
