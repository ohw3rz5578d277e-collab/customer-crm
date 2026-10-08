import assert from 'node:assert/strict';
import {planMemberPiiPurgeReconciliation} from '../src/member-d1-pii-purge-reconciliation-plan.mjs';
const now=Date.UTC(2026,9,8),day=86400000;
const expired={record_id:'rec_1',pii_written_at_ms:now-31*day,pii_present:true,google_synced:false,identity_verified:false,version_verified:false,digest_verified:false};
const plan=planMemberPiiPurgeReconciliation({rows:[expired],now_ms:now});
assert.equal(plan.status,'reconciliation_planned');
assert.equal(plan.execution_allowed,false);
assert.equal(plan.items.length,1);
assert.equal(plan.items[0].needs_deadline_recovery,true);
assert.equal(plan.items[0].retention_deadline_ms,now-day);
assert.equal(plan.items[0].reconciliation_state,'PENDING_EXECUTION');

// Expired PII remains scheduled even when verification metadata is corrupt.
const corrupt=planMemberPiiPurgeReconciliation({rows:[{...expired,record_id:'rec_2',identity_verified:'true',digest_verified:null}],now_ms:now});
assert.equal(corrupt.status,'reconciliation_planned');
assert.equal(corrupt.items.length,1);
assert.equal(corrupt.items[0].needs_deadline_recovery,true);

// Before day 30 the same malformed evidence cannot authorize early purge.
const fresh={...expired,record_id:'rec_3',pii_written_at_ms:now-day,identity_verified:'true'};
assert.equal(planMemberPiiPurgeReconciliation({rows:[fresh],now_ms:now}).status,'invalid_record_evidence');

assert.equal(planMemberPiiPurgeReconciliation({rows:[expired],now_ms:Number.MAX_SAFE_INTEGER+1}).status,'invalid_time');
assert.equal(planMemberPiiPurgeReconciliation({rows:[{...expired,pii_written_at_ms:-1}],now_ms:now}).status,'invalid_record_time');

console.log('D1_PII_PURGE_RECONCILIATION=PASS');
console.log('HARD_DEADLINE_SURVIVES_CORRUPT_METADATA=YES');
console.log('REQUIRES_SEPARATE_EXECUTION_GATE=YES');
