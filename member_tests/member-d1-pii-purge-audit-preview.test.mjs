import assert from 'node:assert/strict';
import {buildMemberPiiPurgeAuditPreview} from '../src/member-d1-pii-purge-audit-preview.mjs';
const now=Date.UTC(2026,9,8),day=86400000;
const row={record_id:'record_1',pii_written_at_ms:now-31*day,pii_present:true,google_synced:false,identity_verified:false,version_verified:false,digest_verified:false};
const result=buildMemberPiiPurgeAuditPreview({rows:[row],now_ms:now});
assert.equal(result.status,'preview_ready');
assert.equal(result.execution_allowed,false);
assert.equal(result.operation_count,1);
assert.equal(result.receipts[0].result,'NOT_EXECUTED');
assert.equal(result.receipts[0].deadline_recovery,true);
assert.equal(result.receipts[0].retention_deadline_ms,now-day);
assert.equal(result.receipts[0].pii_in_receipt,false);

// Corrupt verification evidence cannot suppress an already-expired hard deadline.
const corrupt=buildMemberPiiPurgeAuditPreview({rows:[{...row,google_synced:'true',identity_verified:null,digest_verified:'yes'}],now_ms:now});
assert.equal(corrupt.status,'preview_ready');
assert.equal(corrupt.operation_count,1);
assert.equal(corrupt.receipts[0].deadline_recovery,true);

// Before the deadline, malformed verification evidence remains fail-closed.
const fresh={...row,record_id:'record_2',pii_written_at_ms:now-day,google_synced:'true'};
assert.equal(buildMemberPiiPurgeAuditPreview({rows:[fresh],now_ms:now}).status,'invalid_record_evidence');

console.log('D1_PII_PURGE_AUDIT_PREVIEW=PASS');
console.log('HARD_DEADLINE_SURVIVES_CORRUPT_METADATA=YES');
console.log('PII_IN_RECEIPT=0');
console.log('EXECUTION_ALLOWED=0');
