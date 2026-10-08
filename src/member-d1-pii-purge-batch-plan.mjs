import {planPiiRetention,__test as retentionTest} from './member-d1-pii-retention-plan.mjs';

const isBoolean=value=>typeof value==='boolean';

// Pure, fail-closed executor planner. The caller must separately authorize any D1 mutation.
export function planMemberPiiPurgeBatch({rows,now_ms,max_batch_size=100}={}){
 if(!Array.isArray(rows)||!Number.isSafeInteger(max_batch_size)||max_batch_size<1||max_batch_size>1000)
  return {status:'invalid_input',execution_allowed:false,operations:[]};
 if(typeof now_ms!=='number'||!Number.isSafeInteger(now_ms)||now_ms<0)
  return {status:'invalid_time',execution_allowed:false,operations:[]};
 if(rows.length>max_batch_size) return {status:'batch_limit_exceeded',execution_allowed:false,operations:[]};
 const operations=[];
 const seen=new Set();
 for(const row of rows){
  if(!row||typeof row!=='object'||Array.isArray(row)||typeof row.record_id!=='string'||!/^[-A-Za-z0-9_]{1,128}$/.test(row.record_id)||seen.has(row.record_id))
   return {status:'invalid_record',execution_allowed:false,operations:[]};
  seen.add(row.record_id);
  if(typeof row.pii_written_at_ms!=='number'||!Number.isSafeInteger(row.pii_written_at_ms)||row.pii_written_at_ms<0||row.pii_written_at_ms>now_ms)
   return {status:'invalid_record_time',execution_allowed:false,operations:[]};

  const hardDeadlineReached=now_ms-row.pii_written_at_ms>=retentionTest.hardRetentionMs;
  // At the absolute deadline, corrupt or missing verification metadata must never
  // suppress purge. Only explicit pii_present=false proves there is nothing to remove.
  if(hardDeadlineReached){
   if(row.pii_present===false) continue;
   const fullyVerified=row.google_synced===true&&row.identity_verified===true&&row.version_verified===true&&row.digest_verified===true;
   operations.push({
    record_id:row.record_id,
    action:'PURGE_PII',
    retention_deadline_ms:row.pii_written_at_ms+retentionTest.hardRetentionMs,
    deadline_recovery:!fullyVerified,
    retain_retry_audit_only:!fullyVerified
   });
   continue;
  }

  // Before day 30, early purge is permitted only on complete exact boolean evidence,
  // including digest verification. Malformed evidence fails closed.
  if(!isBoolean(row.pii_present)||!isBoolean(row.google_synced)||!isBoolean(row.identity_verified)||!isBoolean(row.version_verified)||!isBoolean(row.digest_verified))
   return {status:'invalid_record_evidence',execution_allowed:false,operations:[]};
  const decision=planPiiRetention({
   now_ms,
   pii_written_at_ms:row.pii_written_at_ms,
   google_synced:row.google_synced,
   identity_verified:row.identity_verified,
   version_verified:row.version_verified,
   digest_verified:row.digest_verified,
   pii_present:row.pii_present
  });
  if(decision.status==='invalid_time') return {status:'invalid_record_time',execution_allowed:false,operations:[]};
  if(decision.status==='invalid_verification_evidence') return {status:'invalid_record_evidence',execution_allowed:false,operations:[]};
  if(decision.purge_required) operations.push({
   record_id:row.record_id,
   action:'PURGE_PII',
   retention_deadline_ms:decision.retention_deadline_ms,
   deadline_recovery:false,
   retain_retry_audit_only:false
  });
 }
 return {status:'ready',execution_allowed:false,operations,requires_owner_gate:true,contains_pii:false};
}
