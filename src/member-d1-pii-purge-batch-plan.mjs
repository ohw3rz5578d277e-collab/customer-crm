import {planPiiRetention} from './member-d1-pii-retention-plan.mjs';

// Pure, fail-closed executor planner. The caller must separately authorize any D1 mutation.
export function planMemberPiiPurgeBatch({rows,now_ms,max_batch_size=100}={}){
 if(!Array.isArray(rows)||!Number.isSafeInteger(max_batch_size)||max_batch_size<1||max_batch_size>1000)
  return {status:'invalid_input',execution_allowed:false,operations:[]};
 if(typeof now_ms!=='number'||!Number.isFinite(now_ms))
  return {status:'invalid_time',execution_allowed:false,operations:[]};
 if(rows.length>max_batch_size) return {status:'batch_limit_exceeded',execution_allowed:false,operations:[]};
 const operations=[];
 const seen=new Set();
 for(const row of rows){
  if(!row||typeof row!=='object'||Array.isArray(row)||typeof row.record_id!=='string'||!/^[-A-Za-z0-9_]{1,128}$/.test(row.record_id)||seen.has(row.record_id))
   return {status:'invalid_record',execution_allowed:false,operations:[]};
  seen.add(row.record_id);
  if(typeof row.pii_present!=='boolean'||typeof row.google_synced!=='boolean'||typeof row.identity_verified!=='boolean'||typeof row.version_verified!=='boolean') return {status:'invalid_record_evidence',execution_allowed:false,operations:[]};
  if(typeof row.pii_written_at_ms!=='number'||!Number.isFinite(row.pii_written_at_ms)) return {status:'invalid_record_time',execution_allowed:false,operations:[]};
  const decision=planPiiRetention({now_ms,pii_written_at_ms:row.pii_written_at_ms,google_synced:row.google_synced===true,identity_verified:row.identity_verified===true,version_verified:row.version_verified===true,pii_present:row.pii_present===true});
  if(decision.status==='invalid_time') return {status:'invalid_record_time',execution_allowed:false,operations:[]};
  if(decision.purge_required) operations.push({record_id:row.record_id,action:'PURGE_PII',deadline_recovery:decision.status==='DEADLINE_RECOVERY',retain_retry_audit_only:decision.retain_retry_audit_only===true});
 }
 return {status:'ready',execution_allowed:false,operations,requires_owner_gate:true,contains_pii:false};
}
