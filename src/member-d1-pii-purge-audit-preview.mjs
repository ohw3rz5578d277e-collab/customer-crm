import {planMemberPiiPurgeBatch} from './member-d1-pii-purge-batch-plan.mjs';

// Dry-run only: does not accept a database handle and cannot mutate Production.
export function buildMemberPiiPurgeAuditPreview({rows,now_ms,max_batch_size=100}={}){
 const plan=planMemberPiiPurgeBatch({rows,now_ms,max_batch_size});
 if(plan.status!=='ready') return {status:plan.status,execution_allowed:false,receipts:[]};
 const receipts=plan.operations.map(({record_id,deadline_recovery,retain_retry_audit_only})=>({
  record_id,action:'PURGE_PII',deadline_recovery,retain_retry_audit_only,
  result:'NOT_EXECUTED',pii_in_receipt:false
 }));
 return {status:'preview_ready',execution_allowed:false,receipts,operation_count:receipts.length};
}
