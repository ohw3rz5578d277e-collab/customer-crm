import {buildMemberPiiPurgeAuditPreview} from './member-d1-pii-purge-audit-preview.mjs';

// Source-only orchestration contract; no database or network dependencies.
export function planMemberPiiPurgeReconciliation({rows,now_ms,max_batch_size=100}={}){
 const preview=buildMemberPiiPurgeAuditPreview({rows,now_ms,max_batch_size});
 if(preview.status!=='preview_ready') return {status:preview.status,execution_allowed:false,items:[]};
 const items=preview.receipts.map(receipt=>({
  record_id:receipt.record_id,
  required_action:'PURGE_PII',
  retention_deadline_ms:receipt.retention_deadline_ms,
  needs_deadline_recovery:receipt.deadline_recovery,
  preserve_non_pii_retry_audit:receipt.retain_retry_audit_only,
  reconciliation_state:'PENDING_EXECUTION'
 }));
 return {status:'reconciliation_planned',execution_allowed:false,items,requires_separate_execution_gate:true};
}
