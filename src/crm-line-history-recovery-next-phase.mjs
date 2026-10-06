function bool(v){return v===true}

function exactSha(v){return /^[0-9a-f]{40}$/.test(String(v||''))}
function exactInt(v){return Number.isInteger(v)?v:null}

export function isValidOwnerNoWriteCompletionReceipt(receipt,currentMainSha=''){
  if(!receipt||receipt.complete!==true)return false;
  if(String(receipt.receipt_format||'')!=='customer-crm-line-history-no-write-completion-v1')return false;
  if(String(receipt.completion_type||'')!=='OWNER_DECISIONS_NO_WRITE')return false;
  if(!exactSha(currentMainSha)||String(receipt.source_main_sha||'')!==String(currentMainSha))return false;

  const groups=exactInt(receipt.review_queue_groups);
  const submitted=exactInt(receipt.submitted_decisions);
  const noWrite=exactInt(receipt.accepted_no_write_decisions);
  const identityActions=exactInt(receipt.proposed_backfill_identity_actions);
  const physicalWrites=exactInt(receipt.proposed_write_actions);

  if(groups===null||groups<=0)return false;
  if(submitted!==groups||noWrite!==groups)return false;
  if(identityActions!==0||physicalWrites!==0)return false;
  if(receipt.authorization_granted!==false)return false;

  const zeroFields=[
    'production_d1_read',
    'production_d1_write',
    'customer_id_generation',
    'customer_update',
    'customer_delete',
    'customer_merge',
    'line_send',
    'worker_deploy',
    'production_deploy'
  ];
  if(zeroFields.some((key)=>exactInt(receipt[key])!==0))return false;

  return true;
}

export function resolveLineHistoryRecoveryNextPhase({
  candidatesPresent=false,
  customerMasterPresent=false,
  resumeReady=false,
  reviewQueueGroups=null,
  decisionsPresent=false,
  preauthPreviewReady=false,
  d1Packet=null,
  approvalFilePresent=false,
  completionReceipt=null,
  currentMainSha=''
}={}){
  if(completionReceipt&&String(completionReceipt.completion_type||'')==='OWNER_DECISIONS_NO_WRITE'){
    if(isValidOwnerNoWriteCompletionReceipt(completionReceipt,currentMainSha)){
      return {
        stage:'COMPLETE_NO_WRITE',
        next_phase:'none',
        next_action:'Owner review is complete and selected no Production backfill actions. Keep the SHA-bound no-write completion receipt with the run artifacts.',
        production_write_possible:false
      };
    }
  }else if(completionReceipt&&completionReceipt.complete===true){
    return {
      stage:'COMPLETE',
      next_phase:'none',
      next_action:'Recovery flow is complete. Keep the completion receipt with the run artifacts.',
      production_write_possible:false
    };
  }

  if(d1Packet){
    if(
      currentMainSha&&
      /^[0-9a-f]{40}$/.test(String(currentMainSha))&&
      String(d1Packet.source_main_sha||'')!==String(currentMainSha)
    ){
      return {
        stage:'STALE_AUTHORIZATION_PACKET',
        next_phase:'d1-preview',
        next_action:'Re-run the D1 SELECT-only preview to bind a new authorization packet to the current main SHA.',
        production_write_possible:false
      };
    }

    if(d1Packet.packet_ready!==true){
      return {
        stage:'BLOCKED_D1_PREVIEW',
        next_phase:'d1-preview',
        next_action:'Resolve the blocked D1 preview packet before any write authorization.',
        production_write_possible:false
      };
    }

    if(d1Packet.authorization_required!==true){
      return {
        stage:'COMPLETE_NO_WRITE',
        next_phase:'none',
        next_action:'No Production write is required because the preview found zero physical inserts.',
        production_write_possible:false
      };
    }

    if(!approvalFilePresent){
      return {
        stage:'OWNER_EXACT_APPROVAL_REQUIRED',
        next_phase:'approved-write',
        next_action:'Provide the exact approval text from the frozen authorization packet in a local approval file.',
        production_write_possible:false
      };
    }

    return {
      stage:'APPROVED_WRITE_READY',
      next_phase:'approved-write',
      next_action:'Run approved-write only with the explicit production-write confirmation flag.',
      production_write_possible:true
    };
  }

  if(preauthPreviewReady){
    return {
      stage:'D1_PREVIEW_REQUIRED',
      next_phase:'d1-preview',
      next_action:'Run the Production D1 SELECT-only preview to freeze the exact physical insert count.',
      production_write_possible:false
    };
  }

  if(resumeReady&&decisionsPresent){
    return {
      stage:'PREAUTH_REQUIRED',
      next_phase:'preauth',
      next_action:'Build the private decision plan and READ ONLY preview artifacts.',
      production_write_possible:false
    };
  }

  if(resumeReady){
    if(reviewQueueGroups!==null&&reviewQueueGroups!==undefined&&Number(reviewQueueGroups)===0){
      return {
        stage:'COMPLETE_NO_REVIEW',
        next_phase:'none',
        next_action:'No Owner review items remain after the READ ONLY triage.',
        production_write_possible:false
      };
    }

    return {
      stage:'OWNER_REVIEW_REQUIRED',
      next_phase:'preauth',
      next_action:'Open the private local Owner review HTML, record decisions, then export the decision JSON.',
      production_write_possible:false
    };
  }

  if(candidatesPresent&&customerMasterPresent){
    return {
      stage:'READONLY_RESUME_REQUIRED',
      next_phase:'readonly-resume',
      next_action:'Run the one-shot READ ONLY resume to build exact-reservation evidence and Owner review artifacts.',
      production_write_possible:false
    };
  }

  return {
    stage:'INPUTS_REQUIRED',
    next_phase:'readonly-resume',
    next_action:'Provide the candidate snapshot and sanitized Customer Master JSON.',
    production_write_possible:false
  };
}
