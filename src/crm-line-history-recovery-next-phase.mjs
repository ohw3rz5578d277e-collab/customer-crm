function exactSha(v){return /^[0-9a-f]{40}$/.test(String(v||''))}
function exactInt(v){return Number.isInteger(v)?v:null}

const DECISION_SUMMARY_KEYS=[
  'SAME_PERSON',
  'DIFFERENT_PERSON',
  'DEFERRED',
  'NEEDS_MORE_EVIDENCE'
];

function hasExactDecisionSummary(receipt,groups,noWrite){
  const summary=receipt?.decision_summary;
  if(!summary||typeof summary!=='object'||Array.isArray(summary))return false;
  const keys=Object.keys(summary).sort();
  const expected=[...DECISION_SUMMARY_KEYS].sort();
  if(keys.length!==expected.length||keys.some((key,index)=>key!==expected[index]))return false;

  const values={};
  for(const key of DECISION_SUMMARY_KEYS){
    const value=exactInt(summary[key]);
    if(value===null||value<0)return false;
    values[key]=value;
  }

  if(values.SAME_PERSON!==0)return false;
  const noWriteSummary=
    values.DIFFERENT_PERSON+
    values.DEFERRED+
    values.NEEDS_MORE_EVIDENCE;
  return noWriteSummary===groups&&noWriteSummary===noWrite;
}

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
  if(!hasExactDecisionSummary(receipt,groups,noWrite))return false;

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

export function isValidOwnerWriteCompletionReceipt(receipt,currentMainSha=''){
  if(!receipt||receipt.complete!==true)return false;
  if(String(receipt.planner||'')!=='line_history_owner_write_completion_receipt_v1')return false;
  if(!exactSha(currentMainSha)||String(receipt.source_main_sha||'')!==String(currentMainSha))return false;
  if(String(receipt.authorization_scope||'')!=='CUSTOMER_LINE_MESSAGES_INSERT_ONLY')return false;

  const exactRows=exactInt(receipt.exact_physical_insert_rows);
  const blockerCount=exactInt(receipt.blocker_count);
  const remainingRows=exactInt(receipt.post_preview_would_insert_rows);
  if(exactRows===null||exactRows<1)return false;
  if(blockerCount!==0||remainingRows!==0)return false;
  if(!Array.isArray(receipt.blockers)||receipt.blockers.length!==0)return false;

  const safety=receipt.safety;
  if(!safety||typeof safety!=='object'||Array.isArray(safety))return false;
  const zeroSafetyFields=[
    'line_send',
    'customer_id_generation',
    'customer_update',
    'customer_delete',
    'customer_merge',
    'worker_deploy',
    'production_deploy'
  ];
  if(zeroSafetyFields.some((key)=>exactInt(safety[key])!==0))return false;
  if(safety.private_customer_id_output!==false)return false;
  if(safety.private_line_user_id_output!==false)return false;
  if(safety.message_text_output!==false)return false;
  if(safety.customer_name_output!==false)return false;

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
  if(completionReceipt){
    if(isValidOwnerNoWriteCompletionReceipt(completionReceipt,currentMainSha)){
      return {
        stage:'COMPLETE_NO_WRITE',
        next_phase:'none',
        next_action:'Owner review is complete and selected no Production backfill actions. Keep the SHA-bound no-write completion receipt with the run artifacts.',
        production_write_possible:false
      };
    }
    if(isValidOwnerWriteCompletionReceipt(completionReceipt,currentMainSha)){
      return {
        stage:'COMPLETE',
        next_phase:'none',
        next_action:'Recovery flow is complete. Keep the validated write completion receipt with the run artifacts.',
        production_write_possible:false
      };
    }
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
