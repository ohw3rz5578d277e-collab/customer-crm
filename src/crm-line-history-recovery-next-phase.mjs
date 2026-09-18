function bool(v){return v===true}

export function resolveLineHistoryRecoveryNextPhase({
  candidatesPresent=false,
  customerMasterPresent=false,
  resumeReady=false,
  reviewQueueGroups=null,
  decisionsPresent=false,
  preauthPreviewReady=false,
  d1Packet=null,
  approvalFilePresent=false,
  completionReceipt=null
}={}){
  if(completionReceipt&&completionReceipt.complete===true){
    return {
      stage:'COMPLETE',
      next_phase:'none',
      next_action:'Recovery flow is complete. Keep the completion receipt with the run artifacts.',
      production_write_possible:false
    };
  }

  if(d1Packet){
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
