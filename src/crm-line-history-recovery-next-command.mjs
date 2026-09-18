function q(v){
  const s=String(v??'');
  return "'"+s.replaceAll("'","'\\''")+"'";
}

export function buildLineHistoryRecoveryNextCommand({
  stage='',
  operatorPath='scripts/run-line-history-recovery-operator.sh',
  candidates='',
  customerMaster='',
  resumeDir='',
  decisions='',
  preauthDir='',
  d1PreviewDir='',
  approvalFile=''
}={}){
  const base=['bash',operatorPath];

  if(stage==='INPUTS_REQUIRED'){
    return {
      command:'',
      ready:false,
      reason:'INPUT_PATHS_REQUIRED',
      requires_owner_approval:false,
      production_write:false
    };
  }

  if(stage==='READONLY_RESUME_REQUIRED'){
    if(!candidates||!customerMaster){
      return {
        command:'',
        ready:false,
        reason:'READONLY_INPUT_PATHS_REQUIRED',
        requires_owner_approval:false,
        production_write:false
      };
    }
    const args=[
      ...base,
      '--phase','readonly-resume',
      '--candidates',candidates,
      '--customer-master',customerMaster
    ];
    return {
      command:args.map(q).join(' '),
      ready:true,
      reason:'READONLY_RESUME',
      requires_owner_approval:false,
      production_write:false
    };
  }

  if(stage==='OWNER_REVIEW_REQUIRED'){
    return {
      command:'',
      ready:false,
      reason:'OWNER_REVIEW_DECISIONS_REQUIRED',
      requires_owner_approval:false,
      production_write:false
    };
  }

  if(stage==='PREAUTH_REQUIRED'){
    if(!resumeDir||!customerMaster||!decisions||!candidates){
      return {
        command:'',
        ready:false,
        reason:'PREAUTH_INPUT_PATHS_REQUIRED',
        requires_owner_approval:false,
        production_write:false
      };
    }
    const args=[
      ...base,
      '--phase','preauth',
      '--resume-dir',resumeDir,
      '--customer-master',customerMaster,
      '--decisions',decisions,
      '--candidates',candidates
    ];
    return {
      command:args.map(q).join(' '),
      ready:true,
      reason:'PREAUTH',
      requires_owner_approval:false,
      production_write:false
    };
  }

  if(stage==='D1_PREVIEW_REQUIRED'||stage==='STALE_AUTHORIZATION_PACKET'||stage==='BLOCKED_D1_PREVIEW'){
    if(!preauthDir){
      return {
        command:'',
        ready:false,
        reason:'PREAUTH_DIR_REQUIRED',
        requires_owner_approval:false,
        production_write:false
      };
    }
    const args=[
      ...base,
      '--phase','d1-preview',
      '--preauth-dir',preauthDir
    ];
    return {
      command:args.map(q).join(' '),
      ready:true,
      reason:'D1_PREVIEW',
      requires_owner_approval:false,
      production_write:false
    };
  }

  if(stage==='OWNER_EXACT_APPROVAL_REQUIRED'){
    return {
      command:'',
      ready:false,
      reason:'EXACT_OWNER_APPROVAL_FILE_REQUIRED',
      requires_owner_approval:true,
      production_write:false
    };
  }

  if(stage==='APPROVED_WRITE_READY'){
    if(!preauthDir||!d1PreviewDir||!approvalFile){
      return {
        command:'',
        ready:false,
        reason:'APPROVED_WRITE_PATHS_REQUIRED',
        requires_owner_approval:true,
        production_write:false
      };
    }
    const args=[
      ...base,
      '--phase','approved-write',
      '--preauth-dir',preauthDir,
      '--d1-preview-dir',d1PreviewDir,
      '--approval-file',approvalFile,
      '--execute-production-write','YES'
    ];
    return {
      command:args.map(q).join(' '),
      ready:true,
      reason:'APPROVED_WRITE',
      requires_owner_approval:true,
      production_write:true
    };
  }

  if(['COMPLETE','COMPLETE_NO_REVIEW','COMPLETE_NO_WRITE'].includes(stage)){
    return {
      command:'',
      ready:false,
      reason:'NO_NEXT_COMMAND',
      requires_owner_approval:false,
      production_write:false
    };
  }

  return {
    command:'',
    ready:false,
    reason:'UNKNOWN_STAGE',
    requires_owner_approval:false,
    production_write:false
  };
}
