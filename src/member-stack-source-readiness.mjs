export const MEMBER_STACK_EXPECTED=[
 {pr:203,head:'8f7aedce9846f92143ac965a3a96bce7975a7ce9'},
 {pr:204,head:'82b43879837802ef46f742b1927dea989ee0ef69'},
 {pr:205,head:'a6315e4c818a9e30864163d4348ae755819b028e'},
 {pr:206,head:'841cf2e0b9c669c7a9749fd01d21daf5ea75a093'},
 {pr:207,head:'d45c97cc650c70551cc4aec58d14bc15f821aa13'},
 {pr:208,head:'784d99df5ba42eba9c7e3f774f0c46ea9f574ab1'},
 {pr:209,head:'1d2c76dff0705adf0239787ca683a4d62cebccbc'},
 {pr:210,head:'8c2cdabbb44717c7bf0fe79cefd2ca1d310c1b28'},
 {pr:211,head:'f510eca28b184c8b166cf13fd3618d3b3357b805'}
];

const readinessResult=failures=>({
 status:failures.length?'blocked':'source_ready',
 failures,
 merge_order:MEMBER_STACK_EXPECTED.map(x=>x.pr),
 exact_head_required:true,
 sequential_merge_required:true,
 refresh_after_each_merge:true,
 production_ready:false,
 production_deploy_allowed:false,
 production_write_allowed:false
});

export function evaluateMemberStackReadiness(rows=[]){
 const failures=[];
 if(!Array.isArray(rows)) return readinessResult([{pr:null,reason:'invalid_readiness_evidence'}]);
 const byPr=new Map();
 for(const row of rows){
  if(!row||typeof row!=='object'||Array.isArray(row)){
   failures.push({pr:null,reason:'invalid_readiness_evidence'});
   continue;
  }
  const pr=Number(row.pr);
  if(!Number.isSafeInteger(pr)||pr<=0){
   failures.push({pr:null,reason:'invalid_readiness_evidence'});
   continue;
  }
  if(byPr.has(pr)){
   failures.push({pr,reason:'duplicate_pr_evidence'});
   continue;
  }
  byPr.set(pr,row);
 }
 for(const expected of MEMBER_STACK_EXPECTED){
  const row=byPr.get(expected.pr);
  if(!row){failures.push({pr:expected.pr,reason:'missing_pr_evidence'});continue;}
  if(row.head!==expected.head) failures.push({pr:expected.pr,reason:'head_drift'});
  if(row.mergeable!==true) failures.push({pr:expected.pr,reason:'not_mergeable'});
  if(!Number.isInteger(row.unresolved_review_threads)||row.unresolved_review_threads!==0) failures.push({pr:expected.pr,reason:'unresolved_or_unknown_review_threads'});
  if(row.required_checks_passed!==true) failures.push({pr:expected.pr,reason:'required_checks_not_proven'});
  if(row.changed_files_expected!==true) failures.push({pr:expected.pr,reason:'changed_files_not_proven'});
  if(row.parent_head_in_ancestry!==true) failures.push({pr:expected.pr,reason:'parent_head_ancestry_not_proven'});
 }
 return readinessResult(failures);
}
