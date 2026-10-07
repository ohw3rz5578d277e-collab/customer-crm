export const MEMBER_STACK_EXPECTED=[
 {pr:203,head:'8f7aedce9846f92143ac965a3a96bce7975a7ce9'},
 {pr:204,head:'82b43879837802ef46f742b1927dea989ee0ef69'},
 {pr:205,head:'a6315e4c818a9e30864163d4348ae755819b028e'},
 {pr:206,head:'841cf2e0b9c669c7a9749fd01d21daf5ea75a093'},
 {pr:207,head:'d45c97cc650c70551cc4aec58d14bc15f821aa13'},
 {pr:208,head:'784d99df5ba42eba9c7e3f774f0c46ea9f574ab1'},
 {pr:209,head:'1d2c76dff0705adf0239787ca683a4d62cebccbc'},
 {pr:210,head:'14ab11a77b1b53da081d98e5cb7e3804a94aa6e6'},
 {pr:211,head:'20b180eec06fc48edf35b08838168d410ea39df1'}
];

export function evaluateMemberStackReadiness(rows=[]){
 const byPr=new Map((rows||[]).map(x=>[Number(x.pr),x]));
 const failures=[];
 for(const expected of MEMBER_STACK_EXPECTED){
  const row=byPr.get(expected.pr);
  if(!row){failures.push({pr:expected.pr,reason:'missing_pr_evidence'});continue;}
  if(row.head!==expected.head) failures.push({pr:expected.pr,reason:'head_drift'});
  if(row.mergeable!==true) failures.push({pr:expected.pr,reason:'not_mergeable'});
  if(Number(row.unresolved_review_threads)!==0) failures.push({pr:expected.pr,reason:'unresolved_review_threads'});
 }
 return {
  status:failures.length?'blocked':'source_ready',
  failures,
  merge_order:MEMBER_STACK_EXPECTED.map(x=>x.pr),
  exact_head_required:true,
  sequential_merge_required:true,
  refresh_after_each_merge:true,
  production_ready:false,
  production_deploy_allowed:false,
  production_write_allowed:false
 };
}
