export const CLEAN_MEMBER_STACK_ORIGINS=Object.freeze([
 [203,'8f7aedce9846f92143ac965a3a96bce7975a7ce9'],
 [204,'82b43879837802ef46f742b1927dea989ee0ef69'],
 [205,'a6315e4c818a9e30864163d4348ae755819b028e'],
 [206,'841cf2e0b9c669c7a9749fd01d21daf5ea75a093'],
 [207,'d45c97cc650c70551cc4aec58d14bc15f821aa13'],
 [208,'784d99df5ba42eba9c7e3f774f0c46ea9f574ab1'],
 [209,'1d2c76dff0705adf0239787ca683a4d62cebccbc'],
 [210,'8c2cdabbb44717c7bf0fe79cefd2ca1d310c1b28'],
 [211,'f510eca28b184c8b166cf13fd3618d3b3357b805'],
 [212,'f0b81986e62051834bc7d2cc83be062207065548']
]);
export const CLEAN_MEMBER_STACK_BASE='9d282bc2c1906379f33b7d0d8d28604c26cc3881';
export function validateCleanMemberStackOrigins(rows=[]){
 if(!Array.isArray(rows)||rows.length!==CLEAN_MEMBER_STACK_ORIGINS.length) return {status:'blocked',reason:'origin_count_mismatch'};
 for(const [pr,head] of CLEAN_MEMBER_STACK_ORIGINS){
  const row=rows.find(x=>Number(x.pr)===pr);
  if(!row||row.head!==head||row.review_threads_resolved!==true) return {status:'blocked',reason:'origin_evidence_mismatch',pr};
 }
 return {status:'provenance_verified',production_ready:false,merge_allowed:false};
}
