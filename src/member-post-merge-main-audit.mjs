export const MEMBER_POST_MERGE_BASELINE=Object.freeze({
  source_pr:216,
  pre_merge_main:'90b83b8126b0851e3728d904b875347e19191f49',
  approved_head:'0ed3482f745c4338c9d8de443f52cad8e1638627',
  merged_main:'6fd7a851ad0f8df8870d98a7658bad4b6c1e9724'
});

const exactSha=(value,expected)=>typeof value==='string'&&value===expected;
const zeroInteger=value=>Number.isInteger(value)&&value===0;

export function evaluateMemberPostMergeMainAudit(evidence={}){
  const failures=[];
  const expected=MEMBER_POST_MERGE_BASELINE;

  if(!exactSha(evidence.pre_merge_main,expected.pre_merge_main)) failures.push('pre_merge_main_mismatch');
  if(!exactSha(evidence.approved_head,expected.approved_head)) failures.push('approved_head_mismatch');
  if(!exactSha(evidence.merged_main,expected.merged_main)) failures.push('merged_main_mismatch');
  if(evidence.source_pr!==expected.source_pr) failures.push('source_pr_mismatch');
  if(evidence.pr_merged!==true) failures.push('merge_not_proven');
  if(evidence.merge_parent_pre_main!==expected.pre_merge_main) failures.push('pre_main_parent_mismatch');
  if(evidence.merge_parent_approved_head!==expected.approved_head) failures.push('approved_head_parent_mismatch');
  if(!zeroInteger(evidence.approved_head_to_main_file_delta)) failures.push('merged_tree_delta_not_zero');
  if(evidence.fresh_exact_head_ci_passed!==true) failures.push('fresh_ci_not_proven');
  if(evidence.fresh_codex_completed!==true) failures.push('fresh_codex_not_completed');
  if(!zeroInteger(evidence.fresh_codex_new_findings)) failures.push('fresh_codex_findings_not_zero');
  if(!zeroInteger(evidence.unresolved_review_threads)) failures.push('unresolved_review_threads_not_zero');

  const productionActions=Array.isArray(evidence.production_actions_performed)
    ? evidence.production_actions_performed.filter(Boolean)
    : null;
  if(productionActions===null) failures.push('production_action_evidence_missing');
  else if(productionActions.length) failures.push('production_action_detected');

  const passed=failures.length===0;
  return {
    status:passed?'post_merge_source_verified':'blocked',
    failures,
    canonical_main:passed?expected.merged_main:null,
    runtime_source_work_allowed:passed,
    production_ready:false,
    production_deploy_allowed:false,
    production_d1_read_allowed:false,
    production_d1_write_allowed:false,
    migration_apply_allowed:false,
    r2_object_access_allowed:false,
    crm_mutation_allowed:false,
    line_send_allowed:false,
    google_network_send_allowed:false,
    route_activation_allowed:false,
    secret_change_allowed:false
  };
}
