import assert from 'node:assert/strict';
import {MEMBER_POST_MERGE_BASELINE,evaluateMemberPostMergeMainAudit} from '../src/member-post-merge-main-audit.mjs';

const validEvidence={
  source_pr:216,
  pre_merge_main:MEMBER_POST_MERGE_BASELINE.pre_merge_main,
  approved_head:MEMBER_POST_MERGE_BASELINE.approved_head,
  merged_main:MEMBER_POST_MERGE_BASELINE.merged_main,
  pr_merged:true,
  merge_parent_pre_main:MEMBER_POST_MERGE_BASELINE.pre_merge_main,
  merge_parent_approved_head:MEMBER_POST_MERGE_BASELINE.approved_head,
  approved_head_to_main_file_delta:0,
  fresh_exact_head_ci_passed:true,
  fresh_codex_completed:true,
  fresh_codex_new_findings:0,
  unresolved_review_threads:0,
  production_actions_performed:[]
};

let result=evaluateMemberPostMergeMainAudit(validEvidence);
assert.equal(result.status,'post_merge_source_verified');
assert.equal(result.canonical_main,MEMBER_POST_MERGE_BASELINE.merged_main);
assert.equal(result.runtime_source_work_allowed,true);
assert.equal(result.production_ready,false);
assert.equal(result.production_deploy_allowed,false);
assert.equal(result.production_d1_read_allowed,false);
assert.equal(result.production_d1_write_allowed,false);
assert.equal(result.migration_apply_allowed,false);
assert.equal(result.r2_object_access_allowed,false);
assert.equal(result.crm_mutation_allowed,false);
assert.equal(result.line_send_allowed,false);
assert.equal(result.google_network_send_allowed,false);
assert.equal(result.route_activation_allowed,false);
assert.equal(result.secret_change_allowed,false);

for(const [field,badValue,reason] of [
  ['pre_merge_main','deadbeef','pre_merge_main_mismatch'],
  ['approved_head','deadbeef','approved_head_mismatch'],
  ['merged_main','deadbeef','merged_main_mismatch'],
  ['source_pr',215,'source_pr_mismatch'],
  ['pr_merged',false,'merge_not_proven'],
  ['merge_parent_pre_main','deadbeef','pre_main_parent_mismatch'],
  ['merge_parent_approved_head','deadbeef','approved_head_parent_mismatch'],
  ['approved_head_to_main_file_delta',1,'merged_tree_delta_not_zero'],
  ['fresh_exact_head_ci_passed',false,'fresh_ci_not_proven'],
  ['fresh_codex_completed',false,'fresh_codex_not_completed'],
  ['fresh_codex_new_findings',1,'fresh_codex_findings_not_zero'],
  ['unresolved_review_threads',1,'unresolved_review_threads_not_zero']
]){
  result=evaluateMemberPostMergeMainAudit({...validEvidence,[field]:badValue});
  assert.equal(result.status,'blocked');
  assert.equal(result.failures.includes(reason),true);
  assert.equal(result.runtime_source_work_allowed,false);
  assert.equal(result.production_ready,false);
}

result=evaluateMemberPostMergeMainAudit({...validEvidence,production_actions_performed:['migration_apply']});
assert.equal(result.status,'blocked');
assert.equal(result.failures.includes('production_action_detected'),true);

result=evaluateMemberPostMergeMainAudit({...validEvidence,production_actions_performed:null});
assert.equal(result.status,'blocked');
assert.equal(result.failures.includes('production_action_evidence_missing'),true);

console.log('MEMBER_POST_MERGE_MAIN_AUDIT=PASS');
console.log(`CANONICAL_MAIN=${MEMBER_POST_MERGE_BASELINE.merged_main}`);
console.log('APPROVED_HEAD_TO_MAIN_FILE_DELTA=0');
console.log('RUNTIME_SOURCE_WORK_ALLOWED=1');
console.log('PRODUCTION_READY=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('MIGRATION_APPLY=0');
console.log('R2_OBJECT_ACCESS=0');
console.log('CRM_MUTATION=0');
console.log('LINE_SEND=0');
console.log('GOOGLE_NETWORK_SEND=0');
console.log('ROUTE_ACTIVATION=0');
console.log('SECRET_CHANGE=0');
