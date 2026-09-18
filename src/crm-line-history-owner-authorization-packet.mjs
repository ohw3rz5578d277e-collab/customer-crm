import { createHash } from 'node:crypto';

function text(v){return v==null?'':String(v).trim()}
function bool(v){return v===true}
function digest(v){return createHash('sha256').update(v).digest('hex')}

export function buildLineHistoryOwnerAuthorizationPacket({
  plan={},
  sourcePlanBytes='',
  previewResult={},
  sourcePreviewResultBytes='',
  mainSha=''
}={}){
  const sha=text(mainSha);
  const planDigest=digest(sourcePlanBytes);
  const previewDigest=digest(sourcePreviewResultBytes);

  const planSafety=plan.safety||{};
  const previewSafety=previewResult.safety||{};
  const blockers=[];

  if(text(plan.planner)!=='line_history_owner_decision_plan_v1')blockers.push('PLAN_FORMAT_INVALID');
  if(!/^[0-9a-f]{40}$/.test(sha))blockers.push('MAIN_SHA_INVALID');
  if(Number(plan.validation_error_count||0)!==0)blockers.push('PLAN_VALIDATION_ERRORS_PRESENT');
  if(bool(plan.authorization_granted))blockers.push('PLAN_ALREADY_MARKED_AUTHORIZED');
  if(Number(plan.proposed_write_actions||0)!==0)blockers.push('PLAN_PHYSICAL_WRITE_COUNT_NOT_ZERO');
  if(Number(plan.proposed_backfill_identity_actions||0)>0&&!bool(plan.ready_for_readonly_backfill_preview)){
    blockers.push('PLAN_NOT_READY_FOR_READONLY_PREVIEW');
  }
  if(bool(plan.ready_for_separate_write_authorization))blockers.push('PLAN_PREVIEW_GATE_BYPASSED');
  if(Number(planSafety.production_d1_write||0)!==0)blockers.push('PLAN_PRODUCTION_WRITE_NOT_ZERO');
  if(bool(planSafety.generated_sql))blockers.push('PLAN_WRITE_SQL_ALREADY_GENERATED');
  if(bool(planSafety.executed_sql))blockers.push('PLAN_WRITE_SQL_ALREADY_EXECUTED');

  if(text(previewResult.planner)!=='line_history_owner_backfill_preview_result_v1'){
    blockers.push('PREVIEW_RESULT_FORMAT_INVALID');
  }
  if(text(previewResult.source_plan_sha256)!==planDigest){
    blockers.push('PREVIEW_PLAN_SHA_MISMATCH');
  }
  if(!bool(previewResult.preview_pass))blockers.push('PREVIEW_NOT_PASS');
  if(Number(previewResult.validation_error_count||0)!==0)blockers.push('PREVIEW_VALIDATION_ERRORS_PRESENT');
  if(Number(previewResult.target_missing_rows||0)!==0)blockers.push('PREVIEW_TARGET_MISSING');
  if(Number(previewResult.target_line_conflict_rows||0)!==0)blockers.push('PREVIEW_TARGET_LINE_CONFLICT');
  if(bool(previewResult.authorization_granted))blockers.push('PREVIEW_ALREADY_MARKED_AUTHORIZED');
  if(Number(previewSafety.production_d1_write||0)!==0)blockers.push('PREVIEW_PRODUCTION_WRITE_NOT_ZERO');
  if(bool(previewSafety.write_sql_generated))blockers.push('PREVIEW_WRITE_SQL_ALREADY_GENERATED');
  if(bool(previewSafety.write_sql_executed))blockers.push('PREVIEW_WRITE_SQL_ALREADY_EXECUTED');

  const exactRows=Number(previewResult.exact_physical_insert_rows);
  if(!Number.isInteger(exactRows)||exactRows<0)blockers.push('EXACT_PHYSICAL_INSERT_ROWS_INVALID');

  const authorizationRequired=Number.isInteger(exactRows)&&exactRows>0;
  const packetReady=blockers.length===0;

  return {
    planner:'line_history_owner_write_authorization_packet_v2',
    source_plan_sha256:planDigest,
    source_preview_result_sha256:previewDigest,
    source_preview_sql_sha256:text(previewResult.source_preview_sql_sha256),
    source_candidate_snapshot_sha256:text(previewResult.source_candidate_snapshot_sha256),
    selected_private_rows_sha256:text(previewResult.selected_private_rows_sha256),
    source_main_sha:sha,
    submitted_decisions:Number(plan.submitted_decisions||0),
    undecided_groups:Number(plan.undecided_groups||0),
    backfill_identity_actions:Number(plan.proposed_backfill_identity_actions||0),
    already_present_rows:Number(previewResult.already_present_rows||0),
    batch_duplicate_rows:Number(previewResult.batch_duplicate_rows||0),
    exact_physical_insert_rows:Number.isInteger(exactRows)&&exactRows>=0?exactRows:0,
    validation_error_count:
      Number(plan.validation_error_count||0)+Number(previewResult.validation_error_count||0),
    decision_summary:{...(plan.decision_summary||{})},
    authorization_scope:'CUSTOMER_LINE_MESSAGES_INSERT_ONLY',
    authorization_required:authorizationRequired,
    packet_ready:packetReady,
    authorization_granted:false,
    blockers,
    approval_text:packetReady&&authorizationRequired
      ?`PLAN_SHA ${planDigest} / PREVIEW_SHA ${previewDigest} / MAIN_SHA ${sha} / ROWS ${exactRows} のcustomer_line_messages INSERT-only Production D1 writeを承認します`
      :'',
    safety:{
      production_d1_read:0,
      production_d1_write:0,
      sql_generated:false,
      sql_executed:false,
      customer_id_generation:0,
      customer_update:0,
      customer_delete:0,
      customer_merge:0,
      line_send:0,
      worker_deploy:0,
      production_deploy:0,
      private_action_values_output:false
    }
  };
}
