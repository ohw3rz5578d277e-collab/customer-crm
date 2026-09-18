import { createHash } from 'node:crypto';

function text(v){return v==null?'':String(v).trim()}
function bool(v){return v===true}

export function buildLineHistoryOwnerAuthorizationPacket({
  plan={},
  sourcePlanBytes='',
  mainSha=''
}={}){
  const sha=text(mainSha);
  const digest=createHash('sha256').update(sourcePlanBytes).digest('hex');

  const safety=plan.safety||{};
  const validationErrors=Number(plan.validation_error_count||0);
  const proposed=Number(plan.proposed_write_actions||0);
  const ready=bool(plan.ready_for_separate_write_authorization);
  const authGranted=bool(plan.authorization_granted);

  const blockers=[];
  if(text(plan.planner)!=='line_history_owner_decision_plan_v1')blockers.push('PLAN_FORMAT_INVALID');
  if(!/^[0-9a-f]{40}$/.test(sha))blockers.push('MAIN_SHA_INVALID');
  if(validationErrors!==0)blockers.push('PLAN_VALIDATION_ERRORS_PRESENT');
  if(proposed<0)blockers.push('PROPOSED_WRITE_COUNT_INVALID');
  if(proposed>0&&!ready)blockers.push('PLAN_NOT_READY_FOR_AUTHORIZATION');
  if(authGranted)blockers.push('PLAN_ALREADY_MARKED_AUTHORIZED');
  if(Number(safety.production_d1_write||0)!==0)blockers.push('PLAN_PRODUCTION_WRITE_NOT_ZERO');
  if(bool(safety.generated_sql))blockers.push('PLAN_ALREADY_GENERATED_SQL');
  if(bool(safety.executed_sql))blockers.push('PLAN_ALREADY_EXECUTED_SQL');
  if(Number(safety.customer_id_generation||0)!==0)blockers.push('PLAN_CUSTOMER_ID_GENERATION_NOT_ZERO');
  if(Number(safety.customer_update||0)!==0)blockers.push('PLAN_CUSTOMER_UPDATE_NOT_ZERO');
  if(Number(safety.customer_delete||0)!==0)blockers.push('PLAN_CUSTOMER_DELETE_NOT_ZERO');
  if(Number(safety.customer_merge||0)!==0)blockers.push('PLAN_CUSTOMER_MERGE_NOT_ZERO');
  if(Number(safety.line_send||0)!==0)blockers.push('PLAN_LINE_SEND_NOT_ZERO');

  const authorizationRequired=proposed>0;
  const packetReady=blockers.length===0;

  return {
    planner:'line_history_owner_write_authorization_packet_v1',
    source_plan_sha256:digest,
    source_main_sha:sha,
    submitted_decisions:Number(plan.submitted_decisions||0),
    undecided_groups:Number(plan.undecided_groups||0),
    proposed_write_actions:proposed,
    accepted_no_write_decisions:Number(plan.accepted_no_write_decisions||0),
    validation_error_count:validationErrors,
    decision_summary:{...(plan.decision_summary||{})},
    authorization_scope:'LINE_HISTORY_EXISTING_CUSTOMER_LINE_LINK_ONLY',
    authorization_required:authorizationRequired,
    packet_ready:packetReady,
    authorization_granted:false,
    blockers,
    approval_text:packetReady&&authorizationRequired
      ?`PLAN_SHA ${digest} / MAIN_SHA ${sha} のLINE history Production D1 writeを承認します`
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
