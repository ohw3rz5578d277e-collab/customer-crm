import { createHash } from 'node:crypto';
import { buildLineHistoryOwnerReviewQueue } from './crm-line-history-owner-review-queue.mjs';

const CURRENT_ID_RE=/^\d{8}$/;
const LINE_ID_RE=/^U[0-9a-fA-F]{20,}$/;
const ALLOWED_DECISIONS=new Set([
  'SAME_PERSON',
  'DIFFERENT_PERSON',
  'DEFERRED',
  'NEEDS_MORE_EVIDENCE'
]);

function text(v){return v==null?'':String(v).trim()}
function lineHash(v){
  return createHash('sha256').update(text(v)).digest('hex').slice(0,16);
}
function normalizeCustomer(r){
  return {
    customer_id:text(r.customer_id),
    line_user_id:text(r.line_user_id),
    deleted_at:text(r.deleted_at)
  };
}
function normalizeMaster(r){
  return {
    customer_id:text(r.customer_id),
    line_user_id:text(r.line_user_id)
  };
}
function queueItemForRow(row){
  return buildLineHistoryOwnerReviewQueue({
    candidate_message_rows:Number(row.message_rows||0),
    classifications:[row]
  }).items[0]||null;
}

export function buildLineHistoryOwnerDecisionPlan({
  triage={},
  decisions={},
  customerMaster=[],
  customers=[]
}={}){
  const classifications=Array.isArray(triage.classifications)?triage.classifications:[];
  const reviewRows=[];
  for(const row of classifications){
    const item=queueItemForRow(row);
    if(item)reviewRows.push({item,row});
  }

  const byQueue=new Map();
  for(const pair of reviewRows){
    const id=pair.item.queue_id;
    if(!byQueue.has(id))byQueue.set(id,[]);
    byQueue.get(id).push(pair);
  }

  const activeCustomers=(customers||[]).map(normalizeCustomer).filter(x=>x.customer_id&&!x.deleted_at);
  const customerById=new Map(activeCustomers.map(x=>[x.customer_id,x]));
  const masters=(customerMaster||[]).map(normalizeMaster);

  const decisionRows=Array.isArray(decisions)
    ?decisions
    :Array.isArray(decisions?.decisions)
      ?decisions.decisions
      :[];

  const seenDecisionIds=new Set();
  const proposedActions=[];
  const acceptedNoWrite=[];
  const validationErrors=[];
  const decisionSummary={
    SAME_PERSON:0,
    DIFFERENT_PERSON:0,
    DEFERRED:0,
    NEEDS_MORE_EVIDENCE:0
  };

  for(const rawDecision of decisionRows){
    const queueId=text(rawDecision.queue_id);
    const decision=text(rawDecision.decision).toUpperCase();

    if(!queueId){
      validationErrors.push({queue_id:'',code:'QUEUE_ID_MISSING'});
      continue;
    }
    if(seenDecisionIds.has(queueId)){
      validationErrors.push({queue_id:queueId,code:'DUPLICATE_QUEUE_DECISION'});
      continue;
    }
    seenDecisionIds.add(queueId);

    if(!ALLOWED_DECISIONS.has(decision)){
      validationErrors.push({queue_id:queueId,code:'INVALID_DECISION'});
      continue;
    }
    decisionSummary[decision]+=1;

    const matches=byQueue.get(queueId)||[];
    if(matches.length!==1){
      validationErrors.push({
        queue_id:queueId,
        code:matches.length===0?'QUEUE_ID_NOT_FOUND':'QUEUE_ID_AMBIGUOUS'
      });
      continue;
    }

    const {row}=matches[0];

    if(decision!=='SAME_PERSON'){
      acceptedNoWrite.push({queue_id:queueId,decision});
      continue;
    }

    const targetId=text(row.target_customer_id);
    if(!CURRENT_ID_RE.test(targetId)){
      validationErrors.push({queue_id:queueId,code:'SAME_PERSON_TARGET_NOT_CURRENT_ID'});
      continue;
    }

    const target=customerById.get(targetId);
    if(!target){
      validationErrors.push({queue_id:queueId,code:'SAME_PERSON_TARGET_CUSTOMER_MISSING'});
      continue;
    }

    const hash=text(row.line_id_hash);
    if(!hash){
      validationErrors.push({queue_id:queueId,code:'SAME_PERSON_LINE_HASH_MISSING'});
      continue;
    }

    const lines=[...new Set(
      masters
        .map(x=>x.line_user_id)
        .filter(Boolean)
        .filter(v=>lineHash(v)===hash)
    )];

    if(lines.length!==1){
      validationErrors.push({
        queue_id:queueId,
        code:lines.length===0?'SAME_PERSON_LINE_NOT_RECONSTRUCTED':'SAME_PERSON_LINE_AMBIGUOUS'
      });
      continue;
    }

    const candidateLine=lines[0];
    if(!LINE_ID_RE.test(candidateLine)){
      validationErrors.push({queue_id:queueId,code:'SAME_PERSON_LINE_FORMAT_INVALID'});
      continue;
    }

    if(target.line_user_id&&target.line_user_id!==candidateLine){
      validationErrors.push({queue_id:queueId,code:'SAME_PERSON_TARGET_LINE_CONFLICT'});
      continue;
    }

    proposedActions.push({
      queue_id:queueId,
      action:'BACKFILL_LINE_HISTORY_TO_EXISTING_CUSTOMER',
      target_customer_id:targetId,
      line_user_id:candidateLine,
      source_line_id_hash:hash,
      message_rows:Number(row.message_rows||0),
      target_line_state:target.line_user_id===candidateLine?'EXACT':'EMPTY',
      source:'owner_review_same_person'
    });
  }

  const allQueueIds=new Set(reviewRows.map(x=>x.item.queue_id));
  const decidedKnownCount=[...seenDecisionIds].filter(id=>allQueueIds.has(id)).length;

  return {
    planner:'line_history_owner_decision_plan_v1',
    review_queue_groups:reviewRows.length,
    submitted_decisions:decisionRows.length,
    decided_known_groups:decidedKnownCount,
    undecided_groups:Math.max(0,reviewRows.length-decidedKnownCount),
    decision_summary:decisionSummary,
    proposed_write_actions:proposedActions.length,
    accepted_no_write_decisions:acceptedNoWrite.length,
    validation_error_count:validationErrors.length,
    proposed_actions:proposedActions,
    no_write_decisions:acceptedNoWrite,
    validation_errors:validationErrors,
    ready_for_readonly_backfill_preview:
      validationErrors.length===0&&proposedActions.length>0,
    ready_for_separate_write_authorization:false,
    authorization_granted:false,
    safety:{
      production_d1_read:0,
      production_d1_write:0,
      generated_sql:false,
      executed_sql:false,
      customer_id_generation:0,
      customer_update:0,
      customer_delete:0,
      customer_merge:0,
      line_send:0,
      worker_deploy:0,
      production_deploy:0
    }
  };
}
