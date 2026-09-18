import fs from 'node:fs';
import { buildLineHistoryOwnerDecisionPlan } from '../src/crm-line-history-owner-decision-plan.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}
function readRows(path){
  if(!path)return [];
  const raw=JSON.parse(fs.readFileSync(path,'utf8'));
  if(Array.isArray(raw)){
    if(raw.length===1&&raw[0]&&Array.isArray(raw[0].results))return raw[0].results;
    return raw;
  }
  if(raw&&Array.isArray(raw.rows))return raw.rows;
  if(raw&&Array.isArray(raw.results))return raw.results;
  if(raw&&raw.result&&Array.isArray(raw.result.results))return raw.result.results;
  return [];
}

const triagePath=arg('--triage');
const decisionsPath=arg('--decisions');
const masterPath=arg('--customer-master');
const customersPath=arg('--customers');
const outPath=arg('--out')||'line-history-owner-decision-plan-private.json';

if(!triagePath||!decisionsPath||!masterPath||!customersPath){
  console.error(
    'Usage: node scripts/plan-line-history-owner-decisions.mjs '+
    '--triage final-triage.json --decisions owner-review-decisions.json '+
    '--customer-master customer-master.json --customers customers.json '+
    '[--out decision-plan-private.json]'
  );
  process.exit(2);
}

const triage=JSON.parse(fs.readFileSync(triagePath,'utf8'));
const decisions=JSON.parse(fs.readFileSync(decisionsPath,'utf8'));

const plan=buildLineHistoryOwnerDecisionPlan({
  triage,
  decisions,
  customerMaster:readRows(masterPath),
  customers:readRows(customersPath)
});

fs.writeFileSync(outPath,JSON.stringify(plan,null,2)+'\n');

console.log('RESULT=LINE_HISTORY_OWNER_DECISION_PLAN_READY');
console.log('REVIEW_QUEUE_GROUPS='+plan.review_queue_groups);
console.log('SUBMITTED_DECISIONS='+plan.submitted_decisions);
console.log('UNDECIDED_GROUPS='+plan.undecided_groups);
console.log('PROPOSED_WRITE_ACTIONS='+plan.proposed_write_actions);
console.log('ACCEPTED_NO_WRITE_DECISIONS='+plan.accepted_no_write_decisions);
console.log('VALIDATION_ERROR_COUNT='+plan.validation_error_count);
console.log('READY_FOR_READONLY_BACKFILL_PREVIEW='+(plan.ready_for_readonly_backfill_preview?'YES':'NO'));
console.log('READY_FOR_SEPARATE_WRITE_AUTHORIZATION=NO');
console.log('AUTHORIZATION_GRANTED=NO');
console.log('OUTPUT='+outPath);
console.log('PRIVATE_ACTION_VALUES_PRINTED_TO_TERMINAL=0');
console.log('SQL_GENERATED=0');
console.log('SQL_EXECUTED=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('CUSTOMER_MERGE=0');
console.log('LINE_SEND=0');
console.log('WORKER_DEPLOY=0');
console.log('PRODUCTION_DEPLOY=0');
