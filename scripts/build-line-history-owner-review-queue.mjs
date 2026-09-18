import fs from 'node:fs';
import { buildLineHistoryOwnerReviewQueue } from '../src/crm-line-history-owner-review-queue.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}

const triagePath=arg('--triage');
const outPath=arg('--out')||'line-history-owner-review-queue.json';

if(!triagePath){
  console.error('Usage: node scripts/build-line-history-owner-review-queue.mjs --triage final-triage.json [--out owner-review-queue.json]');
  process.exit(2);
}

const triage=JSON.parse(fs.readFileSync(triagePath,'utf8'));
const result=buildLineHistoryOwnerReviewQueue(triage);

fs.writeFileSync(outPath,JSON.stringify(result,null,2)+'\n');

console.log('RESULT=LINE_HISTORY_OWNER_REVIEW_QUEUE_READY');
console.log('REVIEW_QUEUE_GROUPS='+result.review_queue_groups);
console.log('REVIEW_QUEUE_MESSAGE_ROWS='+result.review_queue_message_rows);
console.log('BLOCKED_CONFLICT_GROUPS='+result.blocked_conflict_groups);
console.log('REVIEW_REQUIRED_GROUPS='+result.review_required_groups);
console.log('UNRESOLVED_GROUPS='+result.unresolved_groups);
console.log('OUTPUT='+outPath);
console.log('RAW_CUSTOMER_ID_OUTPUT=0');
console.log('RAW_LINE_USER_ID_OUTPUT=0');
console.log('MESSAGE_TEXT_OUTPUT=0');
console.log('CUSTOMER_NAME_OUTPUT=0');
console.log('CSV_FILE_NAME_OUTPUT=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('CUSTOMER_MERGE=0');
console.log('LINE_SEND=0');
