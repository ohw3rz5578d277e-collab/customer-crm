import fs from 'node:fs';
import { renderLineHistoryOwnerReviewHtml } from '../src/crm-line-history-owner-review-html.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}

const queuePath=arg('--queue');
const outPath=arg('--out')||'line-history-owner-review.html';

if(!queuePath){
  console.error('Usage: node scripts/render-line-history-owner-review-html.mjs --queue owner-review-queue.json [--out owner-review.html]');
  process.exit(2);
}

const queue=JSON.parse(fs.readFileSync(queuePath,'utf8'));
const html=renderLineHistoryOwnerReviewHtml(queue);

fs.writeFileSync(outPath,html,'utf8');

console.log('RESULT=LINE_HISTORY_OWNER_REVIEW_HTML_READY');
console.log('REVIEW_QUEUE_GROUPS='+Number(queue.review_queue_groups||0));
console.log('OUTPUT='+outPath);
console.log('NETWORK_REQUESTS=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('CUSTOMER_MERGE=0');
console.log('LINE_SEND=0');
console.log('WORKER_DEPLOY=0');
console.log('PRODUCTION_DEPLOY=0');
