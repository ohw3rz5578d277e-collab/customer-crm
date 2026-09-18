import fs from 'node:fs';
import { renderLineHistoryOwnerPrivateReviewHtml } from '../src/crm-line-history-owner-private-review.mjs';

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
const masterPath=arg('--customer-master');
const customersPath=arg('--customers');
const outPath=arg('--out')||'line-history-owner-private-review.html';

if(!triagePath||!masterPath||!customersPath){
  console.error('Usage: node scripts/render-line-history-owner-private-review.mjs --triage final-triage.json --customer-master customer-master.json --customers customers.json [--out private-review.html]');
  process.exit(2);
}

const triage=JSON.parse(fs.readFileSync(triagePath,'utf8'));
const html=renderLineHistoryOwnerPrivateReviewHtml({
  triage,
  customerMaster:readRows(masterPath),
  customers:readRows(customersPath)
});

fs.writeFileSync(outPath,html,'utf8');

console.log('RESULT=LINE_HISTORY_OWNER_PRIVATE_REVIEW_READY');
console.log('OUTPUT='+outPath);
console.log('PRIVATE_DATA_OUTPUT=LOCAL_FILE_ONLY');
console.log('PRIVATE_DATA_PRINTED_TO_TERMINAL=0');
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
