import fs from 'node:fs';
import { classifyLineHistoryUnresolved } from '../src/crm-line-history-unresolved-classifier.mjs';

function arg(name){const i=process.argv.indexOf(name);return i>=0?String(process.argv[i+1]||''):''}
function readJson(path,required=true){
  if(!path){
    if(required)throw new Error('missing input path');
    return [];
  }
  const raw=JSON.parse(fs.readFileSync(path,'utf8'));
  if(Array.isArray(raw)){
    if(raw.length===1&&raw[0]&&Array.isArray(raw[0].results))return raw[0].results;
    return raw;
  }
  if(raw&&Array.isArray(raw.results))return raw.results;
  if(raw&&raw.result&&Array.isArray(raw.result.results))return raw.result.results;
  throw new Error('unsupported JSON shape: '+path);
}
const candidatesPath=arg('--candidates');
const customersPath=arg('--customers');
const masterPath=arg('--customer-master');
const reviewsPath=arg('--reviews');
const reservationPath=arg('--reservation-identities');
const outPath=arg('--out')||'line-history-unresolved-triage.json';

if(!candidatesPath||!customersPath){
  console.error('Usage: node scripts/classify-line-history-unresolved.mjs --candidates candidate-snapshot.json --customers production-customers.json [--customer-master master.json] [--reviews reviews.json] [--reservation-identities reservation-identities.json] [--out result.json]');
  process.exit(2);
}

const result=classifyLineHistoryUnresolved({
  candidates:readJson(candidatesPath),
  customers:readJson(customersPath),
  customerMaster:readJson(masterPath,false),
  reviews:readJson(reviewsPath,false),
  reservationIdentities:readJson(reservationPath,false)
});

fs.writeFileSync(outPath,JSON.stringify(result,null,2)+'\n');

console.log('RESULT=LINE_HISTORY_UNRESOLVED_TRIAGE_READY');
console.log('CANDIDATE_MESSAGE_ROWS='+result.candidate_message_rows);
console.log('IDENTITY_GROUPS='+result.identity_groups);
console.log('ALREADY_RESOLVED_MESSAGE_ROWS='+result.already_resolved_message_rows);
console.log('UNRESOLVED_MESSAGE_ROWS='+result.unresolved_message_rows);
console.log('UNRESOLVED_IDENTITY_GROUPS='+result.unresolved_identity_groups);
console.log('AUTO_CONFIRMABLE_GROUPS='+result.auto_confirmable_groups);
console.log('AUTO_CONFIRMABLE_MESSAGE_ROWS='+result.auto_confirmable_message_rows);
console.log('REVIEW_REQUIRED_GROUPS='+result.review_required_groups);
console.log('REVIEW_REQUIRED_MESSAGE_ROWS='+result.review_required_message_rows);
console.log('FULLY_UNRESOLVED_GROUPS='+result.unresolved_groups);
console.log('FULLY_UNRESOLVED_MESSAGE_ROWS='+result.fully_unresolved_message_rows);
console.log('BLOCKED_CONFLICT_GROUPS='+result.blocked_conflict_groups);
console.log('BLOCKED_CONFLICT_MESSAGE_ROWS='+result.blocked_conflict_message_rows);
console.log('OUTPUT='+outPath);
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('LINE_SEND=0');
console.log('MESSAGE_TEXT_OUTPUT=0');
