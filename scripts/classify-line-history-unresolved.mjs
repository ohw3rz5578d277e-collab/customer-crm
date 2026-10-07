import fs from 'node:fs';
import { createHash } from 'node:crypto';
import { classifyLineHistoryUnresolved } from '../src/crm-line-history-unresolved-classifier.mjs';

const CANONICAL_PRODUCTION_IDENTITY_SNAPSHOT='customer-crm-production-identity-snapshot-v1';

function arg(name){const i=process.argv.indexOf(name);return i>=0?String(process.argv[i+1]||''):''}
function exactSha(v){return /^[0-9a-f]{40}$/.test(String(v||'').trim().toLowerCase())}
function exactSha256(v){return /^[0-9a-f]{64}$/.test(String(v||'').trim().toLowerCase())}
function sha256(bytes){return createHash('sha256').update(bytes).digest('hex')}
function readJson(path,required=true,{allowCanonicalCustomers=false,expectedSourceSha='',expectedSnapshotSha256=''}={}){
  if(!path){
    if(required)throw new Error('missing input path');
    return [];
  }
  const bytes=fs.readFileSync(path);
  const raw=JSON.parse(bytes.toString('utf8'));
  if(Array.isArray(raw)){
    if(raw.length===1&&raw[0]&&Array.isArray(raw[0].results))return raw[0].results;
    return raw;
  }
  if(allowCanonicalCustomers&&raw&&typeof raw==='object'&&!Array.isArray(raw)&&raw.snapshot_format===CANONICAL_PRODUCTION_IDENTITY_SNAPSHOT){
    const expectedSource=String(expectedSourceSha||'').trim().toLowerCase();
    const expectedDigest=String(expectedSnapshotSha256||'').trim().toLowerCase();
    if(!exactSha(expectedSource))throw new Error('canonical production identity snapshot expected source sha required: '+path);
    if(!exactSha256(expectedDigest))throw new Error('canonical production identity snapshot expected sha256 required: '+path);
    const actualSource=String(raw.source_main_sha||'').trim().toLowerCase();
    if(!exactSha(actualSource))throw new Error('canonical production identity snapshot source sha invalid: '+path);
    if(actualSource!==expectedSource)throw new Error('canonical production identity snapshot source sha mismatch: '+path);
    if(sha256(bytes)!==expectedDigest)throw new Error('canonical production identity snapshot sha256 mismatch: '+path);
    if(raw.complete!==true)throw new Error('canonical production identity snapshot is incomplete: '+path);
    if(raw.query_scope!=='all_customer_identities')throw new Error('canonical production identity snapshot query scope invalid: '+path);
    if(!Array.isArray(raw.customers))throw new Error('canonical production identity snapshot customers required: '+path);
    if(!Number.isInteger(raw.customer_count)||raw.customer_count!==raw.customers.length)throw new Error('canonical production identity snapshot customer count mismatch: '+path);
    if(raw.customers.length<1)throw new Error('canonical production identity snapshot customers empty: '+path);
    return raw.customers;
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
const exactReservationEvidencePath=arg('--exact-reservation-evidence');
const expectedCustomerSourceSha=arg('--expected-customer-source-sha');
const expectedCustomerSnapshotSha256=arg('--expected-customer-snapshot-sha256');
const outPath=arg('--out')||'line-history-unresolved-triage.json';

if(!candidatesPath||!customersPath){
  console.error('Usage: node scripts/classify-line-history-unresolved.mjs --candidates candidate-snapshot.json --customers production-customers.json [--expected-customer-source-sha <40-hex>] [--expected-customer-snapshot-sha256 <64-hex>] [--customer-master master.json] [--reviews reviews.json] [--reservation-identities reservation-identities.json] [--exact-reservation-evidence exact-reservation-evidence.json] [--out result.json]');
  process.exit(2);
}

const result=classifyLineHistoryUnresolved({
  candidates:readJson(candidatesPath),
  customers:readJson(customersPath,true,{
    allowCanonicalCustomers:true,
    expectedSourceSha:expectedCustomerSourceSha,
    expectedSnapshotSha256:expectedCustomerSnapshotSha256
  }),
  customerMaster:readJson(masterPath,false),
  reviews:readJson(reviewsPath,false),
  reservationIdentities:readJson(reservationPath,false),
  exactReservationEvidence:readJson(exactReservationEvidencePath,false)
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