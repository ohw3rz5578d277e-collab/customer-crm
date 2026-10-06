import fs from 'node:fs';
import {parseProductionSnapshotText} from '../src/crm-sales-snapshot.mjs';

function arg(name){const i=process.argv.indexOf(name);return i>=0?process.argv[i+1]:''}
function text(v){return v==null?'':String(v).trim()}
function fail(code){console.error(`RESULT=STOP_${code}`);process.exit(1)}
function stripTransportNoise(s){return String(s??'').replace(/^\uFEFF/,'').replace(/\x1b\[[0-9;?]*[ -/]*[@-~]/g,'').trim()}

function parseExactJson(raw){
  const source=stripTransportNoise(raw);
  if(!source)fail('INPUT_JSON_EMPTY');
  try{return JSON.parse(source)}catch{fail('INPUT_JSON_INVALID')}
}

function rowsFromParsed(raw){
  if(Array.isArray(raw)){
    if(raw.length===1&&raw[0]&&Array.isArray(raw[0].results))return raw[0].results;
    if(raw.every(x=>x&&typeof x==='object'&&!Array.isArray(x)&&!Object.prototype.hasOwnProperty.call(x,'results')))return raw;
    const rows=[];
    for(const item of raw){if(item&&Array.isArray(item.results))rows.push(...item.results)}
    return rows;
  }
  if(raw&&Array.isArray(raw.results))return raw.results;
  if(raw&&raw.result&&Array.isArray(raw.result.results))return raw.result.results;
  return [];
}

const input=arg('--input');
const output=arg('--output');
const sourceSha=text(arg('--source-sha')).toLowerCase();
if(!input||!output||!sourceSha)fail('ARGS_REQUIRED');
if(!/^[0-9a-f]{40}$/.test(sourceSha))fail('SOURCE_SHA_INVALID');
if(!fs.existsSync(input))fail('INPUT_MISSING');

const parsed=parseExactJson(fs.readFileSync(input,'utf8'));
const rawRows=rowsFromParsed(parsed);
if(!rawRows.length)fail('CUSTOMER_ROWS_EMPTY');

const seen=new Set();
const customers=[];
for(const row of rawRows){
  if(!row||typeof row!=='object'||Array.isArray(row))fail('CUSTOMER_ROW_INVALID');
  const customerId=text(row.customer_id);
  if(!customerId)fail('CUSTOMER_ID_REQUIRED');
  if(seen.has(customerId))fail('DUPLICATE_CUSTOMER_ID');
  seen.add(customerId);
  customers.push({
    customer_id:customerId,
    line_user_id:text(row.line_user_id),
    name:text(row.name),
    deleted_at:text(row.deleted_at)
  });
}
customers.sort((a,b)=>a.customer_id.localeCompare(b.customer_id));

const envelope={
  snapshot_format:'customer-crm-production-identity-snapshot-v1',
  complete:true,
  query_scope:'all_customer_identities',
  source_main_sha:sourceSha,
  generated_at:new Date().toISOString(),
  customer_count:customers.length,
  customers
};

const body=JSON.stringify(envelope,null,2)+'\n';
parseProductionSnapshotText(body);
fs.writeFileSync(output,body,'utf8');

console.log('RESULT=PRODUCTION_IDENTITY_SNAPSHOT_READY');
console.log(`SOURCE_MAIN_SHA=${sourceSha}`);
console.log(`CUSTOMER_COUNT=${customers.length}`);
console.log('SNAPSHOT_FORMAT=customer-crm-production-identity-snapshot-v1');
console.log('QUERY_SCOPE=all_customer_identities');
console.log('COMPLETE=true');
console.log('PRIVATE_CUSTOMER_VALUES_PRINTED=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CRM_MUTATION=0');
console.log('LINE_SEND=0');
