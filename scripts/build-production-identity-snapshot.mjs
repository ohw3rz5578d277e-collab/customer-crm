import fs from 'node:fs';
import {parseProductionSnapshotText} from '../src/crm-sales-snapshot.mjs';

function arg(name){const i=process.argv.indexOf(name);return i>=0?process.argv[i+1]:''}
function text(v){return v==null?'':String(v).trim()}
function fail(code){console.error(`RESULT=STOP_${code}`);process.exit(1)}
function stripTransportNoise(s){return String(s??'').replace(/^\uFEFF/,'').replace(/\x1b\[[0-9;?]*[ -/]*[@-~]/g,'').trim()}
function owns(obj,key){return Object.prototype.hasOwnProperty.call(obj,key)}

function parseExactJson(raw){
  const source=stripTransportNoise(raw);
  if(!source)fail('INPUT_JSON_EMPTY');
  try{return JSON.parse(source)}catch{fail('INPUT_JSON_INVALID')}
}

function validateWrapperEntry(item){
  if(!item||typeof item!=='object'||Array.isArray(item))fail('WRAPPER_ENTRY_INVALID');
  if(!Array.isArray(item.results))fail('WRAPPER_ENTRY_RESULTS_REQUIRED');
  if(owns(item,'success')&&item.success!==true)fail('WRAPPER_ENTRY_UNSUCCESSFUL');
  return item.results;
}

function rowsFromParsed(raw){
  if(Array.isArray(raw)){
    const wrapperLike=raw.some(x=>x&&typeof x==='object'&&!Array.isArray(x)&&(owns(x,'results')||owns(x,'success')||owns(x,'error')));
    if(wrapperLike){
      const rows=[];
      for(const item of raw)rows.push(...validateWrapperEntry(item));
      return rows;
    }
    if(raw.every(x=>x&&typeof x==='object'&&!Array.isArray(x)))return raw;
    fail('INPUT_ARRAY_SHAPE_INVALID');
  }
  if(raw&&typeof raw==='object'&&!Array.isArray(raw)&&Array.isArray(raw.results)){
    if(owns(raw,'success')&&raw.success!==true)fail('WRAPPER_ENTRY_UNSUCCESSFUL');
    return raw.results;
  }
  if(raw&&typeof raw==='object'&&!Array.isArray(raw)&&raw.result){
    const result=raw.result;
    if(!result||typeof result!=='object'||Array.isArray(result)||!Array.isArray(result.results))fail('WRAPPER_ENTRY_RESULTS_REQUIRED');
    if(owns(result,'success')&&result.success!==true)fail('WRAPPER_ENTRY_UNSUCCESSFUL');
    return result.results;
  }
  fail('INPUT_SHAPE_INVALID');
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
const requiredIdentityFields=['customer_id','line_user_id','name','deleted_at'];
for(const row of rawRows){
  if(!row||typeof row!=='object'||Array.isArray(row))fail('CUSTOMER_ROW_INVALID');
  if(requiredIdentityFields.some(field=>!owns(row,field)))fail('CUSTOMER_ROW_IDENTITY_FIELDS_REQUIRED');
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
