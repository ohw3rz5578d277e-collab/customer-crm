import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {parseProductionSnapshotText} from '../src/crm-sales-snapshot.mjs';

const root=fs.mkdtempSync(path.join(os.tmpdir(),'crm-production-snapshot-'));
const sha='a'.repeat(40);

function run(inputValue,name='case'){
  const input=path.join(root,`${name}.raw`);
  const output=path.join(root,`${name}.json`);
  fs.writeFileSync(input,typeof inputValue==='string'?inputValue:JSON.stringify(inputValue),'utf8');
  const result=spawnSync(process.execPath,[
    'scripts/build-production-identity-snapshot.mjs',
    '--input',input,
    '--output',output,
    '--source-sha',sha
  ],{encoding:'utf8'});
  return {result,output};
}

try{
  const rows=[
    {customer_id:'26000002',line_user_id:'U22222222222222222222',name:'Beta',deleted_at:''},
    {customer_id:'26000001',line_user_id:'',name:'Alpha',deleted_at:'2026-01-02'}
  ];

  const wrapped=run([{results:rows,success:true}], 'wrapped');
  assert.equal(wrapped.result.status,0,wrapped.result.stderr||wrapped.result.stdout);
  const body=fs.readFileSync(wrapped.output,'utf8');
  const parsed=JSON.parse(body);
  assert.equal(parsed.snapshot_format,'customer-crm-production-identity-snapshot-v1');
  assert.equal(parsed.complete,true);
  assert.equal(parsed.query_scope,'all_customer_identities');
  assert.equal(parsed.source_main_sha,sha);
  assert.equal(parsed.customer_count,2);
  assert.deepEqual(parsed.customers.map(x=>x.customer_id),['26000001','26000002']);
  assert.deepEqual(parseProductionSnapshotText(body).map(x=>x.customer_id),['26000001','26000002']);
  assert.match(wrapped.result.stdout,/PRIVATE_CUSTOMER_VALUES_PRINTED=0/);
  assert.match(wrapped.result.stdout,/PRODUCTION_D1_WRITE=0/);

  const objectShape=run({results:rows},'object');
  assert.equal(objectShape.result.status,0,objectShape.result.stderr||objectShape.result.stdout);

  const directRows=run(rows,'direct');
  assert.equal(directRows.result.status,0,directRows.result.stderr||directRows.result.stdout);

  const duplicate=run([{results:[rows[0],{...rows[0],name:'Duplicate'}]}],'duplicate');
  assert.notEqual(duplicate.result.status,0);
  assert.match(duplicate.result.stderr,/STOP_DUPLICATE_CUSTOMER_ID/);

  const empty=run([{results:[]}],'empty');
  assert.notEqual(empty.result.status,0);
  assert.match(empty.result.stderr,/STOP_CUSTOMER_ROWS_EMPTY/);

  const badShaInput=path.join(root,'bad-sha.raw');
  const badShaOutput=path.join(root,'bad-sha.json');
  fs.writeFileSync(badShaInput,JSON.stringify([{results:rows}]),'utf8');
  const badSha=spawnSync(process.execPath,[
    'scripts/build-production-identity-snapshot.mjs',
    '--input',badShaInput,
    '--output',badShaOutput,
    '--source-sha','not-a-sha'
  ],{encoding:'utf8'});
  assert.notEqual(badSha.status,0);
  assert.match(badSha.stderr,/STOP_SOURCE_SHA_INVALID/);

  const source=fs.readFileSync('scripts/build-production-identity-snapshot.mjs','utf8');
  assert.doesNotMatch(source,/\bwrangler\b/i);
  assert.doesNotMatch(source,/d1\s+execute/i);
  assert.doesNotMatch(source,/\bfetch\s*\(/i);
  assert.doesNotMatch(source,/node:(?:http|https)/i);
  assert.doesNotMatch(source,/child_process/i);
  assert.doesNotMatch(source,/\bINSERT\s+(?:OR\s+(?:ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)\s+)?INTO\b/i);
  assert.doesNotMatch(source,/\bUPDATE\s+(?:OR\s+(?:ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)\s+)?(?:[A-Za-z_][A-Za-z0-9_$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])\s+SET\b/i);
  assert.doesNotMatch(source,/\bDELETE\s+FROM\b/i);
  assert.doesNotMatch(source,/\bCREATE\s+(?:TEMP(?:ORARY)?\s+)?(?:TABLE|INDEX|TRIGGER|VIEW)\b/i);
  assert.doesNotMatch(source,/\bALTER\s+TABLE\b/i);
  assert.doesNotMatch(source,/\bDROP\s+(?:TABLE|INDEX|TRIGGER|VIEW)\b/i);
  assert.doesNotMatch(source,/\bREPLACE\s+INTO\b/i);
  assert.doesNotMatch(source,/\bTRUNCATE(?:\s+TABLE)?\b/i);
  assert.doesNotMatch(source,/\bUPSERT\s+[A-Za-z_]/i);
  assert.match(source,/\.replace\(/,'local string replace remains allowed');
  assert.match(source,/customer-crm-production-identity-snapshot-v1/);
  assert.match(source,/all_customer_identities/);
  assert.match(source,/parseProductionSnapshotText/);
  assert.match(source,/PRIVATE_CUSTOMER_VALUES_PRINTED=0/);
  assert.match(source,/PRODUCTION_D1_WRITE=0/);

  console.log('PRODUCTION_IDENTITY_SNAPSHOT_WRAPPED_JSON=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_OBJECT_JSON=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_DIRECT_ROWS=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_DUPLICATE_FAIL_CLOSED=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_EMPTY_FAIL_CLOSED=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_SHA_BOUND=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_SQL_CONTEXT_SCAN=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_LOCAL_ONLY=PASS');
  console.log('PRODUCTION_D1_READ=0');
  console.log('PRODUCTION_D1_WRITE=0');
  console.log('NETWORK_ACCESS=0');
}finally{
  fs.rmSync(root,{recursive:true,force:true});
}
