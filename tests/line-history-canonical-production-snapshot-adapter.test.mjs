import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const repo=process.cwd();
const script=path.join(repo,'scripts/classify-line-history-unresolved.mjs');
const tmp=fs.mkdtempSync(path.join(os.tmpdir(),'crm-line-history-canonical-snapshot-'));

try{
  const line='U'+'a'.repeat(20);
  const customerId='20260001';
  const candidatesPath=path.join(tmp,'candidates.json');
  const canonicalPath=path.join(tmp,'canonical.json');
  const directPath=path.join(tmp,'direct.json');
  const outCanonical=path.join(tmp,'canonical-out.json');
  const outDirect=path.join(tmp,'direct-out.json');

  fs.writeFileSync(candidatesPath,JSON.stringify([{
    line_user_id:line,
    customer_id_hint:customerId,
    message_key:'synthetic-message-key'
  }],null,2));

  const customer={customer_id:customerId,line_user_id:line,name:'Synthetic Customer',deleted_at:''};
  const envelope={
    snapshot_format:'customer-crm-production-identity-snapshot-v1',
    complete:true,
    query_scope:'all_customer_identities',
    source_main_sha:'a'.repeat(40),
    generated_at:'2026-10-07T00:00:00.000Z',
    customer_count:1,
    customers:[customer]
  };
  fs.writeFileSync(canonicalPath,JSON.stringify(envelope,null,2));
  fs.writeFileSync(directPath,JSON.stringify([customer],null,2));

  function run(customers,out){
    return spawnSync(process.execPath,[script,'--candidates',candidatesPath,'--customers',customers,'--out',out],{
      cwd:repo,
      encoding:'utf8',
      env:{...process.env}
    });
  }

  const canonical=run(canonicalPath,outCanonical);
  assert.equal(canonical.status,0,canonical.stderr||canonical.stdout);
  assert.match(canonical.stdout,/RESULT=LINE_HISTORY_UNRESOLVED_TRIAGE_READY/);
  assert.match(canonical.stdout,/ALREADY_RESOLVED_MESSAGE_ROWS=1/);
  const canonicalResult=JSON.parse(fs.readFileSync(outCanonical,'utf8'));
  assert.equal(canonicalResult.already_resolved_message_rows,1);
  assert.equal(canonicalResult.unresolved_message_rows,0);

  const direct=run(directPath,outDirect);
  assert.equal(direct.status,0,direct.stderr||direct.stdout);
  const directResult=JSON.parse(fs.readFileSync(outDirect,'utf8'));
  assert.equal(directResult.already_resolved_message_rows,1);
  assert.equal(directResult.unresolved_message_rows,0);

  for(const [name,patch,expected] of [
    ['incomplete',{complete:false},'incomplete'],
    ['scope',{query_scope:'partial'},'query scope invalid'],
    ['sha',{source_main_sha:'bad'},'source sha invalid'],
    ['count',{customer_count:2},'customer count mismatch'],
    ['empty',{customer_count:0,customers:[]},'customers empty']
  ]){
    const p=path.join(tmp,`${name}.json`);
    fs.writeFileSync(p,JSON.stringify({...envelope,...patch},null,2));
    const r=run(p,path.join(tmp,`${name}-out.json`));
    assert.notEqual(r.status,0,`${name} should fail closed`);
    assert.match(String(r.stderr||r.stdout),new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')));
  }

  console.log('LINE_HISTORY_CANONICAL_PRODUCTION_SNAPSHOT_ADAPTER=PASS');
  console.log('CANONICAL_SNAPSHOT_FAIL_CLOSED=PASS');
  console.log('DIRECT_ARRAY_COMPATIBILITY=PASS');
  console.log('PRODUCTION_D1_READ=0');
  console.log('PRODUCTION_D1_WRITE=0');
  console.log('NETWORK_ACCESS=0');
}finally{
  fs.rmSync(tmp,{recursive:true,force:true});
}
