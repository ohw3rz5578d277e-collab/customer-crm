import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';

const repo=process.cwd();
const script=path.join(repo,'scripts/classify-line-history-unresolved.mjs');
const tmp=fs.mkdtempSync(path.join(os.tmpdir(),'crm-line-history-canonical-snapshot-'));
const SOURCE_SHA='a'.repeat(40);
function digest(pathname){return createHash('sha256').update(fs.readFileSync(pathname)).digest('hex')}

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
    source_main_sha:SOURCE_SHA,
    generated_at:'2026-10-07T00:00:00.000Z',
    customer_count:1,
    customers:[customer]
  };
  fs.writeFileSync(canonicalPath,JSON.stringify(envelope,null,2));
  fs.writeFileSync(directPath,JSON.stringify([customer],null,2));

  function run(customers,out,{sourceSha=SOURCE_SHA,snapshotSha=digest(customers),bind=true}={}){
    const args=[script,'--candidates',candidatesPath,'--customers',customers,'--out',out];
    if(bind){
      args.push('--expected-customer-source-sha',sourceSha);
      args.push('--expected-customer-snapshot-sha256',snapshotSha);
    }
    return spawnSync(process.execPath,args,{
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

  const direct=run(directPath,outDirect,{bind:false});
  assert.equal(direct.status,0,direct.stderr||direct.stdout);
  const directResult=JSON.parse(fs.readFileSync(outDirect,'utf8'));
  assert.equal(directResult.already_resolved_message_rows,1);
  assert.equal(directResult.unresolved_message_rows,0);

  const missingBinding=run(canonicalPath,path.join(tmp,'missing-binding-out.json'),{bind:false});
  assert.notEqual(missingBinding.status,0,'canonical envelope without binding should fail closed');
  assert.match(String(missingBinding.stderr||missingBinding.stdout),/expected source sha required/);

  const sourceMismatch=run(canonicalPath,path.join(tmp,'source-mismatch-out.json'),{sourceSha:'b'.repeat(40)});
  assert.notEqual(sourceMismatch.status,0,'source SHA mismatch should fail closed');
  assert.match(String(sourceMismatch.stderr||sourceMismatch.stdout),/source sha mismatch/);

  const digestMismatch=run(canonicalPath,path.join(tmp,'digest-mismatch-out.json'),{snapshotSha:'0'.repeat(64)});
  assert.notEqual(digestMismatch.status,0,'snapshot digest mismatch should fail closed');
  assert.match(String(digestMismatch.stderr||digestMismatch.stdout),/sha256 mismatch/);

  for(const [name,patch,expected] of [
    ['incomplete',{complete:false},'incomplete'],
    ['scope',{query_scope:'partial'},'query scope invalid'],
    ['sha',{source_main_sha:'bad'},'source sha invalid'],
    ['count',{customer_count:2},'customer count mismatch'],
    ['empty',{customer_count:0,customers:[]},'customers empty']
  ]){
    const p=path.join(tmp,`${name}.json`);
    fs.writeFileSync(p,JSON.stringify({...envelope,...patch},null,2));
    const sourceSha=name==='sha'?SOURCE_SHA:String(({...envelope,...patch}).source_main_sha||SOURCE_SHA);
    const r=run(p,path.join(tmp,`${name}-out.json`),{sourceSha,snapshotSha:digest(p)});
    assert.notEqual(r.status,0,`${name} should fail closed`);
    assert.match(String(r.stderr||r.stdout),new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')));
  }

  console.log('LINE_HISTORY_CANONICAL_PRODUCTION_SNAPSHOT_ADAPTER=PASS');
  console.log('CANONICAL_SNAPSHOT_EXACT_SOURCE_BINDING=PASS');
  console.log('CANONICAL_SNAPSHOT_SHA256_BINDING=PASS');
  console.log('CANONICAL_SNAPSHOT_FAIL_CLOSED=PASS');
  console.log('DIRECT_ARRAY_COMPATIBILITY=PASS');
  console.log('PRODUCTION_D1_READ=0');
  console.log('PRODUCTION_D1_WRITE=0');
  console.log('NETWORK_ACCESS=0');
}finally{
  fs.rmSync(tmp,{recursive:true,force:true});
}
