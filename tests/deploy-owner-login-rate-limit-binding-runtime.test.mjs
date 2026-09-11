import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const MIN_WRANGLER='4.36.0';
const BINDING='CRM_OWNER_LOGIN_RATE_LIMITER';

function versionAtLeast(actual,minimum){
  const a=actual.split('.').map(Number);
  const m=minimum.split('.').map(Number);
  for(let i=0;i<Math.max(a.length,m.length);i++){
    const av=a[i]||0,mv=m[i]||0;
    if(av>mv)return true;
    if(av<mv)return false;
  }
  return true;
}

function namedStepBlock(workflow,name){
  const marker=`      - name: ${name}`;
  const start=workflow.indexOf(marker);
  assert.notEqual(start,-1,`workflow step not found: ${name}`);
  const next=workflow.indexOf('\n      - name: ',start+marker.length);
  return workflow.slice(start,next===-1?workflow.length:next);
}

test('release config activates hybrid Owner auth and declares canonical login Rate Limiting binding',()=>{
  const config=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
  assert.equal(config.vars?.CRM_OWNER_AUTH_MODE,'hybrid','Production Owner auth mode must remain hybrid during staged activation');
  const limiter=(config.ratelimits||[]).find(x=>x.name===BINDING);
  assert.ok(limiter,`${BINDING} must be declared in wrangler.jsonc`);
  assert.match(String(limiter.namespace_id),/^\d+$/);
  assert.equal(limiter.simple?.limit,10);
  assert.equal(limiter.simple?.period,60);
});

test(`Wrangler ${MIN_WRANGLER} dry-run recognizes Owner login Rate Limiting binding`,()=>{
  const outdir=fs.mkdtempSync(path.join(os.tmpdir(),'crm-owner-ratelimit-'));
  try{
    const result=spawnSync(
      process.platform==='win32'?'npx.cmd':'npx',
      ['--yes',`wrangler@${MIN_WRANGLER}`,'deploy','--dry-run','--outdir',outdir],
      {
        cwd:process.cwd(),
        encoding:'utf8',
        env:{...process.env,CLOUDFLARE_API_TOKEN:'',CLOUDFLARE_ACCOUNT_ID:''},
        timeout:120000,
      },
    );
    const output=`${result.stdout||''}\n${result.stderr||''}`;
    assert.equal(result.status,0,`Wrangler dry-run failed:\n${output}`);
    assert.doesNotMatch(output,/Unexpected fields found in top-level field:\s*["']ratelimits["']/i);
    assert.match(output,new RegExp(BINDING),`Wrangler dry-run did not expose ${BINDING}:\n${output}`);
    assert.ok(fs.readdirSync(outdir).length>0,'Wrangler dry-run produced no output artifact');
    console.log('OWNER_LOGIN_RATE_LIMIT_BINDING_DRY_RUN=PASS');
    console.log(`WRANGLER_MINIMUM=${MIN_WRANGLER}`);
    console.log('PRODUCTION_D1_WRITE=0');
    console.log('WORKER_DEPLOY=0');
    console.log('CUSTOMER_ID_GENERATION=0');
    console.log('LINE_SEND=0');
  } finally {
    fs.rmSync(outdir,{recursive:true,force:true});
  }
});

test('canonical production deploy step selects Wrangler version that supports Rate Limiting bindings',()=>{
  const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
  const deploy=namedStepBlock(workflow,'Deploy customer-crm-api');
  assert.match(deploy,/uses:\s*cloudflare\/wrangler-action@v4/);
  assert.match(deploy,/command:\s*deploy\b/);
  const match=deploy.match(/wranglerVersion:\s*['"]([^'"]+)['"]/);
  assert.ok(match,'Deploy customer-crm-api must declare wranglerVersion');
  const selected=match[1].trim();
  if(/^\d+$/.test(selected)){
    assert.equal(selected,'4','Canonical production deploy currently requires Wrangler v4 line');
  }else{
    assert.match(selected,/^\d+\.\d+\.\d+$/);
    assert.ok(versionAtLeast(selected,MIN_WRANGLER),`Deploy Wrangler ${selected} is below ${MIN_WRANGLER}`);
  }
});