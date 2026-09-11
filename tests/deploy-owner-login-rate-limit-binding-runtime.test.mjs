import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const MIN_WRANGLER='4.36.0';
const BINDING='CRM_OWNER_LOGIN_RATE_LIMITER';

test('release config declares canonical Owner login Rate Limiting binding',()=>{
  const config=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
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
    console.log(`OWNER_LOGIN_RATE_LIMIT_BINDING_DRY_RUN=PASS`);
    console.log(`WRANGLER_MINIMUM=${MIN_WRANGLER}`);
    console.log('PRODUCTION_D1_WRITE=0');
    console.log('WORKER_DEPLOY=0');
    console.log('CUSTOMER_ID_GENERATION=0');
    console.log('LINE_SEND=0');
  } finally {
    fs.rmSync(outdir,{recursive:true,force:true});
  }
});

test('canonical production deploy action selects current Wrangler v4 line',()=>{
  const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
  assert.match(workflow,/wranglerVersion:\s*['"]4['"]/);
  assert.match(workflow,/command:\s*deploy\b/);
});
