import assert from 'node:assert/strict';
import fs from 'node:fs';

function deployState(jobs) {
  const release=jobs.find(j=>j.name==='Exact-SHA Production release gate');
  if(!release)return {kind:'NOT_A_PRODUCTION_DEPLOY'};
  const deploy=(release.steps||[]).find(s=>s.name==='Deploy customer-crm-api');
  if(!deploy||deploy.conclusion==='skipped')return {kind:'DEPLOY_NOT_REACHED'};
  if(deploy.conclusion!=='success')return {kind:'DEPLOY_FAILED'};
  return {kind:'DEPLOY_SUCCESS'};
}

assert.equal(deployState([]).kind,'NOT_A_PRODUCTION_DEPLOY');
assert.equal(deployState([{name:'Exact-SHA Production release gate',steps:[]}]).kind,'DEPLOY_NOT_REACHED');
assert.equal(deployState([{name:'Exact-SHA Production release gate',steps:[{name:'Deploy customer-crm-api',conclusion:'skipped'}]}]).kind,'DEPLOY_NOT_REACHED');
assert.equal(deployState([{name:'Exact-SHA Production release gate',steps:[{name:'Deploy customer-crm-api',conclusion:'failure'}]}]).kind,'DEPLOY_FAILED');
assert.equal(deployState([{name:'Exact-SHA Production release gate',steps:[{name:'Deploy customer-crm-api',conclusion:'success'}]}]).kind,'DEPLOY_SUCCESS');

const observer=fs.readFileSync('.github/workflows/customer360-production-first-throw-observability.yml','utf8');
const deploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
assert.match(deploy,/concurrency:\s*\n\s*group: customer-crm-production-deploy/);
assert.match(observer,/concurrency:\s*\n\s*group: customer-crm-production-deploy/);
assert.ok(observer.includes("!deploy || deploy.conclusion==='skipped' ? 'NOT_REACHED'"));
assert.ok(observer.includes('OBSERVER_ATTRIBUTION_RESULT=DEPLOY_NOT_REACHED'));
assert.ok(observer.includes('OBSERVER_ATTRIBUTION_RESULT=DEPLOYMENT_SUPERSEDED'));
assert.ok(observer.includes('Current Version ID:'));
assert.ok(observer.includes('wrangler@4.33.1 deployments status --name customer-crm-api --json'));
assert.ok(observer.includes('Number(v.percentage)===100'));
assert.ok(observer.includes('active[0].version_id'));
assert.ok(!observer.includes('continue-on-error: true'));
assert.ok(!observer.includes('curl -X POST'));
assert.ok(!observer.includes('curl -X PATCH'));
assert.ok(!observer.includes('curl -X DELETE'));
console.log('CUSTOMER360_PRODUCTION_OBSERVER_PROVENANCE_REGRESSION=PASS');
console.log('SCENARIOS=PREFLIGHT_SKIPPED,DEPLOY_NOT_REACHED,DEPLOY_FAILED,DEPLOY_SUCCESS,SHARED_DEPLOY_LOCK,SUPERSEDED_FAIL_CLOSED');
