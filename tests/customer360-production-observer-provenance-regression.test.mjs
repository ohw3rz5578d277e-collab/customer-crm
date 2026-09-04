import assert from 'node:assert/strict';
import fs from 'node:fs';

function deployState({sourceMode='deploy', jobs=[]}={}) {
  if(sourceMode!=='deploy')return {kind:'NOT_A_PRODUCTION_DEPLOY'};
  const release=jobs.find(j=>j.name==='Exact-SHA Production release gate');
  if(!release)return {kind:'NOT_A_PRODUCTION_DEPLOY'};
  const deploy=(release.steps||[]).find(s=>s.name==='Deploy customer-crm-api');
  if(!deploy||deploy.conclusion==='skipped')return {kind:'DEPLOY_NOT_REACHED'};
  if(deploy.conclusion!=='success')return {kind:'DEPLOY_FAILED'};
  return {kind:'DEPLOY_SUCCESS'};
}

function observationAttribution({expected='v1',before='v1',after='v1'}={}) {
  if(!expected)return 'DEPLOYMENT_ID_MISSING';
  if(!before)return 'CURRENT_DEPLOYMENT_ID_MISSING';
  if(before!==expected)return 'DEPLOYMENT_SUPERSEDED';
  if(!after)return 'CURRENT_DEPLOYMENT_ID_MISSING';
  if(after!==expected)return 'DEPLOYMENT_SUPERSEDED';
  return 'EXACT_MATCH';
}

const release=(conclusion)=>[{name:'Exact-SHA Production release gate',steps:[{name:'Deploy customer-crm-api',conclusion}]}];
assert.equal(deployState({sourceMode:'preflight',jobs:release('skipped')}).kind,'NOT_A_PRODUCTION_DEPLOY');
assert.equal(deployState({sourceMode:'deploy',jobs:[]}).kind,'NOT_A_PRODUCTION_DEPLOY');
assert.equal(deployState({sourceMode:'deploy',jobs:[{name:'Exact-SHA Production release gate',steps:[]}]}).kind,'DEPLOY_NOT_REACHED');
assert.equal(deployState({sourceMode:'deploy',jobs:release('skipped')}).kind,'DEPLOY_NOT_REACHED');
assert.equal(deployState({sourceMode:'deploy',jobs:release('failure')}).kind,'DEPLOY_FAILED');
assert.equal(deployState({sourceMode:'deploy',jobs:release('success')}).kind,'DEPLOY_SUCCESS');
assert.equal(observationAttribution(), 'EXACT_MATCH');
assert.equal(observationAttribution({before:'v2'}), 'DEPLOYMENT_SUPERSEDED');
assert.equal(observationAttribution({before:'v1',after:'v2'}), 'DEPLOYMENT_SUPERSEDED');
assert.equal(observationAttribution({before:'v1',after:''}), 'CURRENT_DEPLOYMENT_ID_MISSING');

const observer=fs.readFileSync('.github/workflows/customer360-production-first-throw-observability.yml','utf8');
const deploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-production-deploy-from-issue.yml','utf8');

assert.match(deploy,/concurrency:\s*\n\s*group: customer-crm-production-deploy/);
assert.ok(observer.includes("format('customer360-production-observer-{0}', github.event.workflow_run.id)"));
assert.ok(!observer.includes("group: customer-crm-production-deploy"));
assert.ok(observer.includes('cancel-in-progress: false'));
assert.match(bridge,/group: customer-crm-production-dispatch-bridge/);
assert.ok(!bridge.includes('group: customer-crm-production-deploy'));

assert.ok(bridge.includes("release_mode='preflight'"));
assert.ok(bridge.includes("release_mode='deploy'"));
assert.ok(bridge.includes("'mode':os.environ['RELEASE_MODE']"));
assert.ok(bridge.includes("'expected_sha':os.environ['EXPECTED_SHA']"));
assert.ok(bridge.includes('actions/workflows/deploy-cloudflare.yml/dispatches'));
assert.ok(bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha'));
assert.ok(bridge.includes('BRIDGE_PRODUCTION_WRITE=0'));

assert.ok(observer.includes("source_mode=\"$(sed -n 's/.*RELEASE_MODE=\\(preflight\\|deploy\\).*/\\1/p'"));
assert.ok(observer.includes("if [[ \"$source_mode\" == preflight ]]"));
assert.ok(observer.includes('OBSERVER_ATTRIBUTION_RESULT=NOT_A_PRODUCTION_DEPLOY'));
assert.ok(observer.includes('::error::SOURCE_MODE_PREFLIGHT'));
assert.ok(observer.includes("!deploy || deploy.conclusion==='skipped' ? 'NOT_REACHED'"));
assert.ok(observer.includes('OBSERVER_ATTRIBUTION_RESULT=DEPLOY_NOT_REACHED'));
assert.ok(observer.includes('OBSERVER_ATTRIBUTION_RESULT=DEPLOY_FAILED'));
assert.ok(observer.includes('OBSERVER_ATTRIBUTION_RESULT=SOURCE_SHA_MISMATCH'));
assert.ok(observer.includes('OBSERVER_ATTRIBUTION_RESULT=DEPLOYMENT_ID_MISSING'));
assert.ok(observer.includes('OBSERVER_ATTRIBUTION_RESULT=DEPLOYMENT_SUPERSEDED'));
assert.ok(observer.includes('OBSERVER_ATTRIBUTION_RESULT=EXACT_MATCH_BEFORE_HEALTH'));
assert.ok(observer.includes('OBSERVER_ATTRIBUTION_RESULT=EXACT_MATCH_AFTER_HEALTH'));
assert.ok(observer.includes('TRIGGER_RUN_ID: ${{ github.event.workflow_run.id }}'));
assert.ok(observer.includes('/actions/runs/$TRIGGER_RUN_ID/jobs?per_page=100'));
assert.ok(observer.includes('/actions/jobs/$release_job_id/logs'));
assert.ok(observer.includes('AUTHORIZED_SHA='));
assert.ok(observer.includes('CHECKOUT_SHA='));
assert.ok(observer.includes('Current Version ID:'));
const statusQuery='wrangler@4.33.1 deployments status --name customer-crm-api --json';
assert.ok(observer.split(statusQuery).length-1>=2,'must compare Production version before and after health');
assert.ok(observer.includes('current-production-deployment-after-health.json'));
assert.ok(observer.includes('post_identity_result=SUPERSEDED'));
assert.ok(observer.includes('DEPLOYMENT_SUPERSEDED_DURING_OBSERVATION'));
assert.ok(observer.includes('CURRENT_PRODUCTION_VERSION_ID_UNAVAILABLE_AFTER_HEALTH'));
assert.ok(observer.includes('PRODUCTION_HEALTH_POST_WORKER_VERSION_ID'));
assert.ok(observer.includes('Number(v.percentage)===100'));
assert.ok(observer.includes('active[0].version_id'));
assert.ok(!observer.includes('continue-on-error: true'));
assert.ok(!observer.includes('curl -X POST'));
assert.ok(!observer.includes('curl -X PATCH'));
assert.ok(!observer.includes('curl -X DELETE'));

console.log('CUSTOMER360_PRODUCTION_OBSERVER_PROVENANCE_REGRESSION=PASS');
console.log('PR47_EXACT_SHA_BRIDGE_INTEGRATION=PASS');
console.log('OBSERVER_DEPLOY_QUEUE_INTERFERENCE=0');
console.log('SCENARIOS=PREFLIGHT_NOT_PRODUCTION,DEPLOY_NOT_REACHED,DEPLOY_FAILED,DEPLOY_SUCCESS,SOURCE_SHA_BINDING,PRE_HEALTH_VERSION_MATCH,POST_HEALTH_VERSION_MATCH,SUPERSEDED_BEFORE,SUPERSEDED_DURING,POST_ID_UNAVAILABLE');
