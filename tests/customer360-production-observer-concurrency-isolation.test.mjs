import assert from 'node:assert/strict';
import fs from 'node:fs';

const observer=fs.readFileSync('.github/workflows/customer360-production-first-throw-observability.yml','utf8');
const deploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');

const deployConcurrency="group: ${{ github.event_name == 'workflow_dispatch' && inputs.mode == 'deploy' && 'customer-crm-production-deploy' || github.event_name == 'workflow_dispatch' && format('customer-crm-production-preflight-{0}', github.run_id) || format('customer-crm-release-pr-{0}', github.event.pull_request.number) }}";
assert.ok(deploy.includes(deployConcurrency),'Production deploy must keep the shared mutation lock while read-only preflight and PR CI use isolated pending slots');
assert.ok(deploy.includes("inputs.mode == 'deploy' && 'customer-crm-production-deploy'"),'only deploy dispatches may occupy the shared Production mutation lock');
assert.ok(deploy.includes("format('customer-crm-production-preflight-{0}', github.run_id)"),'read-only Production preflight must use a run-unique non-mutating concurrency slot');
assert.ok(!deploy.includes("github.event_name == 'workflow_dispatch' && 'customer-crm-production-deploy' ||"),'workflow_dispatch must not blanket-share the Production mutation pending slot');
assert.ok(observer.includes("format('customer360-production-observer-{0}', github.event.workflow_run.id)"),'each observer must have a run-unique concurrency group');
assert.ok(!observer.includes('group: customer-crm-production-deploy'),'observer must never occupy the Production deploy concurrency pending slot');
assert.ok(observer.includes('cancel-in-progress: false'),'observer must not cancel another observer');
assert.ok(observer.includes("if: ${{ github.event.workflow_run.event == 'workflow_dispatch' && github.event.workflow_run.head_branch == 'main' }}"),'health observer job must remain canonical dispatch/main only');
const statusQuery='wrangler@4.36.0 deployments status --name customer-crm-api --json';
assert.ok(observer.split(statusQuery).length-1>=2,'immutable Production version must be queried before and after health');
assert.ok(observer.includes('EXACT_MATCH_BEFORE_HEALTH'));
assert.ok(observer.includes('current-production-deployment-after-health.json'));
assert.ok(observer.includes('DEPLOYMENT_SUPERSEDED_DURING_OBSERVATION'));
assert.ok(observer.includes('EXACT_MATCH_AFTER_HEALTH'));

console.log('CUSTOMER360_PRODUCTION_OBSERVER_CONCURRENCY_ISOLATION=PASS');
console.log('PRODUCTION_DEPLOY_LOCK_PRESERVED=PASS');
console.log('PRODUCTION_PREFLIGHT_PENDING_SLOT_ISOLATION=PASS');
console.log('OBSERVER_DEPLOY_PENDING_SLOT_INTERFERENCE=0');
console.log('SUPERSCESSION_WINDOW_GUARD=PRE_AND_POST_VERSION_COMPARE');
