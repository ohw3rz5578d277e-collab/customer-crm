import assert from 'node:assert/strict';
import fs from 'node:fs';

const observer=fs.readFileSync('.github/workflows/customer360-production-first-throw-observability.yml','utf8');
const deploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');

assert.match(deploy,/concurrency:\s*\n\s*group: customer-crm-production-deploy/,'canonical Production release must keep its serialization lock');
assert.ok(observer.includes("format('customer360-production-observer-{0}', github.event.workflow_run.id)"),'each observer must have a run-unique concurrency group');
assert.ok(!observer.includes('group: customer-crm-production-deploy'),'observer must never occupy the Production deploy concurrency pending slot');
assert.ok(observer.includes('cancel-in-progress: false'),'observer must not cancel another observer');
assert.ok(observer.includes("if: ${{ github.event.workflow_run.event == 'workflow_dispatch' && github.event.workflow_run.head_branch == 'main' }}"),'health observer job must remain canonical dispatch/main only');
const statusQuery='wrangler@4.33.1 deployments status --name customer-crm-api --json';
assert.ok(observer.split(statusQuery).length-1>=2,'immutable Production version must be queried before and after health');
assert.ok(observer.includes('EXACT_MATCH_BEFORE_HEALTH'));
assert.ok(observer.includes('current-production-deployment-after-health.json'));
assert.ok(observer.includes('DEPLOYMENT_SUPERSEDED_DURING_OBSERVATION'));
assert.ok(observer.includes('EXACT_MATCH_AFTER_HEALTH'));

console.log('CUSTOMER360_PRODUCTION_OBSERVER_CONCURRENCY_ISOLATION=PASS');
console.log('PRODUCTION_DEPLOY_LOCK_PRESERVED=PASS');
console.log('OBSERVER_DEPLOY_PENDING_SLOT_INTERFERENCE=0');
console.log('SUPERSCESSION_WINDOW_GUARD=PRE_AND_POST_VERSION_COMPARE');
