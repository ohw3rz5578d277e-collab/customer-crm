import assert from 'node:assert/strict';
import fs from 'node:fs';

const observer=fs.readFileSync('.github/workflows/customer360-production-first-throw-observability.yml','utf8');
const deploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');

assert.match(deploy,/concurrency:\s*\n\s*group: customer-crm-production-deploy/,'canonical Production release must keep its serialization lock');
assert.ok(observer.includes("github.event.workflow_run.event == 'workflow_dispatch'"),'observer lock must be conditioned on canonical workflow_dispatch');
assert.ok(observer.includes("github.event.workflow_run.head_branch == 'main'"),'observer Production lock must require main');
assert.ok(observer.includes("'customer-crm-production-deploy'"),'real Production observer must share canonical deploy lock');
assert.ok(observer.includes("format('customer360-observer-nondeploy-{0}', github.event.workflow_run.id)"),'non-production observer shells must use a run-unique lock');
assert.ok(observer.includes('cancel-in-progress: false'),'observer must never cancel a Production deployment');
assert.ok(observer.includes("if: ${{ github.event.workflow_run.event == 'workflow_dispatch' && github.event.workflow_run.head_branch == 'main' }}"),'health observer job must remain deploy-run-only');

console.log('CUSTOMER360_PRODUCTION_OBSERVER_CONCURRENCY_ISOLATION=PASS');
console.log('PRODUCTION_DEPLOY_LOCK_PRESERVED=PASS');
console.log('PR_OBSERVER_PENDING_SLOT_INTERFERENCE=0');
