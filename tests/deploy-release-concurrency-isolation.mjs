import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync(new URL('../.github/workflows/deploy-cloudflare.yml',import.meta.url),'utf8');

const deployConcurrency="group: ${{ github.event_name == 'workflow_dispatch' && inputs.mode == 'deploy' && 'customer-crm-production-deploy' || github.event_name == 'workflow_dispatch' && format('customer-crm-production-preflight-{0}', github.run_id) || format('customer-crm-release-pr-{0}', github.event.pull_request.number) }}";

assert.ok(
  workflow.includes(deployConcurrency),
  'deploy, preflight, and PR CI must retain isolated production concurrency groups'
);

assert.ok(
  workflow.includes("inputs.mode == 'deploy' && 'customer-crm-production-deploy'"),
  'only deploy dispatches may occupy the canonical production mutation pending slot'
);

assert.ok(
  workflow.includes("format('customer-crm-production-preflight-{0}', github.run_id)"),
  'read-only preflight must use a run-unique non-mutating pending slot'
);

assert.ok(
  !workflow.includes("github.event_name == 'workflow_dispatch' && 'customer-crm-production-deploy' ||"),
  'workflow_dispatch must not blanket-share the canonical production mutation pending slot'
);

assert.match(
  workflow,
  /cancel-in-progress:\s*\$\{\{\s*github\.event_name\s*==\s*'pull_request'\s*\}\}/,
  'only stale PR validation runs may be cancelled'
);

assert.doesNotMatch(
  workflow,
  /concurrency:\s*\n\s*group:\s*customer-crm-production-deploy\s*\n\s*cancel-in-progress:\s*false/,
  'PR validation must not share the canonical production pending slot'
);

assert.match(workflow,/if:\s*\$\{\{\s*github\.event_name\s*==\s*'pull_request'\s*\}\}/,'PR regression gate must remain PR-only');
assert.match(workflow,/if:\s*\$\{\{[\s\S]*github\.event_name\s*==\s*'workflow_dispatch'/,'canonical release gate must remain workflow_dispatch-only');

console.log('DEPLOY_RELEASE_CONCURRENCY_ISOLATION=PASS');
console.log('PRODUCTION_DEPLOY_LOCK_PRESERVED=PASS');
console.log('PRODUCTION_PREFLIGHT_PENDING_SLOT_ISOLATION=PASS');
console.log('PRODUCTION_PENDING_SLOT_REPLACEMENT_BY_PR=0');
console.log('PRODUCTION_PENDING_SLOT_REPLACEMENT_BY_PREFLIGHT=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
