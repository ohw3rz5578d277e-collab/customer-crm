import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync(new URL('../.github/workflows/deploy-cloudflare.yml',import.meta.url),'utf8');

assert.match(
  workflow,
  /group:\s*\$\{\{\s*github\.event_name\s*==\s*'workflow_dispatch'\s*&&\s*'customer-crm-production-deploy'\s*\|\|\s*format\('customer-crm-release-pr-\{0\}',\s*github\.event\.pull_request\.number\)\s*\}\}/,
  'workflow_dispatch must retain the canonical production concurrency group while PR CI uses a distinct group'
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
console.log('PRODUCTION_PENDING_SLOT_REPLACEMENT_BY_PR=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
