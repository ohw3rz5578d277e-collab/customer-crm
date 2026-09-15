import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');

assert.ok(
  workflow.includes('wrangler@4.131.2 deploy --dry-run'),
  'Owner auth preflight must use a Wrangler version that supports secrets-file'
);

assert.ok(
  workflow.includes("wranglerVersion: '4.131.2'"),
  'Production deploy must pin the same secrets-file capable Wrangler version'
);

assert.ok(
  workflow.includes('deploy --secrets-file /tmp/customer-crm-owner-secrets.json'),
  'Production deploy must upload Owner auth secrets atomically with code'
);

console.log('OWNER_AUTH_SECRETS_FILE_WRANGLER=PASS');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_WRITE=0');
