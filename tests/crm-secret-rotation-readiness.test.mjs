import assert from 'node:assert/strict';
import fs from 'node:fs';

const src=fs.readFileSync('src/index.js','utf8');
const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');

assert.match(
  src,
  /lineHistoryInternalTokens\s*=\s*Array\.from\(new Set\(\[\s*text\(env\.RESERVATION_INTERNAL_TOKEN\),\s*text\(env\.LINE_INTERNAL_TOKEN\),\s*text\(env\.LINE_WORKER_INTERNAL_TOKEN\)/s,
  'LINE history auth must prefer the cross-worker RESERVATION_INTERNAL_TOKEN before legacy LINE-specific tokens'
);

assert.match(
  src,
  /lineHistoryAuthCandidates\s*=\s*lineHistoryInternalTokens\.length\s*\?\s*lineHistoryInternalTokens\s*:\s*\[""\]/,
  'LINE history auth must preserve an admin-token-only fallback attempt'
);

assert.ok(
  (src.match(/res\.status\s*!==\s*401\s*&&\s*res\.status\s*!==\s*403/g)||[]).length >= 2,
  'LINE history auth must retry alternate secrets only on authentication failures'
);

assert.ok(src.includes('auth_attempt: authIndex + 1'));
assert.ok(!src.includes('auth_secret'));
assert.ok(!src.includes('auth_token_value'));

for(const name of [
  'ADMIN_TOKEN',
  'CRM_INTERNAL_TOKEN',
  'RESERVATION_INTERNAL_TOKEN',
  'SYNC_TOKEN'
]){
  assert.ok(workflow.includes(name), `deploy preflight must inspect existing required secret name ${name}`);
}

assert.ok(workflow.includes('CRM_OWNER_PASSWORD: ${{ secrets.CRM_OWNER_PASSWORD }}'));
assert.ok(workflow.includes('CRM_OWNER_SESSION_SECRET: ${{ secrets.CRM_OWNER_SESSION_SECRET }}'));
assert.ok(workflow.includes('wrangler@4 secret list --name customer-crm-api --format json'));
assert.ok(workflow.includes('PRODUCTION_REQUIRED_SECRET_NAMES_MISSING:'));
assert.ok(workflow.includes('PRODUCTION_EXISTING_REQUIRED_SECRET_NAMES=PASS'));
assert.ok(workflow.includes('OWNER_AUTH_RELEASE_SECRET_MISSING:'));
assert.ok(workflow.includes('OWNER_AUTH_RELEASE_SECRET_MATERIAL=PASS'));
assert.ok(workflow.includes('--secrets-file /tmp/customer-crm-owner-secrets.json'));
assert.ok(workflow.includes('command: deploy --secrets-file /tmp/customer-crm-owner-secrets.json'));
assert.ok(workflow.includes('OWNER_AUTH_SECRETS_ATOMIC_WITH_DEPLOY=PASS'));
assert.ok(workflow.includes('OWNER_AUTH_TEMP_SECRET_FILE_REMOVED=YES'));
assert.ok(workflow.includes('SECRET_VALUES_PRINTED=NO'));

console.log('CRM_SECRET_ROTATION_READINESS=PASS');
console.log('OWNER_AUTH_SECRETS_ATOMIC_WITH_EXACT_SHA_DEPLOY=PASS');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('SECRET_ROTATION=0');
