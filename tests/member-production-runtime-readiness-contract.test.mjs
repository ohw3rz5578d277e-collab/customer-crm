import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-runtime-readiness.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-runtime-readiness-from-issue.yml','utf8');

assert.match(workflow,/workflow_dispatch:\s*\n\s*inputs:\s*\n\s*expected_sha:/);
assert.ok(workflow.includes('Checkout exact authorized SHA'));
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'));
assert.ok(workflow.includes('MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA'));
assert.ok(workflow.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=false;'));
assert.ok(workflow.includes('MEMBER_PRODUCTION_ROUTE_MODE_MUST_REMAIN_OFF_DURING_READINESS'));
assert.ok(workflow.includes('PRIVATE_MEDIA_ROUTE_MODE_MUST_REMAIN_OFF_DURING_READINESS'));
assert.ok(workflow.includes('actions/workflows/deploy-cloudflare.yml/runs?event=workflow_dispatch&status=completed&per_page=50'));
assert.ok(workflow.includes('PRODUCTION_RUNTIME_EXACT_MAIN_DEPLOY_RECEIPT_VERIFIED='));
assert.ok(workflow.includes('wrangler secret list --name customer-crm-api --format json'));
for(const name of [
  'MEMBER_SESSION_SECRET',
  'MEMBER_LINE_LOGIN_TRANSACTION_SECRET',
  'MEMBER_LINE_LOGIN_CHANNEL_SECRET',
  'MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET'
]){
  assert.ok(workflow.includes(name),`required Member secret name missing: ${name}`);
}
assert.ok(workflow.includes('SECRET_VALUES_PRINTED=NO'));
assert.ok(workflow.includes('d1 migrations list customer-crm-db --remote'));
assert.ok(workflow.includes('d1 execute customer-crm-db --remote --json --command "SELECT name,type FROM sqlite_master'));
assert.ok(workflow.includes('MEMBER_SCHEMA_RUNTIME_RECEIPT=ALREADY_APPLIED_CONFIRMED'));
assert.ok(workflow.includes('MEMBER_PENDING_MIGRATION_COUNT=0'));
assert.ok(workflow.includes('MEMBER_SCHEMA_TABLE_COUNT=11'));
assert.ok(workflow.includes('MEMBER_SCHEMA_TRACKED_MIGRATION_COUNT=9'));

for(const zero of [
  'MEMBER_PRODUCTION_OWNER_APPROVED_ACTIVATION=0',
  'MEMBER_PRODUCTION_ROUTE_MODE_ENABLEMENT=0',
  'MEMBER_SESSION_PRODUCTION_ACTIVATION=0',
  'LINE_LOGIN_PRODUCTION_ACTIVATION=0',
  'PRIVATE_MEDIA_PRODUCTION_BINDING_CHANGE=0',
  'PUBLIC_ASSET_PRODUCTION_BINDING_CHANGE=0',
  'PRODUCTION_D1_WRITE=0',
  'PRODUCTION_DEPLOY=0',
  'CRM_WRITE=0',
  'LINE_SEND=0'
]){
  assert.ok(workflow.includes(zero),`missing zero declaration: ${zero}`);
}

assert.doesNotMatch(workflow,/\bwrangler(?:@[^\s]+)?\s+deploy\b/i);
assert.doesNotMatch(workflow,/d1\s+migrations\s+apply/i);
assert.doesNotMatch(workflow,/d1\s+execute[^\n]*\b(INSERT|UPDATE|DELETE|REPLACE|DROP|ALTER|CREATE)\b/i);
assert.doesNotMatch(workflow,/request\.(post|put|patch|delete)\s*\(/i);

assert.ok(bridge.includes("github.event.issue.number == 26"));
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("github.event.comment.user.login == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("command_re='^/member-runtime-readiness sha=([0-9a-f]{40})$'"));
assert.ok(bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha'));
assert.ok(bridge.includes('deploy-cloudflare.yml member-production-runtime-readiness.yml'));
assert.ok(bridge.includes('PRODUCTION_READINESS_LOCK_OCCUPIED'));
assert.ok(bridge.includes('member-production-runtime-readiness.yml/dispatches'));
assert.ok(bridge.includes('PRODUCTION_WRITE=0'));
assert.ok(bridge.includes('PRODUCTION_DEPLOY=0'));
assert.doesNotMatch(bridge,/deploy-cloudflare\.yml\/dispatches/);

console.log('MEMBER_PRODUCTION_RUNTIME_READINESS_CONTRACT=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_READINESS_READ_ONLY=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_READINESS_OWNER_GATE=PASS');
