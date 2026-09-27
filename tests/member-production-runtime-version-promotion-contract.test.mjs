import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-runtime-version-promotion.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-runtime-version-promotion-from-issue.yml','utf8');

assert.match(workflow,/workflow_dispatch:\s*\n\s*inputs:\s*\n\s*expected_sha:/);
for(const input of ['staged_version_id','stage_run_id','owner_comment_id','confirmation']){
  assert.ok(workflow.includes(input+':'), 'missing workflow input: '+input);
}
assert.ok(workflow.includes('PROMOTE_MEMBER_RUNTIME_VERSION'));
assert.ok(workflow.includes('Checkout exact authorized SHA'));
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'));
assert.ok(workflow.includes('MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA'));
assert.ok(workflow.includes('OWNER_AUTHORIZATION_COMMENT=PASS'));
assert.ok(workflow.includes("'.github/workflows/member-production-runtime-secret-stage.yml'"));
assert.ok(workflow.includes('MEMBER_RUNTIME_SECRET_STAGE_RECEIPT=PASS'));
assert.ok(workflow.includes('/actions/runs/$STAGE_RUN_ID/jobs?per_page=100'));
assert.ok(workflow.includes('/actions/jobs/$stage_job_id/logs'));
assert.ok(workflow.includes('MEMBER_RUNTIME_SECRET_STAGED_VERSION_ID=$STAGED_VERSION_ID'));
assert.ok(workflow.includes('STAGE_RUN_STAGED_VERSION_ID=PASS'));
assert.ok(workflow.includes('STAGE_RUN_PRODUCTION_TRAFFIC_UNCHANGED_RECEIPT=PASS'));
assert.ok(workflow.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=false;'));
assert.ok(workflow.includes('MEMBER_PRODUCTION_ROUTE_MODE=NOT_ENABLED'));
assert.ok(workflow.includes('MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE=NOT_ENABLED'));

assert.ok(workflow.includes('wrangler versions view "$STAGED_VERSION_ID" --name customer-crm-api --json'));
assert.ok(workflow.includes('STAGED_VERSION_EXACT_SHA_MESSAGE=PASS'));
assert.ok(workflow.includes('STAGED_SECRET_BINDINGS=4'));
assert.ok(workflow.includes('STAGED_EXISTING_REQUIRED_SECRET_BINDINGS=PASS'));
assert.ok(workflow.includes('STAGED_REQUIRED_RESOURCE_BINDINGS=PASS'));
assert.ok(workflow.includes('SECRET_VALUES_PRINTED=NO'));

assert.ok(workflow.includes('wrangler deployments status --name customer-crm-api --json'));
assert.ok(workflow.includes('PRODUCTION_BASELINE_NOT_SINGLE_VERSION'));
assert.ok(workflow.includes('PRODUCTION_BASELINE_NOT_100_PERCENT'));
assert.ok(workflow.includes('STAGED_VERSION_ALREADY_ACTIVE'));
assert.ok(workflow.includes('PRODUCTION_BASELINE_SINGLE_VERSION=PASS'));
assert.ok(workflow.includes('PRODUCTION_BASELINE_100_PERCENT=PASS'));
assert.ok(workflow.includes('STAGED_VERSION_NOT_ACTIVE=PASS'));

assert.ok(workflow.includes('wrangler versions deploy "${STAGED_VERSION_ID}@100%"'));
assert.ok(workflow.includes('--name customer-crm-api'));
assert.ok(workflow.includes('-y'));
assert.ok(workflow.includes('MEMBER_RUNTIME_VERSION_PROMOTION_COMMAND=PASS'));
assert.ok(workflow.includes('POST_PROMOTION_STAGED_VERSION_100_PERCENT=PASS'));
assert.ok(workflow.includes('PRODUCTION_TRAFFIC_TARGET_EXACT=PASS'));

assert.ok(workflow.includes('https://customer-crm-api.ohw3rz5578d277e.workers.dev/health'));
assert.ok(workflow.includes('PRODUCTION_HEALTH_HTTP_STATUS=200'));
assert.ok(workflow.includes('PRODUCTION_RELEASE_SHA_BODY=PASS'));
assert.ok(workflow.includes('PRODUCTION_RELEASE_SHA_HEADER=PASS'));
assert.ok(workflow.includes('POST_PROMOTION_HTTP_READ_ONLY=PASS'));
assert.ok(workflow.includes('TEMP_PROMOTION_SNAPSHOT_MATERIAL_REMOVED=YES'));

for(const zero of [
  'MEMBER_PRODUCTION_OWNER_APPROVED_ACTIVATION=0',
  'MEMBER_PRODUCTION_ROUTE_MODE_ENABLEMENT=0',
  'MEMBER_SESSION_PRODUCTION_ACTIVATION=0',
  'LINE_LOGIN_PRODUCTION_ACTIVATION=0',
  'PRIVATE_MEDIA_PRODUCTION_BINDING_CHANGE=0',
  'PUBLIC_ASSET_PRODUCTION_BINDING_CHANGE=0',
  'PRODUCTION_D1_WRITE=0',
  'CRM_WRITE=0',
  'LINE_SEND=0',
  'CUSTOMER_ID_GENERATION=0',
  'COMMERCE_ACTIVATION=0'
]){
  assert.ok(workflow.includes(zero), 'missing zero declaration: '+zero);
}

assert.doesNotMatch(workflow,/\bwrangler(?:@[^\s]+)?\s+deploy\b/i);
assert.doesNotMatch(workflow,/\bwrangler(?:@[^\s]+)?\s+secret\s+(put|bulk|delete)\b/i);
assert.doesNotMatch(workflow,/\bwrangler(?:@[^\s]+)?\s+versions\s+secret\s+bulk\b/i);
assert.doesNotMatch(workflow,/d1\s+(execute|migrations|export|import)\b/i);
assert.equal((workflow.match(/wrangler versions deploy/g)||[]).length,1,'promotion workflow must have exactly one traffic mutation command');

assert.ok(bridge.includes("github.event.issue.number == 26"));
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("github.event.comment.user.login == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("command_re='^/member-runtime-version-promote sha=([0-9a-f]{40}) version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) stage_run=([0-9]+) confirm=PROMOTE_MEMBER_RUNTIME_VERSION$'"));
assert.ok(bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha'));
assert.ok(bridge.includes('member-production-runtime-version-promotion.yml'));
assert.ok(bridge.includes('MEMBER_RUNTIME_VERSION_PROMOTION_LOCK_OCCUPIED'));
assert.ok(bridge.includes('member-production-runtime-version-promotion.yml/dispatches'));
assert.ok(bridge.includes('PRODUCTION_D1_WRITE=0'));
assert.ok(bridge.includes('CRM_WRITE=0'));
assert.ok(bridge.includes('LINE_SEND=0'));
assert.doesNotMatch(bridge,/wrangler versions deploy/);
assert.doesNotMatch(bridge,/deploy-cloudflare\.yml\/dispatches/);
assert.doesNotMatch(bridge,/member-production-runtime-secret-stage\.yml\/dispatches/);

console.log('MEMBER_PRODUCTION_RUNTIME_VERSION_PROMOTION_CONTRACT=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_VERSION_PROMOTION_OWNER_GATE=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_VERSION_PROMOTION_EXACT_VERSION=PASS');
