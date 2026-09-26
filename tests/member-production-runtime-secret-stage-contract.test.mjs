import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-runtime-secret-stage.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-runtime-secret-stage-from-issue.yml','utf8');

assert.match(workflow,/workflow_dispatch:\s*\n\s*inputs:\s*\n\s*expected_sha:/);
for(const input of ['readiness_run_id','owner_comment_id','confirmation']){
  assert.ok(workflow.includes(input+':'), 'missing workflow input: '+input);
}
assert.ok(workflow.includes("STAGE_MEMBER_RUNTIME_SECRETS"));
assert.ok(workflow.includes('Checkout exact authorized SHA'));
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'));
assert.ok(workflow.includes('MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA'));
assert.ok(workflow.includes('OWNER_AUTHORIZATION_COMMENT=PASS'));
assert.ok(workflow.includes("'.github/workflows/member-production-runtime-readiness.yml'"));
assert.ok(workflow.includes('MEMBER_RUNTIME_READINESS_RECEIPT=PASS'));
assert.ok(workflow.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=false;'));
assert.ok(workflow.includes('MEMBER_PRODUCTION_ROUTE_MODE=NOT_ENABLED'));
assert.ok(workflow.includes('MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE=NOT_ENABLED'));

assert.match(workflow,/secrets\.MEMBER_LINE_LOGIN_CHANNEL_SECRET/);
for(const generated of [
  'MEMBER_SESSION_SECRET',
  'MEMBER_LINE_LOGIN_TRANSACTION_SECRET',
  'MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET'
]){
  assert.ok(workflow.includes(generated+':crypto.randomBytes(48).toString(\'base64url\')'), 'generated secret missing: '+generated);
}
assert.ok(workflow.includes('MEMBER_RUNTIME_SECRET_INPUT_COUNT=4'));
assert.ok(workflow.includes('MEMBER_RUNTIME_GENERATED_SECRET_COUNT=3'));
assert.ok(workflow.includes('MEMBER_RUNTIME_EXTERNAL_SECRET_COUNT=1'));
assert.ok(workflow.includes('SECRET_VALUES_PRINTED=NO'));
assert.ok(workflow.includes('mode:0o600'));

assert.ok(workflow.includes('wrangler versions secret bulk /tmp/member-runtime-secrets.json'));
assert.ok(workflow.includes('wrangler versions list --name customer-crm-api --json'));
assert.ok(workflow.includes('wrangler versions view "$STAGED_VERSION_ID" --name customer-crm-api --json'));
assert.ok(workflow.includes('wrangler deployments status --name customer-crm-api --json'));
assert.ok(workflow.includes('STAGED_VERSION_DELTA_NOT_EXACTLY_ONE'));
assert.ok(workflow.includes('PRODUCTION_DEPLOYMENT_CHANGED_DURING_SECRET_STAGE'));
assert.ok(workflow.includes('STAGED_SECRET_BINDINGS=4'));
assert.ok(workflow.includes('STAGED_EXISTING_REQUIRED_SECRET_BINDINGS=PASS'));
assert.ok(workflow.includes('STAGED_REQUIRED_RESOURCE_BINDINGS=PASS'));
assert.ok(workflow.includes('function canonical(value)'));
assert.ok(workflow.includes('PRODUCTION_DEPLOYMENT_UNCHANGED=PASS'));
assert.ok(workflow.includes('TEMP_SECRET_MATERIAL_REMOVED=YES'));

for(const name of [
  'MEMBER_SESSION_SECRET',
  'MEMBER_LINE_LOGIN_TRANSACTION_SECRET',
  'MEMBER_LINE_LOGIN_CHANNEL_SECRET',
  'MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET'
]){
  assert.ok(workflow.includes(name), 'required Member secret name missing: '+name);
}

for(const zero of [
  'PRODUCTION_TRAFFIC_CHANGE=0',
  'PRODUCTION_DEPLOY=0',
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
assert.doesNotMatch(workflow,/\bwrangler(?:@[^\s]+)?\s+versions\s+deploy\b/i);
assert.doesNotMatch(workflow,/\bwrangler(?:@[^\s]+)?\s+secret\s+(put|bulk|delete)\b/i);
assert.doesNotMatch(workflow,/d1\s+(execute|migrations|export|import)\b/i);

assert.ok(bridge.includes("github.event.issue.number == 26"));
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("github.event.comment.user.login == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("command_re='^/member-runtime-secret-stage sha=([0-9a-f]{40}) readiness_run=([0-9]+) confirm=STAGE_MEMBER_RUNTIME_SECRETS$'"));
assert.ok(bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha'));
assert.ok(bridge.includes('deploy-cloudflare.yml member-production-runtime-readiness.yml member-production-runtime-secret-stage.yml'));
assert.ok(bridge.includes('MEMBER_RUNTIME_SECRET_STAGE_LOCK_OCCUPIED'));
assert.ok(bridge.includes('member-production-runtime-secret-stage.yml/dispatches'));
assert.ok(bridge.includes('PRODUCTION_DEPLOY=0'));
assert.ok(bridge.includes('PRODUCTION_TRAFFIC_CHANGE=0'));
assert.ok(bridge.includes('PRODUCTION_D1_WRITE=0'));
assert.doesNotMatch(bridge,/deploy-cloudflare\.yml\/dispatches/);
assert.doesNotMatch(bridge,/member-production-runtime-readiness\.yml\/dispatches/);

console.log('MEMBER_PRODUCTION_RUNTIME_SECRET_STAGE_CONTRACT=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_SECRET_STAGE_OWNER_GATE=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_SECRET_STAGE_NO_TRAFFIC_CHANGE=PASS');
