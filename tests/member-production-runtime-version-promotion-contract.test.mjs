import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-runtime-version-promotion.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-runtime-version-promotion-from-issue.yml','utf8');

assert.match(workflow,/workflow_dispatch:\s*\n\s*inputs:\s*\n\s*expected_sha:/);
for(const input of [
  'staged_version_id',
  'stage_run_id',
  'expected_active_version_id',
  'owner_comment_id',
  'bridge_run_id',
  'bridge_run_attempt',
  'confirmation'
]){
  assert.ok(workflow.includes(input+':'), 'missing workflow input: '+input);
}

assert.ok(workflow.includes('PROMOTE_MEMBER_RUNTIME_VERSION'));
assert.ok(workflow.includes('RUNTIME_PROMOTION_BRIDGE_RERUN_NOT_AUTHORIZED'));
assert.ok(workflow.includes('Checkout exact authorized SHA'));
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'));
assert.ok(workflow.includes('MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA'));
assert.ok(workflow.includes('CURRENT_MAIN_EXACT_GATE=PASS'));

assert.ok(workflow.includes('replace_active_version=$EXPECTED_ACTIVE_VERSION_ID'));
assert.ok(workflow.includes('OWNER_PROMOTION_AUTHORIZATION_COMMENT=PASS'));
assert.ok(workflow.includes('OWNER_PROMOTION_AUTHORIZATION_UNEDITED=PASS'));
assert.ok(workflow.includes("'.github/workflows/dispatch-member-production-runtime-version-promotion-from-issue.yml'"));
assert.ok(workflow.includes("'run_attempt':r.get('run_attempt')==1"));
assert.ok(workflow.includes('MEMBER_RUNTIME_PROMOTION_ISSUE_BRIDGE_RECEIPT=PASS'));

assert.ok(workflow.includes("'.github/workflows/member-production-runtime-secret-stage.yml'"));
assert.ok(workflow.includes('MEMBER_RUNTIME_SECRET_STAGE_RECEIPT=PASS'));
assert.ok(workflow.includes('/actions/runs/$STAGE_RUN_ID/jobs?per_page=100'));
assert.ok(workflow.includes('/actions/jobs/$stage_job_id/logs'));
assert.ok(workflow.includes('MEMBER_RUNTIME_SECRET_STAGED_VERSION_ID=$STAGED_VERSION_ID'));
assert.ok(workflow.includes('STAGE_RUN_STAGED_VERSION_ID=PASS'));
assert.ok(workflow.includes("grep -F 'PRODUCTION_DEPLOYMENT_UNCHANGED=PASS'"));
assert.ok(workflow.includes("grep -F 'PRODUCTION_TRAFFIC_CHANGE=0'"));

assert.ok(workflow.includes('CLOUDFLARE_AUTH=PASS'));
assert.ok(workflow.includes('wrangler versions view "$STAGED_VERSION_ID" --name customer-crm-api --json'));
assert.ok(workflow.includes('wrangler versions view "$EXPECTED_ACTIVE_VERSION_ID" --name customer-crm-api --json'));
assert.ok(workflow.includes('WORKER_SCRIPT_ETAG_MISSING'));
assert.ok(workflow.includes('STAGED_SCRIPT_ETAG_DIFFERS_FROM_OWNER_AUTHORIZED_ACTIVE'));
assert.ok(workflow.includes('STAGED_SCRIPT_ETAG_MATCHES_OWNER_AUTHORIZED_ACTIVE=PASS'));
assert.ok(workflow.includes('STAGED_SECRET_BINDINGS=4'));
assert.ok(workflow.includes('STAGED_EXISTING_REQUIRED_SECRET_BINDINGS=PASS'));
assert.ok(workflow.includes('STAGED_REQUIRED_RESOURCE_BINDINGS=PASS'));
assert.ok(workflow.includes('SECRET_VALUES_PRINTED=NO'));

assert.ok(workflow.includes('wrangler deployments status --name customer-crm-api --json'));
assert.ok(workflow.includes('PRODUCTION_BASELINE_NOT_SINGLE_VERSION'));
assert.ok(workflow.includes('PRODUCTION_BASELINE_NOT_100_PERCENT'));
assert.ok(workflow.includes('OWNER_AUTHORIZED_ACTIVE_VERSION_MISMATCH'));
assert.ok(workflow.includes('OWNER_AUTHORIZED_ACTIVE_VERSION_MATCH=PASS'));
assert.ok(workflow.includes('PRODUCTION_BASELINE_SINGLE_VERSION=PASS'));
assert.ok(workflow.includes('PRODUCTION_BASELINE_100_PERCENT=PASS'));
assert.ok(workflow.includes('STAGED_VERSION_ALREADY_ACTIVE'));

for(const header of [
  'CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID',
  'CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET',
  'x-admin-token: $ADMIN_TOKEN',
  "Accept: application/json"
]){
  assert.ok(workflow.includes(header), 'missing authenticated health header: '+header);
}
assert.ok(workflow.includes('PRE_PROMOTION_PRODUCTION_HEALTH_HTTP_STATUS=200'));
assert.ok(workflow.includes('PRE_PROMOTION_RELEASE_SHA_BODY=PASS'));
assert.ok(workflow.includes('PRE_PROMOTION_RELEASE_SHA_HEADER=PASS'));
assert.ok(workflow.includes('PRE_PROMOTION_HTTP_READ_ONLY=PASS'));

const preHealthStart=workflow.indexOf('- name: Verify pre-promotion Production health and exact release SHA read only');
const finalGateStart=workflow.indexOf('- name: Final current-main and active-version gate immediately before traffic mutation');
assert.ok(preHealthStart>=0 && finalGateStart>preHealthStart);
assert.doesNotMatch(workflow.slice(preHealthStart,finalGateStart),/--location/);

assert.ok(workflow.includes('IMMEDIATE_PRE_PROMOTION_MAIN_DRIFT'));
assert.ok(workflow.includes('IMMEDIATE_PRE_PROMOTION_MAIN_SHA_GATE=PASS'));
assert.ok(workflow.includes('FINAL_ACTIVE_DEPLOYMENT_NOT_SINGLE_100'));
assert.ok(workflow.includes('FINAL_OWNER_AUTHORIZED_ACTIVE_VERSION_MISMATCH'));
assert.ok(workflow.includes('FINAL_OWNER_AUTHORIZED_ACTIVE_VERSION_MATCH=PASS'));

assert.ok(workflow.includes('wrangler versions deploy "${STAGED_VERSION_ID}@100%"'));
assert.ok(workflow.includes('--name customer-crm-api'));
assert.ok(workflow.includes('--yes'));
assert.ok(workflow.includes('MEMBER_RUNTIME_VERSION_PROMOTION_COMMAND=PASS'));
assert.equal((workflow.match(/wrangler versions deploy/g)||[]).length,1,'promotion workflow must have exactly one traffic mutation command');

assert.ok(workflow.includes('POST_PROMOTION_STAGED_VERSION_100_PERCENT=PASS'));
assert.ok(workflow.includes('PRODUCTION_TRAFFIC_TARGET_EXACT=PASS'));
assert.ok(workflow.includes('PRODUCTION_BASE_URL: https://customer-crm-api.ohw3rz5578d277e.workers.dev'));
assert.ok(workflow.includes('"$PRODUCTION_BASE_URL/health"'));
assert.ok(workflow.includes('PRODUCTION_HEALTH_HTTP_STATUS=200'));
assert.ok(workflow.includes('PRODUCTION_RELEASE_SHA_BODY=PASS'));
assert.ok(workflow.includes('PRODUCTION_RELEASE_SHA_HEADER=PASS'));
assert.ok(workflow.includes('POST_PROMOTION_HTTP_READ_ONLY=PASS'));
assert.ok(workflow.includes('TEMP_PROMOTION_SNAPSHOT_MATERIAL_REMOVED=YES'));

for(const zero of [
  'WRANGLER_DEPLOY_USED=NO',
  'SECRET_MUTATION=0',
  'PRODUCTION_D1_WRITE=0',
  'R2_OBJECT_READ=0',
  'R2_OBJECT_WRITE=0',
  'CRM_WRITE=0',
  'LINE_SEND=0',
  'CUSTOMER_PROSPECT_MUTATION=0',
  'CUSTOMER_ID_GENERATION=0',
  'MEMBER_SESSION_PRODUCTION_ACTIVATION=0',
  'LINE_LOGIN_PRODUCTION_ACTIVATION=0',
  'PRIVATE_MEDIA_CONTENT_ROUTE_ACTIVATION=0',
  'COMMERCE_ACTIVATION=0',
  'BLACK_AUTOMATIC_AWARD=0'
]){
  assert.ok(workflow.includes(zero), 'missing zero declaration: '+zero);
}

assert.doesNotMatch(workflow,/\bwrangler(?:@[^\s]+)?\s+deploy\b/i);
assert.doesNotMatch(workflow,/\bwrangler(?:@[^\s]+)?\s+secret\s+(put|bulk|delete)\b/i);
assert.doesNotMatch(workflow,/\bwrangler(?:@[^\s]+)?\s+versions\s+secret\s+bulk\b/i);
assert.doesNotMatch(workflow,/d1\s+(execute|migrations|export|import)\b/i);

assert.ok(bridge.includes("github.event.issue.number == 26"));
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("github.event.comment.user.login == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("test \"$BRIDGE_RUN_ATTEMPT\" = '1'"));
assert.ok(bridge.includes('RUNTIME_PROMOTION_BRIDGE_RERUN_NOT_AUTHORIZED'));
assert.ok(bridge.includes("command_re='^/member-runtime-version-promote sha=([0-9a-f]{40}) version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) stage_run=([0-9]+) replace_active_version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) confirm=PROMOTE_MEMBER_RUNTIME_VERSION$'"));
assert.ok(bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha'));
assert.ok(bridge.includes("'path': r.get('path')=='.github/workflows/member-production-runtime-secret-stage.yml'"));
assert.ok(bridge.includes("'run_attempt': r.get('run_attempt')==1"));
assert.ok(bridge.includes('PRODUCTION_MUTATION_BUSY_RETRY_REQUIRED'));
assert.ok(bridge.includes('PRODUCTION_MUTATION_QUEUE_POLICY=REJECT_AND_RETRY'));
assert.ok(bridge.includes("'expected_active_version_id':os.environ['EXPECTED_ACTIVE_VERSION_ID']"));
assert.ok(bridge.includes("'bridge_run_attempt':os.environ['BRIDGE_RUN_ATTEMPT']"));
assert.ok(bridge.includes('member-production-runtime-version-promotion.yml/dispatches'));
assert.ok(bridge.includes('PRODUCTION_D1_WRITE=0'));
assert.ok(bridge.includes('CRM_WRITE=0'));
assert.ok(bridge.includes('LINE_SEND=0'));
assert.doesNotMatch(bridge,/wrangler versions deploy/);
assert.doesNotMatch(bridge,/deploy-cloudflare\.yml\/dispatches/);
assert.doesNotMatch(bridge,/member-production-runtime-secret-stage\.yml\/dispatches/);

console.log('MEMBER_PRODUCTION_RUNTIME_VERSION_PROMOTION_CONTRACT=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_VERSION_PROMOTION_OWNER_GATE=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_VERSION_PROMOTION_ACTIVE_VERSION_BINDING=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_VERSION_PROMOTION_FIRST_ATTEMPT_ONLY=PASS');
