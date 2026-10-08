import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-runtime-readiness.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-runtime-readiness-from-issue.yml','utf8');

assert.match(workflow,/workflow_dispatch:\s*\n\s*inputs:\s*\n\s*expected_sha:/);
assert.ok(workflow.includes('owner_comment_id:'));
assert.ok(workflow.includes('issues: read'));
assert.ok(workflow.includes('Checkout exact PR head'));
assert.ok(workflow.includes('Checkout exact authorized SHA'));
assert.ok(workflow.includes('persist-credentials: false'));
assert.ok((workflow.match(/persist-credentials: false/g)||[]).length>=2);
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'));
assert.ok(workflow.includes('MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA'));

assert.ok(workflow.includes("test \"$ACTOR\" = 'github-actions[bot]'"));
assert.ok(workflow.includes("test \"$TRIGGERING_ACTOR\" = 'github-actions[bot]'"));
assert.ok(workflow.includes('DIRECT_RUNTIME_READINESS_DISPATCH_FORBIDDEN'));
assert.ok(workflow.includes('DIRECT_RUNTIME_READINESS_TRIGGER_FORBIDDEN'));
assert.ok(workflow.includes('Verify exact Issue #26 Owner authorization receipt'));
assert.ok(workflow.includes('issues/comments/$OWNER_COMMENT_ID'));
assert.ok(workflow.includes("if(String(p?.user?.login||'')!=='ohw3rz5578d277e-collab')"));
assert.ok(workflow.includes("/issues/26`"));
assert.ok(workflow.includes('EXPECTED_BODY="/member-runtime-readiness sha=$EXPECTED_SHA"'));
assert.ok(workflow.includes('ISSUE_26_OWNER_AUTHORIZATION_RECEIPT=PASS'));
assert.ok(workflow.includes('OWNER_RECEIPT_BODY_PRINTED=NO'));

for(const path of [
  'member_tests/member-runtime-step9-final-gate.test.mjs',
  'docs/member-app/member-runtime-step9-final-gate-20261009.md',
  'src/member-production-backend-sequence-readiness.mjs',
  'src/member-production-activation-readiness-assembly.mjs',
  'member_tests/member-production-backend-sequence-readiness.test.mjs',
  'member_tests/member-production-activation-readiness-assembly.test.mjs',
  'docs/member-app/member-production-backend-sequence-readiness-20261009.md'
]){
  assert.ok(workflow.includes(`- '${path}'`),`runtime readiness PR path missing: ${path}`);
}

const ownerReceiptIndex=workflow.indexOf('Verify exact Issue #26 Owner authorization receipt');
const step9Command='node member_tests/member-runtime-step9-final-gate.test.mjs';
const step9Index=workflow.indexOf(step9Command);
const backendGateIndex=workflow.indexOf('Verify exact-SHA Member backend sequence readiness');
const deployReceiptIndex=workflow.indexOf('Snapshot canonical Production deploy receipt');
const cloudflareAuthIndex=workflow.indexOf('Confirm Cloudflare authentication');
const d1ListIndex=workflow.indexOf('List pending managed Production D1 migrations');
assert.ok(ownerReceiptIndex>=0,'Owner receipt gate must exist');
assert.ok(step9Index>=0,'Step 9 final gate must be required by runtime readiness');
assert.ok(backendGateIndex>=0,'exact-SHA backend sequence gate must exist');
assert.ok(ownerReceiptIndex<step9Index,'Owner receipt must be verified before Step 9/readiness observation');
assert.ok(step9Index<backendGateIndex,'Step 9 final gate must precede backend sequence classifier');
assert.ok(workflow.includes('MEMBER_BACKEND_STEPS_1_9_FINAL_GATE=PASS'));
assert.ok(workflow.includes('PRODUCTION_OPERATION=0'));

assert.ok(workflow.includes('node member_tests/member-production-backend-sequence-readiness.test.mjs'));
assert.ok(workflow.includes('node member_tests/member-production-activation-readiness-assembly.test.mjs'));
assert.ok(workflow.includes("import { buildMemberProductionBackendSequenceReadiness } from './src/member-production-backend-sequence-readiness.mjs';"));
assert.ok(workflow.includes('release_sha:expected'));
assert.ok(workflow.includes('step9_verified_sha:expected'));
assert.ok(workflow.includes('step9_node_major:nodeMajor'));
for(const evidence of [
  'step9_final_gate_passed:true',
  'exact_head_checkout_verified:true',
  'workflow_exact_head_contract_verified:true',
  'all_member_tests_workflow_contract_verified:true',
  'steps_1_8_matrix_verified:true',
  'cross_contract_security_verified:true',
  'production_default_off_verified:true'
]){
  assert.ok(workflow.includes(evidence),`backend sequence evidence missing: ${evidence}`);
}
assert.ok(workflow.includes('MEMBER_BACKEND_SEQUENCE_NOT_READY:'));
assert.ok(workflow.includes('MEMBER_BACKEND_SEQUENCE_SHA_EVIDENCE_MISMATCH'));
assert.ok(workflow.includes('MEMBER_BACKEND_SEQUENCE_AUTHORIZATION_MUST_REMAIN_OFF'));
assert.ok(workflow.includes('MEMBER_BACKEND_SEQUENCE_PRODUCTION_INVARIANT_VIOLATION'));
assert.ok(workflow.includes('MEMBER_BACKEND_SEQUENCE_TECHNICAL_READINESS=PASS'));
assert.ok(workflow.includes('MEMBER_BACKEND_SEQUENCE_PRODUCTION_ACTION_ALLOWED=0'));
assert.ok(deployReceiptIndex>backendGateIndex,'backend sequence gate must run before Production deploy-receipt observation');
assert.ok(cloudflareAuthIndex>backendGateIndex,'backend sequence gate must run before Cloudflare Production observation');
assert.ok(d1ListIndex>backendGateIndex,'backend sequence gate must run before Production D1 observation');

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
assert.ok(bridge.includes('OWNER_COMMENT_ID: ${{ github.event.comment.id }}'));
assert.ok(bridge.includes('owner_comment_id=$OWNER_COMMENT_ID'));
assert.ok(bridge.includes("'owner_comment_id':os.environ['OWNER_COMMENT_ID']"));
assert.ok(bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha'));
assert.ok(bridge.includes('deploy-cloudflare.yml member-production-runtime-readiness.yml'));
assert.ok(bridge.includes('PRODUCTION_READINESS_LOCK_OCCUPIED'));
assert.ok(bridge.includes('member-production-runtime-readiness.yml/dispatches'));
assert.ok(bridge.includes('PRODUCTION_WRITE=0'));
assert.ok(bridge.includes('PRODUCTION_DEPLOY=0'));
assert.doesNotMatch(bridge,/deploy-cloudflare\.yml\/dispatches/);

console.log('MEMBER_PRODUCTION_RUNTIME_READINESS_CONTRACT=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_READINESS_OWNER_RECEIPT=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_READINESS_STEP9_PREREQUISITE=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_READINESS_BACKEND_SEQUENCE_GATE=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_READINESS_READ_ONLY=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_READINESS_OWNER_GATE=PASS');
