import fs from 'node:fs';
import assert from 'node:assert/strict';

const workflow = fs.readFileSync('.github/workflows/member-production-runtime-active-version-read.yml', 'utf8');
const bridge = fs.readFileSync('.github/workflows/dispatch-member-production-runtime-active-version-read-from-issue.yml', 'utf8');

assert.ok(workflow.includes('name: Member Production runtime active version read'));
assert.ok(workflow.includes('Exact-SHA Member Production active version read only'));
assert.ok(workflow.includes("test \"$BRIDGE_RUN_ATTEMPT_RAW\" = '1'"));
assert.ok(workflow.includes('CURRENT_MAIN_EXACT_GATE=PASS'));
assert.ok(workflow.includes('OWNER_ACTIVE_VERSION_READ_AUTHORIZATION_COMMENT=PASS'));
assert.ok(workflow.includes('MEMBER_RUNTIME_ACTIVE_VERSION_READ_BRIDGE_RECEIPT=PASS'));
assert.ok(workflow.includes("'run_attempt':r.get('run_attempt')==1"));
assert.ok(workflow.includes("r.get('path')=='.github/workflows/member-production-runtime-secret-stage.yml'"));
assert.ok(workflow.includes('MEMBER_RUNTIME_SECRET_STAGED_VERSION_ID=$STAGED_VERSION_ID'));
assert.ok(workflow.includes('PRODUCTION_DEPLOYMENT_UNCHANGED=PASS'));
assert.ok(workflow.includes('PRODUCTION_TRAFFIC_CHANGE=0'));
assert.ok(workflow.includes('npx wrangler deployments status --name customer-crm-api --json'));
assert.ok(workflow.includes('npx wrangler versions view "$STAGED_VERSION_ID" --name customer-crm-api --json'));
assert.ok(workflow.includes('MEMBER_RUNTIME_ACTIVE_VERSION_ID='+"'+active"));
assert.ok(workflow.includes('PRODUCTION_BASELINE_SINGLE_VERSION=PASS'));
assert.ok(workflow.includes('PRODUCTION_BASELINE_100_PERCENT=PASS'));
assert.ok(workflow.includes('STAGED_VERSION_TRAFFIC=0'));
assert.ok(workflow.includes('STAGED_SECRET_BINDINGS=4'));
assert.ok(workflow.includes('SECRET_VALUES_PRINTED=NO'));
assert.ok(workflow.includes('FINAL_CURRENT_MAIN_EXACT_GATE=PASS'));
assert.ok(workflow.includes('MEMBER_RUNTIME_ACTIVE_VERSION_READ=PASS'));
assert.ok(workflow.includes('PRODUCTION_D1_READ=0'));
assert.ok(workflow.includes('R2_READ=0'));
assert.ok(workflow.includes('SECRET_CHANGE=0'));
assert.doesNotMatch(workflow, /wrangler\s+deploy\b/i);
assert.doesNotMatch(workflow, /wrangler\s+versions\s+deploy\b/i);
assert.doesNotMatch(workflow, /wrangler\s+versions\s+upload\b/i);
assert.doesNotMatch(workflow, /wrangler\s+versions\s+secret\b/i);
assert.doesNotMatch(workflow, /wrangler\s+d1\b/i);
assert.doesNotMatch(workflow, /wrangler\s+r2\b/i);

assert.ok(bridge.includes("startsWith(github.event.comment.body, '/member-runtime-active-version-read ')"));
assert.ok(bridge.includes("command_re='^/member-runtime-active-version-read sha=([0-9a-f]{40}) version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) stage_run=([0-9]+) confirm=READ_MEMBER_RUNTIME_ACTIVE_VERSION$'"));
assert.ok(bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha'));
assert.ok(bridge.includes('PRODUCTION_MUTATION_LOCK=FREE'));
assert.ok(bridge.includes("'bridge_run_id':os.environ['GITHUB_RUN_ID']"));
assert.ok(bridge.includes("'bridge_run_attempt':os.environ['GITHUB_RUN_ATTEMPT']"));
assert.ok(bridge.includes('member-production-runtime-active-version-read.yml/dispatches'));
assert.doesNotMatch(bridge, /wrangler\s+/i);

console.log('MEMBER_PRODUCTION_RUNTIME_ACTIVE_VERSION_READ_CONTRACT=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_ACTIVE_VERSION_READ_OWNER_GATE=PASS');
console.log('MEMBER_PRODUCTION_RUNTIME_ACTIVE_VERSION_READ_ONLY=PASS');
