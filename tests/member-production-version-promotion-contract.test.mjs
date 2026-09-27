import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-version-promotion.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-version-promotion-from-issue.yml','utf8');
const canonicalDeployBridge=fs.readFileSync('.github/workflows/dispatch-production-deploy-from-issue.yml','utf8');
const schemaApplyBridge=fs.readFileSync('.github/workflows/dispatch-member-schema-apply-from-issue.yml','utf8');
const runtimeSecretStageBridge=fs.readFileSync('.github/workflows/dispatch-member-production-runtime-secret-stage-from-issue.yml','utf8');
const foundation=fs.readFileSync('.github/workflows/member-app-foundation.yml','utf8');
const canonicalDeploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
const schemaApplyWorkflow=fs.readFileSync('.github/workflows/member-production-schema-apply.yml','utf8');
const runtimeSecretStageWorkflow=fs.readFileSync('.github/workflows/member-production-runtime-secret-stage.yml','utf8');
const receiptPath='release/member/member-production-runtime-secret-stage-36285531724.json';
const receipt=JSON.parse(fs.readFileSync(receiptPath,'utf8'));

for(const exact of [
  '41059eb0ca192f29f790abfd4563552581b1a6b8',
  '6dd49589-f01d-473f-876a-034563023b0e',
  '36285531724',
  'PROMOTE_MEMBER_STAGED_VERSION'
]){
  assert.ok(workflow.includes(exact), 'workflow missing exact gate: '+exact);
}
assert.ok(workflow.includes('workflow_call:'), 'promotion workflow must be reusable');
assert.doesNotMatch(workflow,/\bworkflow_dispatch:/, 'manual workflow_dispatch must not exist');
for(const input of ['expected_sha','staged_version_id','expected_active_version_id','staging_run_id','owner_comment_id','bridge_run_id','bridge_run_attempt','confirmation']){
  assert.ok(workflow.includes(input+':'), 'missing workflow input: '+input);
}

assert.ok(canonicalDeploy.includes("inputs.mode == 'deploy' && 'customer-crm-production-deploy'"), 'canonical deploy shared mutation concurrency missing');
assert.ok(canonicalDeploy.includes("format('customer-crm-production-preflight-{0}', github.run_id)"), 'read-only preflight must not occupy mutation concurrency');
assert.ok(workflow.includes("'customer-crm-production-deploy'"), 'promotion must share canonical Production deployment concurrency');
assert.ok(schemaApplyWorkflow.includes("github.event_name == 'workflow_dispatch' && 'customer-crm-production-deploy'"), 'schema apply must share Production mutation concurrency');
assert.ok(runtimeSecretStageWorkflow.includes("github.event_name == 'workflow_dispatch' && 'customer-crm-production-deploy'"), 'runtime stage must share Production mutation concurrency');
assert.doesNotMatch(schemaApplyWorkflow,/workflow_dispatch' && 'member-production-schema-apply'/);
assert.doesNotMatch(runtimeSecretStageWorkflow,/workflow_dispatch' && 'member-production-runtime-secret-stage'/);
assert.ok(workflow.includes('cancel-in-progress: false'));

assert.ok(workflow.includes('MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA'));
assert.ok(workflow.includes('FINAL_MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA'));
assert.ok(workflow.includes('git merge-base --is-ancestor "$STAGING_SOURCE_SHA" "$EXPECTED_SHA"'));
assert.ok(workflow.includes('MEMBER_PROMOTION_ISSUE_BRIDGE_RECEIPT=PASS'));
assert.ok(workflow.includes("r.get('event')=='issue_comment'"));
assert.ok(workflow.includes("r.get('run_attempt')==1"));
assert.ok(workflow.includes("r.get('actor',{}).get('login')=='ohw3rz5578d277e-collab'"));
assert.ok(workflow.includes('BRIDGE_RERUN_NOT_AUTHORIZED'));

for(const freshness of [
  'OWNER_COMMENT_EDITED',
  'OWNER_COMMENT_NOT_FRESH',
  'age > 900',
  'OWNER_PROMOTION_AUTHORIZATION_FRESHNESS=PASS',
  'FINAL_OWNER_COMMENT_EDITED',
  'FINAL_OWNER_COMMENT_NOT_FRESH',
  'FINAL_OWNER_PROMOTION_AUTHORIZATION_FRESHNESS=PASS'
]){
  assert.ok(workflow.includes(freshness), 'missing freshness gate: '+freshness);
}

assert.equal(receipt.schema_version,1);
assert.equal(receipt.worker_name,'customer-crm-api');
assert.equal(receipt.workflow_path,'.github/workflows/member-production-runtime-secret-stage.yml');
assert.equal(receipt.run_id,36285531724);
assert.equal(receipt.job_id,108525472623);
assert.equal(receipt.source_sha,'41059eb0ca192f29f790abfd4563552581b1a6b8');
assert.equal(receipt.status,'completed');
assert.equal(receipt.conclusion,'success');
assert.equal(receipt.staged_version_id,'6dd49589-f01d-473f-876a-034563023b0e');
assert.equal(receipt.worker_version_stage,'PASS');
assert.equal(receipt.production_deployment_unchanged,true);
assert.equal(receipt.production_traffic_change,0);
assert.equal(receipt.production_deploy,0);
assert.equal(receipt.run_created_at_utc,'2026-09-27T01:26:24Z');
assert.equal(receipt.run_updated_at_utc,'2026-09-27T01:26:51Z');
assert.equal(receipt.expected_version_message,'Owner-gated Member runtime secret stage for 41059eb0ca192f29f790abfd4563552581b1a6b8');
assert.match(receipt.authentication_role,/independently validates/);
assert.ok(workflow.includes(receiptPath));
assert.ok(workflow.includes('DURABLE_MEMBER_STAGING_SUCCESS_RECEIPT=PASS'));
assert.ok(workflow.includes('LIVE_MEMBER_STAGING_RUN_METADATA=PASS'));
assert.ok(workflow.includes('LIVE_MEMBER_STAGING_RUN_METADATA=NOT_RETAINED'));
assert.ok(workflow.includes('CLOUDFLARE_STAGED_VERSION_CREATED_WITHIN_RUN_WINDOW=PASS'));
assert.ok(workflow.includes('CLOUDFLARE_STAGED_VERSION_SOURCE_SHA_MESSAGE=PASS'));
assert.ok(workflow.includes('INDEPENDENT_MEMBER_STAGED_VERSION_LINEAGE=PASS'));
assert.ok(workflow.includes('STAGING_SOURCE_WORKFLOW_PROVENANCE_CONTRACT=PASS'));
assert.doesNotMatch(workflow,/actions\/jobs\/\$STAGE_JOB_ID\/logs/);

assert.ok(workflow.includes('ACTIVE_PRODUCTION_DEPLOYMENT_FRESH_SNAPSHOT=PASS'));
assert.ok(workflow.includes('OWNER_AUTHORIZED_ACTIVE_VERSION_MATCH=PASS'));
assert.ok(workflow.includes('CURRENT_ACTIVE_VERSION_NOT_OWNER_AUTHORIZED'));
assert.ok(workflow.includes('PRE_MUTATION_OWNER_AUTHORIZED_ACTIVE_VERSION_MATCH=PASS'));
assert.ok(workflow.includes('FINAL_OWNER_AUTHORIZED_ACTIVE_VERSION_MATCH=PASS'));
assert.ok(workflow.includes('FINAL_ACTIVE_VERSION_NOT_OWNER_AUTHORIZED'));
assert.ok(workflow.includes('ACTIVE_PRODUCTION_DEPLOYMENT_DRIFTED_BEFORE_PROMOTION'));
assert.ok(workflow.includes('ACTIVE_PRODUCTION_DEPLOYMENT_STABLE_BEFORE_PROMOTION=PASS'));
assert.ok(workflow.includes('STAGED_VERSION_ALREADY_PRESENT_IN_ACTIVE_DEPLOYMENT'));
assert.ok(workflow.includes('npx wrangler versions deploy "$STAGED_VERSION_ID@100%"'));
assert.ok(workflow.includes('--name customer-crm-api'));
assert.ok(workflow.includes('--yes'));
assert.ok(workflow.includes('PROMOTED_VERSION_ACTIVE_ENTRY_COUNT_NOT_EXACTLY_ONE'));
assert.ok(workflow.includes('PROMOTED_VERSION_TRAFFIC_NOT_100_PERCENT'));
assert.ok(workflow.includes('ACTIVE_PRODUCTION_100_PERCENT_ENTRY_COUNT_NOT_EXACTLY_ONE'));
assert.ok(workflow.includes('ACTIVE_PRODUCTION_100_PERCENT_VERSION_MISMATCH'));
assert.ok(workflow.includes('ACTIVE_PRODUCTION_PROMOTED_VERSION_TRAFFIC_PERCENT=100'));
assert.ok(workflow.includes("Number(staged[0].percentage)!==100"));
assert.ok(workflow.includes("fullTraffic.length!==1"));

const deployMatches=workflow.match(/\bnpx wrangler versions deploy\b/g)||[];
assert.equal(deployMatches.length,1,'versions deploy must appear exactly once');
assert.doesNotMatch(workflow,/\bnpx wrangler deploy\b/);
assert.doesNotMatch(workflow,/\bwrangler d1\b/i);
assert.doesNotMatch(workflow,/\bwrangler secret (put|bulk|delete)\b/i);
assert.doesNotMatch(workflow,/^\s*npx wrangler versions secret (put|bulk|delete)\b/im);

assert.ok(bridge.includes("github.event.issue.number == 26"));
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("github.event.comment.user.login == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("staged_version=(6dd49589-f01d-473f-876a-034563023b0e)"));
assert.ok(bridge.includes("staging_run=(36285531724)"));
assert.ok(bridge.includes('replace_active_version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'));
assert.ok(bridge.includes('/member-production-promotion-snapshot '));
assert.ok(bridge.includes('ACTIVE_PRODUCTION_VERSION_ID='));
assert.ok(bridge.includes('expected_active_version_id: ${{ needs.validate.outputs.expected_active_version_id }}'));
assert.ok(bridge.includes('PROMOTION_BRIDGE_RERUN_NOT_AUTHORIZED'));
assert.ok(bridge.includes('github.run_attempt'));
assert.ok(bridge.includes('uses: ./.github/workflows/member-production-version-promotion.yml'));
assert.ok(bridge.includes('secrets: inherit'));
assert.doesNotMatch(bridge,/\/dispatches/);
assert.doesNotMatch(bridge,/actions:\s*write/);
assert.doesNotMatch(bridge,/wrangler\s+versions\s+deploy/i);

assert.doesNotMatch(bridge,/wrangler\s+deploy\b/i);

for(const mutationBridge of [canonicalDeployBridge,schemaApplyBridge,runtimeSecretStageBridge,bridge]){
  assert.doesNotMatch(mutationBridge,/\nconcurrency:\s*\n/,'mutation authorization bridge must not queue before busy rejection');
  assert.ok(mutationBridge.includes('PRODUCTION_MUTATION_BUSY_RETRY_REQUIRED'),'busy mutation rejection missing');
  assert.ok(mutationBridge.includes('PRODUCTION_MUTATION_QUEUE_POLICY=REJECT_AND_RETRY'),'reject-and-retry policy missing');
  assert.ok(mutationBridge.includes('dispatch-production-deploy-from-issue.yml'),'canonical bridge mutual exclusion missing');
  assert.ok(mutationBridge.includes('dispatch-member-schema-apply-from-issue.yml'),'schema bridge mutual exclusion missing');
  assert.ok(mutationBridge.includes('dispatch-member-production-runtime-secret-stage-from-issue.yml'),'runtime-stage bridge mutual exclusion missing');
  assert.ok(mutationBridge.includes('dispatch-member-production-version-promotion-from-issue.yml'),'promotion bridge mutual exclusion missing');
  assert.ok(mutationBridge.includes('deploy-cloudflare.yml'),'canonical target conflict check missing');
  assert.ok(mutationBridge.includes('member-production-schema-apply.yml'),'schema target conflict check missing');
  assert.ok(mutationBridge.includes('member-production-runtime-secret-stage.yml'),'runtime-stage target conflict check missing');
  for(const status of ['queued','in_progress','waiting','pending','requested']){
    assert.ok(mutationBridge.includes(status),'mutation status gate missing: '+status);
  }
  assert.ok(mutationBridge.includes('THIS_RUN_ID'),'current bridge run exclusion missing');
}
assert.ok(canonicalDeployBridge.includes("steps.gate.outputs.release_mode == 'deploy'"),'canonical preflight must remain outside mutation rejection gate');

for(const path of [
  '.github/workflows/member-production-version-promotion.yml',
  '.github/workflows/dispatch-member-production-version-promotion-from-issue.yml',
  'tests/member-production-version-promotion-contract.test.mjs',
  receiptPath
]){
  assert.ok(foundation.includes(path), 'Member foundation scope missing: '+path);
}

console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_CONTRACT=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_OWNER_GATE=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_SINGLE_USE_BRIDGE=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_DURABLE_STAGING_RECEIPT=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_INDEPENDENT_CLOUDFLARE_LINEAGE=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_OWNER_AUTHORIZED_REPLACEMENT_VERSION=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_SHARED_CONCURRENCY=PASS');
console.log('MEMBER_PRODUCTION_MUTATION_REJECT_AND_RETRY_GATE=PASS');
console.log('MEMBER_PRODUCTION_MUTATION_BRIDGE_PRECONCURRENCY_REJECTION=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_FINAL_MAIN_RECHECK=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_SOURCE_ONLY_PR_GATE=PASS');
