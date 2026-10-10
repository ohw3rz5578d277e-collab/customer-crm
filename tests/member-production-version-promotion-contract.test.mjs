import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-version-promotion.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-version-promotion-from-issue.yml','utf8');
const routeStage=fs.readFileSync('.github/workflows/member-production-route-stage.yml','utf8');
const canonicalDeployBridge=fs.readFileSync('.github/workflows/dispatch-production-deploy-from-issue.yml','utf8');
const schemaApplyBridge=fs.readFileSync('.github/workflows/dispatch-member-schema-apply-from-issue.yml','utf8');
const runtimeSecretStageBridge=fs.readFileSync('.github/workflows/dispatch-member-production-runtime-secret-stage-from-issue.yml','utf8');
const foundation=fs.readFileSync('.github/workflows/member-app-foundation.yml','utf8');
const canonicalDeploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
const schemaApplyWorkflow=fs.readFileSync('.github/workflows/member-production-schema-apply.yml','utf8');
const runtimeSecretStageWorkflow=fs.readFileSync('.github/workflows/member-production-runtime-secret-stage.yml','utf8');

function pass(name, condition){
  assert.equal(condition,true,name);
  console.log('PASS',name);
}

const legacy={
  source:'41059eb0ca192f29f790abfd4563552581b1a6b8',
  version:'6dd49589-f01d-473f-876a-034563023b0e',
  run:'36285531724',
  receipt:'release/member/member-production-runtime-secret-stage-36285531724.json'
};

pass('promotion workflow is reusable only',
  workflow.includes('workflow_call:')
  && !/\bworkflow_dispatch:/.test(workflow)
);
for(const input of ['expected_sha','staged_version_id','expected_active_version_id','staging_run_id','owner_comment_id','bridge_run_id','bridge_run_attempt','confirmation']){
  pass(`promotion input ${input} exists`,workflow.includes(input+':'));
}

pass('legacy fixed staged candidate is removed from promotion workflow',
  !workflow.includes(legacy.source)
  && !workflow.includes(legacy.version)
  && !workflow.includes(legacy.run)
  && !workflow.includes(legacy.receipt)
  && !workflow.includes('AUTHORIZED_STAGED_VERSION_ID')
  && !workflow.includes('AUTHORIZED_STAGING_RUN_ID')
  && !workflow.includes('DURABLE_STAGING_RECEIPT')
);
pass('legacy fixed staged candidate is removed from promotion bridge',
  !bridge.includes(legacy.version)
  && !bridge.includes(legacy.run)
);

pass('promotion remains exact current-main gated',
  workflow.includes('CHECKOUT_SHA_MISMATCH')
  && workflow.includes('MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA')
  && workflow.includes('FINAL_MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA')
  && workflow.includes('CURRENT_MAIN_EXACT_GATE=PASS')
);
pass('promotion authenticates first-attempt issue bridge',
  workflow.includes("'event': r.get('event')=='issue_comment'")
  && workflow.includes("'path': r.get('path')=='.github/workflows/dispatch-member-production-version-promotion-from-issue.yml'")
  && workflow.includes("'run_attempt': r.get('run_attempt')==1")
  && workflow.includes("'actor': r.get('actor',{}).get('login')=='ohw3rz5578d277e-collab'")
  && workflow.includes('BRIDGE_RERUN_NOT_AUTHORIZED')
);
for(const marker of [
  'OWNER_COMMENT_EDITED',
  'OWNER_COMMENT_NOT_FRESH',
  'OWNER_PROMOTION_AUTHORIZATION_FRESHNESS=PASS',
  'FINAL_OWNER_COMMENT_EDITED',
  'FINAL_OWNER_COMMENT_NOT_FRESH',
  'FINAL_OWNER_PROMOTION_AUTHORIZATION_FRESHNESS=PASS'
]){
  pass(`fresh authorization marker ${marker}`,workflow.includes(marker));
}
pass('promotion Owner command binds route staged version run and active replacement',
  workflow.includes('/member-production-promote sha=$EXPECTED_SHA staged_version=$STAGED_VERSION_ID staging_run=$STAGING_RUN_ID replace_active_version=$EXPECTED_ACTIVE_VERSION_ID confirm=PROMOTE_MEMBER_STAGED_VERSION')
);

pass('promotion requires exact successful route-stage workflow receipt',
  workflow.includes("r.get('status')=='completed'")
  && workflow.includes("r.get('conclusion')=='success'")
  && workflow.includes("r.get('head_sha')==expected_sha")
  && workflow.includes("r.get('event')=='workflow_dispatch'")
  && workflow.includes("r.get('path')=='.github/workflows/member-production-route-stage.yml'")
  && workflow.includes("r.get('run_attempt')==1")
  && workflow.includes("r.get('display_title')==f'Member route candidate stage {expected_sha}'")
  && workflow.includes('MEMBER_ROUTE_STAGE_SUCCESS_RECEIPT=PASS')
);
pass('route-stage lineage is Cloudflare-authenticated independently',
  workflow.includes('npx wrangler versions view "$STAGED_VERSION_ID" --name customer-crm-api --json')
  && workflow.includes('Owner-gated Member route-only stage sha=${process.env.EXPECTED_SHA} run=${process.env.STAGING_RUN_ID}')
  && workflow.includes('STAGED_VERSION_ID_NOT_PRESENT_IN_VERSION_VIEW')
  && workflow.includes('STAGED_VERSION_CREATED_OUTSIDE_AUTHENTICATED_ROUTE_STAGE_RUN_WINDOW')
  && workflow.includes('STAGED_VERSION_ROUTE_STAGE_MESSAGE_MISMATCH')
  && workflow.includes('INDEPENDENT_MEMBER_ROUTE_STAGED_VERSION_LINEAGE=PASS')
);
for(const required of [
  'MEMBER_PRODUCTION_ROUTE_MODE',
  'MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE',
  'MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE',
  'MEMBER_SESSION_SECRET',
  'MEMBER_LINE_LOGIN_TRANSACTION_SECRET',
  'MEMBER_LINE_LOGIN_CHANNEL_SECRET',
  'MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET',
  'MEMBER_PRIVATE_MEDIA_BUCKET',
  'DB',
  'LINE_SERVICE',
  'RESERVATION_SERVICE'
]){
  pass(`staged version binding/mode ${required} is required`,workflow.includes(`'${required}'`));
}
pass('route-stage source provenance is rechecked from exact SHA',
  workflow.includes('git show "$EXPECTED_SHA:.github/workflows/member-production-route-stage.yml"')
  && workflow.includes("MEMBER_PRODUCTION_ROUTE_MODE:'enabled'")
  && workflow.includes("MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE:'disabled'")
  && workflow.includes("MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE:'disabled'")
  && workflow.includes("const trueLine='const MEMBER_PRODUCTION_OWNER_APPROVED=true;'")
  && workflow.includes('npx wrangler versions upload')
  && workflow.includes('STAGED_VERSION_RECEIVED_TRAFFIC')
  && workflow.includes('NEXT_BOUNDARY=FRESH_OWNER_ROUTE_PROMOTION_AUTHORIZATION_REQUIRED')
  && workflow.includes('ROUTE_STAGE_SOURCE_PROVENANCE_CONTRACT=PASS')
);

pass('route stage itself remains stage-only',
  routeStage.includes('npx wrangler versions upload')
  && !routeStage.includes('wrangler versions deploy')
  && routeStage.includes('PRODUCTION_TRAFFIC_CHANGE=0')
  && routeStage.includes('PROMOTION=0')
);
pass('active version is Owner bound before and immediately before promotion',
  workflow.includes('ACTIVE_PRODUCTION_DEPLOYMENT_FRESH_SNAPSHOT=PASS')
  && workflow.includes('OWNER_AUTHORIZED_ACTIVE_VERSION_MATCH=PASS')
  && workflow.includes('PRE_MUTATION_OWNER_AUTHORIZED_ACTIVE_VERSION_MATCH=PASS')
  && workflow.includes('FINAL_OWNER_AUTHORIZED_ACTIVE_VERSION_MATCH=PASS')
  && workflow.includes('CURRENT_ACTIVE_VERSION_NOT_OWNER_AUTHORIZED')
  && workflow.includes('FINAL_ACTIVE_VERSION_NOT_OWNER_AUTHORIZED')
  && workflow.includes('ACTIVE_PRODUCTION_DEPLOYMENT_DRIFTED_BEFORE_PROMOTION')
  && workflow.includes('ACTIVE_PRODUCTION_DEPLOYMENT_STABLE_BEFORE_PROMOTION=PASS')
  && workflow.includes('STAGED_VERSION_ALREADY_PRESENT_IN_ACTIVE_DEPLOYMENT')
);

const promotionMutations=workflow.match(/\bnpx wrangler versions deploy\b/g)||[];
pass('promotion has exactly one intended traffic mutation',promotionMutations.length===1);
pass('promotion targets exact staged version at 100 percent',
  workflow.includes('npx wrangler versions deploy "$STAGED_VERSION_ID@100%"')
  && workflow.includes('--name customer-crm-api')
  && workflow.includes('--yes')
);
pass('promotion does not contain deploy D1 secret or R2 mutation commands',
  !/\bnpx wrangler deploy\b/.test(workflow)
  && !/\bwrangler d1\b/i.test(workflow)
  && !/\bwrangler r2\b/i.test(workflow)
  && !/\bwrangler secret (put|bulk|delete)\b/i.test(workflow)
  && !/^\s*npx wrangler versions secret (put|bulk|delete)\b/im.test(workflow)
);
pass('post promotion requires exact single 100 percent version',
  workflow.includes('PROMOTED_VERSION_ACTIVE_ENTRY_COUNT_NOT_EXACTLY_ONE')
  && workflow.includes('PROMOTED_VERSION_TRAFFIC_NOT_100_PERCENT')
  && workflow.includes('ACTIVE_PRODUCTION_100_PERCENT_ENTRY_COUNT_NOT_EXACTLY_ONE')
  && workflow.includes('ACTIVE_PRODUCTION_100_PERCENT_VERSION_MISMATCH')
  && workflow.includes('ACTIVE_PRODUCTION_PROMOTED_VERSION_TRAFFIC_PERCENT=100')
);

pass('promotion shares canonical Production mutation concurrency',
  canonicalDeploy.includes("inputs.mode == 'deploy' && 'customer-crm-production-deploy'")
  && canonicalDeploy.includes("format('customer-crm-production-preflight-{0}', github.run_id)")
  && workflow.includes("'customer-crm-production-deploy'")
  && schemaApplyWorkflow.includes("github.event_name == 'workflow_dispatch' && 'customer-crm-production-deploy'")
  && runtimeSecretStageWorkflow.includes("github.event_name == 'workflow_dispatch' && 'customer-crm-production-deploy'")
  && workflow.includes('cancel-in-progress: false')
);

pass('snapshot command dynamically binds route-stage candidate',
  bridge.includes("command_re='^/member-production-promotion-snapshot sha=([0-9a-f]{40}) staged_version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) staging_run=([0-9]+)$'")
  && bridge.includes('MEMBER_PROMOTION_ROUTE_STAGE_RECEIPT=PASS')
  && bridge.includes('ACTIVE_PRODUCTION_VERSION_ID=')
  && bridge.includes('promotion_command="/member-production-promote sha=$EXPECTED_SHA staged_version=$STAGED_VERSION_ID staging_run=$STAGING_RUN_ID replace_active_version=$active_version_id confirm=PROMOTE_MEMBER_STAGED_VERSION"')
  && bridge.includes("printf '`%s`\\n' \"$promotion_command\"")
);
pass('promotion bridge accepts dynamic exact candidate and run',
  bridge.includes("command_re='^/member-production-promote sha=([0-9a-f]{40}) staged_version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) staging_run=([0-9]+) replace_active_version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) confirm=PROMOTE_MEMBER_STAGED_VERSION$'")
  && bridge.includes('expected_active_version_id: ${{ needs.validate.outputs.expected_active_version_id }}')
  && bridge.includes('staging_run_id: ${{ needs.validate.outputs.staging_run_id }}')
  && bridge.includes('PROMOTION_BRIDGE_RERUN_NOT_AUTHORIZED')
  && bridge.includes('github.run_attempt')
);
pass('bridge is Owner Issue 26 only',
  bridge.includes('github.event.issue.number == 26')
  && bridge.includes("github.actor == 'ohw3rz5578d277e-collab'")
  && bridge.includes("github.event.comment.user.login == 'ohw3rz5578d277e-collab'")
);
pass('bridge never directly promotes or deploys',
  !/wrangler\s+versions\s+deploy/i.test(bridge)
  && !/wrangler\s+deploy\b/i.test(bridge)
  && !/actions\/workflows\/member-production-version-promotion\.yml\/dispatches/.test(bridge)
  && !/actions:\s*write/.test(bridge)
  && bridge.includes('uses: ./.github/workflows/member-production-version-promotion.yml')
  && bridge.includes('secrets: inherit')
);

for(const mutationBridge of [canonicalDeployBridge,schemaApplyBridge,runtimeSecretStageBridge,bridge]){
  pass('mutation bridge has no workflow-level concurrency before busy rejection',!/\nconcurrency:\s*\n/.test(mutationBridge));
  for(const marker of [
    'run-name: >-',
    'Production mutation bridge:',
    'PRODUCTION_MUTATION_BUSY_RETRY_REQUIRED',
    'PRODUCTION_MUTATION_QUEUE_POLICY=REJECT_AND_RETRY',
    'PRODUCTION_MUTATION_FILTER=ACTUAL_MUTATIONS_ONLY',
    'Production read-only bridge:',
    'Production bridge: ignored',
    'dispatch-production-deploy-from-issue.yml',
    'dispatch-member-schema-apply-from-issue.yml',
    'dispatch-member-production-runtime-secret-stage-from-issue.yml',
    'dispatch-member-production-version-promotion-from-issue.yml',
    'deploy-cloudflare.yml',
    'member-production-schema-apply.yml',
    'member-production-runtime-secret-stage.yml'
  ]){
    pass(`mutation bridge retains ${marker}`,mutationBridge.includes(marker));
  }
}
pass('promotion bridge registers route-stage peer mutation',
  bridge.includes('dispatch-member-production-route-stage-from-issue.yml')
  && bridge.includes('member-production-route-stage.yml')
);
pass('canonical preflight remains excluded from mutation slot',
  canonicalDeployBridge.includes("steps.gate.outputs.release_mode == 'deploy'")
  && canonicalDeployBridge.includes('Production read-only bridge: preflight')
);
pass('run labels distinguish read-only snapshot and mutation',
  bridge.includes('Production read-only bridge: version-snapshot')
  && bridge.includes('Production mutation bridge: version-promotion')
  && runtimeSecretStageBridge.includes('Production mutation bridge: runtime-secret-stage')
  && schemaApplyBridge.includes('Production mutation bridge: schema-apply')
);

for(const path of [
  '.github/workflows/member-production-version-promotion.yml',
  '.github/workflows/dispatch-member-production-version-promotion-from-issue.yml',
  'tests/member-production-version-promotion-contract.test.mjs'
]){
  pass(`Member foundation includes ${path}`,foundation.includes(path));
}

console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_CONTRACT=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_ROUTE_STAGE_LINEAGE=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_OWNER_GATE=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_SINGLE_USE_BRIDGE=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_INDEPENDENT_CLOUDFLARE_LINEAGE=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_OWNER_AUTHORIZED_REPLACEMENT_VERSION=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_SHARED_CONCURRENCY=PASS');
console.log('MEMBER_PRODUCTION_MUTATION_REJECT_AND_RETRY_GATE=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_FINAL_MAIN_RECHECK=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_SOURCE_ONLY_PR_GATE=PASS');
