import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-line-login-stage.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-line-login-stage-from-issue.yml','utf8');
const postBridge=fs.readFileSync('.github/workflows/dispatch-member-production-line-login-stage-after-bridge.yml','utf8');
const planner=fs.readFileSync('src/member-production-line-login-stage-plan.mjs','utf8');
const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const cfg=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
const canonicalDeploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
const schemaApply=fs.readFileSync('.github/workflows/member-production-schema-apply.yml','utf8');
const runtimeSecret=fs.readFileSync('.github/workflows/member-production-runtime-secret-stage.yml','utf8');
const versionPromotion=fs.readFileSync('.github/workflows/member-production-version-promotion.yml','utf8');
const routeStage=fs.readFileSync('.github/workflows/member-production-route-stage.yml','utf8');
const peerMutationBridgePaths=[
  '.github/workflows/dispatch-production-deploy-from-issue.yml',
  '.github/workflows/dispatch-member-schema-apply-from-issue.yml',
  '.github/workflows/dispatch-member-production-runtime-secret-stage-from-issue.yml',
  '.github/workflows/dispatch-member-production-runtime-version-promotion-from-issue.yml',
  '.github/workflows/dispatch-member-production-version-promotion-from-issue.yml',
  '.github/workflows/dispatch-member-production-route-stage-from-issue.yml',
  '.github/workflows/dispatch-member-production-line-login-stage-from-issue.yml',
];
const peerMutationBridges=peerMutationBridgePaths.map(path=>({
  path,
  source:fs.readFileSync(path,'utf8'),
}));

function pass(name,condition){assert.equal(condition,true,name);console.log('PASS',name);}

pass('canonical source remains Owner-default-off',
  (entry.match(/const MEMBER_PRODUCTION_OWNER_APPROVED=false;/g)||[]).length===1
  && !entry.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=true;')
);
pass('canonical source keeps LINE login approval false',
  (entry.match(/line_login_approved:false/g)||[]).length===1
  && !entry.includes('line_login_approved:true')
);
pass('canonical config keeps route and LINE exchange default-off',
  String(cfg?.vars?.MEMBER_PRODUCTION_ROUTE_MODE||'').toLowerCase()!=='enabled'
  && String(cfg?.vars?.MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE||'').toLowerCase()!=='enabled'
);
pass('canonical private media binding stays exact',
  Array.isArray(cfg.r2_buckets)
  && cfg.r2_buckets.length===1
  && cfg.r2_buckets[0]?.binding==='MEMBER_PRIVATE_MEDIA_BUCKET'
  && cfg.r2_buckets[0]?.bucket_name==='customer-crm-member-private-media'
);

pass('planner remains fail-closed on canonical entrypoint drift',
  planner.includes("CANONICAL_ENTRYPOINT='src/production-index-crm-customer360-entry.js'")
  && planner.includes("throw new Error('CANONICAL_ENTRYPOINT_MISMATCH')")
);
pass('stage workflow keeps PR contract but Production entry is reusable-only',
  workflow.includes('pull_request:')
  && workflow.includes('workflow_call:')
  && !/\n\s*workflow_dispatch:/.test(workflow)
  && workflow.includes('caller_run_id:')
  && workflow.includes("test \"$CONFIRMATION_RAW\" = 'STAGE_MEMBER_LINE_LOGIN_CANDIDATE'")
);
pass('stage reusable workflow declares only required Cloudflare secrets',
  workflow.includes('CLOUDFLARE_API_TOKEN:')
  && workflow.includes('CLOUDFLARE_ACCOUNT_ID:')
  && !workflow.includes('actions: write')
);
pass('stage requires exact current main',
  workflow.includes('CURRENT_MAIN_EXACT_GATE=PASS')
  && workflow.includes('MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA')
  && workflow.includes('MAIN_DRIFT_BEFORE_LINE_LOGIN_STAGE_UPLOAD')
);
pass('stage authenticates successful first-attempt Issue 26 bridge provenance',
  workflow.includes("'status': r.get('status')=='completed'")
  && workflow.includes("'conclusion': r.get('conclusion')=='success'")
  && workflow.includes("'path': r.get('path')=='.github/workflows/dispatch-member-production-line-login-stage-from-issue.yml'")
  && workflow.includes("'run_attempt': r.get('run_attempt')==1")
  && workflow.includes("'actor': r.get('actor',{}).get('login')=='ohw3rz5578d277e-collab'")
  && workflow.includes('MEMBER_LINE_LOGIN_STAGE_BRIDGE_SUCCESS=PASS')
);
pass('stage requires exact reusable caller run identity',
  workflow.includes('CALLER_RUN_ID_RAW')
  && workflow.includes('INVALID_CALLER_RUN_ID')
  && workflow.includes('CALLER_RUN_ID_MISMATCH')
  && workflow.includes('test "$CALLER_RUN_ID" = "$GITHUB_RUN_ID"')
  && workflow.includes('REUSABLE_CALLER_RUN_ID_CHANGED')
);
pass('stage authenticates exact workflow_run caller provenance',
  workflow.includes("'event': r.get('event')=='workflow_run'")
  && workflow.includes("'path': r.get('path')=='.github/workflows/dispatch-member-production-line-login-stage-after-bridge.yml'")
  && workflow.includes("'head_sha': r.get('head_sha')==os.environ['EXPECTED_SHA']")
  && workflow.includes("'run_attempt': r.get('run_attempt')==1")
  && workflow.includes('Production mutation bridge: member-line-login-stage-post-bridge bridge=')
  && workflow.includes('MEMBER_LINE_LOGIN_STAGE_REUSABLE_CALLER_PROVENANCE=PASS')
);
pass('stage requires successful post-bridge validation job before mutation',
  workflow.includes("j.get('name')=='Revalidate successful bridge receipt before stage dispatch'")
  && workflow.includes("j.get('conclusion')=='success'")
  && workflow.includes('POST_BRIDGE_VALIDATION_JOB_NOT_EXACTLY_ONE_SUCCESS')
  && workflow.includes('MEMBER_LINE_LOGIN_STAGE_POST_BRIDGE_VALIDATION_JOB=PASS')
);
pass('stage requires fresh unedited exact Owner command',
  workflow.includes('OWNER_COMMENT_EDITED')
  && workflow.includes('OWNER_COMMENT_NOT_FRESH')
  && workflow.includes('/member-production-line-login-stage sha=$EXPECTED_SHA route_stage_run=$ROUTE_STAGE_RUN_ID route_promotion_run=$ROUTE_PROMOTION_RUN_ID active_version=$EXPECTED_ACTIVE_VERSION_ID confirm=STAGE_MEMBER_LINE_LOGIN_CANDIDATE')
);
const preUploadFresh=workflow.indexOf('OWNER_LINE_LOGIN_STAGE_AUTHORIZATION_FRESH_PRE_UPLOAD=PASS');
const callerProvenance=workflow.indexOf('MEMBER_LINE_LOGIN_STAGE_REUSABLE_CALLER_PROVENANCE=PASS');
const validationJob=workflow.indexOf('MEMBER_LINE_LOGIN_STAGE_POST_BRIDGE_VALIDATION_JOB=PASS');
const uploadMutation=workflow.indexOf('npx wrangler versions upload');
pass('caller provenance and validation success are required before version upload mutation',
  callerProvenance>=0
  && validationJob>callerProvenance
  && uploadMutation>validationJob
);
pass('Owner authorization freshness is revalidated immediately before upload mutation',
  preUploadFresh>=0
  && uploadMutation>preUploadFresh
  && workflow.includes('PRE_UPLOAD_OWNER_COMMENT_NOT_FRESH')
  && workflow.includes('PRE_UPLOAD_OWNER_COMMENT_EDITED')
  && workflow.slice(preUploadFresh,uploadMutation).includes('CURRENT_MAIN_PRE_UPLOAD_GATE=PASS')
);
pass('stage requires successful route-only stage lineage',
  workflow.includes("'path':stage.get('path')=='.github/workflows/member-production-route-stage.yml'")
  && workflow.includes('MEMBER_PRODUCTION_ROUTE_STAGE=PASS')
  && workflow.includes('STAGED_VERSION_ID=$EXPECTED_ACTIVE_VERSION_ID')
  && workflow.includes('LINE_LOGIN_EXTERNAL_EXCHANGE_STAGED=disabled')
);
pass('stage requires successful route-only promotion lineage',
  workflow.includes("'path':promotion.get('path')=='.github/workflows/dispatch-member-production-version-promotion-from-issue.yml'")
  && workflow.includes('MEMBER_ROUTE_STAGED_VERSION_PROMOTION=PASS')
  && workflow.includes('MEMBER_STAGED_VERSION_ID=$EXPECTED_ACTIVE_VERSION_ID')
  && workflow.includes('MEMBER_ROUTE_STAGE_RUN_ID=$ROUTE_STAGE_RUN_ID')
);
pass('stage verifies exact active Production route version at 100 percent',
  workflow.includes('CURRENT_ACTIVE_VERSION_NOT_OWNER_AUTHORIZED')
  && workflow.includes('PRODUCTION_BASELINE_NOT_SINGLE_VERSION')
  && workflow.includes('OWNER_AUTHORIZED_ACTIVE_ROUTE_VERSION_MATCH=PASS')
);
pass('candidate uses audited planner and exact two-entry patches',
  workflow.includes("from './src/member-production-line-login-stage-plan.mjs'")
  && workflow.includes('buildMemberLineLoginStageCandidate')
  && workflow.includes('validateMemberLineLoginStageCandidate')
  && workflow.includes('ENTRY_CANDIDATE_NOT_EXACT_TWO_DETERMINISTIC_PATCHES')
);
pass('candidate enables route and LINE Login only',
  workflow.includes('MEMBER_ROUTE_MODE_STAGED=enabled')
  && workflow.includes('LINE_LOGIN_APPROVAL_STAGED=true')
  && workflow.includes('LINE_LOGIN_EXTERNAL_EXCHANGE_STAGED=enabled')
  && workflow.includes('PRIVATE_MEDIA_CONTENT_ROUTE_STAGED=disabled')
  && workflow.includes('FAVORITES_WRITE_STAGED=disabled')
  && workflow.includes('MEMORY_WRITE_STAGED=disabled')
  && workflow.includes('FAMILY_PASS_ENTITLEMENT_WRITE_STAGED=disabled')
);
pass('candidate cannot change resource bindings',
  workflow.includes('RESOURCE_BINDINGS_CHANGED_IN_LINE_LOGIN_CANDIDATE')
);
pass('candidate is dry-run bundled before version upload',
  workflow.indexOf('MEMBER_LINE_LOGIN_CANDIDATE_DRY_RUN=PASS')>=0
  && workflow.indexOf('MEMBER_LINE_LOGIN_CANDIDATE_DRY_RUN=PASS')<uploadMutation
);
pass('stage creates exactly one immutable version and does not promote traffic',
  workflow.includes('STAGED_VERSION_DELTA_NOT_EXACTLY_ONE')
  && workflow.includes('npx wrangler versions upload')
  && !workflow.includes('wrangler versions deploy')
  && workflow.includes('STAGED_VERSION_RECEIVED_TRAFFIC')
  && workflow.includes('PRODUCTION_DEPLOYMENT_CHANGED_DURING_LINE_LOGIN_STAGE')
  && workflow.includes('PRODUCTION_TRAFFIC_CHANGE=0')
);
pass('stage does not execute D1 or R2 object mutations',
  !workflow.includes('wrangler d1 execute')
  && !workflow.includes('wrangler d1 migrations apply')
  && !workflow.includes('wrangler r2 object')
  && !workflow.includes('wrangler r2 bucket create')
  && !workflow.includes('wrangler r2 bucket delete')
);
pass('stage does not mutate secrets',
  !workflow.includes('wrangler secret put')
  && !workflow.includes('wrangler secret bulk')
  && !workflow.includes('wrangler versions secret put')
  && !workflow.includes('wrangler versions secret bulk')
  && !workflow.includes('wrangler versions secret delete')
);
pass('stage preserves required runtime bindings',
  ['MEMBER_SESSION_SECRET','MEMBER_LINE_LOGIN_TRANSACTION_SECRET','MEMBER_LINE_LOGIN_CHANNEL_SECRET','MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET','MEMBER_PRIVATE_MEDIA_BUCKET','DB','LINE_SERVICE','RESERVATION_SERVICE'].every(name=>workflow.includes(name))
);
pass('stage ends at a distinct fresh promotion boundary',
  workflow.includes('NEXT_BOUNDARY=FRESH_OWNER_LINE_LOGIN_PROMOTION_AUTHORIZATION_REQUIRED')
  && workflow.includes('PROMOTION=0')
);

pass('issue bridge is exact issue 26 Owner-only and first-attempt only',
  bridge.includes('github.event.issue.number == 26')
  && bridge.includes("github.actor == 'ohw3rz5578d277e-collab'")
  && bridge.includes("github.event.comment.user.login == 'ohw3rz5578d277e-collab'")
  && bridge.includes('MEMBER_LINE_LOGIN_STAGE_BRIDGE_RERUN_NOT_AUTHORIZED')
  && bridge.includes("test \"$BRIDGE_RUN_ATTEMPT\" = '1'")
);
pass('issue bridge validates exact command main and fresh unedited comment',
  bridge.includes("command_re='^/member-production-line-login-stage sha=([0-9a-f]{40}) route_stage_run=([0-9]+) route_promotion_run=([0-9]+) active_version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) confirm=STAGE_MEMBER_LINE_LOGIN_CANDIDATE$'")
  && bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha')
  && bridge.includes('OWNER_COMMENT_EDITED')
  && bridge.includes('OWNER_COMMENT_NOT_FRESH')
);
pass('issue bridge rejects peer Production mutations before issuing receipt',
  bridge.includes('PRODUCTION_MUTATION_BUSY_RETRY_REQUIRED')
  && bridge.includes('PRODUCTION_MUTATION_QUEUE_POLICY=REJECT_AND_RETRY')
  && bridge.includes('dispatch-member-production-runtime-version-promotion-from-issue.yml')
  && bridge.includes('member-production-runtime-version-promotion.yml')
  && bridge.includes('member-production-line-login-stage.yml')
  && bridge.includes('member-production-route-stage.yml')
  && bridge.includes('member-production-version-promotion.yml')
  && bridge.includes('member-production-runtime-secret-stage.yml')
  && bridge.includes('member-production-schema-apply.yml')
  && bridge.includes('deploy-cloudflare.yml')
);
pass('issue bridge creates one-day validated receipt artifact but cannot call stage directly',
  bridge.includes("'schema':'member-line-login-stage-dispatch-receipt-v1'")
  && bridge.includes('actions/upload-artifact@v4')
  && bridge.includes('retention-days: 1')
  && bridge.includes('MEMBER_LINE_LOGIN_STAGE_BRIDGE=PASS')
  && bridge.includes('WORKFLOW_DISPATCH=0')
  && !bridge.includes('uses: ./.github/workflows/member-production-line-login-stage.yml')
  && !bridge.includes('/actions/workflows/member-production-line-login-stage.yml/dispatches')
  && !bridge.includes('actions: write')
);

pass('post-bridge run-name uses canonical Production mutation classifier prefix',
  postBridge.includes("format('Production mutation bridge: member-line-login-stage-post-bridge bridge={0}'")
  && postBridge.includes("|| 'Production bridge: ignored'")
);
pass('all canonical peer mutation bridges register post-bridge LINE Login mutation',
  peerMutationBridges.every(({source})=>
    source.includes('dispatch-member-production-line-login-stage-after-bridge.yml')
  )
);

pass('post-bridge validator is triggered only after exact bridge completion',
  postBridge.includes('workflow_run:')
  && postBridge.includes('Dispatch Member Production LINE Login candidate stage from release-gate issue')
  && postBridge.includes('types: [completed]')
  && postBridge.includes("github.event.workflow_run.conclusion == 'success'")
  && postBridge.includes("github.event.workflow_run.event == 'issue_comment'")
);
pass('post-bridge validator rechecks completed successful first-attempt bridge metadata',
  postBridge.includes("'status':r.get('status')=='completed'")
  && postBridge.includes("'conclusion':r.get('conclusion')=='success'")
  && postBridge.includes("'path':r.get('path')=='.github/workflows/dispatch-member-production-line-login-stage-from-issue.yml'")
  && postBridge.includes("'run_attempt':r.get('run_attempt')==1")
  && postBridge.includes("'actor':r.get('actor',{}).get('login')=='ohw3rz5578d277e-collab'")
  && postBridge.includes("'display_title':r.get('display_title')=='Production mutation bridge: member-line-login-stage'")
);
pass('post-bridge validator downloads exact source-run receipt and rechecks main/comment freshness',
  postBridge.includes('actions/download-artifact@v4')
  && postBridge.includes('run-id: ${{ github.event.workflow_run.id }}')
  && postBridge.includes('member-line-login-stage-dispatch-receipt-${{ github.event.workflow_run.id }}')
  && postBridge.includes('MAIN_DRIFT_BEFORE_LINE_LOGIN_STAGE_DISPATCH')
  && postBridge.includes('OWNER_COMMENT_NOT_FRESH_BEFORE_DISPATCH')
  && postBridge.includes('OWNER_COMMENT_DIGEST_MISMATCH')
);
pass('post-bridge validator rechecks peer mutation busy state',
  postBridge.includes('PRODUCTION_MUTATION_BUSY_RETRY_REQUIRED')
  && postBridge.includes('PRODUCTION_MUTATION_POST_BRIDGE_GATE=FREE')
  && postBridge.includes('dispatch-member-production-runtime-version-promotion-from-issue.yml')
  && postBridge.includes('member-production-runtime-version-promotion.yml')
  && postBridge.includes('member-production-line-login-stage.yml')
  && postBridge.includes('member-production-route-stage.yml')
);
pass('post-bridge uses canonical reusable workflow call after validation only',
  postBridge.includes('needs: validate')
  && postBridge.includes('uses: ./.github/workflows/member-production-line-login-stage.yml')
  && postBridge.includes('caller_run_id: ${{ github.run_id }}')
  && postBridge.includes('secrets: inherit')
  && postBridge.indexOf('MEMBER_LINE_LOGIN_STAGE_POST_BRIDGE_VALIDATION=PASS')<postBridge.indexOf('uses: ./.github/workflows/member-production-line-login-stage.yml')
);
pass('executable wiring adds neither actions write nor REST workflow dispatch',
  !postBridge.includes('actions: write')
  && !postBridge.includes('/actions/workflows/member-production-line-login-stage.yml/dispatches')
  && !workflow.includes('actions: write')
  && !/\n\s*workflow_dispatch:/.test(workflow)
);

pass('LINE Login stage shares canonical Production mutation execution lock with peer mutations',
  canonicalDeploy.includes("inputs.mode == 'deploy' && 'customer-crm-production-deploy'")
  && schemaApply.includes("'customer-crm-production-deploy'")
  && runtimeSecret.includes("'customer-crm-production-deploy'")
  && versionPromotion.includes("'customer-crm-production-deploy'")
  && routeStage.includes("'customer-crm-production-deploy'")
  && workflow.includes("'customer-crm-production-deploy'")
);

console.log('MEMBER_PRODUCTION_LINE_LOGIN_STAGE_CONTRACT=PASS');
console.log('LINE_LOGIN_STAGE_ISSUE_BRIDGE_RECEIPT=PASS');
console.log('LINE_LOGIN_STAGE_POST_BRIDGE_VALIDATION=PASS');
console.log('LINE_LOGIN_STAGE_REUSABLE_WORKFLOW_CALL=PASS');
console.log('LINE_LOGIN_STAGE_MANUAL_WORKFLOW_DISPATCH=0');
console.log('LINE_LOGIN_STAGE_ACTIONS_WRITE=0');
console.log('LINE_LOGIN_STAGE_REST_WORKFLOW_DISPATCH=0');
console.log('STAGED_MEMBER_ROUTE=YES');
console.log('STAGED_LINE_LOGIN_APPROVAL=YES');
console.log('STAGED_LINE_LOGIN_EXTERNAL_EXCHANGE=YES');
console.log('STAGED_PRIVATE_MEDIA_CONTENT_ROUTE=NO');
console.log('PRODUCTION_TRAFFIC_CHANGE=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('R2_OBJECT_READ=0');
console.log('R2_OBJECT_WRITE=0');
console.log('SECRET_CHANGE=0');
console.log('LINE_SEND=0');
console.log('CUSTOMER_PROSPECT_MUTATION=0');
