import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-line-login-stage.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-line-login-stage-from-issue.yml','utf8');
const planner=fs.readFileSync('src/member-production-line-login-stage-plan.mjs','utf8');
const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const cfg=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));

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
pass('stage workflow has PR contract and explicit workflow dispatch',
  workflow.includes('pull_request:')
  && workflow.includes('workflow_dispatch:')
  && workflow.includes("test \"$CONFIRMATION_RAW\" = 'STAGE_MEMBER_LINE_LOGIN_CANDIDATE'")
);
pass('stage requires exact current main',
  workflow.includes('CURRENT_MAIN_EXACT_GATE=PASS')
  && workflow.includes('MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA')
  && workflow.includes('MAIN_DRIFT_BEFORE_LINE_LOGIN_STAGE_UPLOAD')
);
pass('stage authenticates first-attempt Issue 26 bridge provenance',
  workflow.includes("'path': r.get('path')=='.github/workflows/dispatch-member-production-line-login-stage-from-issue.yml'")
  && workflow.includes("'run_attempt': r.get('run_attempt')==1")
  && workflow.includes("'actor': r.get('actor',{}).get('login')=='ohw3rz5578d277e-collab'")
);
pass('stage requires fresh unedited exact Owner command',
  workflow.includes('OWNER_COMMENT_EDITED')
  && workflow.includes('OWNER_COMMENT_NOT_FRESH')
  && workflow.includes('/member-production-line-login-stage sha=$EXPECTED_SHA route_stage_run=$ROUTE_STAGE_RUN_ID route_promotion_run=$ROUTE_PROMOTION_RUN_ID active_version=$EXPECTED_ACTIVE_VERSION_ID confirm=STAGE_MEMBER_LINE_LOGIN_CANDIDATE')
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
  && workflow.indexOf('MEMBER_LINE_LOGIN_CANDIDATE_DRY_RUN=PASS')<workflow.indexOf('npx wrangler versions upload')
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

pass('issue bridge is exact issue 26 Owner-only',
  bridge.includes('github.event.issue.number == 26')
  && bridge.includes("github.actor == 'ohw3rz5578d277e-collab'")
  && bridge.includes("github.event.comment.user.login == 'ohw3rz5578d277e-collab'")
);
pass('issue bridge validates only exact LINE Login stage command shape',
  bridge.includes("command_re='^/member-production-line-login-stage sha=([0-9a-f]{40}) route_stage_run=([0-9]+) route_promotion_run=([0-9]+) active_version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) confirm=STAGE_MEMBER_LINE_LOGIN_CANDIDATE$'")
  && bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha')
);
pass('issue bridge remains deliberately fail-closed and cannot dispatch',
  bridge.includes('MEMBER_LINE_LOGIN_STAGE_DISPATCH_WIRING_NOT_AUTHORIZED')
  && bridge.includes('WORKFLOW_DISPATCH=0')
  && bridge.includes('PRODUCTION_MUTATION=0')
  && !bridge.includes('/actions/workflows/member-production-line-login-stage.yml/dispatches')
  && !bridge.includes('actions: write')
);

console.log('MEMBER_PRODUCTION_LINE_LOGIN_STAGE_CONTRACT=PASS');
console.log('LINE_LOGIN_STAGE_EXECUTION_BRIDGE=FAIL_CLOSED_PENDING_OWNER_APPROVAL');
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
