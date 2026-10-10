import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-route-stage.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-route-stage-from-issue.yml','utf8');
const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const cfg=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));

function pass(name,condition){
  assert.equal(condition,true,name);
  console.log('PASS',name);
}

pass('canonical source remains Owner-default-off',
  (entry.match(/const MEMBER_PRODUCTION_OWNER_APPROVED=false;/g)||[]).length===1
  && !entry.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=true;')
);
pass('canonical source keeps LINE login approval false',
  (entry.match(/line_login_approved:false/g)||[]).length===1
);
pass('canonical config keeps Member route mode absent',
  String(cfg?.vars?.MEMBER_PRODUCTION_ROUTE_MODE||'').trim().toLowerCase()!=='enabled'
);
pass('canonical config keeps private media content route absent',
  String(cfg?.vars?.MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE||'').trim().toLowerCase()!=='enabled'
);
pass('canonical private media R2 binding stays exact',
  Array.isArray(cfg.r2_buckets)
  && cfg.r2_buckets.length===1
  && cfg.r2_buckets[0]?.binding==='MEMBER_PRIVATE_MEDIA_BUCKET'
  && cfg.r2_buckets[0]?.bucket_name==='customer-crm-member-private-media'
);

pass('workflow has PR contract and Owner-gated dispatch only',
  workflow.includes('pull_request:')
  && workflow.includes('workflow_dispatch:')
  && workflow.includes("test \"$CONFIRMATION_RAW\" = 'STAGE_MEMBER_ROUTE_CANDIDATE'")
);
pass('workflow requires exact main and exact current active version',
  workflow.includes('CURRENT_MAIN_EXACT_GATE=PASS')
  && workflow.includes('CURRENT_ACTIVE_VERSION_NOT_OWNER_AUTHORIZED')
  && workflow.includes('OWNER_AUTHORIZED_ACTIVE_VERSION_MATCH=PASS')
);
pass('workflow authenticates first-attempt issue bridge',
  workflow.includes("'run_attempt': r.get('run_attempt')==1")
  && workflow.includes("'path': r.get('path')=='.github/workflows/dispatch-member-production-route-stage-from-issue.yml'")
  && workflow.includes("'actor': r.get('actor',{}).get('login')=='ohw3rz5578d277e-collab'")
);
pass('workflow requires fresh unedited exact Owner command',
  workflow.includes('OWNER_COMMENT_EDITED')
  && workflow.includes('OWNER_COMMENT_NOT_FRESH')
  && workflow.includes('/member-production-route-stage sha=$EXPECTED_SHA deploy_run=$DEPLOY_RUN_ID storage_preflight_run=$STORAGE_PREFLIGHT_RUN_ID active_version=$EXPECTED_ACTIVE_VERSION_ID confirm=STAGE_MEMBER_ROUTE_CANDIDATE')
);
pass('workflow requires exact successful deploy receipt',
  workflow.includes("verify_run \"$DEPLOY_RUN_ID\" '.github/workflows/deploy-cloudflare.yml' 'deploy'")
);
pass('workflow requires exact successful storage preflight receipt',
  workflow.includes("verify_run \"$STORAGE_PREFLIGHT_RUN_ID\" '.github/workflows/member-production-storage-preflight.yml' 'storage_preflight'")
);
pass('candidate patch changes Owner constant exactly and only ephemerally',
  workflow.includes("const falseLine='const MEMBER_PRODUCTION_OWNER_APPROVED=false;';")
  && workflow.includes("const trueLine='const MEMBER_PRODUCTION_OWNER_APPROVED=true;';")
  && workflow.includes('ENTRY_CANDIDATE_NOT_SINGLE_DETERMINISTIC_PATCH')
  && workflow.includes('src/.member-production-route-stage-entry.js')
  && workflow.includes('EPHEMERAL_CANDIDATE_FILES_REMOVED=PASS')
);
pass('candidate enables only the Member route surface',
  workflow.includes("MEMBER_PRODUCTION_ROUTE_MODE:'enabled'")
  && workflow.includes("MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE:'disabled'")
  && workflow.includes("MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE:'disabled'")
  && workflow.includes("MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'disabled'")
  && workflow.includes("MEMBER_FAVORITES_RATE_LIMIT_MODE:'disabled'")
  && workflow.includes("MEMBER_FAVORITES_WRITE_MODE:'disabled'")
  && workflow.includes("MEMBER_MEMORY_WRITE_MODE:'disabled'")
  && workflow.includes("MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'disabled'")
);
pass('candidate cannot change resource bindings',
  workflow.includes('RESOURCE_BINDINGS_CHANGED_IN_ROUTE_CANDIDATE')
  && workflow.includes('CANONICAL_PRIVATE_MEDIA_BINDING_MISMATCH')
);
pass('candidate is bundled locally before any version mutation',
  workflow.indexOf('MEMBER_ROUTE_CANDIDATE_DRY_RUN=PASS')
  < workflow.indexOf('npx wrangler versions upload')
);
pass('stage creates a version but never promotes traffic',
  workflow.includes('npx wrangler versions upload')
  && !workflow.includes('wrangler versions deploy')
  && !workflow.includes('wrangler deploy --name customer-crm-api')
  && workflow.includes('PRODUCTION_TRAFFIC_CHANGE=0')
  && workflow.includes('STAGED_VERSION_RECEIVED_TRAFFIC')
  && workflow.includes('PRODUCTION_DEPLOYMENT_CHANGED_DURING_ROUTE_STAGE')
);
pass('stage does not use D1 or R2 object commands',
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
pass('staged version must retain required runtime binding names',
  ['MEMBER_SESSION_SECRET','MEMBER_LINE_LOGIN_TRANSACTION_SECRET','MEMBER_LINE_LOGIN_CHANNEL_SECRET','MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET','MEMBER_PRIVATE_MEDIA_BUCKET','DB','LINE_SERVICE','RESERVATION_SERVICE'].every(name=>workflow.includes(name))
);
pass('stage ends at a fresh promotion approval boundary',
  workflow.includes('NEXT_BOUNDARY=FRESH_OWNER_ROUTE_PROMOTION_AUTHORIZATION_REQUIRED')
  && workflow.includes('PROMOTION=0')
);

pass('issue bridge is exact issue 26 Owner-only',
  bridge.includes('github.event.issue.number == 26')
  && bridge.includes("github.actor == 'ohw3rz5578d277e-collab'")
  && bridge.includes("github.event.comment.user.login == 'ohw3rz5578d277e-collab'")
);
pass('issue bridge accepts only the exact stage command shape',
  bridge.includes("command_re='^/member-production-route-stage sha=([0-9a-f]{40}) deploy_run=([0-9]+) storage_preflight_run=([0-9]+) active_version=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) confirm=STAGE_MEMBER_ROUTE_CANDIDATE$'")
);
pass('issue bridge rechecks current main before dispatch',
  bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha')
);
pass('issue bridge rejects concurrent actual Production mutations only',
  bridge.includes('PRODUCTION_MUTATION_BUSY_RETRY_REQUIRED')
  && bridge.includes('PRODUCTION_MUTATION_QUEUE_POLICY=REJECT_AND_RETRY')
  && bridge.includes('PRODUCTION_MUTATION_FILTER=ACTUAL_MUTATIONS_ONLY')
  && bridge.includes("event=='workflow_dispatch'")
  && bridge.includes("title.startswith('Production read-only bridge:')")
  && bridge.includes("title=='Production bridge: ignored'")
  && bridge.includes("title.startswith('Production mutation bridge:')")
  && bridge.includes("'member-production-route-stage.yml'")
);
pass('issue bridge excludes PR contracts from direct mutation workflows',
  bridge.includes('if workflow in direct_mutation_workflows:')
  && bridge.includes("return event=='workflow_dispatch'")
);
pass('issue bridge dispatches stage-only workflow',
  bridge.includes('/actions/workflows/member-production-route-stage.yml/dispatches')
  && bridge.includes("'bridge_run_id':os.environ['BRIDGE_RUN_ID']")
  && bridge.includes("'confirmation':'STAGE_MEMBER_ROUTE_CANDIDATE'")
);

console.log('MEMBER_PRODUCTION_ROUTE_STAGE_CONTRACT=PASS');
console.log('MEMBER_PRODUCTION_MUTATION_FILTER=ACTUAL_MUTATIONS_ONLY_PASS');
console.log('CANONICAL_MEMBER_ROUTE_DEFAULT_OFF=PASS');
console.log('STAGED_MEMBER_ROUTE_ONLY=YES');
console.log('STAGED_LINE_LOGIN_EXTERNAL_EXCHANGE=NO');
console.log('STAGED_PRIVATE_MEDIA_CONTENT_ROUTE=NO');
console.log('PRODUCTION_TRAFFIC_CHANGE=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('R2_OBJECT_READ=0');
console.log('R2_OBJECT_WRITE=0');
console.log('SECRET_CHANGE=0');
console.log('CUSTOMER_PROSPECT_MUTATION=0');
