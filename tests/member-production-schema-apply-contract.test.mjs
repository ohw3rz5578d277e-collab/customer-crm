import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-schema-apply.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-schema-apply-from-issue.yml','utf8');

const migrations=[
  '20260924_member_creative_catalog_foundation.sql',
  '20260924_member_family_identity_foundation.sql',
  '20260924_member_family_pass_entitlement_foundation.sql',
  '20260924_member_favorite_mutation_rate_limit_foundation.sql',
  '20260924_member_memory_core_foundation.sql',
  '20260924_member_memory_favorites_foundation.sql',
  '20260924_member_news_catalog_foundation.sql',
  '20260924_member_public_asset_registry_foundation.sql',
  '20260924_member_shop_catalog_foundation.sql'
];
const expectedTables=[
  'customer_family_groups',
  'customer_family_customer_links',
  'member_memories',
  'member_memory_media',
  'member_memory_favorites',
  'member_favorite_mutation_rate_limits',
  'member_family_pass_entitlements',
  'member_public_assets',
  'member_creative_templates',
  'member_shop_products',
  'member_news_items'
];

assert.ok(workflow.includes('workflow_dispatch:'),'workflow_dispatch missing');
for(const input of ['expected_sha:','preflight_run_id:','owner_comment_id:','confirmation:']){
  assert.ok(workflow.includes(input),`workflow input missing: ${input}`);
}
assert.ok(workflow.includes("test \"$CONFIRMATION_RAW\" = 'APPLY_MEMBER_SCHEMA'"),'exact confirmation gate missing');
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'),'exact current-main gate missing');
assert.ok(workflow.includes('/issues/comments/$OWNER_COMMENT_ID'),'Owner comment receipt lookup missing');
assert.ok(workflow.includes("endswith('/issues/26')"),'Owner comment issue #26 check missing');
assert.ok(workflow.includes("c.get('user',{}).get('login')!='ohw3rz5578d277e-collab'"),'Owner actor receipt check missing');
assert.ok(workflow.includes('/actions/runs/$PREFLIGHT_RUN_ID'),'preflight receipt lookup missing');
assert.ok(workflow.includes("r.get('conclusion')=='success'"),'successful preflight receipt check missing');
assert.ok(workflow.includes("r.get('head_sha')==expected_sha"),'exact-SHA preflight receipt check missing');
assert.ok(workflow.includes("r.get('path')=='.github/workflows/member-production-schema-preflight.yml'"),'canonical preflight workflow receipt check missing');
assert.ok(workflow.includes('BLOCKED_MEMBER_SCHEMA_NOT_ALL_9_PENDING_CLEAN'),'pre-apply clean-state blocker missing');
assert.ok(workflow.includes('BLOCKED_NON_MEMBER_PENDING_MIGRATION'),'non-Member pending blocker missing');

const applyMatches=workflow.match(/d1 migrations apply customer-crm-db --remote/g)||[];
assert.equal(applyMatches.length,1,'schema apply workflow must contain exactly one D1 migration apply command');
assert.ok(!/\bwrangler(?:@[^\s]+)?\s+deploy\b/.test(workflow),'schema apply workflow must not deploy Worker');
assert.ok(workflow.includes('MEMBER_SCHEMA_CLASSIFICATION=ALREADY_APPLIED_CONFIRMED'),'post-apply classification missing');
assert.ok(workflow.includes('MEMBER_SCHEMA_TABLE_COUNT=11'),'post-apply table count evidence missing');
assert.ok(workflow.includes('MEMBER_SCHEMA_TRACKED_MIGRATION_COUNT=9'),'post-apply tracking count evidence missing');
assert.ok(workflow.includes('PRODUCTION_DEPLOY=0'),'deploy zero declaration missing');
assert.ok(workflow.includes('MEMBER_ROUTE_ACTIVATION=0'),'route activation zero declaration missing');
assert.ok(workflow.includes('CRM_WRITE=0'),'CRM write zero declaration missing');
assert.ok(workflow.includes('LINE_SEND=0'),'LINE send zero declaration missing');

for(const migration of migrations){
  assert.ok(workflow.includes(migration),`workflow exact Member migration missing: ${migration}`);
  const raw=fs.readFileSync('migrations_managed/'+migration,'utf8');
  const sql=raw.replace(/--.*$/gm,' ').replace(/\/\*[\s\S]*?\*\//g,' ');
  for(const forbidden of [/\bALTER\b/i,/\bINSERT\b/i,/\bUPDATE\b/i,/\bDELETE\b/i,/\bDROP\b/i,/\bREPLACE\b/i,/\bTRUNCATE\b/i]){
    assert.ok(!forbidden.test(sql),`${migration} contains forbidden statement ${forbidden}`);
  }
  const statements=sql.split(';').map(x=>x.trim()).filter(Boolean);
  assert.ok(statements.length>0,`${migration} is empty`);
  for(const statement of statements){
    assert.match(statement,/^CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX)\s+IF\s+NOT\s+EXISTS\b/i,`${migration} contains non-additive statement`);
  }
  assert.ok(!/\bREFERENCES\b/i.test(sql),`${migration} must remain free of cross-migration FK dependency`);
  assert.ok(!/\bFOREIGN\s+KEY\b/i.test(sql),`${migration} must remain free of cross-migration FK dependency`);
}
for(const table of expectedTables){
  assert.ok(workflow.includes(table),`expected Member schema table missing: ${table}`);
}

assert.ok(bridge.includes("github.event.issue.number == 26"),'bridge must remain pinned to issue #26');
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"),'bridge Owner actor gate missing');
assert.ok(bridge.includes("^/member-schema-apply sha=([0-9a-f]{40}) preflight_run=([0-9]+) confirm=APPLY_MEMBER_SCHEMA$"),'bridge exact apply command missing');
assert.ok(bridge.includes('member-production-schema-apply.yml/dispatches'),'bridge canonical apply workflow target missing');
assert.ok(bridge.includes("'owner_comment_id':os.environ['OWNER_COMMENT_ID']"),'bridge Owner comment receipt forwarding missing');
assert.ok(bridge.includes("'confirmation':'APPLY_MEMBER_SCHEMA'"),'bridge confirmation forwarding missing');
assert.ok(bridge.includes('PRODUCTION_DEPLOY=0'),'bridge deploy zero declaration missing');
assert.ok(bridge.includes('MEMBER_ROUTE_ACTIVATION=0'),'bridge route zero declaration missing');

console.log('MEMBER_PRODUCTION_SCHEMA_APPLY_CONTRACT=PASS');
