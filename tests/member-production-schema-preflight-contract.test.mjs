import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-schema-preflight.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-schema-preflight-from-issue.yml','utf8');

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

assert.match(workflow,/workflow_dispatch:/);
assert.match(workflow,/expected_sha:/);
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'),'fresh current-main gate missing');
assert.ok(workflow.includes('d1 migrations list customer-crm-db --remote'),'read-only pending migration inspection missing');
assert.ok(workflow.includes('d1 execute customer-crm-db --remote --json --command'),'read-only remote schema inspection missing');
const schemaReadCommandLine=workflow.split('\n').find(line=>line.includes('command: d1 execute customer-crm-db --remote --json --command'))||'';
assert.ok(schemaReadCommandLine.includes("SELECT name,type FROM sqlite_master WHERE name IN ("),'schema readback SQL must stay on the same wrangler-action command line');
assert.ok(schemaReadCommandLine.includes("SELECT name FROM d1_migrations_managed WHERE name LIKE '20260924_member_%'"),'migration tracking SELECT must stay on the same wrangler-action command line');
assert.ok(!workflow.includes('command: >-\n            d1 execute customer-crm-db --remote --json --command'),'schema readback must not use a multiline wrangler-action command that can split SQL continuation lines');
assert.ok(workflow.includes('ALL_9_PENDING_CLEAN'),'clean all-pending classification missing');
assert.ok(workflow.includes('ALREADY_APPLIED_CONFIRMED'),'already-applied classification missing');
assert.ok(workflow.includes('INCONSISTENT_MEMBER_SCHEMA_STATE'),'partial/inconsistent schema blocker missing');
assert.ok(workflow.includes('BLOCKED_NON_MEMBER_PENDING_MIGRATION'),'non-Member pending migration blocker missing');
assert.ok(workflow.includes('MEMBER_SCHEMA_APPLY=0'),'schema apply zero declaration missing');
assert.ok(workflow.includes('PRODUCTION_D1_WRITE=0'),'D1 write zero declaration missing');
assert.ok(workflow.includes('PRODUCTION_DEPLOY=0'),'deploy zero declaration missing');
assert.ok(workflow.includes('MEMBER_ROUTE_ACTIVATION=0'),'route activation zero declaration missing');
assert.ok(!workflow.includes('d1 migrations apply customer-crm-db --remote'),'preflight workflow must not apply migrations');
assert.ok(!/\bwrangler(?:@[^\s]+)?\s+deploy\b/.test(workflow),'preflight workflow must not deploy Worker');

for(const migration of migrations){
  assert.ok(workflow.includes(migration),`workflow exact Member migration missing: ${migration}`);
  const path='migrations_managed/'+migration;
  const raw=fs.readFileSync(path,'utf8');
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

for(const table of expectedTables)assert.ok(workflow.includes(table),`remote schema readback table missing: ${table}`);

assert.ok(bridge.includes("github.event.issue.number == 26"),'bridge must remain pinned to release-gate issue #26');
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"),'bridge Owner actor gate missing');
assert.ok(bridge.includes("^/member-schema-preflight sha=([0-9a-f]{40})$"),'bridge exact command format missing');
assert.ok(bridge.includes('member-production-schema-preflight.yml/dispatches'),'bridge canonical workflow target missing');
assert.ok(bridge.includes('BRIDGE_PRODUCTION_WRITE=0'),'bridge zero-write declaration missing');
assert.ok(!bridge.includes('/member-schema-apply '),'bridge must not expose schema apply command');

console.log('MEMBER_PRODUCTION_SCHEMA_PREFLIGHT_CONTRACT=PASS');
