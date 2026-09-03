import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
const familyMigration=fs.readFileSync('migrations_managed/20260828_customer360_family_marketing_foundation.sql','utf8');
const profileMigration=fs.readFileSync('migrations_managed/20260903_customer360_profile_auto_enrichment.sql','utf8');
const wrangler=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));

assert.equal(wrangler.main,'src/production-index-crm-customer360-entry.js','Customer360 must remain authoritative Worker entry');
assert.match(workflow,/workflow_dispatch:/);
assert.match(workflow,/mode:/);
assert.match(workflow,/default:\s*preflight/);
assert.match(workflow,/\n\s*-\s*preflight\s*\n/);
assert.match(workflow,/\n\s*-\s*deploy\s*\n/);
assert.match(workflow,/expected_sha:/);
assert.match(workflow,/required:\s*true/);
assert.ok(workflow.includes("ref: ${{ github.event.pull_request.head.sha }}"),'PR release CI must checkout exact head');
assert.ok(workflow.includes('git rev-parse HEAD'),'exact checkout SHA verification missing');
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'),'current-main exact SHA verification missing');
assert.ok(workflow.includes('src/production-index-crm-customer360-entry.js'),'authoritative entry guard missing');
assert.ok(workflow.includes('d1 migrations list customer-crm-db --remote'),'remote pending migration read missing');
assert.ok(workflow.includes('20260828_customer360_family_marketing_foundation.sql'),'family migration allowlist missing');
assert.ok(workflow.includes('20260903_customer360_profile_auto_enrichment.sql'),'profile migration allowlist missing');
assert.ok(workflow.indexOf('20260828_customer360_family_marketing_foundation.sql')<workflow.indexOf('20260903_customer360_profile_auto_enrichment.sql'),'profile migration must follow family migration');
assert.ok(workflow.includes('CUSTOMER360_PROFILE_MIGRATION_ONLY_PENDING'),'profile-only pending classification missing');
assert.ok(workflow.includes('CUSTOMER360_MIGRATION_SEQUENCE_PENDING'),'family->profile sequence classification missing');
assert.ok(workflow.includes('BLOCKED_PREEXISTING_MANAGED_MIGRATIONS_PENDING'),'preexisting migration blocker missing');
assert.ok(workflow.includes('BLOCKED_UNEXPECTED_MANAGED_MIGRATION'),'unexpected migration blocker missing');
assert.ok(workflow.includes('ALREADY_APPLIED_CONFIRMED'),'already-applied classification missing');
assert.ok(workflow.includes('INCONSISTENT_REMOTE_MIGRATION_STATE'),'inconsistent remote state blocker missing');
for(const token of ['customer_profile_enrichment','customer_family_member_metadata','customer_field_evidence','customer_notes_history','idx_customer_field_evidence_dedupe','PRODUCTION_SCHEMA_READBACK=PASS','EXISTING_CUSTOMERS_PRESERVED=PASS'])assert.ok(workflow.includes(token),`profile Production readback token missing: ${token}`);
for(const token of ['sqlite_master','d1_migrations_managed','PRAGMA table_info','PRAGMA index_list','canonical_customer_id']) assert.ok(workflow.includes(token),`read-only D1 token missing: ${token}`);
assert.ok(workflow.includes("inputs.mode == 'preflight'"),'preflight stop gate missing');
assert.ok(workflow.includes("inputs.mode == 'deploy'"),'deploy-only gate missing');
assert.ok(workflow.includes('PRODUCTION_MIGRATION_APPLY=0'),'preflight migration safety declaration missing');
assert.ok(workflow.includes('WORKER_DEPLOY=0'),'preflight deploy safety declaration missing');
assert.ok(!/\npush:\s*(?:\n|$)/.test(workflow),'push trigger must not be enabled');
assert.ok(!workflow.includes('CRM_CUSTOMER360_WRITE_ENABLED=1'),'release workflow must not enable Customer360 writes');
assert.ok(!/migrations_managed\/\*\.sql/.test(workflow),'migration allowlist must remain explicit, not globbed');

for(const [name,migration] of [['family',familyMigration],['profile',profileMigration]]){
  const sql=migration.replace(/--.*$/gm,' ').replace(/\/\*[\s\S]*?\*\//g,' ');
  for(const pattern of [/\bDROP\b/i,/\bDELETE\b/i,/\bALTER\b/i,/\bUPDATE\b/i,/\bINSERT\b/i,/\bREPLACE\b/i,/\bTRUNCATE\b/i]) assert.ok(!pattern.test(sql),`${name} forbidden migration statement: ${pattern}`);
  const statements=sql.split(';').map(x=>x.trim()).filter(Boolean);
  assert.ok(statements.length>0,`${name} migration is empty`);
  for(const statement of statements) assert.match(statement,/^CREATE\s+(TABLE|INDEX)\s+IF\s+NOT\s+EXISTS\b/i);
}
for(const token of ['customer_profile_enrichment','customer_family_member_metadata','customer_field_evidence','customer_notes_history'])assert.ok(profileMigration.includes(token),`required profile migration object missing: ${token}`);
assert.ok(!/customer_identity_(sequence|registry)/i.test(profileMigration),'profile migration must not mutate identity registry/sequence');

console.log('CUSTOMER360_PRODUCTION_RELEASE_GATE_CONTRACT=PASS');