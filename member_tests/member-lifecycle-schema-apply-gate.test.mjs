import assert from 'node:assert/strict';
import fs from 'node:fs';
import {
  MEMBER_RUNTIME_LEGACY_MIGRATIONS,
  MEMBER_RUNTIME_LEGACY_TABLES,
  MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS,
  MEMBER_RUNTIME_LIFECYCLE_TABLES
} from '../src/member-production-runtime-schema-readiness.mjs';
import {
  MEMBER_LIFECYCLE_SCHEMA_APPLY_BUILD,
  MEMBER_LIFECYCLE_SCHEMA_APPLY_ORDER,
  classifyMemberLifecycleSchemaApplyState
} from '../src/member-lifecycle-schema-apply-gate.mjs';

const lifecycleNames=MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS.map(item=>item.name);
const pendingOutput=names=>names.length?`Migrations to be applied:\n${names.join('\n')}`:'No migrations to apply!';
const schemaOutput=({tables,migrations})=>JSON.stringify([
  {results:tables.map(name=>({name,type:'table'})),success:true},
  {results:migrations.map(name=>({name})),success:true}
]);

const preSchema=schemaOutput({tables:MEMBER_RUNTIME_LEGACY_TABLES,migrations:MEMBER_RUNTIME_LEGACY_MIGRATIONS});
const exactPre=classifyMemberLifecycleSchemaApplyState({pendingOutput:pendingOutput(lifecycleNames),schemaOutput:preSchema,phase:'pre'});
assert.equal(MEMBER_LIFECYCLE_SCHEMA_APPLY_BUILD,'member-lifecycle-schema-apply-gate-20261009-01');
assert.deepEqual(MEMBER_LIFECYCLE_SCHEMA_APPLY_ORDER,[
  '20261007_member_identity_prospect_foundation.sql',
  '20261009_member_registration_consent_event_foundation.sql'
]);
assert.equal(exactPre.ready,true);
assert.equal(exactPre.status,'PRE_APPLY_EXACT_LIFECYCLE_PENDING');
assert.equal(exactPre.runtime.pending.lifecycle.length,2);
assert.equal(exactPre.runtime.pending.unknown.length,0);
assert.equal(exactPre.runtime.receipt.legacy_table_count,11);
assert.equal(exactPre.runtime.receipt.legacy_tracked_migration_count,9);
assert.equal(exactPre.runtime.receipt.lifecycle_table_count,0);
assert.equal(exactPre.runtime.receipt.lifecycle_tracked_migration_count,0);
console.log('PASS exact pre-apply state');

const unknownPre=classifyMemberLifecycleSchemaApplyState({pendingOutput:pendingOutput([...lifecycleNames,'20990101_unknown.sql']),schemaOutput:preSchema,phase:'pre'});
assert.equal(unknownPre.ready,false);
assert.equal(unknownPre.runtime.pending.unknown.length,1);
console.log('PASS unknown pending fails closed');

const first=MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS[0];
const partialTables=[...MEMBER_RUNTIME_LEGACY_TABLES,...first.tables];
const partialMigrations=[...MEMBER_RUNTIME_LEGACY_MIGRATIONS,lifecycleNames[0]];
const partialPre=classifyMemberLifecycleSchemaApplyState({pendingOutput:pendingOutput([lifecycleNames[1]]),schemaOutput:schemaOutput({tables:partialTables,migrations:partialMigrations}),phase:'pre'});
assert.equal(partialPre.ready,false);
assert.equal(partialPre.status,'BLOCKED_PARTIAL_LIFECYCLE_SCHEMA_STATE');
console.log('PASS partial pre-state blocked');

const completePost=classifyMemberLifecycleSchemaApplyState({
  pendingOutput:pendingOutput([]),
  schemaOutput:schemaOutput({tables:[...MEMBER_RUNTIME_LEGACY_TABLES,...MEMBER_RUNTIME_LIFECYCLE_TABLES],migrations:[...MEMBER_RUNTIME_LEGACY_MIGRATIONS,...lifecycleNames]}),
  phase:'post'
});
assert.equal(completePost.ready,true);
assert.equal(completePost.status,'POST_APPLY_LIFECYCLE_SCHEMA_COMPLETE');
assert.equal(completePost.runtime.receipt.legacy_table_count+completePost.runtime.receipt.lifecycle_table_count,17);
assert.equal(completePost.runtime.receipt.legacy_tracked_migration_count+completePost.runtime.receipt.lifecycle_tracked_migration_count,11);
assert.equal(completePost.runtime.pending.all.length,0);
console.log('PASS complete post-state');

const partialPost=classifyMemberLifecycleSchemaApplyState({pendingOutput:pendingOutput([lifecycleNames[1]]),schemaOutput:schemaOutput({tables:partialTables,migrations:partialMigrations}),phase:'post'});
assert.equal(partialPost.ready,false);
assert.equal(partialPost.status,'BLOCKED_PARTIAL_LIFECYCLE_SCHEMA_APPLY');
console.log('PASS partial post-state blocked');

for(const value of Object.values(exactPre.invariant))assert.equal(value,false);
console.log('PASS all non-schema side effects remain unauthorized');

const applyWorkflow=fs.readFileSync('.github/workflows/member-lifecycle-schema-apply.yml','utf8');
const bridgeWorkflow=fs.readFileSync('.github/workflows/dispatch-member-lifecycle-schema-apply-from-issue.yml','utf8');
for(const token of [
  'Verify bridge-only dispatch actor',
  'Verify exact runtime readiness receipt',
  'Require exact two-lifecycle-pending pre-apply state',
  'Final exact current-main gate immediately before mutation',
  'Final exact lifecycle pending-state gate',
  'Last exact current-main gate immediately before apply',
  'continue-on-error: true',
  'if: ${{ always() }}',
  'MEMBER_SCHEMA_TABLE_COUNT=17',
  'MEMBER_SCHEMA_TRACKED_MIGRATION_COUNT=11',
  'AUTOMATIC_RETRY=0',
  'AUTOMATIC_ROLLBACK=0'
])assert.ok(applyWorkflow.includes(token),`missing apply contract token: ${token}`);

const finalStateIndex=applyWorkflow.indexOf('Final exact lifecycle pending-state gate');
const lastMainIndex=applyWorkflow.indexOf('Last exact current-main gate immediately before apply');
const applyIndex=applyWorkflow.indexOf('Apply exact pending Member lifecycle managed migrations');
assert.ok(finalStateIndex>=0,'final lifecycle state gate missing');
assert.ok(lastMainIndex>finalStateIndex,'last exact-main gate must follow final state classification');
assert.ok(applyIndex>lastMainIndex,'apply must occur after last exact-main gate');
console.log('PASS last exact-main gate is after final state gate and immediately before apply path');

for(const token of [
  '/member-lifecycle-schema-apply sha=',
  'readiness_run=',
  'APPLY_MEMBER_LIFECYCLE_SCHEMA',
  'PRODUCTION_MUTATION_QUEUE_POLICY=REJECT_AND_RETRY'
])assert.ok(bridgeWorkflow.includes(token),`missing bridge contract token: ${token}`);
console.log('PASS workflow and bridge contract surface');

console.log('MEMBER_LIFECYCLE_SCHEMA_APPLY_GATE=PASS');
console.log('PRODUCTION_SCHEMA_APPLY=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('MEMBER_ROUTE_ACTIVATION=0');
console.log('CRM_WRITE=0');
console.log('LINE_SEND=0');
console.log('R2_ACCESS=0');
