import assert from 'node:assert/strict';
import {
  MEMBER_RUNTIME_LEGACY_MIGRATIONS,
  MEMBER_RUNTIME_LEGACY_TABLES,
  MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS,
  MEMBER_RUNTIME_LIFECYCLE_TABLES
} from '../src/member-production-runtime-schema-readiness.mjs';
import {
  MEMBER_LIFECYCLE_SCHEMA_OBJECTS,
  classifyMemberLifecycleSchemaGate,
  requireMemberLifecyclePreApplyState,
  requireMemberLifecyclePostApplyState
} from '../src/member-production-lifecycle-schema-gate.mjs';

const lifecycleMigrations = MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS.map(item => item.name);

function schemaOutput({ legacy = true, lifecycle = false, lifecyclePartial = [], trackedLifecycle = [], extraObjects = [] } = {}) {
  const rows = [];
  if (legacy) {
    rows.push(...MEMBER_RUNTIME_LEGACY_TABLES.map(name => ({ name, type: 'table', tbl_name: name })));
    rows.push(...MEMBER_RUNTIME_LEGACY_MIGRATIONS.map(name => ({ name })));
  }
  if (lifecycle) {
    rows.push(...MEMBER_LIFECYCLE_SCHEMA_OBJECTS.map(item => ({ ...item })));
    rows.push(...lifecycleMigrations.map(name => ({ name })));
  } else {
    rows.push(...lifecyclePartial.map(name => ({ name, type: 'table', tbl_name: name })));
    rows.push(...trackedLifecycle.map(name => ({ name })));
  }
  rows.push(...extraObjects);
  return JSON.stringify([{ results: rows }]);
}

const exactPending = lifecycleMigrations.join('\n');
const pre = classifyMemberLifecycleSchemaGate({
  pendingOutput: exactPending,
  schemaOutput: schemaOutput()
});
assert.equal(pre.status, 'LIFECYCLE_APPLY_READY');
assert.equal(pre.pre_apply_ready, true);
assert.equal(pre.post_apply_confirmed, false);
assert.deepEqual([...pre.runtime.pending.lifecycle].sort(), [...lifecycleMigrations].sort());
assert.equal(pre.runtime.receipt.legacy_table_count, 11);
assert.equal(pre.runtime.receipt.legacy_tracked_migration_count, 9);
assert.equal(pre.runtime.receipt.lifecycle_table_count, 0);
assert.equal(pre.runtime.receipt.lifecycle_tracked_migration_count, 0);
assert.equal(pre.evidence.present_lifecycle_schema_objects.length, 0);
assert.doesNotThrow(() => requireMemberLifecyclePreApplyState({ pendingOutput: exactPending, schemaOutput: schemaOutput() }));

const post = classifyMemberLifecycleSchemaGate({
  pendingOutput: 'No migrations to apply',
  schemaOutput: schemaOutput({ lifecycle: true })
});
assert.equal(post.status, 'LIFECYCLE_APPLY_CONFIRMED');
assert.equal(post.pre_apply_ready, false);
assert.equal(post.post_apply_confirmed, true);
assert.equal(post.runtime.receipt.lifecycle_table_count, 6);
assert.equal(post.runtime.receipt.lifecycle_tracked_migration_count, 2);
assert.equal(post.evidence.exact_lifecycle_schema_object_count, MEMBER_LIFECYCLE_SCHEMA_OBJECTS.length);
assert.doesNotThrow(() => requireMemberLifecyclePostApplyState({ pendingOutput: '', schemaOutput: schemaOutput({ lifecycle: true }) }));

for (const [label, input] of [
  ['unknown pending migration', { pendingOutput: exactPending + '\n20990101_unknown.sql', schemaOutput: schemaOutput() }],
  ['legacy migration pending', { pendingOutput: exactPending + '\n' + MEMBER_RUNTIME_LEGACY_MIGRATIONS[0], schemaOutput: schemaOutput() }],
  ['one lifecycle migration only', { pendingOutput: lifecycleMigrations[0], schemaOutput: schemaOutput() }],
  ['partial lifecycle table exists', { pendingOutput: exactPending, schemaOutput: schemaOutput({ lifecyclePartial: [MEMBER_RUNTIME_LIFECYCLE_TABLES[0]] }) }],
  ['pending lifecycle already tracked', { pendingOutput: exactPending, schemaOutput: schemaOutput({ trackedLifecycle: [lifecycleMigrations[0]] }) }],
  ['missing legacy receipt', { pendingOutput: exactPending, schemaOutput: schemaOutput({ legacy: false }) }],
  ['malformed schema receipt', { pendingOutput: exactPending, schemaOutput: '{bad json' }],
  ['lifecycle table name is view', {
    pendingOutput: exactPending,
    schemaOutput: schemaOutput({ extraObjects: [{ name: MEMBER_RUNTIME_LIFECYCLE_TABLES[0], type: 'view', tbl_name: MEMBER_RUNTIME_LIFECYCLE_TABLES[0] }] })
  }],
  ['lifecycle index name is on wrong table', {
    pendingOutput: exactPending,
    schemaOutput: schemaOutput({ extraObjects: [{ name: 'idx_member_identity_customer', type: 'index', tbl_name: 'customers' }] })
  }],
  ['lifecycle trigger name collides before apply', {
    pendingOutput: exactPending,
    schemaOutput: schemaOutput({ extraObjects: [{ name: 'trg_member_consent_evidence_no_delete', type: 'trigger', tbl_name: 'customers' }] })
  }]
]) {
  const result = classifyMemberLifecycleSchemaGate(input);
  assert.equal(result.status, 'BLOCKED_LIFECYCLE_SCHEMA_STATE', label);
  assert.equal(result.pre_apply_ready, false, label);
  assert.equal(result.post_apply_confirmed, false, label);
  assert.throws(() => requireMemberLifecyclePreApplyState(input), /BLOCKED_MEMBER_LIFECYCLE_SCHEMA_PRE_APPLY/, label);
}

const postWrongIndex = schemaOutput({ lifecycle: true, extraObjects: [
  { name: 'idx_member_identity_customer', type: 'index', tbl_name: 'customers' }
] });
const postWrongIndexResult = classifyMemberLifecycleSchemaGate({ pendingOutput: '', schemaOutput: postWrongIndex });
assert.equal(postWrongIndexResult.status, 'BLOCKED_LIFECYCLE_SCHEMA_STATE');
assert.equal(postWrongIndexResult.post_apply_confirmed, false);
assert.throws(
  () => requireMemberLifecyclePostApplyState({ pendingOutput: '', schemaOutput: postWrongIndex }),
  /BLOCKED_MEMBER_LIFECYCLE_SCHEMA_POST_APPLY/
);

console.log('MEMBER_PRODUCTION_LIFECYCLE_SCHEMA_GATE=PASS');
