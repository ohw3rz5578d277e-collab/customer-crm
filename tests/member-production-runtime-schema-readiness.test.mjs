import assert from 'node:assert/strict';
import {
  MEMBER_RUNTIME_LEGACY_MIGRATIONS,
  MEMBER_RUNTIME_LEGACY_TABLES,
  MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS,
  MEMBER_RUNTIME_LIFECYCLE_TABLES,
  classifyMemberProductionRuntimeSchema
} from '../src/member-production-runtime-schema-readiness.mjs';

const requiredTables = new Set([
  ...MEMBER_RUNTIME_LEGACY_TABLES,
  ...MEMBER_RUNTIME_LIFECYCLE_TABLES
]);

function schemaOutput(names, typeOverrides = {}) {
  return JSON.stringify([{ results: names.map(name => {
    if (Object.prototype.hasOwnProperty.call(typeOverrides, name)) {
      return { name, type: typeOverrides[name] };
    }
    return requiredTables.has(name) ? { name, type: 'table' } : { name };
  }) }]);
}

function pendingOutput(names) {
  return names.length
    ? `Migrations to be applied:\n${names.map(name => `│ ${name} │`).join('\n')}`
    : 'No migrations to apply!';
}

const lifecycleNames = MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS.map(item => item.name);
const fullyAppliedNames = [
  ...MEMBER_RUNTIME_LEGACY_TABLES,
  ...MEMBER_RUNTIME_LEGACY_MIGRATIONS,
  ...MEMBER_RUNTIME_LIFECYCLE_TABLES,
  ...lifecycleNames
];

{
  const result = classifyMemberProductionRuntimeSchema({
    pendingOutput: pendingOutput(lifecycleNames),
    schemaOutput: schemaOutput([
      ...MEMBER_RUNTIME_LEGACY_TABLES,
      ...MEMBER_RUNTIME_LEGACY_MIGRATIONS
    ])
  });
  assert.equal(result.ready, false);
  assert.equal(result.status, 'BLOCKED_KNOWN_LIFECYCLE_MIGRATIONS_PENDING');
  assert.deepEqual(result.pending.lifecycle, lifecycleNames);
  assert.deepEqual(result.pending.unknown, []);
  assert.deepEqual(result.blockers, ['known_lifecycle_migrations_pending']);
  assert.equal(result.receipt.legacy_table_count, 11);
  assert.equal(result.receipt.legacy_tracked_migration_count, 9);
  assert.equal(result.receipt.lifecycle_table_count, 0);
  assert.equal(result.receipt.lifecycle_tracked_migration_count, 0);
}

{
  const result = classifyMemberProductionRuntimeSchema({
    pendingOutput: pendingOutput([]),
    schemaOutput: schemaOutput(fullyAppliedNames)
  });
  assert.equal(result.ready, true);
  assert.equal(result.status, 'ALREADY_APPLIED_CONFIRMED');
  assert.deepEqual(result.blockers, []);
  assert.equal(result.receipt.lifecycle_table_count, 6);
  assert.equal(result.receipt.lifecycle_tracked_migration_count, 2);
  assert.equal(result.receipt.total_required_table_count, 17);
  assert.equal(result.receipt.total_required_migration_count, 11);
}

{
  const result = classifyMemberProductionRuntimeSchema({
    pendingOutput: pendingOutput(['20990101_unrelated_future_change.sql']),
    schemaOutput: schemaOutput(fullyAppliedNames)
  });
  assert.equal(result.ready, false);
  assert.equal(result.status, 'BLOCKED_SCHEMA_RECEIPT_INCONSISTENT');
  assert.deepEqual(result.pending.unknown, ['20990101_unrelated_future_change.sql']);
  assert.ok(result.blockers.includes('unknown_pending_migrations'));
}

{
  const first = MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS[0];
  const receipt = fullyAppliedNames.filter(name => name !== first.name);
  const result = classifyMemberProductionRuntimeSchema({
    pendingOutput: pendingOutput([]),
    schemaOutput: schemaOutput(receipt)
  });
  assert.equal(result.ready, false);
  assert.ok(result.blockers.includes(`lifecycle_migration_receipt_missing:${first.name}`));
}

{
  const first = MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS[0];
  const result = classifyMemberProductionRuntimeSchema({
    pendingOutput: pendingOutput([first.name]),
    schemaOutput: schemaOutput([
      ...MEMBER_RUNTIME_LEGACY_TABLES,
      ...MEMBER_RUNTIME_LEGACY_MIGRATIONS,
      first.tables[0]
    ])
  });
  assert.equal(result.ready, false);
  assert.equal(result.status, 'BLOCKED_SCHEMA_RECEIPT_INCONSISTENT');
  assert.ok(result.blockers.includes(`pending_lifecycle_migration_has_partial_schema:${first.name}`));
}

{
  const first = MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS[0];
  const wrongTypeTable = first.tables[0];
  const result = classifyMemberProductionRuntimeSchema({
    pendingOutput: pendingOutput([]),
    schemaOutput: schemaOutput(fullyAppliedNames, { [wrongTypeTable]: 'view' })
  });
  assert.equal(result.ready, false);
  assert.equal(result.status, 'BLOCKED_SCHEMA_RECEIPT_INCONSISTENT');
  assert.equal(result.receipt.lifecycle_table_count, 5);
  assert.ok(result.blockers.includes(`lifecycle_tables_missing:${first.name}`));
}

{
  const wrongTypeTable = MEMBER_RUNTIME_LEGACY_TABLES[0];
  const result = classifyMemberProductionRuntimeSchema({
    pendingOutput: pendingOutput([]),
    schemaOutput: schemaOutput(fullyAppliedNames, { [wrongTypeTable]: 'trigger' })
  });
  assert.equal(result.ready, false);
  assert.equal(result.status, 'BLOCKED_SCHEMA_RECEIPT_INCONSISTENT');
  assert.equal(result.receipt.legacy_table_count, 10);
  assert.ok(result.blockers.includes('legacy_member_tables_missing'));
}

{
  const result = classifyMemberProductionRuntimeSchema({
    pendingOutput: pendingOutput([]),
    schemaOutput: 'not-json'
  });
  assert.equal(result.ready, false);
  assert.equal(result.receipt.malformed, true);
  assert.ok(result.blockers.includes('malformed_schema_receipt'));
}

console.log('MEMBER_PRODUCTION_RUNTIME_SCHEMA_READINESS=PASS');
console.log('KNOWN_LIFECYCLE_PENDING_CLASSIFICATION=PASS');
console.log('UNKNOWN_PENDING_FAIL_CLOSED=PASS');
console.log('LIFECYCLE_RECEIPT_FAIL_CLOSED=PASS');
console.log('REQUIRED_TABLE_TYPE_FAIL_CLOSED=PASS');
console.log('PRODUCTION_OPERATION=0');
