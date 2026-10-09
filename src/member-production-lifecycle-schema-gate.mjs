import {
  MEMBER_RUNTIME_LEGACY_MIGRATIONS,
  MEMBER_RUNTIME_LEGACY_TABLES,
  MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS,
  MEMBER_RUNTIME_LIFECYCLE_TABLES,
  classifyMemberProductionRuntimeSchema
} from './member-production-runtime-schema-readiness.mjs';

export const MEMBER_LIFECYCLE_SCHEMA_MIGRATIONS = Object.freeze(
  MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS.map(item => item.name)
);

export const MEMBER_LIFECYCLE_SCHEMA_TABLES = MEMBER_RUNTIME_LIFECYCLE_TABLES;

export const MEMBER_LIFECYCLE_SCHEMA_OBJECTS = Object.freeze([
  ...MEMBER_RUNTIME_LIFECYCLE_TABLES.map(name => Object.freeze({ name, type: 'table', tbl_name: name })),
  Object.freeze({ name: 'idx_member_identity_customer', type: 'index', tbl_name: 'member_identities' }),
  Object.freeze({ name: 'idx_member_identity_prospect', type: 'index', tbl_name: 'member_identities' }),
  Object.freeze({ name: 'idx_member_customer_invitation_customer', type: 'index', tbl_name: 'member_customer_invitations' }),
  Object.freeze({ name: 'idx_member_registration_events_member_time', type: 'index', tbl_name: 'member_registration_events' }),
  Object.freeze({ name: 'idx_member_registration_events_prospect_time', type: 'index', tbl_name: 'member_registration_events' }),
  Object.freeze({ name: 'idx_member_consent_evidence_member_time', type: 'index', tbl_name: 'member_consent_evidence' }),
  Object.freeze({ name: 'idx_member_consent_evidence_prospect_time', type: 'index', tbl_name: 'member_consent_evidence' }),
  Object.freeze({ name: 'trg_member_registration_events_no_update', type: 'trigger', tbl_name: 'member_registration_events' }),
  Object.freeze({ name: 'trg_member_registration_events_no_delete', type: 'trigger', tbl_name: 'member_registration_events' }),
  Object.freeze({ name: 'trg_member_consent_evidence_no_update', type: 'trigger', tbl_name: 'member_consent_evidence' }),
  Object.freeze({ name: 'trg_member_consent_evidence_no_delete', type: 'trigger', tbl_name: 'member_consent_evidence' })
]);

const REQUIRED_LEGACY_TABLE_OBJECTS = Object.freeze(
  MEMBER_RUNTIME_LEGACY_TABLES.map(name => Object.freeze({ name, type: 'table', tbl_name: name }))
);

const REQUIRED_SCHEMA_OBJECTS = Object.freeze([
  ...REQUIRED_LEGACY_TABLE_OBJECTS,
  ...MEMBER_LIFECYCLE_SCHEMA_OBJECTS
]);

function sameSet(values, expected) {
  if (!Array.isArray(values) || values.length !== expected.length) return false;
  const actual = new Set(values);
  return expected.every(value => actual.has(value));
}

function extractSchemaObjects(schemaOutput) {
  try {
    const parsed = JSON.parse(String(schemaOutput || ''));
    const chunks = Array.isArray(parsed) ? parsed : [parsed];
    const byName = new Map();
    for (const chunk of chunks) {
      const rows = Array.isArray(chunk?.results) ? chunk.results : [];
      for (const row of rows) {
        if (typeof row?.name !== 'string' || !row.name) continue;
        if (typeof row?.type !== 'string' || !row.type) continue;
        byName.set(row.name, Object.freeze({
          name: row.name,
          type: row.type,
          tbl_name: typeof row?.tbl_name === 'string' ? row.tbl_name : ''
        }));
      }
    }
    return Object.freeze({ malformed: false, byName });
  } catch {
    return Object.freeze({ malformed: true, byName: new Map() });
  }
}

function classifyRequiredObjects(schemaOutput) {
  const parsed = extractSchemaObjects(schemaOutput);
  const mismatched = [];
  const presentLifecycle = [];
  const exactLifecycle = [];

  for (const expected of REQUIRED_SCHEMA_OBJECTS) {
    const actual = parsed.byName.get(expected.name);
    if (!actual) continue;
    if (MEMBER_LIFECYCLE_SCHEMA_OBJECTS.some(item => item.name === expected.name)) {
      presentLifecycle.push(expected.name);
    }
    if (actual.type === expected.type && actual.tbl_name === expected.tbl_name) {
      if (MEMBER_LIFECYCLE_SCHEMA_OBJECTS.some(item => item.name === expected.name)) {
        exactLifecycle.push(expected.name);
      }
      continue;
    }
    mismatched.push(`${expected.name}:expected=${expected.type}/${expected.tbl_name}:actual=${actual.type}/${actual.tbl_name || 'EMPTY'}`);
  }

  return Object.freeze({
    malformed: parsed.malformed,
    mismatched: Object.freeze(mismatched),
    present_lifecycle: Object.freeze(presentLifecycle),
    exact_lifecycle: Object.freeze(exactLifecycle)
  });
}

export function classifyMemberLifecycleSchemaGate({ pendingOutput, schemaOutput } = {}) {
  const runtime = classifyMemberProductionRuntimeSchema({ pendingOutput, schemaOutput });
  const objects = classifyRequiredObjects(schemaOutput);

  const legacyApplied =
    runtime.pending.legacy.length === 0 &&
    runtime.receipt.legacy_table_count === MEMBER_RUNTIME_LEGACY_TABLES.length &&
    runtime.receipt.legacy_tracked_migration_count === MEMBER_RUNTIME_LEGACY_MIGRATIONS.length;

  const lifecycleCleanPending =
    sameSet(runtime.pending.lifecycle, MEMBER_LIFECYCLE_SCHEMA_MIGRATIONS) &&
    runtime.pending.unknown.length === 0 &&
    runtime.receipt.lifecycle_table_count === 0 &&
    runtime.receipt.lifecycle_tracked_migration_count === 0 &&
    objects.present_lifecycle.length === 0;

  const lifecycleApplied =
    runtime.pending.lifecycle.length === 0 &&
    runtime.pending.unknown.length === 0 &&
    runtime.receipt.lifecycle_table_count === MEMBER_LIFECYCLE_SCHEMA_TABLES.length &&
    runtime.receipt.lifecycle_tracked_migration_count === MEMBER_LIFECYCLE_SCHEMA_MIGRATIONS.length &&
    objects.exact_lifecycle.length === MEMBER_LIFECYCLE_SCHEMA_OBJECTS.length;

  const schemaObjectsClean = objects.malformed === false && objects.mismatched.length === 0;

  const preApplyReady =
    runtime.status === 'BLOCKED_KNOWN_LIFECYCLE_MIGRATIONS_PENDING' &&
    legacyApplied &&
    lifecycleCleanPending &&
    schemaObjectsClean;

  const postApplyConfirmed =
    runtime.status === 'ALREADY_APPLIED_CONFIRMED' &&
    runtime.ready === true &&
    legacyApplied &&
    lifecycleApplied &&
    runtime.pending.all.length === 0 &&
    schemaObjectsClean;

  return Object.freeze({
    pre_apply_ready: preApplyReady,
    post_apply_confirmed: postApplyConfirmed,
    status: preApplyReady
      ? 'LIFECYCLE_APPLY_READY'
      : postApplyConfirmed
        ? 'LIFECYCLE_APPLY_CONFIRMED'
        : 'BLOCKED_LIFECYCLE_SCHEMA_STATE',
    expected: Object.freeze({
      lifecycle_migrations: MEMBER_LIFECYCLE_SCHEMA_MIGRATIONS,
      lifecycle_tables: MEMBER_LIFECYCLE_SCHEMA_TABLES,
      lifecycle_schema_objects: MEMBER_LIFECYCLE_SCHEMA_OBJECTS,
      legacy_migration_count: MEMBER_RUNTIME_LEGACY_MIGRATIONS.length,
      legacy_table_count: MEMBER_RUNTIME_LEGACY_TABLES.length
    }),
    evidence: Object.freeze({
      malformed_schema_objects: objects.malformed,
      mismatched_required_objects: objects.mismatched,
      present_lifecycle_schema_objects: objects.present_lifecycle,
      exact_lifecycle_schema_object_count: objects.exact_lifecycle.length
    }),
    runtime
  });
}

export function requireMemberLifecyclePreApplyState(input = {}) {
  const result = classifyMemberLifecycleSchemaGate(input);
  if (!result.pre_apply_ready) {
    const error = new Error(`BLOCKED_MEMBER_LIFECYCLE_SCHEMA_PRE_APPLY:${result.status}:${result.runtime.status}`);
    error.code = 'BLOCKED_MEMBER_LIFECYCLE_SCHEMA_PRE_APPLY';
    error.result = result;
    throw error;
  }
  return result;
}

export function requireMemberLifecyclePostApplyState(input = {}) {
  const result = classifyMemberLifecycleSchemaGate(input);
  if (!result.post_apply_confirmed) {
    const error = new Error(`BLOCKED_MEMBER_LIFECYCLE_SCHEMA_POST_APPLY:${result.status}:${result.runtime.status}`);
    error.code = 'BLOCKED_MEMBER_LIFECYCLE_SCHEMA_POST_APPLY';
    error.result = result;
    throw error;
  }
  return result;
}
