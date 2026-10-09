import {
  MEMBER_RUNTIME_LEGACY_MIGRATIONS,
  MEMBER_RUNTIME_LEGACY_TABLES,
  MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS,
  MEMBER_RUNTIME_LIFECYCLE_TABLES,
  classifyMemberProductionRuntimeSchema
} from './member-production-runtime-schema-readiness.mjs';

export const MEMBER_LIFECYCLE_SCHEMA_APPLY_BUILD = 'member-lifecycle-schema-apply-gate-20261009-01';

export const MEMBER_LIFECYCLE_SCHEMA_APPLY_ORDER = Object.freeze(
  MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS.map(item => item.name)
);

function commonInvariant() {
  return Object.freeze({
    production_deploy: false,
    production_worker_activation: false,
    member_route_activation: false,
    private_media_route_activation: false,
    line_login_production_activation: false,
    crm_write: false,
    line_send: false,
    google_write: false,
    r2_access: false,
    customer_id_generation: false,
    family_id_generation: false,
    customer_family_member_prospect_mutation: false,
    black_entitlement_write: false,
    memory_write: false,
    paid_spend: false,
    automatic_retry: false,
    automatic_rollback: false
  });
}

function exactRequiredLifecyclePending(runtime) {
  const required = MEMBER_LIFECYCLE_SCHEMA_APPLY_ORDER;
  const observed = runtime.pending.lifecycle;
  return (
    runtime.status === 'BLOCKED_KNOWN_LIFECYCLE_MIGRATIONS_PENDING' &&
    runtime.pending.all.length === required.length &&
    runtime.pending.legacy.length === 0 &&
    runtime.pending.unknown.length === 0 &&
    observed.length === required.length &&
    required.every(name => observed.includes(name)) &&
    runtime.receipt.legacy_table_count === MEMBER_RUNTIME_LEGACY_TABLES.length &&
    runtime.receipt.legacy_tracked_migration_count === MEMBER_RUNTIME_LEGACY_MIGRATIONS.length &&
    runtime.receipt.lifecycle_table_count === 0 &&
    runtime.receipt.lifecycle_tracked_migration_count === 0
  );
}

function exactCompleteLifecycleState(runtime) {
  return (
    runtime.ready === true &&
    runtime.status === 'ALREADY_APPLIED_CONFIRMED' &&
    runtime.pending.all.length === 0 &&
    runtime.receipt.legacy_table_count === MEMBER_RUNTIME_LEGACY_TABLES.length &&
    runtime.receipt.legacy_tracked_migration_count === MEMBER_RUNTIME_LEGACY_MIGRATIONS.length &&
    runtime.receipt.lifecycle_table_count === MEMBER_RUNTIME_LIFECYCLE_TABLES.length &&
    runtime.receipt.lifecycle_tracked_migration_count === MEMBER_LIFECYCLE_SCHEMA_APPLY_ORDER.length
  );
}

function hasPartialLifecycleEvidence(runtime) {
  const required = MEMBER_LIFECYCLE_SCHEMA_APPLY_ORDER.length;
  return (
    runtime.receipt.lifecycle_table_count > 0 ||
    runtime.receipt.lifecycle_tracked_migration_count > 0 ||
    (runtime.pending.lifecycle.length > 0 && runtime.pending.lifecycle.length < required)
  );
}

export function classifyMemberLifecycleSchemaApplyState({
  pendingOutput,
  schemaOutput,
  phase = 'pre'
} = {}) {
  const runtime = classifyMemberProductionRuntimeSchema({ pendingOutput, schemaOutput });
  const invariant = commonInvariant();

  if (phase === 'pre') {
    const ready = exactRequiredLifecyclePending(runtime);
    return Object.freeze({
      build: MEMBER_LIFECYCLE_SCHEMA_APPLY_BUILD,
      phase,
      ready,
      status: ready
        ? 'PRE_APPLY_EXACT_LIFECYCLE_PENDING'
        : hasPartialLifecycleEvidence(runtime)
          ? 'BLOCKED_PARTIAL_LIFECYCLE_SCHEMA_STATE'
          : 'BLOCKED_PRE_APPLY_SCHEMA_STATE',
      apply_order: MEMBER_LIFECYCLE_SCHEMA_APPLY_ORDER,
      runtime,
      invariant
    });
  }

  if (phase === 'post') {
    const ready = exactCompleteLifecycleState(runtime);
    return Object.freeze({
      build: MEMBER_LIFECYCLE_SCHEMA_APPLY_BUILD,
      phase,
      ready,
      status: ready
        ? 'POST_APPLY_LIFECYCLE_SCHEMA_COMPLETE'
        : hasPartialLifecycleEvidence(runtime)
          ? 'BLOCKED_PARTIAL_LIFECYCLE_SCHEMA_APPLY'
          : 'BLOCKED_POST_APPLY_SCHEMA_STATE',
      apply_order: MEMBER_LIFECYCLE_SCHEMA_APPLY_ORDER,
      runtime,
      invariant
    });
  }

  return Object.freeze({
    build: MEMBER_LIFECYCLE_SCHEMA_APPLY_BUILD,
    phase,
    ready: false,
    status: 'BLOCKED_INVALID_PHASE',
    apply_order: MEMBER_LIFECYCLE_SCHEMA_APPLY_ORDER,
    runtime,
    invariant
  });
}
