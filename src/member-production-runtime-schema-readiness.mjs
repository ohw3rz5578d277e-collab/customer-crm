export const MEMBER_RUNTIME_LEGACY_MIGRATIONS = Object.freeze([
  '20260924_member_creative_catalog_foundation.sql',
  '20260924_member_family_identity_foundation.sql',
  '20260924_member_family_pass_entitlement_foundation.sql',
  '20260924_member_favorite_mutation_rate_limit_foundation.sql',
  '20260924_member_memory_core_foundation.sql',
  '20260924_member_memory_favorites_foundation.sql',
  '20260924_member_news_catalog_foundation.sql',
  '20260924_member_public_asset_registry_foundation.sql',
  '20260924_member_shop_catalog_foundation.sql'
]);

export const MEMBER_RUNTIME_LEGACY_TABLES = Object.freeze([
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
]);

export const MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS = Object.freeze([
  Object.freeze({
    name: '20261007_member_identity_prospect_foundation.sql',
    tables: Object.freeze([
      'member_identities',
      'member_prospects',
      'member_customer_invitations',
      'member_profile_change_review_queue'
    ])
  }),
  Object.freeze({
    name: '20261009_member_registration_consent_event_foundation.sql',
    tables: Object.freeze([
      'member_registration_events',
      'member_consent_evidence'
    ])
  })
]);

export const MEMBER_RUNTIME_LIFECYCLE_TABLES = Object.freeze(
  MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS.flatMap(item => item.tables)
);

function extractPendingSqlNames(output) {
  return [...new Set(
    [...String(output || '').matchAll(/[A-Za-z0-9_.-]+\.sql/g)].map(match => match[0])
  )];
}

function extractReceiptState(output) {
  try {
    const parsed = JSON.parse(String(output || ''));
    const chunks = Array.isArray(parsed) ? parsed : [parsed];
    const names = [];
    const tableNames = [];
    for (const chunk of chunks) {
      const rows = Array.isArray(chunk?.results) ? chunk.results : [];
      for (const row of rows) {
        if (typeof row?.name !== 'string' || !row.name) continue;
        names.push(row.name);
        if (row?.type === 'table') tableNames.push(row.name);
      }
    }
    return {
      names: [...new Set(names)],
      tableNames: [...new Set(tableNames)],
      malformed: false
    };
  } catch {
    return { names: [], tableNames: [], malformed: true };
  }
}

export function classifyMemberProductionRuntimeSchema({ pendingOutput, schemaOutput } = {}) {
  const pendingNames = extractPendingSqlNames(pendingOutput);
  const receipt = extractReceiptState(schemaOutput);
  const receiptNames = new Set(receipt.names);
  const receiptTableNames = new Set(receipt.tableNames);

  const lifecycleNames = MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS.map(item => item.name);
  const knownMigrations = new Set([
    ...MEMBER_RUNTIME_LEGACY_MIGRATIONS,
    ...lifecycleNames
  ]);

  const legacyPending = pendingNames.filter(name => MEMBER_RUNTIME_LEGACY_MIGRATIONS.includes(name));
  const lifecyclePending = pendingNames.filter(name => lifecycleNames.includes(name));
  const unknownPending = pendingNames.filter(name => !knownMigrations.has(name));

  const presentLegacyTables = MEMBER_RUNTIME_LEGACY_TABLES.filter(name => receiptTableNames.has(name));
  const trackedLegacyMigrations = MEMBER_RUNTIME_LEGACY_MIGRATIONS.filter(name => receiptNames.has(name));
  const missingLegacyTables = MEMBER_RUNTIME_LEGACY_TABLES.filter(name => !receiptTableNames.has(name));
  const missingLegacyTrackedMigrations = MEMBER_RUNTIME_LEGACY_MIGRATIONS.filter(name => !receiptNames.has(name));

  const presentLifecycleTables = MEMBER_RUNTIME_LIFECYCLE_TABLES.filter(name => receiptTableNames.has(name));
  const trackedLifecycleMigrations = lifecycleNames.filter(name => receiptNames.has(name));

  const blockers = [];
  if (receipt.malformed) blockers.push('malformed_schema_receipt');
  if (legacyPending.length) blockers.push('legacy_member_migrations_pending');
  if (unknownPending.length) blockers.push('unknown_pending_migrations');
  if (missingLegacyTables.length) blockers.push('legacy_member_tables_missing');
  if (missingLegacyTrackedMigrations.length) blockers.push('legacy_member_migration_receipts_missing');
  if (lifecyclePending.length) blockers.push('known_lifecycle_migrations_pending');

  for (const migration of MEMBER_RUNTIME_LIFECYCLE_MIGRATIONS) {
    const pending = lifecyclePending.includes(migration.name);
    const tracked = receiptNames.has(migration.name);
    const presentTables = migration.tables.filter(name => receiptTableNames.has(name));
    const missingTables = migration.tables.filter(name => !receiptTableNames.has(name));

    if (pending) {
      if (tracked) blockers.push(`pending_lifecycle_migration_already_tracked:${migration.name}`);
      if (presentTables.length) blockers.push(`pending_lifecycle_migration_has_partial_schema:${migration.name}`);
      continue;
    }

    if (!tracked) blockers.push(`lifecycle_migration_receipt_missing:${migration.name}`);
    if (missingTables.length) blockers.push(`lifecycle_tables_missing:${migration.name}`);
  }

  const uniqueBlockers = [...new Set(blockers)];
  const onlyKnownLifecyclePending =
    uniqueBlockers.length === 1 && uniqueBlockers[0] === 'known_lifecycle_migrations_pending';
  const ready = uniqueBlockers.length === 0;

  return Object.freeze({
    ready,
    status: ready
      ? 'ALREADY_APPLIED_CONFIRMED'
      : onlyKnownLifecyclePending
        ? 'BLOCKED_KNOWN_LIFECYCLE_MIGRATIONS_PENDING'
        : 'BLOCKED_SCHEMA_RECEIPT_INCONSISTENT',
    blockers: Object.freeze(uniqueBlockers),
    pending: Object.freeze({
      all: Object.freeze(pendingNames),
      legacy: Object.freeze(legacyPending),
      lifecycle: Object.freeze(lifecyclePending),
      unknown: Object.freeze(unknownPending)
    }),
    receipt: Object.freeze({
      malformed: receipt.malformed,
      legacy_table_count: presentLegacyTables.length,
      legacy_tracked_migration_count: trackedLegacyMigrations.length,
      lifecycle_table_count: presentLifecycleTables.length,
      lifecycle_tracked_migration_count: trackedLifecycleMigrations.length,
      total_required_table_count: MEMBER_RUNTIME_LEGACY_TABLES.length + MEMBER_RUNTIME_LIFECYCLE_TABLES.length,
      total_required_migration_count: MEMBER_RUNTIME_LEGACY_MIGRATIONS.length + lifecycleNames.length
    })
  });
}
