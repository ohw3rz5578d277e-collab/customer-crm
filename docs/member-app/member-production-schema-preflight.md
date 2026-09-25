# MIZUNO PHOTO MEMBER — Production Schema Preflight

This is a **read-only Production schema gate** for the nine source-managed Member migrations dated 2026-09-24.

It does not apply schema, write D1, deploy the Worker, activate Member routes, send LINE messages, or modify CRM customer data.

## Exact migrations

1. `20260924_member_creative_catalog_foundation.sql`
2. `20260924_member_family_identity_foundation.sql`
3. `20260924_member_family_pass_entitlement_foundation.sql`
4. `20260924_member_favorite_mutation_rate_limit_foundation.sql`
5. `20260924_member_memory_core_foundation.sql`
6. `20260924_member_memory_favorites_foundation.sql`
7. `20260924_member_news_catalog_foundation.sql`
8. `20260924_member_public_asset_registry_foundation.sql`
9. `20260924_member_shop_catalog_foundation.sql`

The source audit requires every statement to be additive `CREATE TABLE IF NOT EXISTS` or `CREATE INDEX IF NOT EXISTS`. ALTER, DML, DROP, REPLACE, TRUNCATE, FOREIGN KEY, and REFERENCES are rejected.

## Production read-only classification

The preflight reads:

- current remote pending D1 migrations;
- the eleven expected Member/Family tables in `sqlite_master`;
- Member migration tracking rows in `d1_migrations_managed`.

Accepted states are:

- `ALL_9_PENDING_CLEAN`: all nine exact migrations pending, no Member schema table present, no Member migration tracked.
- `ALREADY_APPLIED_CONFIRMED`: no Member migration pending, all expected tables present, all nine migrations tracked.

Any partial state fails closed with `INCONSISTENT_MEMBER_SCHEMA_STATE`.

Any unrelated pending migration fails closed with `BLOCKED_NON_MEMBER_PENDING_MIGRATION`.

## Dispatch

After this workflow is merged, the Owner-only release-gate command is:

`/member-schema-preflight sha=<exact-current-main-sha>`

The bridge is pinned to issue #26 and requires the exact current main SHA.

There is intentionally **no schema-apply command** in this phase.

## Safety outputs

- `MEMBER_SCHEMA_APPLY=0`
- `PRODUCTION_D1_WRITE=0`
- `PRODUCTION_DEPLOY=0`
- `CRM_WRITE=0`
- `LINE_SEND=0`
