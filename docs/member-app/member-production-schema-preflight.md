# MIZUNO PHOTO MEMBER — Production Lifecycle Schema Preflight

This is a **read-only Production schema gate** for the two Member lifecycle migrations introduced after the original 2026-09-24 Member schema was already applied.

It does not apply schema, write D1, deploy the Worker, activate Member routes, send LINE messages, modify CRM customer data, access R2 objects, or change secrets.

## Canonical Production baseline

The preflight accepts only the exact lifecycle-pending state:

- the original 11 Member/Family tables exist as real SQLite `table` objects;
- the original nine `20260924_member_*` migrations are tracked and are not pending;
- exactly these two lifecycle migrations are pending:
  1. `20261007_member_identity_prospect_foundation.sql`
  2. `20261009_member_registration_consent_event_foundation.sql`
- none of the lifecycle schema objects exists yet;
- neither lifecycle migration is already tracked;
- no unrelated migration is pending.

Lifecycle schema objects means the six lifecycle tables, seven named indexes, and four append-only triggers introduced by the two migrations. A same-named SQLite object of the wrong `type`, or an index/trigger bound to the wrong `tbl_name`, is a blocker.

Any partial, contradictory, malformed, collision, or unknown state fails closed.

## Lifecycle schema objects

The six lifecycle tables are:

- `member_identities`
- `member_prospects`
- `member_customer_invitations`
- `member_profile_change_review_queue`
- `member_registration_events`
- `member_consent_evidence`

The migrations also define seven named indexes and four append-only triggers. Preflight reads each expected object's exact `name`, `type`, and `tbl_name`; the gate therefore rejects a pre-existing VIEW, wrong-table index, wrong-table trigger, or other same-name collision before a future apply can be authorized.

Neither migration alters or writes the canonical `customers`, `customer_reservations`, or `customer_delivery_links` tables.

## Classification

`src/member-production-lifecycle-schema-gate.mjs` reuses the exact runtime schema receipt classifier and emits:

- `LIFECYCLE_APPLY_READY` only for the exact baseline above;
- `LIFECYCLE_APPLY_CONFIRMED` only after both migrations are fully applied, all 17 required tables are real tables, all seven indexes/four triggers have exact bindings, and all 11 migration receipts are present;
- `BLOCKED_LIFECYCLE_SCHEMA_STATE` for every other state.

## Owner authorization

The only preflight command is:

`/member-schema-preflight sha=<exact-current-main-sha>`

The bridge is pinned to release-gate issue #26 and Owner `ohw3rz5578d277e-collab`. It forwards the exact Owner comment ID. The workflow independently re-reads that comment and requires exact actor, exact body, exact issue #26, and exact current-main SHA before any Cloudflare/D1 observation.

## Read-only observations

The workflow may only:

- authenticate to Cloudflare;
- list pending managed D1 migrations;
- SELECT exact schema object names/types/table bindings from `sqlite_master`;
- SELECT exact migration receipt names from `d1_migrations_managed`.

## Safety outputs

- `MEMBER_SCHEMA_APPLY=0`
- `PRODUCTION_D1_WRITE=0`
- `PRODUCTION_DEPLOY=0`
- `MEMBER_ROUTE_ACTIVATION=0`
- `CRM_WRITE=0`
- `LINE_SEND=0`

A successful preflight is only a receipt. It does **not** authorize schema apply. Apply requires a separate fresh exact-SHA Owner command and a successful preflight run ID.
