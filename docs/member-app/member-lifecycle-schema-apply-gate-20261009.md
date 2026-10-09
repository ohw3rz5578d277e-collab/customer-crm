# Member lifecycle schema apply gate — 2026-10-09

## Purpose

This gate prepares a separately Owner-authorized Production schema mutation for exactly these two additive lifecycle migrations:

1. `20261007_member_identity_prospect_foundation.sql`
2. `20261009_member_registration_consent_event_foundation.sql`

It does not authorize or perform an apply merely by being merged. Actual execution remains behind a fresh Issue #26 exact-SHA Owner command.

## Owner command

The only accepted command shape is:

`/member-lifecycle-schema-apply sha=<40-char-lowercase-main-sha> readiness_run=<runtime-readiness-run-id> confirm=APPLY_MEMBER_LIFECYCLE_SCHEMA`

The issue bridge requires:

- Issue #26;
- Owner actor `ohw3rz5578d277e-collab`;
- exact lowercase 40-character SHA;
- exact current `main` equality;
- numeric runtime-readiness run ID;
- exact confirmation token;
- no concurrent known Production mutation.

The worker workflow additionally refuses direct human `workflow_dispatch`; it must be dispatched by the validated issue bridge.

## Required runtime-readiness receipt

Before any schema mutation, the gate requires the supplied run to be:

- `Member Production runtime readiness`;
- `workflow_dispatch`;
- completed on the exact authorized SHA;
- dispatched by `github-actions[bot]` through the bridge path;
- failed closed at `Classify read-only Member runtime readiness`;
- successful through all preceding exact-SHA, Steps 1–9, default-off, Cloudflare auth, secret-name, pending-migration and schema-receipt observations.

A failed readiness run is acceptable only because the live pre-apply gate independently re-reads Production and requires the precise known-lifecycle-pending state described below. A stale or unrelated failure cannot authorize schema mutation.

## Exact pre-apply Production state

The gate performs read-only D1 observations twice before the mutation. Both observations must show exactly:

- legacy Member tables: `11`;
- legacy tracked Member migrations: `9`;
- lifecycle tables: `0`;
- lifecycle tracked migrations: `0`;
- legacy pending migrations: `0`;
- unknown pending migrations: `0`;
- lifecycle pending migrations: exactly `2`;
- pending lifecycle names equal exactly the two migrations listed above.

Any partial lifecycle schema, unknown pending migration, missing legacy table/receipt, extra pending migration, malformed receipt, main drift, or receipt mismatch fails closed before mutation.

## Apply sequence

The source lifecycle order is:

1. identity / prospect foundation;
2. registration / consent event foundation.

The Production mutation uses the canonical D1 managed-migration mechanism and only after the gate proves the remote pending set contains exactly these two files and nothing else. It does not bypass `d1_migrations_managed` with raw ad-hoc SQL.

The two migrations are additive and the second does not declare a SQL foreign-key dependency on the first. The managed migration engine remains responsible for the actual pending execution sequence.

## Partial failure handling

The mutation step is deliberately allowed to return control to the workflow even when the managed apply command fails. The workflow then performs read-only post-state observations.

There is no automatic retry and no automatic rollback.

If any partial state is observed — for example only one lifecycle migration tracked or any lifecycle tables present while the full target is not complete — the workflow fails closed as:

`BLOCKED_PARTIAL_LIFECYCLE_SCHEMA_APPLY`

A new Owner decision and fresh exact-state evidence are required before any later mutation.

## Required post-apply state

Success requires exactly:

- total required tables: `17` (`11` legacy + `6` lifecycle);
- total tracked required migrations: `11` (`9` legacy + `2` lifecycle);
- pending migrations: `0`;
- both lifecycle migration receipts present;
- all six lifecycle tables present as real SQLite tables.

The workflow must fail if the managed apply command outcome is not successful even if later observations appear ambiguous.

## Non-authorization boundary

This schema gate does not authorize or perform:

- Production deploy;
- Production Worker activation;
- Member route activation;
- private-media route activation;
- LINE Login Production activation;
- secret or token creation/change/rotation;
- Customer / Family / Member / Prospect data mutation;
- CRM write or mutation;
- Customer ID or Family ID generation/update/delete/merge;
- LINE send;
- Google/GAS write;
- R2 object access;
- BLACK entitlement write/backfill;
- MEMORY write/backfill;
- commerce or discount enforcement;
- paid spend.

Schema apply itself requires a separate fresh exact-SHA Owner authorization after this source gate has been reviewed and merged.
