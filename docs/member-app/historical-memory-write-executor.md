# MIZUNO PHOTO MEMBER — Historical MEMORY Write Executor

Baseline: 2026-09-24 JST

## Purpose

This phase implements the source-only executor that can eventually apply an already-validated historical MEMORY sync plan.

It exists so existing customers can later receive their completed historical shoots as Member MEMORIES without rebuilding identity logic.

This phase does not execute any Production write, does not apply the Member schema, does not wire a Production route, and does not deploy Production.

## Write boundary

The executor is intentionally narrower than the planner.

It may write only:

- `member_memories`

It must not write:

- `customers`
- `customer_reservations`
- `customer_delivery_links`
- `customer_family_groups`
- `customer_family_customer_links`
- `member_memory_media`

## Required gates before any write

Two independent gates are required in source:

1. caller must pass `approved: true`;
2. environment must contain exactly:
   `MEMBER_MEMORY_WRITE_MODE=enabled`.

Default behavior is disabled.

If either gate is absent, the executor returns without writing.

This does not replace Owner authorization for Production. It is an additional technical fail-closed control.

## Fresh-plan rule

The executor never accepts a precomputed `to_create` array from a request.

Immediately before writing it calls the existing family-scoped planner again:

`buildMemberMemoryPlanForFamily(env, { family_id, customer_id })`

Therefore the write is based on current CRM/Family/MEMORY state, not stale client input.

The fresh plan must prove:

- canonical eight-digit Customer ID;
- exact active Customer -> Family link;
- same requested Family ID;
- completed/controlled-legacy source eligibility;
- stable reservation ID;
- no duplicate source reservation in the batch;
- no existing cross-Family conflict;
- no existing cross-Customer conflict;
- Member MEMORY schema already applied.

Any conflict stops before the write.

## Atomic write requirement

The executor requires `env.DB.batch`.

It will not fall back to sequential independent INSERT statements.

This keeps a multi-MEMORY bootstrap from intentionally using a partial-write strategy.

The batch is limited to a bounded number of rows.

## Idempotency

The Member schema already contains:

`UNIQUE(source_system, source_reservation_id)`

The executor additionally rebuilds the planner before every execution.

If all eligible memories are already synchronized:

- no INSERT is prepared;
- `write_executed=false`;
- the result is an idempotent no-op.

## Deterministic MEMORY ID

New MEMORY IDs are deterministic opaque IDs derived from:

`source_system + source_reservation_id`

using SHA-256.

The customer-facing ID does not contain the raw reservation ID.

Recomputing for the same source reservation yields the same MEMORY ID.

The canonical idempotency authority remains the database unique source key, not only the generated MEMORY ID.

## Data written

For each approved missing MEMORY:

- deterministic `memory_id`
- exact `family_id`
- `source_system`
- exact canonical `source_customer_id`
- stable `source_reservation_id`
- shoot date
- genre
- title
- exact reservation-scoped delivery link ID when present
- validated HTTPS Amazon Photos URL when present
- published flag from the trusted plan

No photo binaries are written.

## Concurrent writer behavior

The executor does not blindly retry an INSERT batch after an error.

After a batch failure it rebuilds the authoritative plan.

If another writer has already safely completed the same source reservations for the same Customer/Family, the result is treated as an idempotent concurrent-writer no-op.

Otherwise the failure is returned for review.

## Post-write verification

After a successful atomic batch, the executor rebuilds the plan again.

Expected postcondition:

- plan status is OK;
- conflicts = 0;
- `to_create = 0`.

If that postcondition is not true, the executor reports a post-write verification failure requiring review.

## Production status

Current state:

- executor source exists;
- regression tests exist;
- Production route wiring = 0;
- Production write mode activation = 0;
- Production schema apply = 0;
- Production D1 write = 0;
- LINE send = 0;
- Production deploy = 0.

The committed health contract reports Production write as disabled even if a test/local environment enables the executor.

## Required Production gates

Before the first real historical MEMORY write:

1. Member Family schema must be applied under a separate Owner-approved schema gate;
2. Member MEMORY schema must be applied under a separate Owner-approved schema gate;
3. exact Production Customer/Family evidence must be verified;
4. a read-only historical plan must be reviewed;
5. a fresh Owner authorization must approve the exact Production write scope;
6. write mode may then be enabled only for the approved execution path;
7. post-write verification must pass;
8. write mode should be returned to disabled when the controlled operation is complete unless a separately approved runtime design replaces this one.

## Not included

This phase does not include:

- schema application;
- Member registration write;
- Family creation/link write;
- Customer creation;
- Customer ID generation;
- session issuance;
- LINE Login;
- private media upload;
- media binary storage;
- customer-facing route wiring;
- Production deployment.
