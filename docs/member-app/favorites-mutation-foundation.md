# MIZUNO PHOTO MEMBER — Favorite Mutation Foundation

Baseline: 2026-09-25 JST

## Purpose

This phase adds a guarded source-only write foundation for changing one Member customer's Favorite state on one visible MEMORY.

It does not activate a Production route or UI mutation.

## Declarative mutation contract

The mutation input is:

- exact server Member session
- exact MEMORY ID
- `desired_favorite=true|false`

The caller does not send an imperative "toggle".

This avoids race-prone state inversion.

The server first rebuilds the current state and decides one of:

- `add`
- `remove`
- `none`

If current state already equals desired state, the operation is an idempotent no-op.

## Identity and MEMORY authorization

Before any write plan can exist:

1. canonical 8-digit Customer ID must be valid;
2. explicit Customer -> Family link must resolve exactly;
3. session Family must match that link;
4. Favorite schema must exist;
5. MEMORY schema must exist;
6. exact MEMORY must belong to the same Family;
7. MEMORY must be published and not deleted.

Another-Family, draft, deleted, or nonexistent MEMORY is returned as `memory_not_found`.

No fuzzy identity is allowed.

## Dual write gate

The executor requires both:

1. `approved === true`
2. `MEMBER_FAVORITES_WRITE_MODE=enabled`

Without both, zero write statements execute.

The default environment state is disabled.

## Exact-key writes

Add uses only:

`INSERT INTO member_memory_favorites (family_id, customer_id, memory_id)`

Remove uses only:

`DELETE FROM member_memory_favorites WHERE family_id=? AND customer_id=? AND memory_id=?`

There is no UPDATE.

The executor never writes:

- customers
- customer_reservations
- customer_delivery_links
- Family identity
- MEMORIES
- LINE data

## Idempotency and concurrency

Before write, the plan is rebuilt from current database state.

After write, the plan is rebuilt again.

Success requires the post-write plan to say:

- action = none
- current Favorite equals desired Favorite

If a write throws, the executor re-reads state before returning failure.

If another writer has already achieved the same desired state, the executor returns an idempotent concurrent-writer no-op rather than retrying blindly.

## Current MEMORIES behavior

Even after this foundation exists, customer-facing MEMORIES remains:

- `favorite_mutable=false`

The write executor being source-ready does not imply a route or UI is active.

A later route/UI phase must be separately reviewed.

## Production status

This source change does not authorize or perform:

- Production schema apply
- Production route wiring
- Production Favorite writes
- final MEMORIES UI
- Member session issuance
- LINE send
- Production deploy

## Safety status

Current foundation provides:

- exact Family + Customer + MEMORY scope
- published/non-deleted MEMORY requirement
- declarative desired state
- explicit approved flag
- env write-mode gate
- default write disabled
- idempotent add
- idempotent remove
- pre-write plan rebuild
- post-write verification
- concurrent writer re-read
- UPDATE support = 0
- canonical CRM write = 0
- LINE send = 0
- Production route = 0
- Production write enabled = 0
