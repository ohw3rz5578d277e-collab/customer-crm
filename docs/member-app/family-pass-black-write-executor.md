# MIZUNO PHOTO MEMBER — BLACK Entitlement Write Executor

Baseline: 2026-09-24 JST

## Purpose

This phase adds a guarded source-only executor for persisting the lifetime BLACK Family Pass entitlement proposed by the existing read-only achievement planner.

It does not apply the Production schema, activate a Production route, or execute any Production write.

## Write target

The executor may INSERT only into:

`member_family_pass_entitlements`

It must not write:

- customers
- reservations
- delivery links
- Family identity tables
- MEMORIES
- MEMORY media
- orders
- coupons
- pricing
- LINE data

It does not support UPDATE or DELETE.

## Required technical gates

Before any BLACK entitlement INSERT can occur, both source-level gates must be true:

1. caller passes `approved: true`
2. `MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE=enabled`

Default behavior is disabled.

These technical gates do not replace Owner authorization for a Production write. They are additional fail-closed controls.

## Fresh-plan rule

The executor never accepts a client-supplied entitlement record.

Immediately before write it rebuilds:

`buildMemberFamilyPassBlackAchievementPlan(env, session)`

The authoritative plan must confirm:

- server Member session identity
- canonical Customer ID
- exact explicit Customer -> Family link
- exact Family scope
- entitlement schema exists
- current published, non-deleted MEMORY count is still at least 10
- no valid durable BLACK record already exists

If any of these conditions change before the write, the executor does not award BLACK.

## INSERT-only lifetime state

The BLACK table has one row per Family.

The executor uses a single INSERT containing:

- exact Family ID
- `black_lifetime=1`
- server-generated `black_achieved_at`
- current qualifying MEMORY count
- `achievement_source=published-member-memories`

It never:

- updates the first achieved timestamp
- downgrades BLACK
- deletes BLACK
- replaces a valid row

A second execution for a Family that already has durable BLACK is an idempotent no-op.

## Concurrent writer behavior

The executor does not blind-retry a failed INSERT.

After an INSERT failure it re-reads the authoritative Family Pass state.

If another writer has safely created the same Family's durable BLACK entitlement, the result is treated as an idempotent concurrent-writer no-op.

If durable BLACK is still absent, the executor returns a review-required write failure.

## Post-write verification

After a successful INSERT the executor re-reads the Family Pass.

Expected postcondition:

- same Family ID
- durable BLACK entitlement exists
- effective tier is BLACK
- entitlement metadata is readable

Failure of this postcondition is reported as a post-write verification failure requiring review.

## Timestamp rule

`black_achieved_at` is generated server-side by the executor.

It is not supplied by the browser or customer request.

For the normal forward runtime this records when the durable BLACK entitlement was first awarded.

Historical backfill remains a separate future operation because reconstructing the exact historical moment of first reaching 10 MEMORIES may require independent evidence.

This executor does not perform automatic backfill.

## Commerce boundary

BLACK product contract remains:

**PHOTO GOODS 10% OFF FOREVER**

This executor does not:

- create a coupon
- change WooCommerce prices
- change Square prices
- write an order discount
- discount photography shooting fees

Commerce enforcement remains a separate future phase.

## Current status

Implemented in source:

- explicit approved flag required
- write mode defaults disabled
- fresh achievement plan rebuilt before write
- exact Family scope
- threshold rechecked immediately before write
- INSERT only
- Family primary-key idempotency
- no blind retry
- concurrent-writer re-read
- post-write verification
- no downgrade/delete/update
- no automatic backfill
- no discount enforcement

Not performed:

- Production schema apply
- Production entitlement write
- Production route wiring
- Production deploy
- automatic entitlement backfill
- BLACK downgrade/delete
- coupon or discount enforcement
- WooCommerce/Square write
- Customer/Family write
- Member session issuance
- LINE send

## Required Production gates

Before the first real BLACK entitlement write:

1. Member Family schema must already be valid;
2. Member MEMORY schema must already be valid;
3. BLACK entitlement schema must be separately approved and applied;
4. exact Family identity must be verified;
5. read-only Family Pass must show 10+ qualifying MEMORIES;
6. read-only BLACK achievement plan must propose `award_black_lifetime`;
7. Owner must explicitly authorize the exact Production write scope;
8. technical write mode may be enabled only for the approved operation;
9. post-write verification must confirm durable BLACK;
10. write mode should return to disabled unless a separately approved runtime design replaces this controlled operation.
