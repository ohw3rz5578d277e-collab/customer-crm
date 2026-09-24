# MIZUNO PHOTO MEMBER — Durable BLACK Entitlement Foundation

Baseline: 2026-09-24 JST

## Purpose

FAMILY PASS requires BLACK to be permanent after a Family first reaches 10 qualifying MEMORIES.

A current MEMORY count alone cannot guarantee that rule because a later correction may reduce the visible count. This phase adds a source-only durable BLACK entitlement model without applying Production schema or writing Production data.

## Additive schema

Source migration:

`migrations_managed/20260924_member_family_pass_entitlement_foundation.sql`

Table:

`member_family_pass_entitlements`

One row per Family stores only:

- exact `family_id`
- `black_lifetime=1`
- first BLACK achieved timestamp
- qualifying MEMORY count, constrained to at least 10
- achievement source
- audit timestamps

No customer PII, payment data, coupon data, order data, or photo binaries are stored.

There is no automatic backfill.

## Read behavior

The FAMILY PASS read model remains backward compatible.

If the entitlement table does not exist:

- current MEMORY count continues to determine the current tier;
- current count >=10 can display BLACK;
- lifetime persistence is reported as unavailable.

If the entitlement table exists and the Family has a valid BLACK row:

- BLACK takes precedence over current MEMORY count;
- even if current count later becomes 9, 5, or lower, the effective tier remains BLACK;
- `black_currently_qualified` continues to describe the present count;
- `black_lifetime_entitled` describes the durable lifetime state;
- `tier_basis` indicates whether the tier comes from current count or durable entitlement.

This distinction prevents data corrections from silently revoking lifetime BLACK status.

## Achievement plan

Source-only planner:

`buildMemberFamilyPassBlackAchievementPlan(env, session)`

It is read-only.

Behavior:

- below 10 qualifying MEMORIES -> no action;
- already has durable BLACK -> idempotent no-op;
- 10+ MEMORIES but entitlement schema missing -> blocked;
- 10+ MEMORIES, schema exists, no entitlement -> proposes `award_black_lifetime`.

The planner never performs an INSERT.

The proposed record is Family-scoped and uses the current count of published, non-deleted MEMORIES.

## Benefit contract

Confirmed product rule remains:

**BLACK = PHOTO GOODS 10% OFF FOREVER**

It does not apply to shooting fees.

This phase does not enforce the benefit. No WooCommerce, Square, coupon, price, or order mutation is performed.

## Future write executor

A later separately reviewed phase may implement an idempotent write executor for the proposed BLACK award.

That executor must:

1. rebuild the Family-scoped achievement plan immediately before write;
2. require explicit technical write enablement and Owner authorization;
3. insert only when 10+ qualifying MEMORIES are still present;
4. use Family ID as the unique lifetime entitlement key;
5. treat an existing valid BLACK row as success/no-op;
6. never delete or downgrade BLACK;
7. verify the durable row after write;
8. not enforce commerce discounts in the same operation.

## Current exclusions

Not included:

- Production schema apply
- Production entitlement write
- automatic backfill
- BLACK downgrade/delete
- goods discount enforcement
- coupon creation
- WooCommerce/Square write
- shooting fee discount
- Production route wiring
- Production deploy

## Safety

Current phase:

- additive Member-only schema source
- Family-scoped read support
- read-only achievement plan
- no canonical CRM mutation
- no entitlement write executor
- no Production D1 write
- no LINE send
