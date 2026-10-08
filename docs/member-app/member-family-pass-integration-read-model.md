# Member / FAMILY PASS integration read-model contract

## Purpose

This layer connects the new Member/Prospect lifecycle foundation to the already-established FAMILY PASS, MEMORIES, Family Passport, TODAY'S MEMORY, NEXT MEMORY, and BLACK contracts without changing those existing product rules.

This document is also the source-only contract for Member backend sequence **step 8: family-scoped MEMORY count + durable BLACK read evidence**.

## Existing Customer Member

An existing Customer Member must have:

- canonical Member Identity;
- canonical Customer ID;
- exact verified Member -> Customer binding;
- exact verified Customer -> Family link;
- explicitly verified published/non-deleted Family MEMORY count evidence;
- exact Family scope attached to that count evidence;
- verified entitlement-schema state;
- when the entitlement schema exists, an explicitly verified durable-BLACK query scoped to the same exact Family.

The integration delegates tier computation to the existing Member FAMILY PASS tier calculator rather than duplicating thresholds.

## Step 8 MEMORY-count evidence

A numeric count by itself is not sufficient evidence.

Before the integration can use a MEMORY count, all of the following must be true:

- `memory_count_verified === true`;
- the persisted count scope is the exact verified Family ID;
- the count is a non-negative JavaScript safe integer;
- booleans, arrays, objects, bigint values, numeric strings, blank strings, and unsafe integers are rejected;
- the count represents only `published=1` and non-deleted `member_memories` rows;
- one MEMORY equals one shoot.

The server-side FAMILY PASS reader uses an exact `family_id=?` query and exposes `count_verified=true` plus the exact evidence Family ID. A malformed database count fails closed instead of being coerced.

## Step 8 durable BLACK evidence

`durable_black_entitlement=true` or `false` by itself is never sufficient.

If the entitlement schema exists, the read evidence must establish:

- the entitlement read was verified;
- the entitlement query was scoped to the exact verified Family ID;
- the result cardinality is exactly `0` or `1`;
- `durable_black_entitlement=false` requires record count `0`;
- `durable_black_entitlement=true` requires record count `1`;
- the durable row's `family_id` matches the verified Family exactly;
- `black_lifetime` is the integer `1`, not a coerced string/boolean;
- `black_achieved_at` is a canonical millisecond UTC instant (`YYYY-MM-DDTHH:mm:ss.sssZ`);
- `qualifying_memory_count` is a non-negative safe integer and is at least `10`;
- `achievement_source` is exactly `published-member-memories`.

Malformed or crossed evidence fails closed and requires review. It must not silently downgrade a malformed durable BLACK row into an ordinary count-only tier.

If the entitlement schema is not applied, the integration may still compute the current tier from verified MEMORY count evidence, but it must report that lifetime BLACK persistence is unavailable and it must not invent a durable entitlement.

## Locked product rules

- FAMILY PASS is Family-scoped.
- Only published, non-deleted MEMORIES count.
- One MEMORY equals one shoot.
- BLACK threshold is exactly 10 MEMORIES.
- Durable BLACK remains lifetime entitlement when valid evidence is already persisted.
- BLACK commercial contract remains **PHOTO GOODS 10% OFF FOREVER**.
- BLACK does not discount shooting fees.
- BLACK award is not automatic.
- BLACK discount enforcement is not automatic.

Family Passport, TODAY'S MEMORY, and NEXT MEMORY remain owned by their existing read models.

## Prospect Member

A Prospect has no canonical Customer ID or verified Family link yet.

Therefore a Prospect must not receive another Customer's:

- MEMORIES;
- Family Passport;
- TODAY'S MEMORY;
- NEXT MEMORY;
- FAMILY PASS tier/BLACK state.

Prospect-visible Member data may include their own consent, acquisition attribution, and signup-benefit state.

## Promotion

Prospect -> Customer promotion does not itself grant Family/MEMORIES access. The promoted Member gains those reads only after the canonical Customer ID, exact Member -> Customer binding, and exact Customer -> Family link are independently verified.

## Mutation boundary

This integration layer is read-only. It cannot award BLACK, backfill MEMORIES, create Family links, generate Customer IDs, redeem signup benefits, send LINE, or apply discounts.

A valid step-8 read result is evidence for presentation/read decisions only. It is not authorization to execute the existing BLACK write executor or any commerce operation.

## Authorization boundary

No Production read/write, D1 mutation/migration, CRM/Google write, Customer ID or Family ID generation/change/merge, LINE send, deploy, route/secret/R2/UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized by this source-only step.
