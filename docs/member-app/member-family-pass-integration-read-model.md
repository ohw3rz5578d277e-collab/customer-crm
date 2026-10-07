# Member / FAMILY PASS integration read-model contract

## Purpose

This layer connects the new Member/Prospect lifecycle foundation to the already-established FAMILY PASS, MEMORIES, Family Passport, TODAY'S MEMORY, NEXT MEMORY, and BLACK contracts without changing those existing product rules.

## Existing Customer Member

An existing Customer Member must have:

- canonical Member Identity
- canonical Customer ID
- exact verified Family link
- explicit published/non-deleted family Memory count evidence

The integration delegates tier computation to the existing member-family-pass read model rather than duplicating thresholds.

Locked rules remain:

- FAMILY PASS is family-scoped
- only published, non-deleted MEMORIES count
- one MEMORY equals one shoot
- BLACK threshold is exactly 10 MEMORIES
- durable BLACK remains lifetime entitlement when already persisted
- BLACK commercial contract remains PHOTO GOODS 10% OFF FOREVER
- BLACK does not discount shooting fees
- BLACK award/enforcement is not automatic

Family Passport, TODAY'S MEMORY, and NEXT MEMORY remain owned by their existing read models.

## Prospect Member

A Prospect has no canonical Customer ID or verified Family link yet.

Therefore a Prospect must not receive another Customer's:

- MEMORIES
- Family Passport
- TODAY'S MEMORY
- NEXT MEMORY
- FAMILY PASS tier/BLACK state

Prospect-visible Member data may include their own consent, acquisition attribution, and signup-benefit state.

## Promotion

Prospect -> Customer promotion does not itself grant Family/MEMORIES access. The promoted Member gains those reads only after the canonical Customer ID and exact Family link are independently verified by the existing family identity contract.

## Mutation boundary

This integration layer is read-only. It cannot award BLACK, backfill MEMORIES, create Family links, generate Customer IDs, redeem signup benefits, send LINE, or apply discounts.

## Authorization boundary

No Production read/write, D1 mutation/migration, CRM/Google write, Customer ID generation/change/merge, LINE send, deploy, route/secret/R2/UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
