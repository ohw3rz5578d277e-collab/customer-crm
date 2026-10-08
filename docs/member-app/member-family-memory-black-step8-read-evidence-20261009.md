# Member Step 8 — Family-scoped MEMORY and durable BLACK read evidence

Baseline: 2026-10-09 JST

## Scope

This document implements Member backend sequence Step 8 as a source-only read-evidence contract.

It does not apply a migration, read Production D1, award BLACK, write an entitlement, enforce a discount, mutate Customer/Family/Member data, or activate a Production route.

## Identity prerequisite

A Customer-side FAMILY PASS read must be bound to the exact authenticated Member / canonical CRM Customer / Family tuple.

The read gate requires:

- valid Member Identity;
- valid canonical Customer ID;
- exact persisted Member -> Customer binding;
- eligible Member status;
- verified active Family record;
- exact active Customer -> Family link.

A Prospect remains isolated from Customer Family, MEMORY, Passport, TODAY'S MEMORY, NEXT MEMORY, and BLACK state.

## Family-scoped MEMORY count

The tier input is accepted only when all of these are explicit persisted evidence:

- count evidence is verified;
- count is scoped to the exact Family ID;
- only published MEMORY rows are included;
- deleted rows are excluded;
- count is a non-negative JavaScript safe integer supplied as a number.

Numeric strings are rejected. Missing filter evidence fails closed.

The current threshold model remains:

- FAMILY: 1
- WELCOME_BACK: 2
- SILVER: 3
- GOLD: 5
- BLACK: 10

A current count of 10 can display BLACK, but it does not by itself invent a durable BLACK entitlement.

## Durable BLACK read evidence

The existing `member_family_pass_entitlements` foundation remains the durable source candidate. This Step 8 change does not alter or apply that schema.

When the entitlement schema is reported as applied, the read gate requires exact Family-scoped row cardinality evidence:

- zero rows means no durable entitlement;
- exactly one row may establish durable BLACK;
- more than one row is a collision and fails closed.

A durable BLACK row is trusted only when:

- the row is explicitly verified;
- the row Family ID exactly equals the requested Family;
- `black_lifetime` is numeric integer `1`;
- `black_achieved_at` is a valid UTC timestamp ending in `Z`;
- `qualifying_memory_count` is a numeric safe integer >= 10;
- `achievement_source` is exactly `published-member-memories`.

A durable BLACK row from another Family can never elevate the current Family.

## Lifetime behavior

When exact durable BLACK evidence is present, BLACK survives a later current MEMORY count below 10. The same Family remains BLACK because the durable entitlement is historical achievement evidence.

This does not authorize automatic entitlement creation or backfill.

## Safety invariants

- no fuzzy Family/Customer/Member matching;
- no numeric-string coercion for cardinality or count evidence;
- no cross-Family MEMORY access;
- no cross-Family BLACK entitlement read;
- no automatic BLACK award;
- no shooting-fee discount;
- no automatic commerce enforcement;
- no Production write;
- no schema application;
- no route/deploy/secret/security mutation.

## Production boundary

`read_ready=true` means only that supplied source-level evidence is internally consistent. It is not Production authorization.

Any Production D1 read, entitlement persistence, BLACK award, migration apply, route activation, deploy, commerce enforcement, or Customer/Family mutation requires a separate fresh Owner exact-SHA authorization.
