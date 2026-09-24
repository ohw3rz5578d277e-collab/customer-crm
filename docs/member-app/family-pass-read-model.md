# MIZUNO PHOTO MEMBER — FAMILY PASS Read Model

Baseline: 2026-09-24 JST

## Purpose

This phase provides the read-only FAMILY PASS model for the customer Member experience.

FAMILY PASS is derived from a family's completed, customer-visible photography history already represented as Member MEMORIES.

Current rule:

**1 published, non-deleted MEMORY = 1 shoot**

The model does not write tier state, grant benefits, apply discounts, or wire a Production route.

## Canonical current-count thresholds

| MEMORY count | Current tier |
| ---: | --- |
| 0 | `UNRANKED` |
| 1 | `FAMILY` |
| 2 | `WELCOME_BACK` |
| 3–4 | `SILVER` |
| 5–9 | `GOLD` |
| 10+ | `BLACK` |

Customer-facing rank names already decided by product direction are preserved:

- FAMILY
- WELCOME BACK
- SILVER
- GOLD
- BLACK

`UNRANKED` is an internal zero-state code only. It is not a new customer-facing brand name.

## Count source

The read model counts only rows satisfying all of:

- exact `family_id`
- `published=1`
- `deleted_at` empty

from:

`member_memories`

It does not count:

- unpublished MEMORY drafts
- deleted MEMORY rows
- reservations directly
- CRM rows by name
- media rows
- albums/products/orders
- unrelated Family rows

This keeps FAMILY PASS aligned to the same customer-visible MEMORY history used by the Member app.

## Identity / authorization

Input identity comes only from the server-verified Member session:

- canonical 8-digit Customer ID
- Family ID

The read model re-validates:

1. canonical Customer ID;
2. current explicit Customer -> Family link;
3. exact session Family ID equality.

A session for another Family cannot query a Family's pass status.

No request-supplied Customer ID or Family ID is accepted by the HTTP contract.

## Read output

The model returns:

- current MEMORY count
- current tier
- current threshold
- next tier
- next threshold
- MEMORIES remaining to next tier
- progress ratio
- milestone achieved flags
- whether BLACK is currently qualified by the current count

The model remains read-only.

## BLACK lifetime requirement

Product direction states that once a family reaches BLACK, it must never rank down.

That requirement cannot be truthfully implemented from the current MEMORY count alone.

Example:

- a family reaches 10 qualifying MEMORIES;
- later an administrative correction removes or unpublishes one MEMORY;
- current count becomes 9.

A count-only model would return GOLD even though the intended product rule says BLACK should persist.

Therefore this read-only phase deliberately reports:

`black_lifetime_persistence_supported=false`

It must not claim permanent BLACK entitlement until an explicit durable achievement source exists.

## Future durable BLACK state

A later separately reviewed phase may introduce an additive Member Core achievement/entitlement record such as:

- first BLACK achieved timestamp
- qualifying Family ID
- durable BLACK entitlement flag
- audit metadata

That future write model must be idempotent and Family-scoped.

It must not infer BLACK history from names, addresses, phone numbers, or current count alone.

Production schema application and writes require separate Owner authorization.

## BLACK benefit contract

Confirmed product rule:

**BLACK = PHOTO GOODS 10% OFF FOREVER**

This does **not** apply to the photography shooting fee.

The current read model exposes the product contract only as metadata:

- goods discount percent = 10
- shooting fee discount = false
- enforcement ready = false

It does not:

- change a WooCommerce/Square price
- issue a coupon
- grant an order discount
- mutate an entitlement
- change a photography plan price

Discount enforcement belongs to a later commerce integration phase after persistent BLACK entitlement exists.

## HTTP contract

Conceptual source-only endpoint:

`GET /api/internal/member/family-pass`

Identity is injected by the future server Member session layer.

The endpoint does not accept:

- `customer_id`
- `family_id`

from query/body/request parameters.

Current source is not Production route-wired.

## Error boundaries

Fail-closed examples:

- invalid/missing Member session
- Family schema not applied
- MEMORY schema not applied
- unlinked Customer
- inactive/missing Family
- duplicate active Family links
- session Family mismatch

Cross-Family access returns no Family Pass data.

## Current exclusions

Not included:

- durable BLACK achievement state
- rank persistence writes
- goods discount enforcement
- WooCommerce/Square integration
- coupon generation
- photography fee discounts
- FAMILY PASS UI
- Production route wiring
- Production D1 schema apply
- Production D1 write
- Production deploy

## Safety status

Current implementation is:

- Family-scoped
- exact Customer identity only
- explicit Family link only
- published MEMORY only
- deleted MEMORY hidden
- read-only
- Production route wiring = 0
- Production write = 0
