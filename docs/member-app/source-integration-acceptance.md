# MIZUNO PHOTO MEMBER — Source Integration Acceptance

Baseline: 2026-09-25 JST

## Purpose

This phase connects the five already-reviewed customer UI foundations inside the canonical Member app shell without activating Production routes or writes.

Canonical tabs:

1. HOME
2. MEMORIES
3. CREATE
4. SHOP
5. MY

This is the first source-level acceptance point where all five primary customer surfaces exist together.

## Integration rule

The integration controller requires all five canonical shell mounts to exist before mounting any feature UI.

Required destinations:

- `data-member-mount="home"`
- `data-member-mount="memories"`
- `data-member-mount="create"`
- `data-member-mount="shop"`
- `data-member-mount="my"`

If any destination is missing, integration fails before partial mounting.

If a later feature mount unexpectedly fails, previously mounted feature handles are destroyed before failure is returned.

## Existing modules only

This phase composes the existing reviewed modules:

- Member app shell UI
- HOME UI
- MEMORIES UI
- CREATE shell integration
- SHOP UI
- MY UI

It does not create a second customer read model or duplicate business logic.

## Automatic-action boundary

Mounting the complete app performs no automatic:

- network fetch
- MEMORY detail fetch
- Creative composition planning
- Creative rendering
- Creative download
- Favorite mutation
- profile/family mutation
- settings mutation
- checkout
- payment
- reservation creation
- LINE send

MEMORY detail and Creative actions remain user-triggered inside their existing controllers.

## Identity boundary

The integration layer does not accept or expose:

- Customer ID
- Family ID
- linked Customer IDs
- child IDs
- private storage keys

Each feature continues to rely on its separately reviewed authorized server read model.

## Private media boundary

Production private media delivery is still inactive.

Therefore the integrated app still does not render customer photo binaries from private storage.

HOME and MEMORIES retain their safe placeholder behavior.

CREATE remains blocked unless its existing runtime readiness contract is satisfied.

## Commerce boundary

SHOP remains presentation-only.

Not activated:

- authoritative price
- inventory
- cart
- checkout
- order creation
- payment
- Family Pass BLACK discount enforcement

## Lifecycle

The integration returns one parent source controller.

It can:

- switch among the five canonical tabs locally
- expose the five mounted feature controllers
- destroy all five feature handles together

Unknown tab input continues through the existing shell normalization and resolves to HOME.

## Production boundary

Not included:

- Production Member route wiring
- Production Member session activation
- Production D1/schema apply
- Production private media activation
- Favorite mutation activation
- profile/settings mutation activation
- WordPress/WooCommerce/Square activation
- checkout/payment activation
- BLACK discount enforcement
- LINE send
- reservation creation
- Production deploy

## Acceptance status

At this source-only stage:

- canonical 5-tab shell = PASS
- HOME mount = PASS
- MEMORIES mount = PASS
- CREATE mount = PASS
- SHOP mount = PASS
- MY mount = PASS
- partial mount fail-closed = PASS
- automatic fetch = 0
- automatic Creative render/download = 0
- private customer photo binary rendering = 0
- Customer/Family/child ID exposure = 0
- Favorite mutation = 0
- profile/settings mutation = 0
- checkout/payment = 0
- BLACK discount enforcement = 0
- reservation creation = 0
- automatic contact = 0
- LINE send = 0
- Production route wiring = 0
- Production write = 0
