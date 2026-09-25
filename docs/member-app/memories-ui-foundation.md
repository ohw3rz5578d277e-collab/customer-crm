# MIZUNO PHOTO MEMBER — MEMORIES UI Foundation

Baseline: 2026-09-25 JST

## Purpose

This phase adds the source-only customer-facing MEMORIES timeline and MEMORY detail UI.

It mounts into the canonical Member shell:

`[data-member-mount="memories"]`

The design remains **Luxury Minimal × Family Story**.

## Timeline

The MEMORIES timeline uses the existing authorized MEMORIES list read model.

Each visible card can present:

- title
- shoot date
- genre
- preview count
- Favorite state
- non-secret cover dimensions/role metadata

The MEMORY ID remains the existing opaque read-contract identifier used to request one exact authorized detail.

The UI does not accept Customer ID or Family ID.

## Private media boundary

Production private media delivery is still inactive.

Therefore this phase does **not** invent or expose:

- R2 storage key
- signed provider URL
- public customer-photo URL
- Blob URL
- private image binary

Timeline and detail photo positions use intentional visual placeholders.

A later separately gated private-media integration can replace those placeholders after server-side grant/content authorization.

## Detail loading

The detail reader is injected into the controller.

There is no automatic detail fetch.

A detail read occurs only after the Member explicitly opens a visible MEMORY card.

The returned detail must contain the same exact MEMORY ID that was requested.

A mismatched detail fails closed.

A MEMORY not present in the authorized list cannot be opened through this controller.

## Favorite boundary

The UI can display the existing per-Member Favorite state.

Favorite mutation remains inactive.

This phase does not enable:

- add Favorite
- remove Favorite
- optimistic Favorite state
- mutation HTTP route
- mutation rate-limit mode
- Favorite write mode

The existing source mutation foundations remain separate and Production-gated.

## Amazon Photos boundary

The read model may indicate that an exact stored Amazon Photos link exists.

This UI phase only displays an availability label.

It intentionally does not expose or navigate to that external URL yet.

## Detail actions

CREATE and SHOP detail actions remain inactive when the existing read model reports them inactive.

This UI does not override those feature flags.

## Production boundary

Not included:

- Production Member route wiring
- Production private media activation
- Production Favorite mutation
- Production D1/schema changes
- Member session activation
- Customer/Family writes
- LINE send
- automatic contact
- Production deploy

## Safety status

- canonical MEMORIES shell mount = implemented
- photo-led timeline = implemented
- user-triggered detail = implemented
- exact visible MEMORY detail scope = implemented
- Favorite read state = implemented
- Favorite mutation = 0
- private storage key exposure = 0
- signed private media URL exposure = 0
- customer photo binary rendering = 0
- Amazon Photos external URL exposure = 0
- Customer ID exposure = 0
- Family ID exposure = 0
- automatic detail fetch = 0
- LINE send = 0
- Production route wiring = 0
- Production write = 0
