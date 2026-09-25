# MIZUNO PHOTO MEMBER — Production Entry Default-Off Source Manifest

State: post-PR #164, 2026-09-25 JST

## Current canonical source state

The default-off Member wiring has already been merged into the canonical Production Worker **source**.

- Production entry path: `src/production-index-crm-customer360-entry.js`
- pre-wiring blob: `9791174a5be2aa8861134f6881e2cee451f50966`
- canonical applied default-off source blob: `9b325cb61c3fe67006cef2ecc03d5769d7a693a4`

This is a source-state statement only. It does **not** mean that the Worker carrying this source has been deployed to Production.

## Default-off source invariants

The canonical source inspection requires:

- `MEMBER_PRODUCTION_OWNER_APPROVED=false`
- Member route mode remains a separate `MEMBER_PRODUCTION_ROUTE_MODE` gate
- LINE Login external exchange approval remains `false`
- public asset adapter remains `null`
- private media storage adapter remains `null`
- LINE callback boundary exception is exact GET only
- callback exception requires both Owner flag and route mode
- Member dispatch remains after the global Production request boundary
- existing CRM dispatch remains the fallback

## Manifest behavior

The manifest supports both audited source states:

1. **pre-wiring blob** — generate and inspect the exact default-off candidate;
2. **applied default-off source blob** — inspect the current source directly and return `already_applied_default_off`.

An unknown Production entry blob fails closed.

The main SHA is never statically pinned. A fresh observed main SHA must exactly match the expected SHA supplied to the manifest.

## Separate Production gates

The merged source does not authorize or imply:

- Production deploy
- Member route activation
- `MEMBER_PRODUCTION_OWNER_APPROVED` activation
- `MEMBER_PRODUCTION_ROUTE_MODE` enablement
- LINE Login external exchange enablement
- public/private asset Production binding or fetch
- Member schema apply
- Production D1 write
- Favorites/MEMORY/BLACK writes
- checkout/payment/commerce

## Current safety state

- canonical source default-off wiring applied = yes
- Production runtime deployed by this manifest = no
- Production route activation = no
- Production schema apply = no
- Production D1 write = no
- CRM write = no
- LINE send = no
