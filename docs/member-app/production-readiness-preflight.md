# MIZUNO PHOTO MEMBER — Production Readiness Preflight

Baseline: 2026-09-25 JST

## Purpose

`src/member-production-readiness-preflight.mjs` is a source-only readiness classifier for the MIZUNO PHOTO MEMBER app.

It does **not** deploy, apply schema, write D1, wire routes, send LINE messages, create Customer IDs, or activate commerce. Production state is accepted only as explicit caller-provided evidence in `observed`.

The preflight intentionally does not reduce Production readiness to one opaque boolean. It evaluates eight independent gates:

1. `source_ui`
2. `auth_session`
3. `read_only_app`
4. `historical_bootstrap`
5. `private_media`
6. `favorites_write`
7. `black_entitlement`
8. `commerce`

Each gate returns:

- `source_ready`
- `activation_ready`
- `required`
- `blockers`
- `notes`

## Basic Member activation

`main_activation_ready` depends only on these required gates:

- `auth_session`
- `read_only_app`
- `private_media`

Favorite mutation, BLACK entitlement writes, historical bootstrap writes, and commerce are separately gated and do not become hidden prerequisites for the basic app.

## Explicit observation model

Callers provide:

```js
buildMemberProductionReadinessPreflight({
  env: {},
  observed: {}
})
```

`env` may contain source/runtime configuration values. The preflight inspects presence or enabled mode only; it never returns secret values.

`observed` is explicit external evidence. Important fields include:

- `applied_migrations`
- `line_external_token_exchange_ready`
- `line_external_id_token_verification_ready`
- `login_routes_wired`
- `member_read_routes_wired`
- `production_member_route_entry_ready`
- `historical_memory_plan_verified`
- `historical_memory_write_owner_authorized`
- `private_media_storage_adapter_ready`
- `private_media_storage_binding_ready`
- `private_media_routes_wired`
- `private_media_owner_authorized`
- `favorite_mutation_route_wired`
- `favorite_write_owner_authorized`
- `black_exact_family_plan_verified`
- `black_qualifying_memory_count`
- `black_entitlement_write_owner_authorized`
- `commerce_owner_authorized`

Missing evidence is always treated as not verified. The preflight never discovers or infers Production state by itself.

## Gate summary

### source_ui

Source-ready after the canonical five-tab integration:

- HOME
- MEMORIES
- CREATE
- SHOP
- MY

No Production action is performed by this gate.

### auth_session

Source foundations already exist for:

- signed Member session
- LINE OAuth transaction with state / nonce / PKCE
- LINE token verification

Activation requires configured Member/LINE transaction values plus explicit evidence for external token verification and route wiring.

Identity remains:

verified LINE subject → exact `line_user_id` → canonical Customer ID → explicit Customer/Family link → Family ID → Member session.

No display-name, email, phone, customer-name, child-name, or fuzzy fallback is accepted.

### read_only_app

Activation requires:

- `auth_session` activation-ready
- Family identity migration verified applied
- MEMORY core migration verified applied
- five-tab source UI ready
- Production Member route entry explicitly verified

Optional catalog and Favorite schemas may degrade locally without blocking the basic read-only app.

### historical_bootstrap

The bootstrap planner remains read-only. The historical MEMORY write executor is activation-ready only when:

- core schemas are verified
- `MEMBER_MEMORY_WRITE_MODE=enabled`
- a fresh historical plan is explicitly verified
- fresh Owner Production D1 write authorization is explicitly provided

Old authorization is never reusable.

### private_media

Activation requires:

- `MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET`
- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE=enabled`
- trusted storage adapter evidence
- actual Production storage binding evidence
- grant/content route wiring evidence
- fresh Owner private-media Production authorization

Private media remains same-Family, published/non-deleted MEMORY scoped and must not expose storage keys or redirect externally.

### favorites_write

Optional for basic app activation. Requires:

- Favorite schema verified
- Favorite rate-limit schema verified
- mutation route wired
- `MEMBER_FAVORITES_MUTATION_ROUTE_MODE=enabled`
- `MEMBER_FAVORITES_RATE_LIMIT_MODE=enabled`
- `MEMBER_FAVORITES_WRITE_MODE=enabled`
- fresh Owner write authorization

Runtime contract still requires signed Member session, same-origin POST, exact JSON, current authorization, and rate limiting.

### black_entitlement

Optional for basic app activation. Requires:

- BLACK entitlement schema verified
- exact current Family plan verified
- explicit qualifying MEMORY count of at least 10
- `MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE=enabled`
- fresh Owner BLACK entitlement write authorization

BLACK is a durable lifetime entitlement. No downgrade/delete behavior is introduced.

BLACK commerce benefit remains separate: **PHOTO GOODS 10% OFF FOREVER**, not shooting-fee discount.

### commerce

Current state remains:

- `source_ready=false`
- `activation_ready=false`

Blockers remain:

- authoritative price source
- checkout/payment provider
- BLACK photo-goods discount enforcement
- Owner commerce activation authorization

SHOP remains presentation-only. Prices must not be guessed.

## Migration inventory

The preflight knows the current Member migration filenames but never treats file existence as proof of Production application.

`migration_inventory.items[].verified_applied` is true only when the exact migration filename is included in `observed.applied_migrations`.

## Safety invariants

Every preflight result preserves these values as false:

- `automatic_contact`
- `line_send`
- `customer_id_generation`
- `canonical_crm_write`
- `production_deploy_executed`
- `production_schema_apply_executed`
- `production_write_executed`

Health also reports:

- `source_only=true`
- Production observation is explicit input only
- secret values exposed = false
- Production route mutation = false
- schema apply = false
- write = false
- deploy = false
- LINE send = false
- Customer ID generation = false

## Owner gate

This source does not authorize anything.

Production deploy, schema apply, route activation, private-media activation, MEMORY write, Favorite write, BLACK entitlement write, commerce activation, and related runtime changes each require fresh explicit Owner authorization for the exact intended scope. A main merge approval is not Production authorization.
