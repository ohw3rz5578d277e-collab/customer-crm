# MIZUNO PHOTO MEMBER — Production Integration Acceptance

Baseline: 2026-10-07 JST

## Purpose

Record the current source-only Member Production integration stage after the canonical default-off Production entry wiring and canonical private-media binding declaration were merged, and after the private-media read-only adapter was separately authorized for source-only runtime wiring.

The current source stage:

1. keeps the single Production-facing Member request composition;
2. keeps the existing double activation gate default-off;
3. keeps public assets unbound;
4. explicitly wraps `env?.MEMBER_PRIVATE_MEDIA_BUCKET` with the reviewed read-only private-media storage adapter;
5. passes that adapter into `private_media_storage_adapter` without executing a storage fetch.

This stage changes source only. It does not authorize or execute Production deployment, route activation, storage fetch, R2 object access, LINE Login Production, D1 mutation, CRM mutation, LINE send, or Customer ID mutation.

## Production-facing Member request composition

`handleMemberProductionRequest(...)` combines the three already-reviewed customer-facing surfaces:

1. `/member-assets/...`
2. `/member...`
3. `/api/member/...`

The order is intentional: assets, browser, then API.

The handler only considers these Member namespaces and never claims CRM routes.

### Double activation gate

The production-facing source handler requires both:

- `MEMBER_PRODUCTION_ROUTE_MODE=enabled`
- explicit trusted-caller `approved:true`

The route mode remains disabled in canonical source configuration.

`MEMBER_PRODUCTION_OWNER_APPROVED` remains `false`.

When route mode is disabled, Member requests return `null` so the imported integration preserves existing Production fallthrough behavior.

When route mode is enabled but trusted approval is missing, the Member namespace fails closed with 503.

### Separate LINE activation

Overall Member route approval does not imply LINE Login external token exchange approval.

`line_login_approved` remains explicitly `false` in the Production entry.

### Explicit storage dependencies

The source handler accepts only explicit:

- `public_asset_adapter`
- `private_media_storage_adapter`

It never discovers either storage dependency inside the handler from arbitrary `env` keys.

The Production entry now owns the exact private-media binding handoff and wires:

`private_media_storage_adapter:createMemberPrivateMediaStorageAdapter(env?.MEMBER_PRIVATE_MEDIA_BUCKET)`

The Production entry does not hardcode the bucket name. Public assets remain `public_asset_adapter:null`.

## Private-media adapter safety

`createMemberPrivateMediaStorageAdapter(binding)` returns a frozen read-only adapter with only `get(key)`.

Adapter construction itself performs no R2 object access. The underlying `binding.get(...)` is called only if the returned adapter's `get(key)` is later called.

Therefore the current source wiring alone does not execute Production storage fetch. With Member route mode disabled, the Member request composition returns before the Member API handler can call the storage adapter.

## Canonical source inspection

Static acceptance now requires the current Production entry to contain all of the following:

- one Member request-composition import;
- one private-media adapter factory import;
- `MEMBER_PRODUCTION_OWNER_APPROVED=false`;
- the existing route-mode + Owner-flag boundary helper;
- the exact LINE callback GET exception gated by both Owner flag and route mode;
- Member dispatch after the global Production boundary and before existing CRM dispatch;
- `line_login_approved:false`;
- `public_asset_adapter:null`;
- exact private-media wiring through `createMemberPrivateMediaStorageAdapter(env?.MEMBER_PRIVATE_MEDIA_BUCKET)`;
- existing CRM dispatch preserved exactly once.

The acceptance remains static and reports `production_activation_ready=false` even when all observed evidence flags are true.

## Recommended release sequence from this stage

The acceptance sequence remains intentionally staged:

1. merge the source-only private-media runtime wiring after CI/Codex review and separate exact-HEAD Owner merge approval;
2. verify existing CRM regressions while Member routes remain inactive;
3. verify Member runtime dependencies without activating routes or executing storage fetch;
4. obtain a separate fresh exact-SHA/scope Owner authorization for route activation;
5. activate route mode and Owner flag only in the exact separately approved release;
6. run a separately authorized read-only Member canary;
7. activate the private-media content route only under its own separate gate;
8. keep Favorites write, historical MEMORY write, BLACK entitlement write, checkout/payment, and commerce activation separate.

Production deploy, Worker activation, Production traffic change, storage fetch, route activation, and LINE Login Production remain separate gates.

## Current Production state

This source-only phase performs:

- Production Worker deploy = 0
- Worker activation = 0
- Production traffic change = 0
- Production route activation = 0
- LINE callback Production exception activation = 0
- Production storage fetch = 0
- R2 object read/write/list/delete = 0
- public asset runtime binding/fetch change = 0
- Production schema apply = 0
- Production D1 write = 0
- CRM write = 0
- LINE send = 0
- Customer ID generation/update/delete/merge = 0
- checkout/payment = 0
- paid spend = 0
