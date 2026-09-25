# MIZUNO PHOTO MEMBER — Production Route Wiring Preflight

Baseline: 2026-09-25 JST

## Purpose

This phase does **not** wire Member routes into the Production Worker.

It statically audits the current canonical Production entry and records the exact topology required before a future Owner-authorized Production route change.

The analyzer accepts source text as input and performs no network, D1, storage, schema, or deployment action.

## Current canonical Production entry

The current entry has a stable top-level flow:

1. `enforceProductionRequestBoundary(request)`
2. existing Customer CRM handling
3. `hardenProductionResponse(...)`

The Member composition is not currently imported or dispatched.

This is intentional and means merging this preflight does not activate Member routes.

## Important finding — LINE callback conflict

The current global boundary blocks cross-site requests to any `/api/*` path.

The Member LINE callback source contract is:

`GET /api/member/login/line/callback`

An OAuth browser return from LINE can be a cross-site top-level navigation. Therefore the current blanket cross-site API rule can reject the callback before the Member LINE transaction verifier has a chance to validate:

- signed transaction cookie;
- state;
- nonce;
- PKCE;
- issuer/audience/time;
- exact verified LINE subject.

A future Production route patch must add a **narrow exception for this exact GET callback path only**.

It must not disable the cross-site API boundary generally.

The future exception must continue to preserve:

- URL token rejection;
- exact callback path;
- GET-only scope;
- state/nonce/PKCE verification inside the Member login contract;
- all other cross-site API blocking.

## Required future dispatch order

The safe target order is:

1. enforce the global Production request boundary;
2. dispatch the Member composition using the raw Member request;
3. harden a handled Member response with the existing Production response hardener;
4. otherwise fall through unchanged to the existing Customer CRM request path.

This keeps existing CRM behavior as the fallback and avoids injecting Owner principal state into the Member session boundary.

## Namespace audit

The current Production entry has no existing `/api/member/*` route, so no namespace collision was found.

The preflight treats any independent future use of the Member API namespace as a collision unless it is the reviewed Member composition itself.

## Additional blockers before customer-facing activation

The static audit also identifies two browser-delivery gaps that remain outside the API composition:

- no Production `/member` browser page route;
- no Production `/member-assets/` delivery route.

The current five-tab UI modules are source-ready, but source-ready UI modules are not the same thing as a customer-facing HTML route.

A future source-only browser document/asset delivery layer should be reviewed before Production route activation.

## Private media binding

The private-media composition intentionally accepts only an explicitly injected trusted storage adapter.

The current `wrangler.jsonc` does not declare an implicit Member private-media binding, which is the safe current state.

Future Production activation requires separately verified evidence that:

- the intended Production storage binding exists;
- the adapter is explicitly constructed from that binding;
- no fallback binding discovery is used;
- storage keys remain server-only;
- the existing grant/content authorization chain remains intact.

## Owner gate

Even if every structural blocker is resolved, this preflight always reports:

`production_activation_ready=false`

Static analysis must never convert source state into Production authorization.

A fresh exact-scope Owner authorization remains required for any future:

- Production Worker entry modification;
- Production route activation;
- Production storage binding;
- Production deploy;
- Production schema/write operation.

## Current expected blockers

On the current canonical main, the analyzer should report:

- `MEMBER_PRODUCTION_COMPOSITION_NOT_WIRED`
- `LINE_CALLBACK_CROSS_SITE_BOUNDARY_EXCEPTION_NOT_READY`
- `MEMBER_BROWSER_PAGE_ROUTE_NOT_READY`
- `MEMBER_ASSET_ROUTE_NOT_READY`
- `PRIVATE_MEDIA_PRODUCTION_STORAGE_BINDING_NOT_VERIFIED`
- `OWNER_PRODUCTION_ROUTE_AUTHORIZATION_REQUIRED`

These are expected readiness facts, not errors in the source-only phase.

## Safety invariant

This phase performs:

- Production entry modification = 0
- Production route activation = 0
- Production deploy = 0
- Production storage binding change = 0
- Production storage fetch = 0
- Production schema apply = 0
- Production write = 0
- CRM write = 0
- LINE send = 0
- Customer ID generation = 0
