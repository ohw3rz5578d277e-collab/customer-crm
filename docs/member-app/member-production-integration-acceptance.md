# MIZUNO PHOTO MEMBER — Production Integration Acceptance

Baseline: 2026-09-25 JST

## Purpose

This is the final source-only acceptance layer before any future Production Worker entry change.

It adds two source foundations:

1. a single Production-facing Member request composition;
2. a deterministic default-off wiring candidate for the current canonical Production entry.

Nothing in this phase modifies the actual Production Worker entry.

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

The route mode defaults disabled.

The explicit approval defaults false.

When route mode is disabled, Member requests return `null` so an imported-but-inactive integration can preserve existing Production fallthrough behavior.

When route mode is enabled but trusted approval is missing, the Member namespace fails closed with 503.

### Separate LINE activation

Overall Member route approval does not imply LINE Login external token exchange approval.

`line_login_approved` separately defaults false.

### Explicit storage dependencies

The source handler accepts only explicit:

- `public_asset_adapter`
- `private_media_storage_adapter`

It never discovers either storage binding from `env`.

## Exact default-off Production entry candidate

The acceptance module can deterministically transform the current canonical Production entry **in memory only**.

The candidate is deliberately default-off.

It would add:

- one import for the Production-facing Member handler;
- `MEMBER_PRODUCTION_OWNER_APPROVED=false`;
- a route-mode + Owner-flag boundary helper;
- Member browser pages to the sensitive/no-store Production path set;
- an exact LINE callback GET exception that exists only when both the Owner flag and route mode are active;
- Member dispatch after the global Production boundary and before the existing CRM request path;
- `line_login_approved:false`;
- `public_asset_adapter:null`;
- `private_media_storage_adapter:null`.

The existing CRM dispatch remains the fallback.

## Why the callback exception is double-gated

The current Production boundary blocks cross-site `/api/*`.

LINE OAuth return can be a cross-site top-level GET.

The candidate does not create a permanent unconditional exception.

The exception becomes effective only when:

1. the compiled Owner activation flag is true; and
2. `MEMBER_PRODUCTION_ROUTE_MODE=enabled`; and
3. the request is exactly GET `/api/member/login/line/callback`.

All other cross-site API requests remain blocked.

## Recommended release sequence

The acceptance sequence is intentionally staged:

1. merge this source-only acceptance;
2. obtain fresh exact-SHA Owner authorization for Production entry modification;
3. apply the exact default-off entry candidate;
4. verify existing CRM regressions while Member routes remain inactive;
5. configure and verify Member runtime dependencies without route activation;
6. obtain a separate fresh exact-SHA/scope Owner authorization for route activation;
7. activate route mode and Owner flag in the exact approved release;
8. run a read-only Member canary;
9. activate private media only under its separate binding gate;
10. keep Favorites write, historical MEMORY write, BLACK entitlement write, checkout/payment, and commerce activation separate.

## Static evidence never authorizes Production

Even if every observed evidence field is true, the source acceptance always reports:

`production_activation_ready=false`

This prevents source code, CI, Preview, or an evidence collector from converting themselves into Production authorization.

## Current Production state

This phase performs:

- Production Worker entry modification = 0
- Production route activation = 0
- LINE callback Production exception activation = 0
- Production deploy = 0
- Production schema apply = 0
- Production D1 write = 0
- public asset Production binding/fetch = 0
- private media Production binding/fetch = 0
- CRM write = 0
- LINE send = 0
- Customer ID generation = 0
- checkout/payment = 0
- paid spend = 0
