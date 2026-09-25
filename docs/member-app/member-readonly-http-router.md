# MIZUNO PHOTO MEMBER — Read-only HTTP Router

Baseline: 2026-09-25 JST

## Purpose

This source-only router creates one browser-facing read boundary for the MIZUNO PHOTO MEMBER app.

It maps public Member read paths to the already-reviewed internal read models only after verifying the signed Member session.

It is **not wired into the Production Worker entry** in this phase.

## Public read routes

The router defines these GET-only source contracts:

- `/api/member/home`
- `/api/member/memories`
- `/api/member/memories/:memory_id`
- `/api/member/my`
- `/api/member/shop/products`
- `/api/member/news`
- `/api/member/create/templates`
- `/api/member/favorites`
- `/api/member/family-pass`
- `/api/member/family-passport`
- `/api/member/next-memory`
- `/api/member/todays-memory`

The router does not expose write routes.

## Signed-session boundary

For a recognized Member read path:

1. non-GET methods are rejected;
2. the existing `verifyMemberSessionRequest(...)` contract verifies `__Host-mizuno_member_session`;
3. verification must return the canonical Customer ID and explicit Family ID already embedded in the signed session;
4. only then is the verified session forwarded to the existing internal read model.

The router verifies the session once at the public boundary. Internal read models still validate the session shape they receive.

## Public-to-internal mapping

The browser-facing namespace is:

`/api/member/...`

The existing internal read models remain:

`/api/internal/member/...`

Examples:

- `/api/member/home` → `/api/internal/member/home`
- `/api/member/memories` → `/api/internal/member/memories`
- `/api/member/memories/memory-001` → `/api/internal/member/memories/memory-001`
- `/api/member/create/templates` → `/api/internal/member/creative/templates`

Query strings are preserved.

## Included capabilities

This router is intentionally read-only.

Included:

- HOME read model
- MEMORIES list/detail
- MY
- SHOP presentation catalog
- NEWS
- CREATE template catalog
- Favorites read model
- Family Pass read model
- Family Passport read model
- next-memory
- today's-memory

Not included:

- Favorite mutation
- historical MEMORY write
- BLACK entitlement write
- private-media grant
- private-media content delivery
- public asset resolve POST
- creative composition POST
- checkout/payment
- CRM mutation
- LINE send

Those remain separately gated.

## Response hardening

Responses passing through the public Member router are forced to:

- `Cache-Control: no-store`
- `Pragma: no-cache`
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `X-Robots-Tag: noindex, nofollow, noarchive`
- `Referrer-Policy: no-referrer`
- `Cross-Origin-Opener-Policy: same-origin`

## Production status

Current health must report:

- `source_only=true`
- `signed_member_session_required=true`
- `get_only=true`
- `read_only=true`
- `production_route_wired=false`
- `production_write=false`
- Customer creation = 0
- Customer ID generation = 0
- Family creation/auto-link = 0
- canonical CRM write = 0
- LINE send = 0

## Readiness integration

The Production Readiness Preflight should treat the basic read-only Member app as source-ready only when:

- five-tab UI source is ready;
- Member auth/session source chain is ready;
- this signed-session read-only router source is ready.

Production activation still requires explicit observed evidence for actual route wiring, schemas, external LINE runtime, and private-media readiness.
