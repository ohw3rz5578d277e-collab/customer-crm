# MIZUNO PHOTO MEMBER — LINE Login HTTP Contract

Baseline: 2026-09-25 JST

## Purpose

This source-only contract provides the narrow browser-facing HTTP boundary for the existing Member LINE Login foundations.

It adds source handlers for:

- `GET /api/member/login/line/start`
- `GET /api/member/login/line/callback`

It does **not** wire either route into the Production Worker entry.

## Start route

The start route:

1. accepts only `GET`;
2. accepts only the optional `return_to` query parameter;
3. restricts `return_to` to `/member` or a child path under `/member/`;
4. requires the configured LINE redirect URI pathname to equal the callback contract pathname;
5. calls the existing signed LINE transaction builder;
6. responds with `302` to the LINE Login authorization URL;
7. sets the short-lived `__Host-mizuno_member_login_tx` cookie.

It does not call LINE's token endpoint and performs no D1 write.

## Callback route

The callback route:

1. accepts only `GET`;
2. calls the existing callback transaction verifier;
3. verifies the one-shot signed transaction, state, redirect configuration, nonce material, and PKCE material before exchange;
4. clears the transaction cookie on failed callback verification;
5. passes only a verified callback result into the guarded exchange executor;
6. does not invent runtime activation approval;
7. on success, revalidates the signed local Member return target;
8. sets the signed Member session cookie;
9. clears the one-shot transaction cookie;
10. responds with `303` to the local Member target.

The executor remains default-off unless its own runtime gate is enabled and a trusted caller explicitly supplies approval.

## Return target boundary

The HTTP contract is stricter than the lower-level transaction helper.

Allowed examples:

- `/member`
- `/member/memories`
- `/member/my?tab=family`

Rejected examples:

- `/admin`
- `/api/customer360`
- `//evil.example`
- any absolute external URL

This prevents a valid login transaction from redirecting into unrelated Owner/CRM surfaces.

## Response security

Start, callback, and error responses use:

- `Cache-Control: no-store`
- `Pragma: no-cache`
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `X-Robots-Tag: noindex, nofollow, noarchive`
- `Referrer-Policy: no-referrer`
- `Cross-Origin-Opener-Policy: same-origin`

## Identity and mutation policy

The contract preserves the canonical identity chain implemented by the exchange executor:

verified LINE ID-token `sub`
→ exact `line_user_id`
→ canonical Customer ID
→ explicit Customer/Family link
→ signed Member session

The contract does not:

- use LINE display name;
- use email/phone/name fallback;
- create a Customer;
- generate a Customer ID;
- create a Family;
- auto-link a Family;
- write canonical CRM data;
- send LINE messages;
- write historical MEMORY;
- write Favorites;
- write BLACK entitlement.

## Production status

Current source health must continue to report:

- `source_only=true`
- `production_route_wired=false`
- `line_login_activated=false`
- external exchange default disabled
- explicit activation approval required
- `production_write=false`

No Production route activation, secret configuration, deploy, schema apply, or D1 write is part of this phase.

## Readiness integration

The Production Readiness Preflight should count auth/session source readiness only when all reviewed auth source layers exist:

- Member session foundation
- LINE Login transaction foundation
- LINE token verification foundation
- guarded LINE Login exchange executor
- LINE Login HTTP contract

This still does not imply Production activation. Runtime readiness remains separately evidence-gated.
