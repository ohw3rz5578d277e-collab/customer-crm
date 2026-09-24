# MIZUNO PHOTO MEMBER — Signed Member Session Foundation

Baseline: 2026-09-24 JST

## Purpose

The Member read model and private media authorization already require a server-verified Member session.

This phase provides the source-only cryptographic session foundation for that requirement.

It does not activate LINE Login, add a Production login route, apply schema, write D1, or deploy Production.

## Session identity

A Member session contains only the minimum identity required by the Member Core:

- canonical 8-digit Customer ID
- explicit Family ID
- issued-at timestamp
- expiry timestamp
- fixed token version
- fixed subject
- fixed audience

The token intentionally excludes:

- customer name
- address
- phone
- email
- LINE user ID
- LINE display name
- child data
- reservation data
- photo URLs

## Issuance contract

A session may be issued only after:

1. a trusted upstream identity flow has already produced the canonical Customer ID;
2. the requested Customer ID has exactly one active explicit Family link;
3. the linked Family ID exactly equals the Family ID to be placed in the session;
4. the Member session secret is configured.

If the Customer ID is unlinked, ambiguously linked, linked to another Family, or the Family schema is unavailable, issuance fails closed.

There is no name/phone/email/fuzzy fallback.

## Cryptography

Token format:

`base64url(payload).base64url(HMAC-SHA256(payload, MEMBER_SESSION_SECRET))`

The Member session secret is separate from the Owner session secret.

The current source contract requires a minimum 32-character Member secret before issuing or verifying a session.

No secret is committed to the repository.

## Cookie contract

Reserved cookie name:

`__Host-mizuno_member_session`

Attributes:

- `Path=/`
- `HttpOnly`
- `Secure`
- `SameSite=Lax`
- no Domain attribute
- bounded Max-Age

The `__Host-` prefix prevents use with a Domain attribute and requires Secure + root Path in supporting browsers.

`SameSite=Lax` is intentional because the future Member sign-in may return from an external identity provider using top-level navigation.

## Session lifetime

Current source default:

7 days

This is a source-level default only. Production activation must review the final login UX and security requirements before route wiring.

The verifier rejects:

- malformed token format
- invalid signature
- wrong secret
- invalid version/subject/audience
- non-canonical Customer ID
- invalid Family ID
- invalid timestamps
- excessive token lifetime
- materially future-issued tokens
- expired tokens

A small clock-skew allowance is used only for timestamp validation.

## Defense in depth

Cryptographic verification does not replace Family authorization.

After token verification, Member read operations continue to re-resolve the current explicit Customer ID -> Family ID link before returning family data.

Therefore a signed token alone is not treated as authority to bypass current Family state.

## Source APIs

### Issue

`issueMemberSessionForVerifiedCustomer(env, { customer_id, family_id, now_seconds })`

This is a trusted server-side primitive.

It is not exposed as an HTTP route in this phase.

### Verify token

`verifyMemberSessionToken(env, token)`

Returns a server-marked verified Member session when signature and claims are valid.

### Verify request cookie

`verifyMemberSessionRequest(request, env)`

Reads only the dedicated `__Host-mizuno_member_session` cookie and verifies it.

## Important separation

Owner authentication remains separate.

Member sessions:

- use `MEMBER_SESSION_SECRET`
- use Member-only subject/audience
- use a distinct cookie
- cannot authenticate the CRM Owner
- cannot reuse the Owner password/session contract

Owner sessions must not be accepted as Member sessions.

## Current exclusions

Not included:

- LINE Login activation
- LINE OAuth callback
- account registration route
- login/logout HTTP route wiring
- Production secret creation
- Production cookie issuance
- session revocation store
- Production D1 schema apply
- Production D1 write
- LINE send
- Production deploy

## Zero-cost-first

The session is stateless and HMAC-signed.

It does not require:

- a paid authentication SaaS
- a new session database
- per-request external API calls

This preserves the zero-cost-first architecture.

## Production activation gate

Before activating Member login in Production:

1. choose and configure the Member session secret;
2. implement and verify the trusted upstream identity flow;
3. ensure the upstream flow yields the canonical Customer ID without fuzzy matching;
4. re-check the explicit Family link before issuance;
5. wire login/session routes separately;
6. run security regression;
7. obtain fresh Owner authorization for Production deployment and any required schema/write operations.

This foundation alone performs no Production action.
