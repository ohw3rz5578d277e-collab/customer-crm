# MIZUNO PHOTO MEMBER — LINE Login Exchange Executor

Baseline: 2026-09-25 JST

## Purpose

This source-only phase connects the already-reviewed LINE Login transaction and token-verification foundations into a guarded server-side exchange executor.

It can consume a **previously verified callback result** and, only when explicitly enabled and approved by its caller, execute the two LINE Login API calls required to establish a verified Member identity.

This file is not wired into the Production Worker entry.

## Existing contracts reused

The executor reuses:

- `buildMemberLineTokenExchangeRequest(...)`
- `validateMemberLineTokenExchangeResponse(...)`
- `buildMemberLineIdTokenVerificationRequest(...)`
- `validateVerifiedMemberLineIdTokenPayload(...)`
- `resolveCanonicalCustomerByVerifiedLineUserId(...)`
- `readMemberFamilyByCustomer(...)`
- `issueMemberSessionForVerifiedCustomer(...)`

It does not introduce a second identity model.

## Default-off runtime gate

External calls are blocked unless both are true:

1. `MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE=enabled`
2. the trusted caller passes `approved: true`

The default is disabled.

This is an additional technical safety boundary. It is not a substitute for fresh Owner authorization before future Production LINE Login activation.

## Input boundary

The executor accepts only a successful result produced by the existing LINE callback transaction verifier.

Required callback material:

- authorization code
- nonce
- PKCE code verifier
- exact channel ID
- exact redirect URI
- local return target
- one-shot transaction-cookie clear instruction

The callback channel ID and redirect URI are checked again against current environment configuration before any external call.

## External sequence

When enabled and explicitly approved by its trusted caller:

1. build the LINE token exchange request;
2. call `https://api.line.me/oauth2/v2.1/token`;
3. reject non-2xx, malformed, or oversized responses;
4. validate Bearer token response and OpenID scope;
5. retain only the ID token needed for verification;
6. build the LINE ID-token verification request;
7. call `https://api.line.me/oauth2/v2.1/verify`;
8. reject non-2xx, malformed, or oversized responses;
9. locally revalidate issuer, audience, nonce, issued-at, expiry, and formal LINE subject;
10. pass only the verified `sub` into the exact CRM LINE identity resolver.

## Identity chain

The executor preserves the canonical chain:

```
verified LINE callback transaction
        |
        v
LINE token exchange
        |
        v
verified LINE ID-token response
        |
        v
verified LINE sub only
        |
        v
exact line_user_id lookup
        |
        v
canonical 8-digit Customer ID
        |
        v
explicit Customer -> Family link
        |
        v
signed Member session
```

No fallback is permitted from:

- LINE display name
- customer name
- email
- phone
- child name
- profile image
- access token

An unlinked LINE subject remains unlinked. The executor does not create a Customer, generate a Customer ID, create a Family, or auto-link a Family.

Ambiguous identity states fail closed.

## Token handling

The channel secret is read only to construct the token exchange request.

The executor does not return or persist:

- LINE access token
- LINE refresh token
- channel secret

The verified LINE user ID, canonical Customer ID, and Family ID are also not returned in the final success result.

The success result contains only what a future HTTP route needs to complete login:

- local `return_to`
- transaction-cookie clear value
- signed Member-session cookie
- non-sensitive execution status

## No Production activation in this phase

Not included:

- Production Worker route wiring
- Production LINE Login activation
- Production secret configuration
- Production environment mode enablement
- Production deploy
- D1 schema apply
- D1 write
- Customer creation
- Customer ID generation
- Family creation/link write
- historical MEMORY write
- Favorite write
- BLACK entitlement write
- LINE Messaging API send
- checkout/payment
- paid spend

## Next phase

After this executor is reviewed and merged, the next source-only phase can add a narrow HTTP login contract that:

- starts the LINE authorization transaction;
- verifies callback transaction state;
- invokes this executor behind its disabled runtime gate;
- sets the Member session cookie only after successful exact identity resolution;
- clears the one-shot transaction cookie;
- performs no Customer or Family mutation.

Production entry wiring remains a later, separately Owner-gated phase.
