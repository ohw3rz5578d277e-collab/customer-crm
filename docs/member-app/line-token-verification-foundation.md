# MIZUNO PHOTO MEMBER — LINE Token Exchange / ID Token Verification Foundation

Baseline: 2026-09-24 JST

## Purpose

This phase prepares the source-only boundary between a verified LINE Login callback transaction and the existing exact LINE identity resolver.

It defines:

1. how the authorization code exchange request must be built;
2. how the LINE token endpoint response is normalized;
3. how the ID token verification request must be built;
4. how the verified LINE ID token payload must be validated;
5. which single identity claim is allowed to continue into Customer CRM.

No external LINE API call is executed by this foundation.

## Official LINE endpoints

Token endpoint:

`POST https://api.line.me/oauth2/v2.1/token`

ID token verification endpoint:

`POST https://api.line.me/oauth2/v2.1/verify`

Expected issuer for a verified LINE ID token:

`https://access.line.me`

## Authorization-code exchange request

`buildMemberLineTokenExchangeRequest(...)` builds, but does not send, a form-encoded request containing:

- `grant_type=authorization_code`
- authorization code
- the same HTTPS redirect URI used for authorization
- LINE Login channel ID
- LINE Login channel secret
- PKCE `code_verifier`

The request builder performs no network activity.

The channel secret is required to construct the request but is not returned as a normalized result field and must never be logged.

## Token endpoint response normalization

The token response validator requires:

- `token_type=Bearer`
- an access token to exist in the raw LINE response
- a valid non-empty ID token
- positive expiry duration
- `openid` scope

Only the ID token needed for the next identity-verification step is retained in the normalized result.

Raw access-token and refresh-token values are deliberately not copied into the normalized identity result.

This reduces the chance that long-lived token material is accidentally propagated through Member identity code.

## ID token verification request

`buildMemberLineIdTokenVerificationRequest(...)` builds, but does not send:

- `id_token`
- `client_id`

to the LINE Login v2.1 ID token verification endpoint.

No external call is executed by this source phase.

## Verified ID token payload validation

After a future trusted caller receives a successful response from LINE's verify endpoint, the response must still pass local boundary checks.

Required:

- `iss === https://access.line.me`
- `aud === expected channel ID`
- `nonce === transaction nonce`
- integer `iat` and `exp`
- `exp > iat`
- `iat` is not materially in the future
- `exp` is not expired
- `sub` is a formal LINE user ID

A small clock-skew allowance is used for time validation.

Any mismatch fails closed.

## Identity claim policy

Only:

`sub`

from the verified LINE ID token is allowed to become the Member LINE identity.

The resulting field is:

`verified_line_user_id`

with identity source:

`verified_line_id_token_sub`

The following claims are not used to identify or link a CRM customer:

- name
- display name
- picture
- email
- access token
- refresh token
- any other profile field

Those values must never substitute for exact LINE user ID matching.

## Next exact-identity step

After ID token verification succeeds:

```
verified LINE ID token sub
        |
        v
verified_line_user_id
        |
        v
resolveCanonicalCustomerByVerifiedLineUserId(...)
        |
        v
canonical 8-digit Customer ID
```

The existing resolver then continues to enforce:

- exact `line_user_id` match only;
- no name fallback;
- no phone fallback;
- no email fallback;
- no display-name fallback;
- duplicate LINE identity fails closed;
- noncanonical Customer ID fails closed.

## External-call boundary

This PR does not contain a function that calls `fetch()` against LINE.

A later separately gated executor may:

1. consume the already-verified callback transaction;
2. read the configured LINE Login channel secret;
3. send the token request;
4. validate and normalize the token response;
5. send the ID token verification request;
6. locally validate the verified payload;
7. pass only `verified_line_user_id` into the exact CRM resolver.

That executor must remain separately reviewable because it is the first phase that actually uses the channel secret and calls the LINE Login API.

## Current exclusions

Not included:

- Production LINE Login activation;
- Production route wiring;
- actual token endpoint call;
- actual ID token verification endpoint call;
- Production LINE channel secret usage;
- access-token persistence;
- refresh-token persistence;
- profile API call;
- Member session issuance;
- Customer creation;
- Customer ID generation;
- Family creation/link write;
- historical MEMORY write;
- Production D1 schema apply;
- Production D1 write;
- LINE Messaging API send;
- Production deploy.

## Safety status

Current source guarantees:

- request-plan only for token exchange;
- request-plan only for ID token verification;
- PKCE verifier required;
- `openid` response required;
- issuer checked;
- audience checked;
- nonce checked;
- expiry and issued-at checked;
- formal LINE subject required;
- profile/email claims not used for identity;
- access token not used for identity;
- Member session issue = 0;
- Production route wiring = 0;
- Production write = 0.
