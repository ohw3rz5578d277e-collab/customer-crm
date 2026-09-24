# MIZUNO PHOTO MEMBER — LINE Login Transaction Security Foundation

Baseline: 2026-09-24 JST

## Purpose

This phase establishes the security boundary for starting a future LINE Login v2.1 authorization flow and validating the callback transaction before any token exchange.

It does not activate LINE Login in Production.

It does not exchange an authorization code, verify an ID token, issue a Member session, write D1, send LINE messages, or wire a Production route.

## Official protocol assumptions

The transaction contract follows the current LINE Login v2.1 web authorization model:

- OAuth 2.0 authorization code flow;
- authorization endpoint:
  `https://access.line.me/oauth2/v2.1/authorize`;
- `state` for CSRF protection;
- `nonce` for replay protection in the later OpenID Connect ID-token step;
- PKCE with `S256`;
- minimal requested scope: `openid`.

No `profile` or `email` scope is requested by this foundation because Member identity is based on the LINE subject and the canonical CRM Customer ID, not LINE profile fields.

## Configuration

Source contract expects:

- `MEMBER_LINE_LOGIN_CHANNEL_ID`
- `MEMBER_LINE_LOGIN_REDIRECT_URI`
- `MEMBER_LINE_LOGIN_TRANSACTION_SECRET`

The transaction secret:

- is separate from the CRM Owner session secret;
- is separate from the Member session secret;
- must be at least 32 characters;
- is never committed to source control.

The redirect URI must be HTTPS and must not contain embedded credentials or a fragment.

## Start transaction

`createMemberLineLoginTransaction(env, options)` generates:

- cryptographically random `state`;
- cryptographically random `nonce`;
- PKCE `code_verifier`;
- SHA-256 `code_challenge`;
- `code_challenge_method=S256`;
- a short-lived signed transaction cookie;
- the LINE authorization URL.

The PKCE verifier is never placed in the authorization URL.

## Signed transient cookie

Cookie name:

`__Host-mizuno_member_login_tx`

Attributes:

- `Path=/`
- `HttpOnly`
- `Secure`
- `SameSite=Lax`
- no Domain attribute
- ten-minute Max-Age

The signed payload contains only transaction material:

- version/purpose
- configured LINE channel ID
- configured redirect URI
- state
- nonce
- code verifier
- local return target
- issued/expiry timestamps

No customer identity, Family ID, LINE user ID, name, email, phone, or Member session token is stored in this transaction cookie.

## Why SameSite=Lax

LINE Login returns through a top-level navigation from the LINE authorization domain.

The transaction cookie therefore uses `SameSite=Lax` so the short-lived HttpOnly cookie can be sent on the top-level callback while still avoiding a broad cross-site cookie policy.

## Return target

The optional `return_to` value is restricted to a local absolute-path reference such as:

`/member/memories`

The contract rejects:

- full external URLs;
- protocol-relative URLs such as `//evil.example`;
- control characters;
- oversized values.

This prevents the login transaction from becoming an open redirect.

## Callback verification

`verifyMemberLineLoginCallback(request, env)` verifies the transaction before token exchange.

Checks include:

1. current configuration is valid;
2. callback origin/path match the configured redirect URI;
3. any query parameters embedded in the configured redirect URI are still present;
4. LINE did not return an authorization error;
5. `code` exists;
6. `state` exists;
7. the signed transaction cookie exists and is within size bounds;
8. HMAC signature is valid;
9. transaction is not expired or materially future-issued;
10. callback `state` exactly matches the transaction state;
11. transaction channel ID still matches current config;
12. transaction redirect URI still matches current config.

On any callback result, the caller receives a cookie-clear instruction so the transaction can remain one-shot in the future route implementation.

## Successful callback output

A successful callback verification returns server-only exchange material:

- authorization code;
- expected nonce;
- PKCE code verifier;
- channel ID;
- redirect URI;
- local return target.

It still reports:

- token exchange = not executed;
- ID token verification = not executed;
- Member session issuance = not executed;
- Production write = 0.

## Next phase: token exchange and ID-token verification

The next source-only phase can consume the verified callback output and implement:

1. authorization-code exchange against LINE Login v2.1;
2. PKCE `code_verifier` submission;
3. ID token verification using LINE's verification endpoint or an equivalent verified OpenID Connect implementation;
4. exact checks for channel/audience, issuer, expiry, nonce, and formal LINE `sub`;
5. forwarding only the verified LINE subject into the existing exact LINE identity resolver.

No Customer ID may be inferred from LINE display name, email, profile image, or other profile claims.

## Current exclusions

Not included:

- Production LINE Login activation;
- token endpoint call;
- ID token verification endpoint call;
- channel secret usage;
- Member session issuance;
- registration writes;
- Customer creation;
- Customer ID generation;
- Family creation/link writes;
- historical MEMORY writes;
- Production D1 schema apply;
- Production D1 write;
- LINE Messaging API send;
- Production deploy.

## Safety status

Current source guarantees:

- state required;
- nonce required;
- PKCE S256 required;
- signed transient transaction;
- callback redirect revalidation;
- local return target only;
- no profile/email scope;
- no token exchange;
- no Member session issuance;
- no Production route wiring;
- no Production write.
