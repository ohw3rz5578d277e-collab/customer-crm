# MIZUNO PHOTO MEMBER — Private Media Delivery Grant Foundation

Baseline: 2026-09-25 JST

## Purpose

The existing private-media authorization layer can safely resolve an internal private `storage_key` after exact Member/Family authorization.

This phase adds a short-lived delivery grant that can later sit between the browser and a trusted binary-delivery route.

It does **not** fetch or return media bytes.

## Grant shape

A grant is:

`v1.<issued_at>.<expires_at>.<HMAC>`

The grant lifetime is:

**120 seconds**

Clock skew allowance:

**30 seconds**

The HMAC is calculated over:

- exact Family ID from the verified Member session
- exact canonical Customer ID from the verified Member session
- exact media ID
- issued-at timestamp
- expiry timestamp

Those identity values are **binding inputs**, not token payload fields.

The token itself does not contain:

- Customer ID
- Family ID
- storage key

## Separate secret

Grant signing uses:

`MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET`

Minimum length:

32 characters

This is intentionally separate from the Member session signing secret.

Missing/short secret fails closed.

## Issue flow

A grant can be issued only after:

1. signed Member session verification;
2. same-origin POST boundary;
3. exact JSON body containing only `media_id`;
4. current Customer -> Family authorization;
5. exact same-Family private-media lookup;
6. published/non-deleted parent MEMORY;
7. non-deleted media;
8. safe internal storage-key validation.

The customer-facing response includes:

- media ID
- MEMORY ID
- media type
- role
- dimensions
- grant
- expiry
- conceptual binary-delivery contract

It does **not** include the storage key.

## Verification flow

Before a future binary route may fetch a private object, it must call:

`verifyMemberPrivateMediaDeliveryGrant`

Verification requires:

1. valid short-lived grant structure;
2. current Member session;
3. exact media ID;
4. HMAC binding to current Family + Customer + media;
5. grant time validity;
6. full private-media authorization **again**.

This second authorization means a still-valid grant cannot bypass a Family-link change or a media/MEMORY visibility change.

## No grant in URL

The future delivery contract is intentionally:

`POST /api/internal/member/media/content`

with body keys:

- `media_id`
- `grant`

The grant is not placed in a query string or path.

This reduces accidental exposure through referrers and copied URLs.

The binary content route itself is not implemented in this phase.

## Existing private-media hardening

This phase also tightens existing private-media validation.

`media_id` and `storage_key` now reject raw values containing:

- control characters
- outer whitespace

before normalization.

The existing restrictions against absolute URLs, absolute paths, traversal, and excessive length remain.

## Conceptual grant endpoint

Source-only endpoint:

`POST /api/internal/member/media/grant`

Requirements:

- signed Member session Cookie
- exact same Origin
- JSON
- body contains only `media_id`
- current private-media authorization

Production route wiring remains false.

## Storage boundary

The internal grant verifier may return the authorized private `storage_key` only to a trusted server-side binary-delivery layer.

Customer-facing JSON must never contain that field.

## Current exclusions

Not included:

- private binary content route
- storage provider binding
- R2/S3 fetch
- signed storage-provider URL
- streaming/range requests
- image transformation
- browser object-URL handling
- cache policy for private bytes
- Production route wiring
- Production storage fetch
- Production deploy
- Production write

## Safety status

Current foundation is:

- source-only
- read-only
- separate grant-signing secret
- 120-second grant TTL
- exact session + media HMAC binding
- token Customer ID = 0
- token Family ID = 0
- token storage key = 0
- same-origin grant issuance
- signed Member session required
- private media reauthorized on issue
- private media reauthorized on verification
- grant kept out of URL
- customer JSON storage key = 0
- binary route = 0
- Production route wiring = 0
- Production storage fetch = 0
- Production write = 0
