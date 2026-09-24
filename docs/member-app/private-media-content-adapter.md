# MIZUNO PHOTO MEMBER — Private Media Binary Content Adapter Foundation

Baseline: 2026-09-25 JST

## Purpose

This phase defines the trusted server-side boundary that may later convert an authorized private-media grant into actual image bytes.

It remains provider-neutral and Production-inactive.

## No implicit storage binding

The content reader requires an explicit trusted storage adapter argument.

It does not read an R2/S3/bucket binding implicitly from `env`.

This is intentional.

Until a later reviewed integration passes a concrete adapter into the handler, the source cannot fetch Production storage.

The required adapter contract is:

`storageAdapter.get(storage_key)`

The private `storage_key` is supplied only after:

1. signed Member session verification;
2. short-lived grant verification;
3. exact HMAC binding validation;
4. current Customer -> Family reauthorization;
5. current same-Family media authorization;
6. published/non-deleted MEMORY checks.

## Authorization before storage observation

Grant verification and private-media authorization happen before the content layer checks whether a storage adapter is available.

This avoids exposing storage integration state to a request that has not passed the private-media authorization boundary.

Invalid grants never reach `storageAdapter.get()`.

## Conceptual endpoint

Source-only endpoint:

`POST /api/internal/member/media/content`

Body:

```json
{
  "media_id": "media_...",
  "grant": "v1...."
}
```

Only those two exact keys are accepted.

Requirements:

- route mode `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE=enabled`
- signed Member session Cookie
- exact same Origin
- JSON body
- valid private-media delivery grant
- explicit trusted storage adapter

The route mode defaults disabled.

Production route wiring remains false.

## Storage object contract

A trusted adapter must return a normalized object containing:

- `body`
- `size`
- `content_type`
- optional `etag`

Initial content-type allowlist:

- image/jpeg
- image/png
- image/webp

Initial media type:

- image only

Maximum object size:

**50 MiB**

Unknown MIME types, invalid/missing size metadata, empty/invalid bodies, malformed ETags, and oversized objects fail closed.

No provider redirect URL is accepted.

## Binary response security

Successful binary response uses:

- exact allowlisted image Content-Type
- Content-Length
- `Cache-Control: private, no-store, max-age=0`
- `Pragma: no-cache`
- `X-Content-Type-Options: nosniff`
- `Referrer-Policy: no-referrer`
- `Cross-Origin-Resource-Policy: same-origin`
- `Content-Security-Policy: default-src 'none'; sandbox`
- `Accept-Ranges: none`

The source does not redirect to an external or signed storage URL.

## Range requests

Range requests are not supported in this first image-only foundation.

A request containing `Range` is rejected before storage fetch.

This keeps the first delivery contract simple and avoids implementing partial-content semantics incorrectly.

Future video delivery, if needed, should receive a separate explicit design and review.

## Grant/body privacy

Customer-facing binary response does not expose:

- storage key
- Customer ID
- Family ID
- storage provider metadata
- external URL

The grant remains in the POST body and not in a URL.

## Production status

This phase does not:

- bind Cloudflare R2
- bind S3 or another provider
- wire the content handler into Production routing
- enable `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE` in Production
- fetch Production storage
- issue public/signed provider URLs
- change D1
- send LINE
- deploy Production

## Next integration gate

A later storage-binding phase can implement one narrow adapter for the selected existing/private storage mechanism.

That phase must keep this ordering:

Member session -> grant verification -> current private-media authorization -> trusted adapter -> validated image object -> private no-store response.

## Safety status

Current foundation is:

- source-only
- read-only
- route default-disabled
- same-origin POST only
- signed Member session required
- grant required
- grant reauthorization required
- explicit trusted storage adapter only
- no implicit env storage binding
- image MIME allowlist
- 50 MiB maximum
- range support = 0
- no-store
- nosniff
- same-origin resource policy
- external redirect = 0
- storage key public response = 0
- Production storage binding = 0
- Production route wiring = 0
- Production storage fetch = 0
- Production write = 0
