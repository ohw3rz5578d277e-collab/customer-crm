# MIZUNO PHOTO MEMBER — Private Media HTTP Router

Baseline: 2026-09-25 JST

## Purpose

This source-only router creates the browser-facing boundary for private Member photo delivery.

It exposes two source contracts:

- `POST /api/member/media/grant`
- `POST /api/member/media/content`

and delegates them to the already-reviewed internal private-media contracts.

It is **not wired into the Production Worker entry** in this phase.

## Grant flow

Public source path:

`POST /api/member/media/grant`

Internal contract:

`POST /api/internal/member/media/grant`

The existing internal grant contract still enforces:

- exact same-origin request;
- JSON-only body;
- signed `__Host-mizuno_member_session`;
- exact `media_id`;
- current Customer → Family authorization;
- published MEMORY requirement;
- media belongs to the current Family;
- valid private storage key;
- short-lived signed delivery grant.

The grant contains no Customer ID, Family ID, or storage key.

## Content flow

Public source path:

`POST /api/member/media/content`

Internal contract:

`POST /api/internal/member/media/content`

The content path requires an explicitly injected trusted storage adapter.

The router does not inspect `env` for a storage binding and does not auto-select an R2/KV/other storage object.

A future Production composition layer must explicitly provide:

`storage_adapter.get(storage_key)`

before binary delivery can occur.

The existing internal content adapter still enforces:

- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE=enabled`;
- signed Member session;
- same-origin POST;
- exact `media_id + grant` body;
- delivery-grant verification;
- current private-media authorization again at content time;
- trusted adapter only;
- image content types only;
- size limits;
- no Range support;
- no storage-key response;
- no signed storage URL;
- no external redirect.

## Defense in depth

The public router independently rejects:

- non-POST methods;
- missing or cross-origin `Origin`;
- missing explicit storage adapter on the content path;
- missing internal handler;
- internal handler contract mismatch.

The internal grant/content contracts then re-check their own security conditions.

## Response security

All responses are hardened with:

- `Cache-Control: no-store`
- `Pragma: no-cache`
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `X-Robots-Tag: noindex, nofollow, noarchive`
- `Referrer-Policy: no-referrer`
- `Cross-Origin-Opener-Policy: same-origin`
- `Cross-Origin-Resource-Policy: same-origin`

Binary `Content-Type` from the reviewed internal content adapter is preserved.

## Excluded from this phase

Not included:

- Production route wiring;
- Production storage binding;
- Production storage fetch;
- Production deploy;
- D1 schema apply;
- D1 write;
- Customer creation;
- Customer ID generation;
- Family creation/link write;
- MEMORY write;
- Favorite write;
- BLACK entitlement write;
- LINE send;
- checkout/payment;
- commerce activation.

## Production readiness

The Production Readiness Preflight should treat private-media **source readiness** as requiring all four source layers:

1. private-media authorization;
2. signed delivery grant;
3. binary content adapter;
4. this public private-media router.

Activation readiness remains separate and still requires explicit observed Production evidence for:

- delivery secret;
- content route mode;
- trusted storage adapter;
- Production storage binding;
- Production route wiring;
- fresh Owner private-media Production authorization.
