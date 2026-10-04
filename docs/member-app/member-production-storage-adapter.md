# MIZUNO PHOTO MEMBER — Production Storage Adapter Foundation

Baseline: 2026-10-04 JST

## Purpose

This source-only foundation defines the explicit read-only adapter boundary required before Member public catalog assets or private media can be connected to a Production object-storage binding.

It does not configure a Cloudflare storage binding and it does not activate any Member Production route.

## Explicit binding only

The production request composition already requires adapters to be passed explicitly. This foundation preserves that contract.

The module exports:

- `createMemberPublicAssetStorageAdapter(binding)`
- `createMemberPrivateMediaStorageAdapter(binding)`

The caller must supply the binding object directly. The adapter never discovers a binding from `env` and never guesses a binding name.

## Read-only behavior

Both adapters expose only:

`get(storage_key)`

They do not expose `put`, `delete`, multipart upload, listing, or any other storage mutation.

The underlying object is normalized to the existing Member delivery contracts:

- `body`
- `size`
- `content_type`
- `etag` for private media when present

Content type may be read from an R2-compatible object's `httpMetadata.contentType` while the customer-facing handlers continue to enforce their own MIME and size allowlists.

## Key safety

Public asset keys must remain under `/member-assets/`.

Private media keys must be relative storage keys and must not begin with `/`.

Both reject:

- absolute HTTP/HTTPS URLs;
- traversal segments (`..`);
- backslashes;
- query or fragment syntax;
- control characters;
- surrounding whitespace;
- overlong keys.

Rejected keys do not call the supplied storage binding.

## Authorization boundaries

This foundation does **not** authorize or perform:

- Production storage binding configuration;
- Production storage fetch;
- `MEMBER_PRODUCTION_ROUTE_MODE=enabled`;
- `MEMBER_PRODUCTION_OWNER_APPROVED=true`;
- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE=enabled`;
- LINE Login external exchange activation;
- Production deploy or traffic change;
- Production D1 write or migration apply;
- CRM write;
- LINE send;
- Customer ID generation;
- Favorites or historical MEMORY writes;
- BLACK entitlement writes;
- checkout/payment or commerce activation;
- paid spend.

A future Production binding must be independently identified and verified before it can be passed to either adapter. Private media remains separately Owner-gated.
