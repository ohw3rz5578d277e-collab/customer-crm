# MIZUNO PHOTO MEMBER — Production R2 Provisioning Plan

Baseline: 2026-10-07 JST

## Purpose

Define the source-only, default-off contract for provisioning a dedicated Cloudflare R2 bucket for Member private media before any Production binding or route activation is authorized.

This document does not create a bucket, configure a binding, read an object, deploy Production, activate traffic, or write Production data.

## Canonical first provisioning target

- Proposed bucket name: `customer-crm-member-private-media`
- Proposed Worker binding: `MEMBER_PRIVATE_MEDIA_BUCKET`
- Authorized jurisdiction target after separate Owner approval: `default`
- Intended storage class: Standard/default
- Public access: disabled
- Custom domain: none
- Public development URL: none
- CORS: none by default
- Lifecycle / retention rule: none by default

The first bucket is private-media-only. Public Member assets remain unbound and must not reuse this binding automatically.

## Why private-only first

The existing production storage adapter already keeps public and private delivery as distinct explicit adapters:

- `createMemberPublicAssetStorageAdapter(binding)`
- `createMemberPrivateMediaStorageAdapter(binding)`

The first Production R2 target is intentionally limited to private media so that public-catalog delivery and private customer media do not become coupled by an implicit storage decision.

## Explicit binding contract

`MEMBER_PRIVATE_MEDIA_BUCKET` is the only proposed binding identifier for the first private-media bucket.

The application must continue to receive the binding explicitly. Source code must not:

- discover an R2 bucket by name at runtime;
- enumerate account buckets at runtime;
- infer a binding from arbitrary `env` keys;
- fall back to another R2 binding;
- reuse album, manga, Instagram, or unrelated storage;
- create, rename, delete, or mutate buckets.

## Existing bucket isolation

The following existing account buckets are unrelated and must not be reused for Member private media:

- `ai-manga-publisher-assets`
- `album-originals`
- `album-previews`
- `instagram-thumbs`

Their current presence is inventory evidence only. No ownership or lifecycle relationship with Customer CRM / Member is implied.

## Object-access contract

The existing Member Production storage adapter remains read-only and exposes only `get(storage_key)`.

For private media:

- storage keys remain relative;
- storage keys must not begin with `/`;
- URL, traversal, backslash, query, fragment, control-character, surrounding-whitespace, and overlong-key forms remain rejected;
- no `put`, `delete`, multipart upload, object listing, bucket listing, or mutation is added by this provisioning plan.

## Provisioning sequence

Each stage is separately gated.

1. Source-only provisioning plan merged.
2. Owner separately authorizes creation of exactly one R2 bucket named `customer-crm-member-private-media` in jurisdiction `default`.
3. Bucket is created with public access disabled and without binding it to `customer-crm-api`.
4. A read-only bucket digest index is run under a separate exact-SHA Owner authorization.
5. The locally calculated SHA-256 of the exact new bucket name is compared with the sanitized digest index.
6. A separate candidate verify run must prove exact-one-match for that digest.
7. Only after successful verification may a separate source PR propose `MEMBER_PRIVATE_MEDIA_BUCKET` in `wrangler.jsonc`.
8. Adding the binding, Production deploy, private-media route activation, Member route activation, and Production storage fetch each remain separately Owner-gated.

No step inherits authorization from a previous step.

## Fail-closed conditions

Stop without mutation if any of the following occurs:

- current main SHA drift;
- active Production Worker version drift during a read-only gate;
- candidate digest matches zero or more than one bucket;
- reported jurisdiction is not `default`;
- the new bucket name differs from `customer-crm-member-private-media`;
- the proposed binding differs from `MEMBER_PRIVATE_MEDIA_BUCKET`;
- any unrelated existing bucket would be reused;
- any route or Owner approval source gate is already enabled unexpectedly;
- canonical `wrangler.jsonc` already contains an unreviewed R2 binding.

## Authorization boundaries

This source-only plan does **not** authorize:

- R2 bucket creation, update, rename, deletion, or lifecycle changes;
- R2 object read, write, list, multipart upload, or delete;
- Production storage binding change;
- Production storage fetch;
- `MEMBER_PRODUCTION_OWNER_APPROVED=true`;
- `MEMBER_PRODUCTION_ROUTE_MODE=enabled`;
- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE=enabled`;
- private-media route activation;
- LINE Login Production activation;
- Cloudflare token or secret changes;
- Production deploy or Worker activation;
- Production traffic change;
- Production D1 write or migration apply;
- CRM write;
- LINE send;
- Customer ID generation;
- security policy change;
- commerce activation;
- paid spend.

## Current expected source state

Before the future bucket-create authorization is requested, canonical source must still satisfy all of the following:

- Member Production Owner approval default is false;
- Member Production route is not enabled;
- private-media route is not enabled;
- canonical R2 binding is absent;
- private-media storage adapter remains explicit-binding and read-only;
- public Member asset storage remains unbound.
