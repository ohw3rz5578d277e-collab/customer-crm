# MIZUNO PHOTO MEMBER — Production R2 Provisioning Plan

Baseline: 2026-10-07 JST

## Purpose

Record the completed provisioning history and the current source-only runtime-wiring stage for the dedicated Cloudflare R2 bucket used for Member private media.

The canonical source binding remains declared in `wrangler.jsonc` as exactly:

`MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`

The current source stage additionally creates the reviewed get-only private-media adapter from `env?.MEMBER_PRIVATE_MEDIA_BUCKET` and passes that adapter explicitly into the Member Production request composition.

This source wiring does not execute Production storage fetch, does not activate Member/private-media routes or LINE Login Production, and does not deploy or activate Production traffic.

## Canonical private-media target

- Canonical bucket name: `customer-crm-member-private-media`
- Canonical Worker binding: `MEMBER_PRIVATE_MEDIA_BUCKET`
- Verified jurisdiction: `default`
- Intended storage class: Standard/default
- Public access: disabled
- Custom domain: none
- Public development URL: none
- CORS: none by default
- Lifecycle / retention rule: none by default

The earlier create response validated `Standard`. The later read-only list verification did not independently expose storage-class metadata for the candidate record, so this plan does not treat list-level storage class as fresh independent evidence.

The first bucket is private-media-only. Public Member assets remain unbound and must not reuse this binding automatically.

## Current source-only runtime-wiring stage

Canonical source requires exactly one default-scope R2 binding:

```json
{
  "binding": "MEMBER_PRIVATE_MEDIA_BUCKET",
  "bucket_name": "customer-crm-member-private-media"
}
```

The private-media adapter is explicitly created from the canonical binding in source:

`createMemberPrivateMediaStorageAdapter(env?.MEMBER_PRIVATE_MEDIA_BUCKET)`

and is passed as the Member Production request composition's `private_media_storage_adapter` dependency.

The source-only runtime-wiring stage remains default-off operationally:

- `MEMBER_PRODUCTION_OWNER_APPROVED` remains `false`;
- `public_asset_adapter` remains literal `null` in the Production entry;
- the private-media adapter is explicitly wired only from `MEMBER_PRIVATE_MEDIA_BUCKET`;
- adapter construction performs no R2 object access;
- Production storage fetch remains unapproved and off;
- Member Production route and private-media route remain unapproved and off;
- LINE Login Production activation remains unapproved;
- no env-scoped or additional R2 binding is permitted;
- public Member asset storage remains unbound;
- Production deploy, Worker activation, and Production traffic change remain separately Owner-gated.

Source-only adapter wiring and live Production storage fetch remain separate gates.

## Explicit binding contract

`MEMBER_PRIVATE_MEDIA_BUCKET` is the only approved source binding identifier for the first private-media bucket.

The Production entry may explicitly pass that exact binding to the reviewed adapter factory. Downstream Member compositions must continue to receive the resulting storage adapter explicitly. Source code must not:

- hardcode the canonical bucket name inside Production runtime logic;
- discover an R2 bucket by account inventory at runtime;
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

Their presence is inventory evidence only. No ownership or lifecycle relationship with Customer CRM / Member is implied.

## Object-access contract

The existing Member Production storage adapter remains read-only and exposes only `get(storage_key)`.

For private media:

- storage keys remain relative;
- storage keys must not begin with `/`;
- URL, traversal, backslash, query, fragment, control-character, surrounding-whitespace, and overlong-key forms remain rejected;
- no `put`, `delete`, multipart upload, object listing, bucket listing, or mutation is added by this runtime-wiring stage.

Calling `createMemberPrivateMediaStorageAdapter(binding)` only wraps the binding. It does not call `binding.get(...)`. R2 object access occurs only if a later authorized request path calls the returned adapter's `get(key)` method.

With `MEMBER_PRODUCTION_ROUTE_MODE` disabled, the Member Production composition returns fallthrough before its API handler is invoked, so source-only wiring does not itself execute an object fetch.

## Provisioning sequence and current stage

Each stage is separately gated. Earlier completed stages are retained here as provenance and do not authorize reruns.

1. Source-only provisioning plan was reviewed and merged.
2. The Owner separately authorized creation of exactly one R2 bucket named `customer-crm-member-private-media` in jurisdiction `default`.
3. Exactly one bucket-create attempt that reached the create POST created the canonical bucket; this history is not authorization to create, recreate, update, or delete it again.
4. A separately authorized read-only verification proved the canonical bucket digest matched exactly once within the authorized `default` jurisdiction inventory.
5. A source-only binding proposal was reviewed and merged.
6. Exactly one canonical source binding was declared in `wrangler.jsonc`: `MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`.
7. The current stage source-wires that exact binding through `createMemberPrivateMediaStorageAdapter(...)` into the explicit `private_media_storage_adapter` dependency while keeping all route gates off.
8. Production storage fetch remains a separate future Owner gate.
9. Member Production route activation, private-media route activation, and LINE Login Production activation remain separate future Owner gates.
10. Production deploy, Worker activation, and Production traffic change remain separately Owner-gated.

No step inherits authorization from a previous step.

## Fail-closed conditions

Stop without mutation if any of the following occurs:

- current main SHA drift during an authorized read-only gate;
- active Production Worker version drift during a read-only gate;
- canonical `wrangler.jsonc` R2 binding count is not exactly one;
- the declared binding differs from `MEMBER_PRIVATE_MEDIA_BUCKET`;
- the declared bucket differs from `customer-crm-member-private-media`;
- any additional or env-scoped R2 binding appears;
- any unrelated existing bucket would be reused;
- Production entry hardcodes the bucket name instead of using the canonical binding identifier;
- private-media adapter wiring uses any factory or binding other than `createMemberPrivateMediaStorageAdapter(env?.MEMBER_PRIVATE_MEDIA_BUCKET)`;
- public asset adapter becomes bound without separate authorization;
- Production storage fetch appears before separate authorization;
- Member Production route or private-media route is unexpectedly enabled;
- LINE Login Production is unexpectedly activated.

## Authorization boundaries

This source-only runtime-wiring plan does **not** authorize:

- adding, removing, renaming, or otherwise changing the canonical R2 binding beyond the already reviewed source declaration;
- R2 bucket creation, recreation, update, rename, deletion, or lifecycle changes;
- R2 object read, write, list, multipart upload, or delete;
- Production storage fetch;
- `storage_adapter.get(...)` against Production R2;
- `MEMBER_PRODUCTION_OWNER_APPROVED=true`;
- `MEMBER_PRODUCTION_ROUTE_MODE=enabled`;
- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE=enabled`;
- Member Production route or private-media route activation;
- LINE Login Production activation;
- Cloudflare token or secret creation, change, deletion, or revoke;
- Production deploy;
- Worker activation;
- Production traffic change;
- Production D1 read/write or migration apply;
- CRM write;
- LINE send;
- Customer ID generation, update, delete, or merge;
- security policy change;
- commerce activation;
- paid spend.

## Current expected source state

Canonical source at this runtime-wiring stage must satisfy all of the following:

- exactly one canonical R2 binding exists: `MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`;
- Member Production Owner approval remains false;
- public Production storage adapter remains literal `null`;
- private-media adapter is explicitly wired from `env?.MEMBER_PRIVATE_MEDIA_BUCKET` through the reviewed read-only factory;
- adapter construction does not access an R2 object;
- Production storage fetch remains off;
- Member Production route remains disabled;
- private-media route remains disabled;
- LINE Login Production remains unactivated;
- public Member asset storage remains unbound;
- no additional or env-scoped R2 binding exists.
