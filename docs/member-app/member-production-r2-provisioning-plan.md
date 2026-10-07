# MIZUNO PHOTO MEMBER — Production R2 Provisioning Plan

Baseline: 2026-10-07 JST

## Purpose

Record the completed provisioning history and the current source-only declared-binding stage for the dedicated Cloudflare R2 bucket used for Member private media.

The canonical source binding is now declared in `wrangler.jsonc` as exactly:

`MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`

This source declaration does not wire the binding into Production runtime, does not enable Production storage fetch, does not activate Member/private-media routes or LINE Login Production, and does not deploy or activate Production traffic.

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

## Current source-only declared-binding state

Canonical source now requires exactly one default-scope R2 binding:

```json
{
  "binding": "MEMBER_PRIVATE_MEDIA_BUCKET",
  "bucket_name": "customer-crm-member-private-media"
}
```

The declared-binding stage remains default-off at runtime:

- `MEMBER_PRODUCTION_OWNER_APPROVED` remains `false`;
- `public_asset_adapter` remains literal `null` in the Production entry;
- `private_media_storage_adapter` remains literal `null` in the Production entry;
- runtime adapter consumption remains unapproved and off;
- Production storage fetch remains unapproved and off;
- Member Production route and private-media route remain unapproved and off;
- LINE Login Production activation remains unapproved;
- no env-scoped or additional R2 binding is permitted;
- public Member asset storage remains unbound.

Binding declaration and runtime consumption remain separate gates.

## Explicit binding contract

`MEMBER_PRIVATE_MEDIA_BUCKET` is the only approved source binding identifier for the first private-media bucket.

The application must continue to receive any storage adapter explicitly. Source code must not:

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

Their presence is inventory evidence only. No ownership or lifecycle relationship with Customer CRM / Member is implied.

## Object-access contract

The existing Member Production storage adapter remains read-only and exposes only `get(storage_key)`.

For private media:

- storage keys remain relative;
- storage keys must not begin with `/`;
- URL, traversal, backslash, query, fragment, control-character, surrounding-whitespace, and overlong-key forms remain rejected;
- no `put`, `delete`, multipart upload, object listing, bucket listing, or mutation is added by this provisioning plan.

The source binding declaration does not itself authorize calling `get()` against Production storage.

## Provisioning sequence and current stage

Each stage is separately gated. Earlier completed stages are retained here as provenance and do not authorize reruns.

1. Source-only provisioning plan was reviewed and merged.
2. The Owner separately authorized creation of exactly one R2 bucket named `customer-crm-member-private-media` in jurisdiction `default`.
3. Exactly one bucket-create attempt that reached the create POST created the canonical bucket; this history is not authorization to create, recreate, update, or delete it again.
4. A separately authorized read-only verification proved the canonical bucket digest matched exactly once within the authorized `default` jurisdiction inventory.
5. A source-only binding proposal was reviewed and merged.
6. The current stage declares exactly one canonical source binding in `wrangler.jsonc`: `MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`.
7. Runtime adapter wiring remains a separate future Owner gate.
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
- runtime storage adapter consumption appears before separate authorization;
- Production storage fetch appears before separate authorization;
- Member Production route or private-media route is unexpectedly enabled;
- LINE Login Production is unexpectedly activated.

## Authorization boundaries

This source-only declared-binding plan does **not** authorize:

- adding, removing, renaming, or otherwise changing the canonical R2 binding beyond the already reviewed source declaration;
- R2 bucket creation, recreation, update, rename, deletion, or lifecycle changes;
- R2 object read, write, list, multipart upload, or delete;
- runtime consumption of `env.MEMBER_PRIVATE_MEDIA_BUCKET`;
- `createMemberPrivateMediaStorageAdapter(env.MEMBER_PRIVATE_MEDIA_BUCKET)` in Production runtime;
- Production storage fetch;
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

Canonical source at this declared-binding stage must satisfy all of the following:

- exactly one canonical R2 binding exists: `MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`;
- Member Production Owner approval remains false;
- public and private Production storage adapters remain literal `null`;
- runtime adapter consumption remains off;
- Production storage fetch remains off;
- Member Production route remains disabled;
- private-media route remains disabled;
- LINE Login Production remains unactivated;
- public Member asset storage remains unbound;
- no additional or env-scoped R2 binding exists.
