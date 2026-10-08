# MIZUNO PHOTO MEMBER — Production R2 Provisioning Plan

Baseline: 2026-10-08 JST

## Purpose

Record the completed provisioning history and the current **source-only runtime-wiring stage** for the dedicated Member private-media R2 bucket.

Canonical binding:

`MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`

The binding is now explicitly consumed in source by `createMemberPrivateMediaStorageAdapter(env.MEMBER_PRIVATE_MEDIA_BUCKET)` and passed to Member Production request composition as `private_media_storage_adapter`.

This stage does **not** enable a Member route, private-media content route, LINE Login Production, Production storage fetch, deploy, Worker activation, or Production traffic.

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

The first bucket is private-media-only. Public Member assets remain unbound.

## Current source-only runtime-wiring state

Exactly one canonical R2 binding exists:

```json
{
  "binding": "MEMBER_PRIVATE_MEDIA_BUCKET",
  "bucket_name": "customer-crm-member-private-media"
}
```

Current invariants:

- `MEMBER_PRODUCTION_OWNER_APPROVED` remains `false`;
- `public_asset_adapter` remains literal `null`;
- the private binding is explicitly wrapped with `createMemberPrivateMediaStorageAdapter(...)`;
- the resulting read-only adapter is passed explicitly as `private_media_storage_adapter`;
- adapter creation itself performs no R2 access;
- route OFF prevents the adapter from being used and therefore prevents R2 object fetch;
- Production storage fetch remains unapproved and off;
- Member Production route and private-media route remain unapproved and off;
- LINE Login Production activation remains unapproved;
- no env-scoped or additional R2 binding is permitted;
- public Member asset storage remains unbound.

Source wiring and Production storage fetch remain separate gates.

## Explicit binding contract

`MEMBER_PRIVATE_MEDIA_BUCKET` is the only approved binding identifier for the first private-media bucket.

The Production entry may explicitly read that exact binding to construct the approved read-only adapter. Lower Member composition layers must not:

- discover an R2 bucket by bucket name;
- enumerate account buckets;
- infer a binding from arbitrary `env` keys;
- fall back to another R2 binding;
- reuse album, manga, Instagram, or unrelated storage;
- create, rename, delete, or mutate buckets.

## Existing bucket isolation

The following existing account buckets remain unrelated and must not be reused:

- `ai-manga-publisher-assets`
- `album-originals`
- `album-previews`
- `instagram-thumbs`

## Object-access contract

The Member Production storage adapter remains read-only and get-only.

For private media:

- storage keys remain relative and may not begin with `/`;
- URL, traversal, backslash, query, fragment, control-character, surrounding-whitespace, and overlong-key forms remain rejected;
- no `put`, `delete`, multipart upload, object listing, bucket listing, or mutation is exposed;
- adapter construction must not call `get()`;
- route OFF must result in zero R2 object fetches.

The current source-only wiring does not authorize executing `get()` against Production R2.

## Provisioning sequence and current stage

Earlier completed stages are provenance only and do not authorize reruns.

1. Source-only provisioning plan was reviewed and merged.
2. The Owner separately authorized creation of exactly one R2 bucket named `customer-crm-member-private-media` in jurisdiction `default`.
3. The canonical bucket was created once.
4. A separately authorized read-only verification proved the canonical bucket digest matched exactly once.
5. A source-only binding proposal was reviewed and merged.
6. Exactly one canonical source binding was declared in `wrangler.jsonc`.
7. The Owner separately authorized the current source-only runtime adapter wiring at main `c2ee3a2d79af72fccea3d38d776d060d516e99f9`.
8. The current stage wires the canonical binding into the explicit read-only private-media adapter while all Member routes remain off.
9. Production storage fetch remains a separate future Owner gate.
10. Member Production route activation, private-media route activation, and LINE Login Production activation remain separate future Owner gates.
11. Production deploy, Worker activation, and Production traffic change remain separately Owner-gated.

No step inherits authorization from a previous step.

## Fail-closed conditions

Stop without mutation if:

- the canonical R2 binding count is not exactly one;
- the binding or bucket name differs from the canonical mapping;
- any additional/env-scoped R2 binding appears;
- the public asset adapter becomes wired by this stage;
- the private adapter exposes mutation methods;
- adapter construction performs R2 access;
- an R2 object fetch occurs while Member route mode is OFF;
- `MEMBER_PRODUCTION_OWNER_APPROVED` becomes true;
- Member Production/private-media route is unexpectedly enabled;
- LINE Login Production is unexpectedly activated.

## Authorization boundaries

This stage does **not** authorize:

- R2 bucket create/recreate/update/rename/delete/lifecycle changes;
- R2 object read, write, list, multipart upload, or delete;
- Production storage fetch;
- `MEMBER_PRODUCTION_OWNER_APPROVED=true`;
- `MEMBER_PRODUCTION_ROUTE_MODE=enabled`;
- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE=enabled`;
- Member Production or private-media route activation;
- LINE Login Production activation;
- token/secret creation, change, deletion, or revoke;
- Production deploy, Worker activation, or Production traffic change;
- Production D1 read/write or migration apply;
- CRM write, LINE send, or Customer ID mutation;
- security policy change, commerce activation, or paid spend;
- merge to main.

## Current expected source state

- exactly one canonical R2 binding exists: `MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`;
- Owner approval remains false;
- public adapter remains literal `null`;
- private adapter is explicitly source-wired from `MEMBER_PRIVATE_MEDIA_BUCKET`;
- adapter is read-only/get-only;
- Production storage fetch remains off;
- Member Production route remains disabled;
- private-media route remains disabled;
- LINE Login Production remains unactivated;
- public Member asset storage remains unbound;
- no additional/env-scoped R2 binding exists.
