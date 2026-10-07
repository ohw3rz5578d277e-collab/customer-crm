# Member Production R2 binding proposal

Baseline: 2026-10-07 JST

## Purpose

Record the next source-only stage after the first dedicated Member private-media R2 bucket was created and independently verified by a read-only exact-SHA storage preflight.

This document proposes the canonical Worker binding only. It does not add the binding to `wrangler.jsonc`, does not wire the binding into runtime code, does not read an R2 object, and does not deploy or activate Production.

## Verified bucket evidence

The verified canonical target is:

- bucket name: `customer-crm-member-private-media`
- bucket-name SHA-256: `6f1f7fa25143081a302fcd148a52ae660195b2a31cf93a55f9ed39cf2e25f388`
- observed jurisdiction: `default`
- exact-one-match verification: PASS
- read-only verification run: `37560368471`
- inspected main SHA: `660620477f1d2ef9f9d1a49183c58957cf13e7eb`
- Production Worker version remained stable during verification: `6dd49589-f01d-473f-876a-034563023b0e`

The read-only list response did not independently expose storage-class metadata for the candidate record, so this proposal does not treat list-level `storage_class` as fresh independent evidence. The earlier create response validated `Standard`; no new mutation is authorized by this document.

## Canonical proposed binding

The only acceptable first private-media binding proposal is:

```json
{
  "binding": "MEMBER_PRIVATE_MEDIA_BUCKET",
  "bucket_name": "customer-crm-member-private-media"
}
```

This proposal is private-media-only. Public Member assets remain unbound.

## Required source state before binding change

Until a separate Owner authorization explicitly approves the binding source change, canonical source must continue to satisfy all of the following:

- `wrangler.jsonc` contains no `r2_buckets` binding;
- `MEMBER_PRODUCTION_OWNER_APPROVED` remains `false`;
- `public_asset_adapter` remains literal `null` in the Production entry;
- `private_media_storage_adapter` remains literal `null` in the Production entry;
- Member Production route remains disabled;
- private-media content route remains disabled;
- no Production storage fetch occurs;
- the storage adapter remains explicit-binding and read-only.

## Future binding source change boundary

A later, separately approved source change may add exactly one `wrangler.jsonc` R2 binding with:

- binding: `MEMBER_PRIVATE_MEDIA_BUCKET`
- bucket name: `customer-crm-member-private-media`

That later source change must not, by itself:

- pass `env.MEMBER_PRIVATE_MEDIA_BUCKET` into the Production request composition;
- create `createMemberPrivateMediaStorageAdapter(env.MEMBER_PRIVATE_MEDIA_BUCKET)` in Production runtime;
- enable Member Production routes;
- enable private-media content routes;
- read any R2 object;
- deploy Production or activate a Worker version.

Binding declaration and runtime consumption remain separate gates.

## Authorization boundaries

Merging this proposal document and its contract test does **not** authorize:

- `wrangler.jsonc` R2 binding changes;
- R2 bucket create, update, rename, delete, lifecycle, CORS, domain, or public-access changes;
- R2 object read, write, list, multipart upload, or delete;
- Production storage fetch;
- Member Production route activation;
- private-media route activation;
- LINE Login Production activation;
- Production deploy, Worker activation, or traffic change;
- Production D1 read/write or migration apply;
- CRM write;
- LINE send;
- Customer ID generation;
- token or secret change/revoke/delete;
- security policy change;
- commerce activation;
- paid spend.

No authorization from the bucket-create or read-only verification stages carries forward automatically.
