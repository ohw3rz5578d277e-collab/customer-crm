# Member Production R2 binding proposal

Baseline: 2026-10-07 JST

## Purpose

Record the approved source-only stage after the first dedicated Member private-media R2 bucket was created, independently verified by read-only exact-SHA storage preflight, and separately approved for canonical binding declaration in source.

The canonical Worker binding is now declared in `wrangler.jsonc` as part of this source-only stage. This stage does not wire the binding into Production runtime code, does not read an R2 object, does not enable storage fetch or routes, and does not deploy or activate Production.

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

## Canonical declared binding

The only acceptable first private-media binding declaration is:

```json
{
  "binding": "MEMBER_PRIVATE_MEDIA_BUCKET",
  "bucket_name": "customer-crm-member-private-media"
}
```

This source-only binding is private-media-only. Public Member assets remain unbound.

## Required source state at the declared-binding stage

Canonical source must continue to satisfy all of the following:

- `wrangler.jsonc` contains exactly one `r2_buckets` binding;
- that binding is exactly `MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`;
- no additional or env-scoped R2 binding is present;
- `MEMBER_PRODUCTION_OWNER_APPROVED` remains `false`;
- `public_asset_adapter` remains literal `null` in the Production entry;
- `private_media_storage_adapter` remains literal `null` in the Production entry;
- Member Production route remains disabled;
- private-media content route remains disabled;
- no Production storage fetch occurs;
- the storage adapter remains explicit-binding and read-only.

## Runtime consumption boundary

Binding declaration and runtime consumption remain separate gates.

The approved source-only declaration must not, by itself:

- pass `env.MEMBER_PRIVATE_MEDIA_BUCKET` into the Production request composition;
- create or wire `createMemberPrivateMediaStorageAdapter(env.MEMBER_PRIVATE_MEDIA_BUCKET)` in Production runtime;
- enable Member Production routes;
- enable private-media content routes;
- activate LINE Login Production;
- read any R2 object;
- enable Production storage fetch;
- deploy Production or activate a Worker version;
- change Production traffic.

Any transition from source declaration to runtime consumption requires a separate fresh Owner authorization at the then-current exact SHA and scope.

## Authorization boundaries

Recording this approved source-only declaration does **not** authorize:

- any additional or different `wrangler.jsonc` R2 binding change;
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
- Customer ID generation, update, delete, or merge;
- token or secret creation/change/revoke/delete;
- security policy change;
- commerce activation;
- paid spend.

No authorization from the bucket-create, read-only verification, or source-binding declaration stage carries forward automatically to runtime consumption, route activation, or Production deployment.
