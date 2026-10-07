# Member Production R2 binding proposal

Baseline: 2026-10-07 JST

## Purpose

Record the approved source-only stage after the first dedicated Member private-media R2 bucket was created, independently verified by read-only exact-SHA storage preflight, declared as the canonical Worker binding, and separately approved for source-only runtime adapter wiring.

The canonical Worker binding remains declared in `wrangler.jsonc`. The current source stage additionally passes that binding into the reviewed read-only private-media storage adapter and passes the resulting adapter explicitly into the Member Production request composition.

This runtime wiring is source-only and does not authorize storage fetch, R2 object access, route activation, LINE Login Production, Production deploy, Worker activation, or traffic change.

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

This source binding is private-media-only. Public Member assets remain unbound.

## Required source state at the runtime-wiring stage

Canonical source must continue to satisfy all of the following:

- `wrangler.jsonc` contains exactly one `r2_buckets` binding;
- that binding is exactly `MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`;
- no additional or env-scoped R2 binding is present;
- `MEMBER_PRODUCTION_OWNER_APPROVED` remains `false`;
- `public_asset_adapter` remains literal `null` in the Production entry;
- `createMemberPrivateMediaStorageAdapter` is imported exactly once from `member-production-storage-adapter.mjs`;
- `private_media_storage_adapter` is wired exactly as `createMemberPrivateMediaStorageAdapter(env?.MEMBER_PRIVATE_MEDIA_BUCKET)`;
- the Production entry does not hardcode `customer-crm-member-private-media`;
- Member Production route remains disabled;
- private-media content route remains disabled;
- LINE Login Production remains unactivated;
- no Production storage fetch occurs;
- the storage adapter remains explicit-binding, read-only, and get-only;
- adapter construction itself performs no R2 object request.

## Runtime wiring boundary

Binding declaration and source-only runtime adapter wiring are now completed source stages, but storage fetch and route activation remain separate gates.

The approved source-only runtime wiring does not, by itself:

- call `storage_adapter.get(...)`;
- read, list, write, or delete any R2 object;
- enable Member Production routes;
- enable private-media content routes;
- activate LINE Login Production;
- enable Production storage fetch;
- deploy Production or activate a Worker version;
- change Production traffic.

The source call `createMemberPrivateMediaStorageAdapter(env?.MEMBER_PRIVATE_MEDIA_BUCKET)` only constructs a get-only adapter around the explicit canonical binding. The reviewed adapter performs `binding.get(...)` only when its returned `get(key)` method is later called. With Member route mode disabled, Member composition falls through without invoking the Member API handler or storage adapter.

Any transition from source-only wiring to live Production storage fetch, route activation, or deployment requires a separate fresh Owner authorization at the then-current exact SHA and scope.

## Authorization boundaries

Recording this approved source-only runtime wiring does **not** authorize:

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

No authorization from the bucket-create, read-only verification, source-binding declaration, or source-only runtime-wiring stage carries forward automatically to storage fetch, route activation, or Production deployment.
