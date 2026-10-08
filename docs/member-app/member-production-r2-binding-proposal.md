# Member Production R2 binding proposal

Baseline: 2026-10-07 JST

## Purpose

Record the approved source-only stages for the dedicated Member private-media R2 bucket: bucket creation and verification, canonical binding declaration, and the later source-only runtime wiring stage.

The canonical Worker binding is declared in `wrangler.jsonc` and the Production entry now explicitly constructs the private-media read-only adapter in source. None of these source stages enable Member routes, perform an R2 object read, enable Production storage fetch, or deploy/activate Production.

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

The only acceptable private-media binding declaration is:

```json
{
  "binding": "MEMBER_PRIVATE_MEDIA_BUCKET",
  "bucket_name": "customer-crm-member-private-media"
}
```

This binding is private-media-only. Public Member assets remain unbound.

## Historical declared-binding-only stage

Before the 2026-10-08 runtime-wiring authorization, the declared-binding-only stage required `private_media_storage_adapter:null` and prohibited passing `env.MEMBER_PRIVATE_MEDIA_BUCKET` into Production request composition. Those requirements are historical provenance for the earlier stage and are **not** the current canonical source-state requirement.

## Current required source state — source-only runtime wiring stage

Canonical source must satisfy all of the following:

- `wrangler.jsonc` contains exactly one `r2_buckets` binding;
- that binding is exactly `MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`;
- no additional or env-scoped R2 binding is present;
- `MEMBER_PRODUCTION_OWNER_APPROVED` remains `false`;
- `public_asset_adapter` remains literal `null` in the Production entry;
- the Production entry explicitly constructs `createMemberPrivateMediaStorageAdapter(env.MEMBER_PRIVATE_MEDIA_BUCKET)`;
- that adapter is passed explicitly as `private_media_storage_adapter`;
- Member Production route remains disabled;
- private-media content route remains disabled;
- LINE Login Production remains unapproved;
- adapter construction itself performs zero R2 access;
- route-OFF causes zero R2 object fetch;
- no Production storage fetch occurs;
- the storage adapter remains explicit-binding, read-only, and get-only.

## Runtime consumption boundary

Source wiring and Production storage fetch remain separate gates.

The approved source-only wiring may construct the adapter and pass it into Member Production request composition, but it must not, by itself:

- enable Member Production routes;
- enable private-media content routes;
- activate LINE Login Production;
- read any R2 object;
- enable Production storage fetch;
- deploy Production or activate a Worker version;
- change Production traffic.

Any transition from source-only wiring to route activation, object fetch, or Production deployment requires a separate fresh Owner authorization at the then-current exact SHA and scope.

## Authorization boundaries

Recording these source-only stages does **not** authorize:

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

No authorization from the bucket-create, read-only verification, source-binding declaration, or source-only runtime-wiring stage carries forward automatically to route activation or Production deployment.

## Source-only runtime wiring stage — 2026-10-08

Owner authorization at baseline main `c2ee3a2d79af72fccea3d38d776d060d516e99f9` permits the source-only runtime-wiring stage only.

The Production entry may construct:

`createMemberPrivateMediaStorageAdapter(env.MEMBER_PRIVATE_MEDIA_BUCKET)`

and pass that read-only adapter explicitly as:

`private_media_storage_adapter`

to Member Production request composition.

This is wiring in source only. Adapter construction must not call R2. Because `MEMBER_PRODUCTION_OWNER_APPROVED` remains `false` and Member route modes remain disabled, this stage must not cause an R2 object fetch.

The following remain invariant:

- `MEMBER_PRODUCTION_OWNER_APPROVED=false`;
- `MEMBER_PRODUCTION_ROUTE_MODE` is not enabled;
- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE` is not enabled;
- LINE Login Production remains unapproved;
- adapter remains read-only and get-only;
- `public_asset_adapter:null` remains unchanged;
- no R2 object read/write/list/delete is executed;
- no Production storage fetch is executed;
- no Production deploy, Worker activation, or traffic change is executed.

Any route activation, Production fetch, deploy, merge to main, or broader storage operation requires a separate fresh Owner authorization.
