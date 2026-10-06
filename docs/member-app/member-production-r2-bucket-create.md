# Member Production R2 bucket create gate

Baseline: 2026-10-07 JST

## Purpose

Provide one exact-SHA, one-bucket mutation gate for creating the first dedicated Member private-media R2 bucket after a separate explicit Owner authorization.

Canonical target:

- bucket name: `customer-crm-member-private-media`
- bucket-name SHA-256: `6f1f7fa25143081a302fcd148a52ae660195b2a31cf93a55f9ed39cf2e25f388`
- jurisdiction: `default`
- storage class: `Standard`
- public access: disabled by default; this gate does not enable public access
- custom domain: none
- public development URL: none
- Worker binding: none

## Authorization model

Merging this source does not authorize a Production mutation.

A future create run requires all of the following at the same time:

1. exact current `main` SHA;
2. exact canonical bucket-name SHA-256;
3. jurisdiction exactly `default`;
4. exact Owner receipt `AUTHORIZE_MEMBER_R2_BUCKET_CREATE:<main-sha>:<bucket-sha256>:default`;
5. Issue #26 command posted by repository Owner `ohw3rz5578d277e-collab`;
6. Member storage remains default-off and has no R2 binding in default or any `env.*` Wrangler scope.

The canonical Issue #26 command shape is:

`/member-production-r2-bucket-create sha=<40hex> bucket_sha256=6f1f7fa25143081a302fcd148a52ae660195b2a31cf93a55f9ed39cf2e25f388 jurisdiction=default owner_ack=AUTHORIZE_MEMBER_R2_BUCKET_CREATE:<40hex>:6f1f7fa25143081a302fcd148a52ae660195b2a31cf93a55f9ed39cf2e25f388:default`

## Mutation boundary

The authorized runtime is limited to:

1. bucket-metadata List GET in jurisdiction `default` to prove the canonical digest is absent;
2. exactly one `POST /accounts/{account_id}/r2/buckets` request;
3. bucket-metadata List GET in jurisdiction `default` to prove the canonical digest now matches exactly one bucket;
4. read-only active Production Worker version snapshots before and after;
5. read-only current-main checks before and after.

The create request contains only the canonical bucket name and `storageClass=Standard`, with `cf-r2-jurisdiction: default`.

## Explicitly not authorized

This gate does not authorize:

- creation of any second bucket;
- bucket rename, patch, delete, lifecycle, CORS, custom-domain, public-domain, or public-development-URL changes;
- R2 object read, write, list, multipart upload, or delete;
- reuse or mutation of existing album, manga, or Instagram buckets;
- `wrangler.jsonc` R2 binding changes;
- Production storage fetch;
- Member Production route activation;
- private-media route activation;
- LINE Login Production activation;
- Cloudflare token creation, rotation, permission change, or GitHub secret change;
- Production deploy, Worker activation, or traffic change;
- Production D1 write or migration;
- CRM write;
- LINE send;
- Customer ID generation;
- security policy change;
- commerce activation;
- paid spend.

## Fail closed and no automatic rollback

Before the POST, stop without mutation if the canonical digest already exists, if the exact main SHA drifted, if Member/default-off source state changed, if R2 metadata is malformed or paginated, or if Cloudflare authentication fails.

After the POST, any verification failure is treated as an indeterminate create result. The workflow must not delete, recreate, patch, rename, or retry the bucket automatically. A fresh read-only inventory/digest check and fresh Owner authorization are required before any subsequent mutation attempt.

The workflow must never print Cloudflare token values or raw Cloudflare response bodies.

## After successful creation

A successful create does not authorize binding or application access. The next stages remain separate:

1. fresh read-only digest index under separate exact-SHA Owner authorization;
2. exact-one-match candidate verification;
3. separate source PR for `MEMBER_PRIVATE_MEDIA_BUCKET` binding;
4. separate Production deploy authorization;
5. separate Production storage-fetch and route-activation authorization.
