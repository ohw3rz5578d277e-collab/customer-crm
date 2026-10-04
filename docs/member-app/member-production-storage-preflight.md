# MIZUNO PHOTO MEMBER — Production Storage Read-Only Preflight

Baseline: 2026-10-04 JST

## Purpose

This gate discovers whether the Cloudflare account has R2 bucket resources that could later be reviewed as an explicit Member Production storage source.

It does not select a bucket automatically and it does not create, bind, read from, write to, or delete any R2 object.

## Why this exists

The canonical Member Production contract intentionally did not invent a bucket name, binding name, or storage provider. The Production entry is still default-off and currently passes `null` for both public-asset and private-media adapters.

Before any future binding change, the repository needs durable evidence that:

1. the inspected main SHA is still current;
2. the Member Production route and private-media route remain disabled;
3. no canonical `r2_buckets` binding has silently appeared in `wrangler.jsonc`;
4. Cloudflare authentication can perform account-level read-only inventory;
5. every documented R2 jurisdiction is inspected;
6. the active 100%-traffic Worker version remains stable during the observation;
7. a candidate bucket, represented only by its SHA-256 name digest in GitHub metadata, exists exactly once across the observed jurisdiction inventories.

## Jurisdiction-complete inventory

Cloudflare R2 bucket listing is jurisdiction-scoped. The preflight therefore queries each documented jurisdiction independently:

- `default`
- `eu`
- `us`
- `fedramp`
- `fedramp-high`

Each request is an account-level `GET /accounts/{account_id}/r2/buckets` with the matching `cf-r2-jurisdiction` header.

The workflow fails closed if any jurisdiction request does not succeed or if a response requires pagination beyond the single complete page supported by this gate.

## Modes

### `inventory`

Command shape:

`/member-production-storage-preflight sha=<40hex> mode=inventory`

The workflow retrieves only account-level bucket metadata.

It outputs:

- combined bucket count across all scanned jurisdictions;
- SHA-256 digest of sorted `jurisdiction:bucket-name` inventory entries;
- current active 100%-traffic Worker version;
- exact inspected main SHA.

Bucket names are not printed by inventory mode.

### `verify`

GitHub must never receive the raw candidate bucket name. The Owner computes the SHA-256 of the exact bucket name locally and supplies only the lowercase 64-hex digest.

Command shape:

`/member-production-storage-preflight sha=<40hex> mode=verify candidate_bucket_sha256=<64hex>`

The workflow hashes each observed bucket name internally and requires the supplied digest to match exactly one bucket across all scanned jurisdictions. It outputs only the digest plus non-secret bucket metadata such as observed jurisdiction, reported jurisdiction, location, and storage class.

The raw candidate bucket name is not written to the Issue #26 command, workflow input, logs, outputs, or summary by this contract.

This is existence evidence only. It does not mean the candidate is approved for Member data.

## Drift protection

The workflow fails closed if:

- main no longer equals the authorized SHA;
- the active 100%-traffic Worker version changes during the inventory;
- any jurisdiction R2 list response is unsuccessful;
- any jurisdiction list is paginated beyond the single complete page expected by this gate;
- verify mode does not find the candidate digest;
- the candidate digest is ambiguous across observed jurisdictions;
- canonical Member route flags are unexpectedly enabled;
- a canonical R2 binding is already declared without review.

## Privacy and mutation boundary

The preflight performs:

- raw candidate bucket name in GitHub metadata = 0;
- R2 object read = 0;
- R2 object write = 0;
- bucket create/delete/update = 0;
- Production storage binding change = 0;
- Production storage fetch = 0;
- Member route activation = 0;
- private-media route activation = 0;
- LINE Login activation = 0;
- Production deploy = 0;
- Production traffic change = 0;
- Production D1 write = 0;
- CRM write = 0;
- LINE send = 0;
- Customer ID generation = 0;
- secret creation/change = 0;
- commerce activation = 0;
- paid spend = 0.

## Next gate

A successful inventory or candidate verification is not authorization to edit `wrangler.jsonc` or activate Member routes.

Any future R2 binding change must use a separately reviewed exact bucket and binding name, remain default-off first, and receive fresh Owner exact-SHA/scope authorization before any Production deployment or route activation.
