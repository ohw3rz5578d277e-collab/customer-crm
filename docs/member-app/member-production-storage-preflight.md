# MIZUNO PHOTO MEMBER — Production Storage Read-Only Preflight

Baseline: 2026-10-05 JST

## Purpose

This gate discovers whether the Cloudflare account has R2 bucket resources that could later be reviewed as an explicit Member Production storage source.

It does not select a bucket automatically and it does not create, bind, read from, write to, or delete any R2 object.

## Authentication split

The preflight deliberately separates two Cloudflare credentials:

- `CLOUDFLARE_API_TOKEN` remains the existing Worker-management credential used only for Wrangler authentication and read-only active Worker version snapshots.
- `CLOUDFLARE_R2_READ_API_TOKEN` is reserved for R2 bucket inventory only.

The R2 token must be least-privilege and limited to Cloudflare's `Workers R2 Storage Read` permission for the intended account. The workflow does not fall back to the Worker-management token for R2 inventory.

This repository change does **not** create, rotate, stage, or modify either secret. Until `CLOUDFLARE_R2_READ_API_TOKEN` is separately provisioned under fresh Owner authorization, the inventory step fails closed with `CLOUDFLARE_R2_READ_API_TOKEN_MISSING`.

## Why this exists

The canonical Member Production contract intentionally did not invent a bucket name, binding name, or storage provider. The Production entry is still default-off and currently passes `null` for both public-asset and private-media adapters.

Before any future binding change, the repository needs durable evidence that:

1. the inspected main SHA is still current;
2. the Member Production route and private-media route remain disabled;
3. no canonical `r2_buckets` binding has silently appeared in `wrangler.jsonc`;
4. Worker-management authentication can read the current Production deployment state;
5. a dedicated least-privilege R2 read token can perform account-level bucket inventory;
6. every documented R2 jurisdiction is inspected;
7. the active 100%-traffic Worker version remains stable during the observation;
8. a candidate bucket, represented only by its SHA-256 name digest in GitHub metadata, exists exactly once across the observed jurisdiction inventories.

## Jurisdiction-complete inventory

The preflight queries each supported List Buckets jurisdiction independently:

- `default`
- `eu`
- `us`
- `fedramp`
- `fedramp-high`

Each request is an account-level `GET /accounts/{account_id}/r2/buckets` with the matching `cf-r2-jurisdiction` header and the dedicated R2 read token.

The workflow fails closed if any jurisdiction request does not succeed or if a response requires pagination beyond the single complete page supported by this gate.

On a non-200 response, the response body is not printed. Only numeric Cloudflare error codes are extracted for diagnostics. Bucket names and Cloudflare error messages are not emitted by that failure path.

## Modes

### `inventory`

Command shape:

`/member-production-storage-preflight sha=<40hex> mode=inventory`

The workflow retrieves only account-level bucket metadata.

On success it outputs:

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

The raw candidate bucket name is not written to Issue #26, workflow input, logs, outputs, or summary by this contract.

This is existence evidence only. It does not mean the candidate is approved for Member data.

## Drift protection

The workflow fails closed if:

- main no longer equals the authorized SHA;
- the active 100%-traffic Worker version changes during the observation;
- the dedicated R2 read secret is absent;
- any jurisdiction R2 list response is unsuccessful;
- any jurisdiction list is paginated beyond the single complete page expected by this gate;
- verify mode does not find the candidate digest;
- the candidate digest is ambiguous across observed jurisdictions;
- canonical Member route flags are unexpectedly enabled;
- a canonical R2 binding is already declared without review.

The active Worker version and main SHA postflight check runs with `always()` after a successful pre-inventory snapshot, so an R2 inventory failure cannot suppress the drift evidence.

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

Before retrying inventory, `CLOUDFLARE_R2_READ_API_TOKEN` must be separately created/staged with only `Workers R2 Storage Read` permission under fresh Owner authorization. Secret values must never be printed or pasted into Issue #26.

Any future R2 binding change must use a separately reviewed exact bucket and binding name, remain default-off first, and receive fresh Owner exact-SHA/scope authorization before any Production deployment or route activation.
