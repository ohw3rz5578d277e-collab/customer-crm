# MIZUNO PHOTO MEMBER — Production Storage Read-Only Preflight

Baseline: 2026-10-07 JST

## Purpose

This gate verifies the Cloudflare account R2 inventory and an explicit Member Production storage candidate without reading or mutating R2 objects.

The canonical private-media binding is declared in source as exactly:

`MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`

The Production source now also explicitly wraps `env?.MEMBER_PRIVATE_MEDIA_BUCKET` with `createMemberPrivateMediaStorageAdapter(...)` and passes the resulting get-only adapter as `private_media_storage_adapter`.

The preflight does not select a different bucket automatically, does not change bindings, does not execute runtime storage fetch, and does not create, read from, write to, list objects in, or delete any R2 object.

## Authentication split

The preflight deliberately separates two Cloudflare credentials:

- `CLOUDFLARE_API_TOKEN` remains the existing Worker-management credential used only for Wrangler authentication and read-only active Worker version snapshots.
- `CLOUDFLARE_R2_READ_API_TOKEN` is reserved for Cloudflare REST API token verification and R2 bucket inventory only.

The R2 credential must be a **Cloudflare REST API bearer token** with least-privilege `Workers R2 Storage Read` permission for the intended account. It is not an R2 S3-compatible `Access Key ID` or `Secret Access Key`. R2 S3 credentials use AWS Signature Version 4 and are not valid substitutes for the `Authorization: Bearer <token>` contract used by `GET /user/tokens/verify` and `GET /accounts/{account_id}/r2/buckets`.

Before any bucket request, the workflow calls Cloudflare's read-only `GET /user/tokens/verify` endpoint with the dedicated token and requires an active token. It emits only PASS or sanitized HTTP/numeric Cloudflare error codes; token IDs, token values, response messages, and response bodies are not printed.

The workflow does not fall back to the Worker-management token for R2 verification or inventory.

This repository change does **not** create, rotate, stage, or modify either secret. If `CLOUDFLARE_R2_READ_API_TOKEN` is absent, contains whitespace or control characters, is not accepted as a Cloudflare REST bearer token, or is not active, the preflight fails closed before bucket inventory.

## Why this exists

The Production source remains operationally default-off:

- `MEMBER_PRODUCTION_OWNER_APPROVED=false`;
- Member Production route mode is not enabled;
- private-media content route mode is not enabled;
- LINE Login Production remains unactivated;
- `public_asset_adapter` remains `null`;
- the private-media adapter is source-wired only from the canonical binding;
- adapter construction performs no object request;
- Production storage fetch remains off.

The repository needs durable evidence that:

1. the inspected main SHA is still current;
2. the Member Production route and private-media route remain disabled;
3. `wrangler.jsonc` contains exactly one canonical R2 binding, `MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`;
4. no additional, wrong, or env-scoped R2 binding has appeared;
5. the Production entry contains exactly the reviewed source-only private-media adapter wiring and does not hardcode the bucket name;
6. Worker-management authentication can read the current Production deployment state;
7. the dedicated R2 credential is an active Cloudflare REST API bearer token;
8. that least-privilege token can perform account-level bucket inventory for the **Owner-authorized jurisdiction subset only**;
9. no unapproved jurisdiction is silently added or normalized by the workflow;
10. the active 100%-traffic Worker version remains stable during the observation;
11. a candidate bucket, represented only by its SHA-256 name digest in GitHub metadata, exists exactly once across the authorized jurisdiction inventories.

## Explicit jurisdiction authorization

Supported jurisdiction values remain:

- `default`
- `eu`
- `us`
- `fedramp`
- `fedramp-high`

The preflight does not assume that every account can access all five. Each execution must include an explicit `jurisdictions=` argument.

The value is an **exact lowercase** comma-separated subset in canonical order:

`default,eu,us,fedramp,fedramp-high`

Examples:

- standard non-FedRAMP scope: `jurisdictions=default,eu,us`
- FedRAMP-only scope: `jurisdictions=fedramp,fedramp-high`
- one jurisdiction only: `jurisdictions=default`

There is intentionally no shorthand such as `all`, `standard`, `auto`, or an omitted/default scope. Unknown values, uppercase/mixed-case spellings, duplicates, whitespace, leading/trailing commas, empty comma-separated entries, or non-canonical ordering fail closed before any Cloudflare bucket request. The workflow does not lowercase or otherwise normalize the submitted jurisdiction string before authorization evidence is recorded.

The workflow queries only the exact explicit subset. Each request is an account-level `GET /accounts/{account_id}/r2/buckets` with the matching `cf-r2-jurisdiction` header and the dedicated R2 REST read token.

The workflow fails closed if any authorized jurisdiction request does not succeed or if a response requires pagination beyond the single complete page supported by this gate.

On a non-200 response, the response body is not printed. Only numeric Cloudflare error codes are extracted for diagnostics. Bucket names and Cloudflare error messages are not emitted by that failure path.

Temporary response files are removed on success and failure via an EXIT cleanup trap.

## Modes

### `inventory`

Command shape:

`/member-production-storage-preflight sha=<40hex> mode=inventory jurisdictions=<canonical-comma-separated-subset>`

Example for the currently intended standard scope:

`/member-production-storage-preflight sha=<40hex> mode=inventory jurisdictions=default,eu,us`

The workflow first verifies the dedicated REST API token, then retrieves only account-level bucket metadata for the exact authorized jurisdictions.

On success it outputs:

- the exact authorized/scanned jurisdiction list;
- combined bucket count across those jurisdictions;
- SHA-256 digest of sorted `jurisdiction:bucket-name` inventory entries;
- current active 100%-traffic Worker version;
- exact inspected main SHA.

Bucket names are not printed by inventory mode.

### `verify`

GitHub must never receive the raw candidate bucket name. The Owner computes the SHA-256 of the exact bucket name locally and supplies only the lowercase 64-hex digest.

Command shape:

`/member-production-storage-preflight sha=<40hex> mode=verify jurisdictions=<canonical-comma-separated-subset> candidate_bucket_sha256=<64hex>`

The workflow hashes each observed bucket name internally and requires the supplied digest to match exactly one bucket across the authorized jurisdictions. It outputs only the digest plus non-secret bucket metadata such as observed jurisdiction, reported jurisdiction, location, and storage class.

The raw candidate bucket name is not written to Issue #26, workflow input, logs, outputs, or summary by this contract.

This is existence evidence only. It does not authorize storage fetch or route activation.

## Drift protection

The workflow fails closed if:

- main no longer equals the authorized SHA;
- the active 100%-traffic Worker version changes during the observation;
- the dedicated R2 read secret is absent;
- the dedicated R2 secret contains whitespace or control characters or is not accepted as an active Cloudflare REST bearer token;
- `jurisdictions` is missing or contains whitespace;
- jurisdiction casing is not exact lowercase canonical form;
- the jurisdiction string contains a leading/trailing comma or an empty entry;
- a jurisdiction value is unsupported;
- the same jurisdiction is repeated;
- jurisdictions are not listed in canonical order;
- any authorized jurisdiction R2 list response is unsuccessful;
- any authorized jurisdiction list is paginated beyond the single complete page expected by this gate;
- verify mode does not find the candidate digest;
- the candidate digest is ambiguous across observed jurisdictions;
- canonical Member route flags are unexpectedly enabled;
- the R2 binding count is not exactly one;
- the binding name or bucket name differs from the canonical source declaration;
- unexpected binding configuration or any env-scoped R2 binding is present;
- `public_asset_adapter` is no longer literal `null`;
- the private-media adapter factory import or exact canonical source wiring is missing;
- the private-media adapter regresses to literal `null`;
- the Production entry hardcodes `customer-crm-member-private-media`;
- the Production entry references `MEMBER_PRIVATE_MEDIA_BUCKET` more or less than once.

The active Worker version and main SHA postflight check runs with `always()` after a successful pre-inventory snapshot, so an R2 token verification or inventory failure cannot suppress the drift evidence.

## Privacy and mutation boundary

The preflight performs:

- raw candidate bucket name in GitHub metadata = 0;
- token value / token ID logging = 0;
- source-only private-media runtime wiring = observed/validated only;
- R2 object read = 0;
- R2 object write = 0;
- bucket create/delete/update = 0;
- Production storage binding change = 0;
- Production storage fetch = 0;
- Member route activation = 0;
- private-media route activation = 0;
- LINE Login activation = 0;
- Production deploy = 0;
- Worker activation = 0;
- Production traffic change = 0;
- Production D1 write = 0;
- CRM write = 0;
- LINE send = 0;
- Customer ID generation = 0;
- secret creation/change = 0;
- commerce activation = 0;
- paid spend = 0.

## Failure evidence from 2026-10-06

A read-only inventory using the prior all-jurisdiction contract successfully passed dedicated R2 token verification and reached `fedramp`, where Cloudflare returned HTTP 403 with sanitized numeric code `10003`. The run then failed closed. The active Production Worker version and current main SHA remained stable, and no object access or mutation occurred.

This evidence is why jurisdiction selection is explicit rather than automatically expanding to all five values.

## Next gate

A successful token verification, inventory, or candidate verification is not authorization to execute `storage_adapter.get(...)`, enable Production storage fetch, activate Member/private-media routes, activate LINE Login Production, deploy Production, activate a Worker version, or change Production traffic.

Any future R2 inventory run must use a fresh Owner authorization that names the exact current main SHA and exact jurisdiction subset.

Any additional or different R2 binding source change, Production storage fetch, route activation, Production deployment, Worker activation, or Production traffic change requires separate fresh Owner exact-SHA/scope authorization.
