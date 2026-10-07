# MIZUNO PHOTO MEMBER — R2 Bucket Digest Index

Baseline: 2026-10-07 JST

## Purpose

This is a read-only diagnostic gate for reconciling a locally derived candidate bucket-name SHA-256 with the actual R2 bucket inventory without exposing raw bucket names to GitHub.

The canonical source binding is now declared as exactly:

`MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`

This gate does not select a bucket automatically, approve runtime consumption, change a binding, fetch an object, activate a route, or deploy Production.

## Evidence model

For the exact Owner-authorized jurisdiction subset, the workflow performs only account-level R2 bucket metadata List GET operations. It hashes each observed raw bucket name in memory and emits only:

- one SHA-256 digest per observed bucket;
- the observed jurisdiction for each digest;
- total bucket count;
- the existing combined inventory SHA-256 over sorted `jurisdiction:bucket-name` entries;
- exact current main SHA;
- active Production Worker version before/after drift evidence.

Raw bucket names are never intentionally printed to logs, summaries, Issue comments, PR comments, or chat by this contract. Temporary Cloudflare response files are deleted with an EXIT cleanup trap.

The digest index is intended for local digest reconciliation only. A local script may hash plausible tokens from the Owner's Cloudflare UI selection and compare those local hashes with this index. GitHub still never receives the raw local selection or raw bucket name.

## Owner-gated command

The only Issue #26 command shape is:

`/member-production-r2-bucket-digest-index sha=<40hex> jurisdictions=<canonical-comma-separated-subset>`

Example standard scope:

`/member-production-r2-bucket-digest-index sha=<40hex> jurisdictions=default,eu,us`

Supported jurisdiction values remain:

- `default`
- `eu`
- `us`
- `fedramp`
- `fedramp-high`

The subset must be exact lowercase canonical order. Unknown values, duplicates, whitespace, mixed case, and non-canonical ordering fail closed before an R2 bucket request.

## Authentication and drift boundaries

The workflow:

1. checks out the exact approved current main SHA;
2. requires current main to equal that SHA;
3. requires Member runtime storage consumption to remain default-off;
4. requires exactly one canonical source binding, `MEMBER_PRIVATE_MEDIA_BUCKET -> customer-crm-member-private-media`;
5. rejects additional, wrong, unexpected, or env-scoped R2 bindings;
6. requires `public_asset_adapter` and `private_media_storage_adapter` to remain literal `null` and Member/private-media routes to remain disabled;
7. uses the existing Worker-management credential only for read-only Worker authentication and active deployment snapshots;
8. verifies `CLOUDFLARE_R2_READ_API_TOKEN` with read-only `GET /user/tokens/verify` without relying on token-prefix classification;
9. rejects an empty token and tokens containing whitespace or control characters before verification;
10. performs R2 account-level bucket metadata List GET only for the exact Owner-authorized jurisdictions;
11. fails closed on pagination, HTTP/API failure, malformed inventory, main drift, or active Production Worker version drift.

## Privacy and mutation boundary

This gate authorizes or performs none of the following by itself:

- raw bucket name logging;
- R2 object read;
- R2 object write/delete;
- bucket create/delete/update;
- Production storage binding change;
- Production storage fetch;
- Member Production route activation;
- private-media route activation;
- LINE Login Production activation;
- Production deploy;
- Worker activation;
- Production traffic change;
- Production D1 read/write;
- migration apply;
- CRM write;
- LINE send;
- Customer ID generation, update, delete, or merge;
- secret/token creation, change, deletion, or revoke;
- security policy change;
- commerce activation;
- paid spend.

## Relationship to candidate verify

A digest-index run is not a substitute for candidate verification. Its purpose is to identify which locally generated SHA-256, if any, corresponds to an actual observed R2 bucket without disclosing names.

After a local candidate digest matches exactly one digest-index entry, the existing `mode=verify` storage preflight can be requested separately to prove exact-one-match and return sanitized bucket metadata.

Any digest-index execution requires its own fresh Owner authorization naming the exact current main SHA and exact jurisdiction subset.

Any subsequent candidate verify requires another fresh Owner authorization.

The existing source-only binding declaration does not authorize runtime use. Any additional or different source binding modification, Production storage fetch, runtime adapter wiring, Member/private-media route activation, LINE Login Production activation, Production deploy, Worker activation, or traffic change remains outside this gate and requires separate fresh Owner authorization at the appropriate exact SHA and scope.
