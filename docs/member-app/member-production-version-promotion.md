# Member Production staged-version promotion gate

## Purpose

This source-only change adds the permanent Owner-gated path for promoting the already staged immutable Worker version:

- staging source SHA: `41059eb0ca192f29f790abfd4563552581b1a6b8`
- staging run: `36285531724` (SUCCESS)
- staged Worker version: `6dd49589-f01d-473f-876a-034563023b0e`
- release-gate issue: `#26`

Merging this source does **not** authorize or perform Production promotion.

## Fresh Owner authorization required after merge

The promotion bridge accepts only an exact new comment by `ohw3rz5578d277e-collab` on issue #26:

```text
/member-production-promote sha=<FRESH_CURRENT_MAIN_40_SHA> staged_version=6dd49589-f01d-473f-876a-034563023b0e staging_run=36285531724 confirm=PROMOTE_MEMBER_STAGED_VERSION
```

The SHA must equal current `main` at comment-processing time. The promotion workflow independently re-checks current `main`, the Owner comment, the exact staging run receipt, and the exact staged version.

## Fail-closed gates

Before any traffic mutation the workflow must prove all of the following:

1. checkout SHA equals the freshly Owner-authorized current `main`;
2. staging source SHA is an ancestor of that current `main`;
3. Owner authorization comment is exact, belongs to issue #26, was authored by the Owner account, is unedited, and is no more than 15 minutes old;
4. staging run `36285531724` is completed/SUCCESS, is a `workflow_dispatch` run of `member-production-runtime-secret-stage.yml`, and has head `41059eb0ca192f29f790abfd4563552581b1a6b8`;
5. the successful staging job log proves `MEMBER_RUNTIME_SECRET_STAGED_VERSION_ID=6dd49589-f01d-473f-876a-034563023b0e`;
6. the immutable staged version still exists in Cloudflare;
7. a fresh active Production deployment snapshot is captured;
8. the exact staged version is not already active;
9. conflicting Production workflows are not queued or running;
10. a second active deployment snapshot immediately before mutation is byte-semantically equal to the first snapshot.

Any mismatch stops the run before promotion.

## Only mutation in the promotion workflow

After all gates pass, the only intended traffic mutation is:

```text
npx wrangler versions deploy 6dd49589-f01d-473f-876a-034563023b0e@100% --name customer-crm-api --yes
```

The workflow does not use `wrangler deploy`, D1 mutation commands, CRM write APIs, LINE send APIs, Customer ID generation, commerce activation, or paid services.

## Current authorization boundary

The current Owner authorization covers source changes, PR creation, CI, and review only.

It does **not** authorize:
- Production promotion or traffic change;
- `wrangler versions deploy` execution;
- `wrangler deploy` / Production Worker deploy;
- Member route activation;
- `MEMBER_PRODUCTION_OWNER_APPROVED` activation;
- `MEMBER_PRODUCTION_ROUTE_MODE` enablement;
- Member session Production activation;
- LINE Login Production activation;
- Production D1 writes;
- CRM writes;
- LINE sends;
- Customer ID generation;
- commerce activation;
- paid spend.
