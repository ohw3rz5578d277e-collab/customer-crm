# Member Production staged-version promotion gate

## Purpose

This source-only change adds the permanent Owner-gated path for promoting the already staged immutable Worker version:

- staging source SHA: `41059eb0ca192f29f790abfd4563552581b1a6b8`
- staging run: `36285531724` — SUCCESS
- staging job: `108525472623` — SUCCESS
- staged Worker version: `6dd49589-f01d-473f-876a-034563023b0e`
- durable receipt: `release/member/member-production-runtime-secret-stage-36285531724.json`
- release-gate issue: `#26`

Merging this source does **not** authorize or perform Production promotion.

## Fresh Owner authorization required after merge

The only promotion entrypoint is a new exact comment by `ohw3rz5578d277e-collab` on issue #26:

```text
/member-production-promote sha=<FRESH_CURRENT_MAIN_40_SHA> staged_version=6dd49589-f01d-473f-876a-034563023b0e staging_run=36285531724 confirm=PROMOTE_MEMBER_STAGED_VERSION
```

There is no manual `workflow_dispatch` trigger on the promotion workflow. The issue-comment bridge validates the command on its first run attempt and calls the promotion workflow as a reusable workflow. Re-running the bridge is rejected.

The Owner comment must remain unedited and no more than 15 minutes old both when the promotion starts and immediately before the traffic mutation.

## Fail-closed gates

Before any traffic mutation the workflow must prove all of the following:

1. checkout SHA equals the freshly Owner-authorized current `main`;
2. staging source SHA is an ancestor of that current `main`;
3. invocation is the exact first-attempt Issue #26 bridge run, triggered by the Owner account;
4. Owner authorization comment is exact, belongs to issue #26, is unedited, and is no more than 15 minutes old;
5. the durable source receipt proves staging run `36285531724` / job `108525472623` completed SUCCESS at source SHA `41059eb0ca192f29f790abfd4563552581b1a6b8` and produced staged version `6dd49589-f01d-473f-876a-034563023b0e`;
6. the immutable staged version still exists in Cloudflare and contains the required Member secret bindings;
7. a fresh active Production deployment snapshot is captured;
8. the exact staged version is not already active;
9. the workflow shares the exact `customer-crm-production-deploy` concurrency group with the canonical Production deployment workflow, so the two traffic mutations cannot overlap;
10. non-serialized Member Production operations are absent;
11. a second active deployment snapshot immediately before mutation is equal to the first snapshot;
12. immediately before mutation, current `main`, Owner-comment freshness and non-serialized operation state are checked again.

Any mismatch stops the run before promotion.

## Durable staging receipt

The promotion path does not depend on retained GitHub Actions job logs. The exact staging evidence already verified from run `36285531724` is stored as a version-controlled receipt. The promotion workflow validates every security-relevant receipt field and then independently verifies the immutable staged Worker version directly against Cloudflare.

## Only mutation in the promotion workflow

After all gates pass, the only intended traffic mutation is:

```text
npx wrangler versions deploy 6dd49589-f01d-473f-876a-034563023b0e@100% --name customer-crm-api --yes
```

The workflow does not use `wrangler deploy`, D1 mutation commands, secret mutation commands, CRM write APIs, LINE send APIs, Customer ID generation, commerce activation, or paid services.

## Current authorization boundary

The current Owner authorization covers source changes, PR creation, CI, review, and source-only fixes to review findings.

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
