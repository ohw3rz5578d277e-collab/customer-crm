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

Production promotion uses two Owner actions on issue #26.

First, obtain a fresh **read-only** active-version snapshot:

```text
/member-production-promotion-snapshot sha=<FRESH_CURRENT_MAIN_40_SHA>
```

That command performs no traffic mutation and reports the exact 100%-traffic `ACTIVE_PRODUCTION_VERSION_ID`.

Then create a new promotion authorization that explicitly names the active version being replaced:

```text
/member-production-promote sha=<FRESH_CURRENT_MAIN_40_SHA> staged_version=6dd49589-f01d-473f-876a-034563023b0e staging_run=36285531724 replace_active_version=<FRESH_ACTIVE_PRODUCTION_VERSION_ID> confirm=PROMOTE_MEMBER_STAGED_VERSION
```

There is no manual `workflow_dispatch` trigger on the promotion workflow. The issue-comment bridge validates the command on its first run attempt and calls the promotion workflow as a reusable workflow. Re-running the bridge is rejected.

The Owner comment must remain unedited and no more than 15 minutes old both when the promotion starts and immediately before the traffic mutation.

## Fail-closed gates

Before any traffic mutation the workflow must prove all of the following:

1. checkout SHA equals the freshly Owner-authorized current `main`;
2. staging source SHA is an ancestor of that current `main`;
3. invocation is the exact first-attempt Issue #26 bridge run, triggered by the Owner account;
4. Owner authorization comment is exact, belongs to issue #26, is unedited, and is no more than 15 minutes old;
5. the checked-in receipt is only supporting evidence; while GitHub still retains run `36285531724`, its live run metadata must independently match SUCCESS, exact source SHA, exact workflow and the exact recorded time window;
6. Cloudflare's immutable staged-version metadata must independently prove the exact version ID, creation inside the staging run window, the exact source-SHA staging message, and the required Member secret bindings;
7. the staging-source workflow at `41059eb0...` must contain the exact version-secret staging provenance contract;
8. the active 100%-traffic Production version must exactly equal the `replace_active_version` named by the fresh Owner authorization;
9. the exact staged version must not already be active;
10. the promotion workflow, canonical Production deployment, Member Production schema apply, and Member Production runtime secret stage share the exact `customer-crm-production-deploy` concurrency group for real Production executions, so those mutation windows cannot overlap;
11. non-serialized Member Production operations are absent;
12. a second active deployment snapshot immediately before mutation must be unchanged and must still equal the Owner-authorized active version;
13. immediately before mutation, current `main`, Owner-comment freshness, the active 100%-traffic version, and non-serialized operation state are checked again.

Any mismatch stops the run before promotion.

## Durable staging receipt

The promotion path does not depend on retained GitHub Actions job logs. The version-controlled receipt is **supporting evidence, not a trust anchor by itself**. While GitHub retains the staging run, live run metadata is revalidated. Independently of log retention, the immutable Cloudflare version must report a creation timestamp inside the exact staging run window and the source-SHA staging message produced by the staging workflow, and its required bindings must still be present.

## Only mutation in the promotion workflow

After all gates pass, the only intended traffic mutation is:

```text
npx wrangler versions deploy 6dd49589-f01d-473f-876a-034563023b0e@100% --name customer-crm-api --yes
```

The workflow does not use `wrangler deploy`, D1 mutation commands, secret mutation commands, CRM write APIs, LINE send APIs, Customer ID generation, commerce activation, or paid services.

After the promotion command, the workflow parses the fresh active deployment and requires exactly one entry for the staged Worker version at `percentage === 100`. It also requires that this staged version is the only 100%-traffic version before declaring promotion success.

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


## Sequential rollback protection

The shared concurrency lock prevents overlapping Production traffic mutations. In addition, the Owner must explicitly authorize replacing one exact currently-active Worker version. If any canonical Production deploy changes the 100%-traffic version after the snapshot or before the promotion acquires or uses the lock, the promotion fails closed with an active-version mismatch. A newer completed Production deployment is therefore never replaced implicitly.


## Shared Production mutation lock

For real Production executions, these workflows share the same GitHub Actions concurrency group `customer-crm-production-deploy`:

- canonical Cloudflare Production deploy;
- Member staged-version Production promotion;
- Member Production schema apply;
- Member Production runtime secret stage.

Their pull-request contract jobs keep PR-specific concurrency groups. Schema and runtime-stage mutation workflows therefore queue behind the same Production lock instead of relying on a point-in-time status poll before promotion.
