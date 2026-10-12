# Member Production LINE Login candidate stage

## Purpose

This stage prepares exactly one immutable Worker version for Member LINE Login without changing Production traffic.

The candidate is built from the exact current `main` and the audited `member-production-line-login-stage-plan.mjs` planner. It is allowed to change only the LINE Login activation surface required for the next Member authentication step:

- ephemeral `MEMBER_PRODUCTION_OWNER_APPROVED=true`;
- ephemeral trusted composition `line_login_approved=true`;
- `MEMBER_PRODUCTION_ROUTE_MODE=enabled`;
- `MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE=enabled`.

The following remain disabled:

- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE`;
- Favorites mutation/rate-limit/write modes;
- MEMORY write mode;
- FAMILY PASS entitlement write mode.

The canonical source and `wrangler.jsonc` remain default-off in the repository.

## Required lineage

A future authorized stage must provide:

- exact current main SHA;
- exact successful route-only candidate stage run ID;
- exact successful route-only Production promotion run ID;
- exact currently active route-only Worker version UUID at 100% traffic;
- fresh unedited Owner authorization comment from release-gate Issue #26;
- first-attempt Issue #26 bridge run ID;
- exact post-bridge reusable caller run ID;
- confirmation token `STAGE_MEMBER_LINE_LOGIN_CANDIDATE`.

The stage verifies that the route-stage receipt created the exact currently active Worker version while LINE Login external exchange was disabled, and that the route-promotion receipt promoted that same version to 100% traffic.

## Stage behavior

After all gates pass, the workflow:

1. requires `caller_run_id` to equal the current GitHub run ID;
2. verifies the caller is the exact first-attempt `workflow_run` for `.github/workflows/dispatch-member-production-line-login-stage-after-bridge.yml` on the authorized SHA;
3. verifies the canonical post-bridge validation job already completed successfully in that same run;
4. re-checks exact current `main`;
5. verifies the active Production deployment is exactly the Owner-authorized route-only version at 100%;
6. builds the LINE Login candidate using the audited planner;
7. verifies resource bindings are unchanged;
8. performs a local Wrangler dry-run bundle;
9. re-checks the fresh unedited Owner authorization and current `main` immediately before the only mutation;
10. uploads exactly one immutable Worker version with `wrangler versions upload`;
11. verifies the active Production deployment is unchanged and the staged version receives 0% traffic;
12. removes all ephemeral candidate files.

The stage does not use `wrangler deploy` or `wrangler versions deploy`.

## Explicitly excluded operations

The stage does not authorize or perform:

- Production traffic promotion/change;
- Production deploy;
- D1 read/write or migration apply;
- R2 object read/write or bucket mutation;
- secret mutation or value display;
- Customer / Prospect / Customer ID mutation;
- Family mutation;
- LINE send;
- private-media content route activation;
- Favorites/MEMORY/FAMILY PASS writes;
- Commerce activation;
- BLACK automatic award;
- paid spend.

## Issue #26 command and reusable bridge design

The reserved Owner command is:

`/member-production-line-login-stage sha=<exact-main-sha> route_stage_run=<run-id> route_promotion_run=<run-id> active_version=<uuid> confirm=STAGE_MEMBER_LINE_LOGIN_CANDIDATE`

The architecture separates Owner-command validation from the Production stage and uses the repository's canonical reusable-workflow pattern instead of a REST workflow dispatch.

### Stage A — Issue #26 bridge

`.github/workflows/dispatch-member-production-line-login-stage-from-issue.yml`:

1. accepts only Issue #26 comments from the Owner account;
2. requires first attempt only;
3. validates the exact command shape and exact current `main`;
4. re-fetches the Owner comment and requires it to be unedited and no more than 900 seconds old;
5. rejects when another canonical Production mutation is queued or running;
6. writes a bounded receipt containing only IDs, exact SHA, active-version UUID, confirmation and a SHA-256 digest of the exact command;
7. uploads that receipt as a one-day artifact;
8. performs no Production mutation and does not call the stage directly.

The bridge has no `actions: write` permission.

### Stage B — completed-bridge validator and reusable caller

`.github/workflows/dispatch-member-production-line-login-stage-after-bridge.yml` is triggered only by `workflow_run` completion of Stage A.

Its `validate` job requires:

- exact source workflow path;
- `status=completed`;
- `conclusion=success`;
- `event=issue_comment`;
- `run_attempt=1`;
- Owner actor;
- exact bridge display title;
- bridge `head_sha` still equal to current `main`;
- exact source-run artifact receipt;
- exact receipt schema and IDs;
- exact Owner comment body and SHA-256 digest;
- the Owner comment still unedited and within the 900-second freshness window;
- no conflicting canonical Production mutation before the next boundary.

Only after `validate` succeeds, the workflow calls:

`uses: ./.github/workflows/member-production-line-login-stage.yml`

with the exact validated receipt-bound inputs and `secrets: inherit`.

No `actions: write` permission and no GitHub REST `/dispatches` POST are used.

### Stage C — reusable LINE Login candidate stage

`.github/workflows/member-production-line-login-stage.yml` keeps the pull-request contract entrypoint but uses `workflow_call` as its only Production execution entrypoint. It has no manual `workflow_dispatch` trigger.

Before any Worker upload, the called stage verifies:

- `caller_run_id == GITHUB_RUN_ID`;
- the current run is `event=workflow_run`;
- the run path is exactly `.github/workflows/dispatch-member-production-line-login-stage-after-bridge.yml`;
- the run `head_sha` equals the authorized exact `main`;
- `run_attempt=1`;
- the run display title is bound to the exact source bridge run;
- exactly one canonical `Revalidate successful bridge receipt before stage dispatch` job has already completed successfully;
- the Issue #26 bridge remains a completed successful first-attempt Owner run;
- the Owner comment is still fresh and unedited;
- the route-stage and route-promotion lineage remains exact.

This removes the direct/manual stage dispatch surface while preserving the completed-bridge, freshness, lineage and Production mutation exclusion gates.

## Shared Production mutation exclusion

The actual LINE Login candidate stage uses the canonical `customer-crm-production-deploy` concurrency group, shared with canonical deploy and the existing Member Production mutations. The Issue #26 bridge and post-bridge validator also perform explicit reject-and-retry checks against registered peer mutation workflows before issuing or consuming the dispatch receipt.

This provides two layers:

1. fail-closed busy checks before the reusable stage boundary;
2. canonical execution-layer mutual exclusion if two workflows are scheduled close together.

## Promotion remains separate

Even after a successful candidate stage, Production traffic remains on the prior active route-only version. The final stage receipt ends at:

`NEXT_BOUNDARY=FRESH_OWNER_LINE_LOGIN_PROMOTION_AUTHORIZATION_REQUIRED`

A fresh, separately reviewed promotion implementation and separate Owner authorization are required before the staged LINE Login candidate can receive Production traffic.

## Authorization boundary

Merging source wiring does not itself authorize a Production stage. Actual execution still requires a fresh exact Owner command on Issue #26 after the executable source is merged and the current main SHA is freshly verified.
