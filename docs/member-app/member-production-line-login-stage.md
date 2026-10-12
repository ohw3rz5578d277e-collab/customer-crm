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
- confirmation token `STAGE_MEMBER_LINE_LOGIN_CANDIDATE`.

The stage verifies that the route-stage receipt created the exact currently active Worker version while LINE Login external exchange was disabled, and that the route-promotion receipt promoted that same version to 100% traffic.

## Stage behavior

After all gates pass, the workflow:

1. re-checks exact current `main`;
2. verifies the active Production deployment is exactly the Owner-authorized route-only version at 100%;
3. builds the LINE Login candidate using the audited planner;
4. verifies resource bindings are unchanged;
5. performs a local Wrangler dry-run bundle;
6. re-checks the fresh unedited Owner authorization and current `main` immediately before the only mutation;
7. uploads exactly one immutable Worker version with `wrangler versions upload`;
8. verifies the active Production deployment is unchanged and the staged version receives 0% traffic;
9. removes all ephemeral candidate files.

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

## Issue #26 command and two-stage bridge design

The reserved Owner command is:

`/member-production-line-login-stage sha=<exact-main-sha> route_stage_run=<run-id> route_promotion_run=<run-id> active_version=<uuid> confirm=STAGE_MEMBER_LINE_LOGIN_CANDIDATE`

The bridge architecture deliberately separates Owner-command validation from stage dispatch so the stage can require a **completed + successful first-attempt bridge run** as provenance.

### Stage A — Issue #26 bridge

`.github/workflows/dispatch-member-production-line-login-stage-from-issue.yml`:

1. accepts only Issue #26 comments from the Owner account;
2. requires first attempt only;
3. validates the exact command shape and exact current `main`;
4. re-fetches the Owner comment and requires it to be unedited and no more than 900 seconds old;
5. rejects when another canonical Production mutation is queued or running;
6. writes a bounded receipt containing only IDs, exact SHA, active-version UUID, confirmation and a SHA-256 digest of the exact command;
7. uploads that receipt as a one-day artifact;
8. performs no Production mutation and does not directly dispatch the stage.

The bridge itself has no `actions: write` permission.

### Stage B — completed-bridge validator

`.github/workflows/dispatch-member-production-line-login-stage-after-bridge.yml` is triggered only by `workflow_run` completion of Stage A.

It requires:

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

This two-stage shape preserves the stage workflow's existing provenance requirement instead of weakening it to accept an in-progress bridge.

## Current executable-dispatch status

The post-bridge validator currently **fails closed before the actual `member-production-line-login-stage.yml` workflow dispatch** with:

`MEMBER_LINE_LOGIN_STAGE_POST_BRIDGE_EXECUTABLE_DISPATCH_NOT_INSTALLED`

This is intentional in the current source-only work-in-progress because the repository-writing tool rejected creation of the executable dispatch step. The source does not weaken any existing stage provenance check to work around that rejection.

Therefore, even if this work-in-progress branch were mistakenly merged, the Issue #26 command could not reach `wrangler versions upload`: the post-bridge workflow ends with `LINE_LOGIN_STAGE_WORKFLOW_DISPATCH=0` and a failing conclusion.

The remaining source step is to install the audited post-bridge stage dispatch while preserving all completed-bridge, freshness, current-main and mutation-busy gates. That source change still requires review before merge.

## Shared Production mutation exclusion

The actual LINE Login candidate stage uses the canonical `customer-crm-production-deploy` concurrency group, shared with canonical deploy and the existing Member Production mutations. The Issue #26 bridge and post-bridge validator also perform explicit reject-and-retry checks against the currently registered peer mutation workflows before issuing or consuming a dispatch receipt.

This provides two layers:

1. fail-closed busy checks before the dispatch boundary;
2. canonical execution-layer mutual exclusion if two workflows are scheduled close together.

## Promotion remains separate

Even after a successful candidate stage, Production traffic remains on the prior active route-only version. The final stage receipt ends at:

`NEXT_BOUNDARY=FRESH_OWNER_LINE_LOGIN_PROMOTION_AUTHORIZATION_REQUIRED`

A fresh, separately reviewed promotion implementation and separate Owner authorization are required before the staged LINE Login candidate can receive Production traffic.
