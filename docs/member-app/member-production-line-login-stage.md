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
6. re-checks current `main` immediately before the only mutation;
7. uploads exactly one immutable Worker version with `wrangler versions upload`;
8. verifies the active Production deployment is byte-for-byte unchanged and the staged version receives 0% traffic;
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

## Current fail-closed bridge state

The Issue #26 command shape is reserved as:

`/member-production-line-login-stage sha=<exact-main-sha> route_stage_run=<run-id> route_promotion_run=<run-id> active_version=<uuid> confirm=STAGE_MEMBER_LINE_LOGIN_CANDIDATE`

The repository currently contains a **validation-only fail-closed bridge**. It validates the exact Owner command and current-main match, then stops with `MEMBER_LINE_LOGIN_STAGE_DISPATCH_WIRING_NOT_AUTHORIZED`.

It has no `actions: write` permission and cannot dispatch the stage workflow. Wiring that bridge to actual `workflow_dispatch` is a separate Owner approval boundary.

## Promotion remains separate

Even after a successful candidate stage, Production traffic remains on the prior active route-only version. The final stage receipt ends at:

`NEXT_BOUNDARY=FRESH_OWNER_LINE_LOGIN_PROMOTION_AUTHORIZATION_REQUIRED`

A fresh, separately reviewed promotion implementation and separate Owner authorization are required before the staged LINE Login candidate can receive Production traffic.
