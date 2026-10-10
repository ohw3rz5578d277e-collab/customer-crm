# Member Production route-stage version promotion gate

## Purpose

This gate promotes exactly one immutable Worker version created by the canonical Member Production route candidate stage.

The promotion path is no longer bound to the historical 2026-09-27 runtime-secret staged version. Instead, every candidate must be identified by a fresh exact pair:

- `staged_version=<exact immutable Worker version UUID>`
- `staging_run=<exact successful member-production-route-stage.yml run ID>`

The route-stage run, staged version, current `main`, and currently active Production version are all independently revalidated before any traffic mutation.

Merging this source does **not** authorize or perform a Production promotion.

## Required sequence

A future route promotion has three separately gated stages.

1. A successful exact-current-main `member-production-route-stage.yml` run creates an immutable route-only candidate at **0% Production traffic**.
2. A fresh read-only promotion snapshot binds that exact candidate/run to the exact currently active 100%-traffic Production version.
3. A separate fresh Owner promotion authorization may promote that exact candidate to 100%.

The route-stage itself explicitly leaves:

- LINE Login external exchange disabled;
- private-media content route disabled;
- Favorites writes disabled;
- MEMORY writes disabled;
- FAMILY PASS / BLACK entitlement writes disabled.

Only the Member route boundary candidate is staged.

## Read-only promotion snapshot

After a successful route-stage run, issue #26 may be used for a fresh read-only snapshot:

```text
/member-production-promotion-snapshot sha=<FRESH_CURRENT_MAIN_40_SHA> staged_version=<ROUTE_STAGED_VERSION_UUID> staging_run=<SUCCESSFUL_ROUTE_STAGE_RUN_ID>
```

The bridge first verifies that `staging_run` is:

- completed;
- successful;
- `workflow_dispatch`;
- first attempt;
- from `.github/workflows/member-production-route-stage.yml`;
- for the exact supplied current-main SHA;
- titled `Member route candidate stage <SHA>`.

It then reads the current 100%-traffic `customer-crm-api` Worker version and emits a promotion command template.

The snapshot performs:

- Production traffic change: `0`;
- Production deploy: `0`;
- Production D1 write: `0`;
- R2 object read/write: `0`.

## Fresh Owner promotion authorization

The promotion command is dynamic and must bind all four identities exactly:

```text
/member-production-promote sha=<FRESH_CURRENT_MAIN_40_SHA> staged_version=<ROUTE_STAGED_VERSION_UUID> staging_run=<SUCCESSFUL_ROUTE_STAGE_RUN_ID> replace_active_version=<FRESH_ACTIVE_PRODUCTION_VERSION_UUID> confirm=PROMOTE_MEMBER_STAGED_VERSION
```

There is no manual `workflow_dispatch` trigger on the promotion workflow. The Issue #26 bridge is first-attempt only and calls the reusable promotion workflow after validating the exact command.

The Owner comment must be unedited and no more than 15 minutes old at the initial gate and again immediately before traffic mutation.

## Route-stage lineage authentication

Before promotion, the workflow authenticates the candidate through independent evidence.

### GitHub run receipt

The exact `staging_run` must still identify a successful first-attempt route-stage workflow for the exact current-main SHA.

### Cloudflare immutable version

`wrangler versions view <staged_version>` must prove:

- the exact staged version ID exists;
- its creation timestamp falls inside the authenticated route-stage run window, with only a small clock/API tolerance;
- its version message equals:

```text
Owner-gated Member route-only stage sha=<SHA> run=<STAGING_RUN_ID>
```

The staged version must expose the expected binding/mode names, including:

- `MEMBER_PRODUCTION_ROUTE_MODE`
- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE`
- `MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE`
- `MEMBER_SESSION_SECRET`
- `MEMBER_LINE_LOGIN_TRANSACTION_SECRET`
- `MEMBER_LINE_LOGIN_CHANNEL_SECRET`
- `MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET`
- `MEMBER_PRIVATE_MEDIA_BUCKET`
- `DB`
- `LINE_SERVICE`
- `RESERVATION_SERVICE`

No secret value is read or printed.

### Exact route-stage source provenance

The promotion workflow re-reads `.github/workflows/member-production-route-stage.yml` from the exact Owner-authorized SHA and requires the deterministic stage contract:

- ephemeral Owner approval patch only;
- Member route mode staged `enabled`;
- private-media content route staged `disabled`;
- LINE external exchange staged `disabled`;
- candidate created by `wrangler versions upload`;
- staged candidate verified to receive no Production traffic;
- route-stage stops at `FRESH_OWNER_ROUTE_PROMOTION_AUTHORIZATION_REQUIRED`.

## Active Production replacement protection

The exact active 100%-traffic Worker version named in `replace_active_version` must match:

1. the first fresh Production snapshot;
2. the immediate pre-mutation snapshot;
3. the final gate immediately before promotion.

Any canonical deploy or other change that alters the active version makes the authorization stale and the workflow fails closed. A newer Production deployment can never be replaced implicitly.

## Only intended traffic mutation

After every gate passes, the promotion workflow contains exactly one intended traffic mutation:

```text
npx wrangler versions deploy "$STAGED_VERSION_ID@100%" --name customer-crm-api --yes
```

The workflow does not use:

- `wrangler deploy`;
- D1 mutation commands;
- R2 mutation/object commands;
- secret mutation commands;
- CRM writes;
- LINE sends;
- Customer / Prospect mutation;
- Customer ID generation;
- Commerce activation;
- BLACK automatic entitlement writes.

Post-promotion verification requires exactly one 100%-traffic Production version, and it must be the exact staged version authorized by the Owner.

## Shared Production mutation lock

Real Production mutations share `customer-crm-production-deploy` concurrency with:

- canonical CRM Production deploy;
- Member staged-version promotion;
- Member Production schema apply;
- Member runtime secret stage;
- Member route candidate stage.

Authorization bridges use reject-and-retry checks rather than serving as a queue. A conflicting active or queued Production mutation causes fail-closed behavior and requires a fresh Owner authorization later.

Read-only canonical preflights and explicit read-only snapshot bridges remain outside the mutation slot.

## Authorization boundary

This source-only gate does **not** itself authorize:

- route-stage version upload;
- Production version promotion / traffic change;
- CRM Production deploy/redeploy;
- Member session activation;
- LINE Login activation;
- private-media content route activation;
- R2 create/delete/upload/write/binding change;
- Production D1 write or migration apply;
- Customer / Prospect / Customer ID mutation;
- secret creation/change/value display;
- security policy change;
- Commerce activation;
- BLACK automatic award;
- paid spend.

Every future Production stage requires its own fresh exact Owner authorization.
