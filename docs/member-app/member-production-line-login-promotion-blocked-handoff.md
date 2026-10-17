# Member Production LINE Login promotion blocked handoff

## Purpose

This document records the safe stopping point after the LINE Login promotion readiness work.

The LINE Login candidate is ready for a separately Owner-gated Production promotion boundary, but creation of the executable repository workflow `.github/workflows/member-production-line-login-promotion.yml` was blocked by repository-writing safety control. That block was not bypassed.

This handoff is an immutable source-only audit snapshot. It is valid only for the exact audited Customer CRM main recorded below. If `main` advances for any reason, the packet must fail closed and the drift audit must be refreshed before it can become ready again.

## Exact candidate lineage

- candidate source SHA: `fdfa65fa797c3ee89885ee99244e7a9ad691b124`
- stage caller run: `38233408939`
- staged Worker version: `be711f54-8716-4178-9949-d5ced183d9dc`
- candidate SHA-256: `af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8`
- expected active Worker version: `6d34db85-9a52-45e9-8f7d-094f9fc7957a`
- exact audited current main: `fe9bbcf8c650941b9c0788f633cd4796bbe6cb41`

The staged candidate keeps Member route and LINE Login external exchange enabled while private-media content routing, Favorites writes, MEMORY writes, and FAMILY PASS entitlement writes remain disabled.

## Runtime drift audit

The compare from candidate source SHA `fdfa65fa797c3ee89885ee99244e7a9ad691b124` to exact audited main `fe9bbcf8c650941b9c0788f633cd4796bbe6cb41` contains **exactly five files**.

The three PR #264 readiness files are:

1. `src/member-production-line-login-promotion-readiness.mjs`
2. `member_tests/member-production-line-login-promotion-readiness.test.mjs`
3. `docs/member-app/member-production-line-login-promotion-readiness.md`

Their exact audited Git blob SHAs are pinned:

- `src/member-production-line-login-promotion-readiness.mjs` -> `8be93701a8fb5e3a2e9fd72776a7a2ce698aa140`
- `member_tests/member-production-line-login-promotion-readiness.test.mjs` -> `a26ba0302d9e315e642c747dc8498df6cda4790e`
- `docs/member-app/member-production-line-login-promotion-readiness.md` -> `ec1ed3fdc1fac59f70447eadfc9df7db6a972287`

After PR #267 and PR #268 advanced `main`, the candidate-to-current-main comparison still contains exactly two inherited current-main files:

1. `src/crm-customer-list-daily-operations.mjs`
2. `tests/customer-list-daily-operations.mjs`

PR #268 updated those same inherited paths, so the audited blob evidence was refreshed to exact current main rather than reusing the earlier PR #267 blobs.

Their exact audited Git blob SHAs are:

- `src/crm-customer-list-daily-operations.mjs` -> `06bd614f97bcb94992dcedbe47f48990f0a56b1f`
- `tests/customer-list-daily-operations.mjs` -> `ca1ec71457b8ea5a5507d33ff85d1f686d4261a4`

The Production entrypoint, `wrangler.jsonc`, and executable Production workflows are required to remain unchanged by this candidate-to-audited-main comparison.

Filename allowlisting alone is insufficient. A packet is not ready unless all of the following are true:

- `current_main_sha` is exactly `fe9bbcf8c650941b9c0788f633cd4796bbe6cb41`;
- drift `head_sha` is exactly the same audited main;
- the changed-file evidence is exactly the five paths above, with no missing, duplicate, or extra path;
- readiness blob evidence is explicitly checked and contains exactly the three pinned readiness blobs;
- inherited blob evidence is explicitly checked and contains exactly the two pinned current-main blobs.

The three blocked-handoff source files themselves are **not** part of this pinned candidate-to-audited-main comparison and must not be added to `runtime_drift_evidence.changed_files`. If this PR or any other change advances main, this snapshot becomes stale by design and requires a new audit against that new exact main.

## Blocked-handoff packet

`src/member-production-line-login-promotion-blocked-handoff.mjs` is a pure source-only validator. It performs no network calls and no Production mutation.

It carries immutable candidate constants and an immutable audited-current-main SHA instead of trusting caller-provided current-main identity. The existing promotion-readiness validator remains an additional validation layer, and its returned candidate identity must agree with the local immutable constants.

It requires all of the following before returning `packet_ready=true`:

- the exact immutable candidate identity recorded by this packet;
- promotion-readiness evidence remains `PASS` and returns the same candidate identity;
- `current_main_sha` equals the exact audited main `fe9bbcf8c650941b9c0788f633cd4796bbe6cb41`;
- candidate-source-to-audited-main changed-file evidence equals exactly the three readiness files plus the two inherited current-main files;
- the three readiness paths have the exact pinned Git blob SHAs above;
- the two inherited paths have the exact pinned Git blob SHAs above;
- Production entrypoint drift is explicitly false;
- `wrangler.jsonc` drift is explicitly false;
- runtime workflow drift is explicitly false;
- the executable workflow write block is explicitly recorded;
- the blocked path is exactly `.github/workflows/member-production-line-login-promotion.yml`;
- no bypass was attempted;
- no executable promotion path is claimed to exist.

Any stale/fabricated main SHA, changed-file extra, complete or partial handoff-file extra set, duplicate path, missing path, stale blob, missing blob, extra blob, or unchecked blob evidence fails closed.

Even when the packet is ready, it always returns:

- `authorization_granted=false`;
- `executable_promotion_available=false`;
- empty `approval_text`;
- Production traffic change `0`;
- Production deploy `0`;
- Worker version upload `0`;
- Production D1 read/write `0`;
- R2 object read/write `0`;
- secret change `0`;
- LINE send `0`;
- Customer/Prospect mutation `0`;
- Issue #26 command post `0`.

## Explicit non-circumvention rule

The safety block must not be bypassed through another GitHub API, git object/tree manipulation, an unrelated workflow, an existing workflow with weakened provenance, or a manually posted Issue #26 promotion command.

This handoff does not create or authorize any executable promotion mechanism.

## Next boundary

`REPOSITORY_SAFE_EXECUTABLE_PROMOTION_PATH_REQUIRED`

The next valid step is a repository-safe way to install a separately reviewed, Owner-gated executable promotion path without weakening existing route-only/runtime-secret promotion contracts. Until such a path exists and receives fresh Owner authorization, the candidate remains at zero Production traffic.
