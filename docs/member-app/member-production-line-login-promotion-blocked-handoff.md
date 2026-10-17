# Member Production LINE Login promotion blocked handoff

## Purpose

This document records the safe stopping point after the LINE Login promotion readiness work.

The LINE Login candidate is ready for a separately Owner-gated Production promotion boundary, but creation of the executable repository workflow `.github/workflows/member-production-line-login-promotion.yml` was blocked by repository-writing safety control. That block was not bypassed.

## Exact candidate lineage

- candidate source SHA: `fdfa65fa797c3ee89885ee99244e7a9ad691b124`
- stage caller run: `38233408939`
- staged Worker version: `be711f54-8716-4178-9949-d5ced183d9dc`
- candidate SHA-256: `af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8`
- expected active Worker version: `6d34db85-9a52-45e9-8f7d-094f9fc7957a`
- current audited main for this replacement handoff: `7e10f06f4cf3c7d6fe823682344237956afcf183`

The staged candidate keeps Member route and LINE Login external exchange enabled while private-media content routing, Favorites writes, MEMORY writes, and FAMILY PASS entitlement writes remain disabled.

## Runtime drift audit

The compare from candidate source SHA `fdfa65fa797c3ee89885ee99244e7a9ad691b124` to current audited main `7e10f06f4cf3c7d6fe823682344237956afcf183` contains exactly five files.

The three PR #264 readiness files are:

1. `src/member-production-line-login-promotion-readiness.mjs`
2. `member_tests/member-production-line-login-promotion-readiness.test.mjs`
3. `docs/member-app/member-production-line-login-promotion-readiness.md`

Their exact audited Git blob SHAs are pinned:

- `src/member-production-line-login-promotion-readiness.mjs` -> `8be93701a8fb5e3a2e9fd72776a7a2ce698aa140`
- `member_tests/member-production-line-login-promotion-readiness.test.mjs` -> `a26ba0302d9e315e642c747dc8498df6cda4790e`
- `docs/member-app/member-production-line-login-promotion-readiness.md` -> `ec1ed3fdc1fac59f70447eadfc9df7db6a972287`

After PR #267 advanced `main`, the same candidate-to-current-main comparison also contains exactly two inherited current-main files:

1. `src/crm-customer-list-daily-operations.mjs`
2. `tests/customer-list-daily-operations.mjs`

Those inherited files are not silently ignored. They are explicitly audited and pinned to exact Git blob SHAs:

- `src/crm-customer-list-daily-operations.mjs` -> `39f8abcb8671f8dd2d2f33c6c63e60228a29c992`
- `tests/customer-list-daily-operations.mjs` -> `e853fc46f8c2fd4ff84726eba99f82942ffe9b68`

The Production entrypoint, `wrangler.jsonc`, and executable Production workflows are still required to remain unchanged by the candidate-to-current-main audit.

Filename allowlisting alone is insufficient. A packet is not ready unless read-only drift evidence states that blob evidence was checked, provides exactly the pinned readiness paths and inherited paths, and every path has the exact audited blob SHA above.

A merged blocked-handoff change would itself add three additional source-only handoff files:

1. `src/member-production-line-login-promotion-blocked-handoff.mjs`
2. `member_tests/member-production-line-login-promotion-blocked-handoff.test.mjs`
3. `docs/member-app/member-production-line-login-promotion-blocked-handoff.md`

The validator therefore requires all three readiness files and both audited inherited files, and permits either none or all three of the blocked-handoff files. Any other changed file, any partial blocked-handoff set, any duplicate changed-file evidence, any missing required drift file, or any readiness/inherited blob mismatch fails closed.

## Blocked-handoff packet

`src/member-production-line-login-promotion-blocked-handoff.mjs` is a pure source-only validator. It performs no network calls and no Production mutation.

It carries its own immutable candidate constants instead of treating imported readiness constants as authority. The existing promotion-readiness validator remains an additional validation layer, and its returned identity must agree with those local immutable constants.

It requires all of the following before returning `packet_ready=true`:

- the exact immutable candidate identity recorded by this packet;
- promotion-readiness evidence remains `PASS` and returns the same candidate identity;
- current main is syntactically valid and separately tracked from the candidate source SHA;
- candidate-source-to-current-main changed-file evidence contains all three readiness files;
- candidate-source-to-current-main changed-file evidence contains both audited inherited PR #267 files;
- the three readiness paths have the exact pinned Git blob SHAs above;
- the two inherited paths have the exact pinned Git blob SHAs above;
- changed files are restricted to the immutable audited allowlist above plus the complete optional blocked-handoff set;
- the blocked-handoff file set is either absent or complete;
- Production entrypoint drift is explicitly false;
- `wrangler.jsonc` drift is explicitly false;
- runtime workflow drift is explicitly false;
- the executable workflow write block is explicitly recorded;
- the blocked path is exactly `.github/workflows/member-production-line-login-promotion.yml`;
- no bypass was attempted;
- no executable promotion path is claimed to exist.

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
