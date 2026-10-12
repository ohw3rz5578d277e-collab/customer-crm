# Member Production LINE Login promotion readiness

## Current candidate

The successful LINE Login candidate stage produced the following exact evidence:

- candidate source main SHA at stage time: `fdfa65fa797c3ee89885ee99244e7a9ad691b124`
- stage caller run: `38233408939`
- staged Worker version: `be711f54-8716-4178-9949-d5ced183d9dc`
- candidate SHA-256: `af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8`
- unchanged active Worker version: `6d34db85-9a52-45e9-8f7d-094f9fc7957a`
- staged candidate Production traffic: `0%`

The candidate has Member route, LINE Login approval, and LINE Login external exchange enabled. Private-media content routing, Favorites writes, MEMORY writes, and FAMILY PASS entitlement writes remain disabled.

## Source-only readiness planner

`src/member-production-line-login-promotion-readiness.mjs` is a pure fail-closed validator. It performs no network calls and no Production mutation.

The exact candidate identity above is pinned in immutable source constants. Caller-provided expectation fields are not treated as authority: each must match the pinned candidate identity before receipt, deployment, or staged-version evidence is evaluated.

It requires exact agreement among:

1. pinned candidate source SHA;
2. pinned staged Worker version ID;
3. pinned candidate SHA-256 digest;
4. pinned LINE Login stage run ID;
5. pinned expected active 100%-traffic Worker version;
6. successful stage receipt markers matching those pinned identifiers;
7. a single-version Production baseline;
8. candidate traffic remaining at zero;
9. candidate LINE Login modes being enabled;
10. all unrelated write/activation modes remaining disabled;
11. resource bindings remaining unchanged.

A syntactically valid, internally consistent but different candidate identity fails closed. Any other mismatch also fails closed.

## Explicit exclusions

This readiness source does not perform or authorize:

- Production traffic promotion;
- `wrangler versions deploy`;
- Production deploy;
- Production D1 read/write;
- R2 object read/write;
- LINE send;
- Customer, Prospect, Customer ID, or Family mutation;
- secret changes;
- private-media route activation;
- Favorites/MEMORY/FAMILY PASS write activation;
- Commerce activation;
- BLACK automatic award;
- paid spend.

## Next boundary

A source-only executable promotion workflow was intentionally not installed by this readiness change. The next boundary remains a separately reviewed, Owner-gated LINE Login promotion implementation and a fresh exact Owner authorization before Production traffic can move from `6d34db85-9a52-45e9-8f7d-094f9fc7957a` to `be711f54-8716-4178-9949-d5ced183d9dc`.

The immutable candidate source SHA is the SHA that produced the staged candidate; a future executable promotion boundary must separately verify the then-current `main` SHA before any traffic mutation.

The repository must continue to fail closed if current main, active Worker version, candidate version, candidate digest, stage lineage, resource bindings, or safety modes drift.
