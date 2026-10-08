# Member backend Step 9 final integration/security gate

Status: source/test/docs-only. Not a Production activation approval.

Canonical baseline at branch creation:

`91ca85446f1d94d132a38fbef8774f6e3d734df3`

This refresh is based on canonical main after PR #234 (`fix(member): harden step8 server FAMILY PASS read evidence`).

## Purpose

Step 9 closes the source-level Member backend sequence by proving, on one exact PR HEAD under Node 22, that the representative security/regression contracts for Steps 1 through 8 still pass together and that Production activation remains default-off.

This gate does not add runtime behavior. It does not deploy, apply schema, read or write Production data, call Google/GAS/LINE, access Production R2 objects, award BLACK, or mutate Customer/Family/Member/Prospect state.

## Exact Node contract

`.github/workflows/member-app-foundation.yml` already:

- checks out `${{ github.event.pull_request.head.sha }}`;
- uses `actions/setup-node@v5` with `node-version: '22'`;
- runs every `member_tests/*.test.mjs` file on that exact PR HEAD.

The Step 9 final-gate test additionally asserts that `process.versions.node` has major version 22 before running its matrix.

## Final gate matrix

| Backend step | Representative test | Contract proved |
| --- | --- | --- |
| 1 | `member-exact-identity-binding-plan.test.mjs` | exact Member / Prospect / CRM Customer / Family identity binding |
| 2 | `member-invitation-redemption-plan.test.mjs` | one-time hashed invitation validation and atomic redemption plan |
| 3 | `member-prospect-registration-consent-plan.test.mjs` | Prospect registration plus append-only consent plan |
| 3 | `member-prospect-registration-consent-event-schema.test.mjs` | append-only registration / consent event schema contract |
| 4 | `member-profile-review-queue-plan.test.mjs` | review queue before any Master update |
| 5 | `member-google-profile-sync-plan.test.mjs` | server-side Google profile sync planning |
| 5 | `member-google-sync-delivery-plan.test.mjs` | idempotent bounded delivery / retry / reconciliation planning |
| 6 | `member-d1-pii-retention-plan.test.mjs` | 7-day cache freshness and 30-day absolute PII hard-retention contract |
| 6 | `member-profile-cache-read-gate.test.mjs` | cache read fail-closed at TTL / hard-retention boundaries |
| 7 | `member-prospect-customer-promotion-plan.test.mjs` | exact Prospect -> canonical CRM Customer promotion planning with continuity |
| 8 | `member-family-pass-integration-read-model.test.mjs` | exact Family-scoped MEMORY count and durable BLACK integration read evidence |
| 8 | `member-family-pass-black-entitlement.test.mjs` | server-side FAMILY PASS exact-Family MEMORY count evidence and durable BLACK read hardening introduced by PR #234 |
| 9 | `member-lifecycle-cross-contract-security.test.mjs` | cross-stage identity, isolation, write-block and lifecycle security regression |
| 9 | `member-production-entry-default-off-manifest.test.mjs` | Production entry/activation remains default-off |

`member-runtime-step9-final-gate.test.mjs` executes every file above with the same Node executable. Any missing contract, non-zero exit, spawn error, signal termination, or Node-major mismatch fails the final gate.

The ordinary Member regression job also continues to run the complete `member_tests/*.test.mjs` suite; Step 9 does not replace or narrow that suite.

## Step 8 refresh after PR #234

PR #234 hardened the existing server-side `member-family-pass-read-model.mjs` path after the original Step 9 branch had already been created. The refreshed final gate therefore explicitly includes `member-family-pass-black-entitlement.test.mjs` in addition to the Step 8 integration read-model test.

This preserves explicit final-gate coverage for:

- exact Family-scoped published/non-deleted MEMORY count evidence;
- non-coerced non-negative safe-integer MEMORY counts;
- exact Family durable BLACK evidence;
- canonical millisecond UTC `black_achieved_at`;
- numeric `black_lifetime=1`;
- qualifying MEMORY count >=10;
- exact `achievement_source='published-member-memories'`;
- malformed evidence failing closed;
- no automatic BLACK award or discount enforcement.

## Security invariants

The final gate is intended to preserve these source-level invariants across Steps 1-8:

- Customer CRM remains the only canonical Customer ID authority.
- no fuzzy identity matching or silent Customer merge;
- Prospect remains isolated from another Customer/Family private state;
- invite redemption remains one-time and server-record bound;
- consent evidence remains append-only;
- profile mismatch routes to review rather than direct Master overwrite;
- Google delivery remains server-side, idempotent and separately gated;
- D1 profile PII obeys 7-day cache freshness and 30-day absolute hard purge deadline;
- Prospect promotion preserves the stable Member identity and append-only history;
- MEMORY count and durable BLACK reads remain exact Family-scoped;
- malformed Step 8 runtime read evidence fails closed;
- automatic BLACK award and shooting-fee discount remain unauthorized;
- Production activation remains default-off.

## Non-authorization

Passing or merging Step 9 does **not** authorize any Production action.

It does not authorize:

- Production deploy or Worker activation;
- Production D1 read/write/delete;
- migration/schema apply;
- actual invitation redemption, registration, promotion, BLACK award, MEMORY write, or purge execution;
- Customer / Family / Member / Prospect mutation;
- Customer ID or Family ID generation/update/delete/merge;
- Google/GAS network operation or Customer/Prospect Master mutation;
- LINE send;
- R2 object read/write/list/delete or private-media access;
- secret/token changes, route activation or security-policy changes;
- Commerce activation, discount enforcement, BLACK automatic award, or paid spend.

Every Production gate remains separately Owner-authorized with a fresh exact SHA and its own preflight/rollback/health requirements.
