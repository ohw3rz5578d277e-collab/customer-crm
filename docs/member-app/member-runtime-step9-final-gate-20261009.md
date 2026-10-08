# Member backend Step 9 final integration/security gate

Status: source/test/docs-only. Not a Production activation approval.

Canonical baseline at branch creation:

`91ca85446f1d94d132a38fbef8774f6e3d734df3`

## Purpose

Step 9 closes the source-level Member backend sequence by proving, on one exact PR HEAD under Node 22, that representative security/regression contracts for Steps 1 through 8 pass together and that Production activation remains default-off.

This gate adds no runtime behavior. It does not deploy, apply schema, read or write Production data, call Google/GAS/LINE, access Production R2 objects, award BLACK, write MEMORY, or mutate Customer/Family/Member/Prospect state.

## Exact HEAD / Node 22 contract

The existing `.github/workflows/member-app-foundation.yml` is itself part of the Step 9 evidence. The final-gate test statically verifies that the workflow:

- is the `Member app foundation` workflow;
- checks out `${{ github.event.pull_request.head.sha }}`;
- uses `persist-credentials: false`;
- configures Node major `22`;
- discovers every `member_tests/*.test.mjs` file;
- executes each discovered test with Node.

The Step 9 final-gate test also asserts that its own `process.versions.node` major version is exactly 22. It therefore fails closed if the CI runtime or exact-HEAD/full-suite workflow contract drifts.

## Final gate matrix

| Backend step | Representative test | Contract proved |
| --- | --- | --- |
| 1 | `member-exact-identity-binding-plan.test.mjs` | exact Member / Prospect / CRM Customer / Family identity binding |
| 2 | `member-invitation-redemption-plan.test.mjs` | one-time hashed invitation validation and atomic redemption planning |
| 3 | `member-prospect-registration-consent-plan.test.mjs` | Prospect registration plus append-only consent planning |
| 3 | `member-prospect-registration-consent-event-schema.test.mjs` | append-only registration / consent event schema contract |
| 4 | `member-profile-review-queue-plan.test.mjs` | review queue before any Master update |
| 5 | `member-google-profile-sync-plan.test.mjs` | server-side Google profile sync planning |
| 5 | `member-google-sync-delivery-plan.test.mjs` | idempotent bounded delivery / retry / reconciliation planning |
| 5 | `member-google-server-adapter.test.mjs` | server-only authenticated Google sync transport boundary |
| 6 | `member-d1-pii-retention-plan.test.mjs` | 7-day cache freshness and 30-day absolute PII hard-retention contract |
| 6 | `member-profile-cache-read-gate.test.mjs` | cache read fail-closed at TTL / hard-retention boundaries |
| 7 | `member-prospect-customer-promotion-plan.test.mjs` | exact Prospect -> canonical CRM Customer promotion planning with continuity |
| 8 | `member-family-pass-integration-read-model.test.mjs` | exact Family-scoped MEMORY count and durable BLACK lifecycle integration evidence |
| 8 | `member-family-pass-black-entitlement.test.mjs` | server-side Family MEMORY count and durable BLACK read evidence, including the Step 8 runtime-read hardening merged in PR #234 |
| 9 | `member-lifecycle-cross-contract-security.test.mjs` | cross-stage identity, isolation, write-block and lifecycle security regression |
| 9 | `member-production-entry-default-off-manifest.test.mjs` | Production entry/activation remains default-off |

`member-runtime-step9-final-gate.test.mjs` executes every representative file above using the same `process.execPath` as the final gate. A missing file, spawn error, signal termination, non-zero exit, duplicate matrix entry, missing Step 1-8 coverage, or Node-major mismatch fails the gate.

The ordinary Member regression job continues to execute the complete `member_tests/*.test.mjs` suite on the exact PR HEAD. Step 9 does not replace or narrow the complete regression suite.

## Security invariants

The final gate preserves these source-level invariants across Steps 1-8:

- Customer CRM remains the sole canonical Customer ID authority;
- no fuzzy identity matching or silent Customer merge;
- Prospect remains isolated from another Customer/Family private state;
- invitation redemption remains one-time and server-record bound;
- consent evidence remains append-only;
- profile mismatch routes to Review Queue rather than direct Master overwrite;
- Google delivery remains server-side, authenticated, idempotent, bounded, and separately gated;
- D1 profile PII obeys 7-day cache freshness and a 30-day absolute hard purge deadline;
- Prospect promotion preserves stable Member identity and append-only history;
- MEMORY count and durable BLACK evidence remain exact Family-scoped in both lifecycle integration and server-side FAMILY PASS read paths;
- BLACK threshold remains exactly 10 qualifying MEMORIES;
- BLACK is lifetime only when durable evidence is valid;
- BLACK commercial benefit remains PHOTO GOODS 10% OFF FOREVER, not a shooting-fee discount;
- automatic BLACK award/enforcement remains unauthorized;
- Production activation remains default-off.

## Non-authorization

Passing or merging Step 9 does **not** authorize any Production action.

It does not authorize:

- Production deploy or Worker activation;
- Production D1 read/write/delete;
- migration/schema apply;
- actual invitation redemption, registration, promotion, profile sync, BLACK award, MEMORY write, cache refresh, or purge execution;
- Customer / Family / Member / Prospect mutation;
- Customer ID or Family ID generation/update/delete/merge;
- Google/GAS network operation or Customer/Prospect Master mutation;
- LINE send;
- R2 object read/write/list/delete or private-media access;
- secret/token changes, route activation or security-policy changes;
- Commerce activation, discount enforcement, BLACK automatic award, or paid spend;
- UI changes.

Every Production gate remains separately Owner-authorized with a fresh exact SHA and its own preflight, rollback, and health requirements.
