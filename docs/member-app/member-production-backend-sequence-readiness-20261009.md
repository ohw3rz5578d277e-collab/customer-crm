# Member Production backend sequence readiness gate — 2026-10-09

## Purpose

This source-only gate connects the completed Member backend sequence Steps 1–9 to the existing Member Production readiness model without performing any Production operation.

It exists to prevent an old Step 9 receipt or a CI result from another commit from being reused for a later Production activation candidate.

## Canonical source contracts

- `src/member-production-backend-sequence-readiness.mjs`
- `src/member-production-activation-readiness-assembly.mjs`

The assembly composes the existing `member-production-readiness-preflight.mjs` result with the new backend sequence evidence gate.

## Exact release evidence

Backend sequence readiness requires all of the following as explicit evidence:

1. `release_sha` is an exact lowercase 40-character Git SHA.
2. `step9_verified_sha` is an exact lowercase 40-character Git SHA.
3. `release_sha === step9_verified_sha`.
4. Step 9 final gate passed on that exact SHA.
5. Node major is exactly `22` as an integer, not a coerced string.
6. Exact-head checkout was verified.
7. The workflow exact-head contract was verified.
8. The all-Member-tests workflow contract was verified.
9. Steps 1–8 representative matrix was verified.
10. Lifecycle cross-contract security was verified.
11. Production-default-off was verified.

Missing, malformed, stale, mismatched, or type-coerced evidence fails closed.

## Production activation assembly

`buildMemberProductionActivationReadinessAssembly(...)` requires both:

- the pre-existing Member runtime readiness gate; and
- the exact-SHA backend sequence readiness gate.

It reports `technical_activation_ready=true` only when both are ready.

Technical readiness does **not** authorize activation. The assembly always keeps:

- `activation_allowed=false`;
- `production_activation_authorized=false`;
- `owner_authorization_required=true`.

A separate fresh explicit Owner authorization for the exact current-main SHA is required after technical readiness is established.

## Non-authorization boundary

This stage does not authorize or execute:

- Production deploy or Worker activation;
- Member route activation;
- Production D1 read/write/delete;
- migration/schema apply;
- Customer / Family / Member / Prospect mutation;
- CRM write;
- LINE send;
- Google/GAS network send;
- R2 access or private-media fetch;
- secret/token mutation;
- security-policy change;
- commerce activation;
- BLACK or MEMORY write/backfill;
- Customer ID or Family ID generation;
- paid spend.

## Next operational stage

A later Owner-gated read-only workflow may collect exact-current-main evidence and feed it to this assembly. Merely merging this source gate does not authorize that dispatch and does not activate Production.
