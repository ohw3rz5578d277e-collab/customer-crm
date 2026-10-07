# Member lifecycle stack source-readiness gate — 2026-10-07

## Scope

This gate covers PRs #203 through #211 only. It is a source-review/merge-order gate, not a Production release authorization.

## Exact reviewed heads

| Order | PR | Exact HEAD |
|---|---:|---|
| 1 | #203 | 8f7aedce9846f92143ac965a3a96bce7975a7ce9 |
| 2 | #204 | 82b43879837802ef46f742b1927dea989ee0ef69 |
| 3 | #205 | a6315e4c818a9e30864163d4348ae755819b028e |
| 4 | #206 | 841cf2e0b9c669c7a9749fd01d21daf5ea75a093 |
| 5 | #207 | d45c97cc650c70551cc4aec58d14bc15f821aa13 |
| 6 | #208 | 784d99df5ba42eba9c7e3f774f0c46ea9f574ab1 |
| 7 | #209 | 1d2c76dff0705adf0239787ca683a4d62cebccbc |
| 8 | #210 | 8c2cdabbb44717c7bf0fe79cefd2ca1d310c1b28 |
| 9 | #211 | f510eca28b184c8b166cf13fd3618d3b3357b805 |

## Stack drift found by fresh audit

Some downstream PRs were originally branched before later Codex-review fixes landed in their immediate parent PR.

This is not accepted as proof that the final combined tree is ready.

The merge process must therefore:

1. merge only in the order above;
2. require the exact reviewed HEAD for each PR;
3. refresh the next PR after every parent merge;
4. verify that the next PR contains the just-merged parent fixes or otherwise rebase/update it before proceeding;
5. rerun/fresh-check review threads and applicable CI after every update;
6. stop on any HEAD drift, conflict, unresolved review, failed required check, or unexpected changed file.

No bulk merge is permitted.

## Source-ready is not Production-ready

Passing this gate means only that the source stack is eligible for sequential merge review.

It does not authorize:

- Production deploy
- Production D1 read/write
- migration apply
- Google/GAS live access
- Google Customer Master mutation
- CRM mutation
- Customer ID generation/update/delete/merge
- LINE send
- R2 access/binding
- route activation
- secret change
- security-policy change
- UI activation/change
- BLACK/MEMORY write
- signup-benefit redemption or commerce enforcement
- paid spend

A separate post-merge exact-main audit and Production-readiness process is required.
