# Clean Member lifecycle integration candidate

This current-main reconstruction is based on canonical main exact SHA `90b83b8126b0851e3728d904b875347e19191f49` after PR #214.

Its reviewed lifecycle source candidate is PR #213 exact HEAD `1de2de91d4378df134acc1bee0afadf78dcca671`. PR #213 itself was previously reconstructed from PR #202 HEAD `9d282bc2c1906379f33b7d0d8d28604c26cc3881`; that value is retained as historical construction provenance, not as the base of this current-main PR.

This candidate carries the reviewed lifecycle contents originating from PRs #203 through #212, including their later Codex fixes, onto current main while preserving the already-merged PR #214 private-media runtime wiring and other current-main files.

Origin exact HEADs are encoded in `src/member-clean-stack-provenance.mjs`. The current-main base, the reviewed PR #213 source candidate, and the historical PR #202 construction base are recorded separately so automation cannot confuse them.

`migrations_managed/20261007_member_identity_prospect_foundation.sql` is repository source only. Its presence does not authorize or perform migration apply.

This candidate exists to eliminate parent/child ancestry drift before any merge decision. It does not supersede the requirement for fresh exact-HEAD CI, fresh Codex review, changed-file verification, zero unresolved review threads, and explicit Owner exact-HEAD merge approval.

No Production deploy/read/write, migration apply, Google/GAS live operation, CRM mutation, Customer ID generation/update/delete/merge, LINE send, R2 object access/storage fetch, route/secret/security change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
