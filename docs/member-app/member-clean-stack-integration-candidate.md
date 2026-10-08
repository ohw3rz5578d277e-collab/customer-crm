# Clean Member lifecycle integration candidate

This branch was rebuilt from exact PR #202 HEAD `9d282bc2c1906379f33b7d0d8d28604c26cc3881`.

It carries the latest reviewed file contents from PRs #203 through #212, including all later Codex fixes, into one linear source-only candidate. The older stacked PRs remain unchanged as review/audit evidence.

Origin exact HEADs are encoded in `src/member-clean-stack-provenance.mjs`.

This candidate exists to eliminate parent/child ancestry drift before any merge decision. It does not supersede the requirement for fresh CI, fresh Codex review, changed-file verification, and explicit Owner merge approval.

No Production deploy/read/write, migration apply, Google/GAS live operation, CRM mutation, Customer ID generation/update/delete/merge, LINE send, R2 access/binding, route/secret/security/UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
