# Member R2 bucket create — authorization hardening

Baseline: 2026-10-07 JST

The bucket-create mutation gate requires the GitHub Issue #26 Owner authorization comment itself as evidence, not only copied workflow inputs.

Before any Cloudflare bucket mutation, the workflow must verify:

- the supplied source comment ID exists;
- the comment belongs to Issue #26;
- the comment author is `ohw3rz5578d277e-collab`;
- the comment body exactly equals the canonical bucket-create command for the supplied exact main SHA, canonical bucket digest, jurisdiction `default`, and Owner receipt;
- the current `main` still equals the approved exact SHA;
- the active Production Worker version still equals the version captured before the R2 preflight.

The main SHA and active Worker checks are repeated immediately after the read-only bucket-absence preflight and immediately before the single bucket-create POST.

A manual workflow dispatch without a matching live Issue #26 Owner authorization comment must fail closed. The receipt check does not authorize a second bucket: the canonical-digest absence check must still prove zero matches before the single POST.

No automatic bucket delete, patch, rename, recreate, retry, R2 object operation, binding change, Production deploy, route activation, Production D1 write, CRM write, LINE send, or Customer ID generation is introduced by this hardening.
