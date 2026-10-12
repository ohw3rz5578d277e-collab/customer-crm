# LINE Login post-bridge executable dispatch boundary

The previous source-only blocker was caused by the attempted REST `workflow_dispatch` handoff from the completed-bridge validator into `member-production-line-login-stage.yml`.

That design is no longer used.

The canonical source design now follows the repository's existing reusable-workflow pattern:

1. Issue #26 bridge validates the exact fresh Owner command and writes a bounded one-day receipt artifact.
2. `dispatch-member-production-line-login-stage-after-bridge.yml` starts only after the exact bridge run has completed successfully.
3. Its `validate` job re-checks bridge provenance, exact current `main`, receipt integrity, Owner comment freshness, and the Production mutation busy gate.
4. Only after `validate` succeeds, the workflow calls `member-production-line-login-stage.yml` with `uses: ./.github/workflows/member-production-line-login-stage.yml` and `secrets: inherit`.
5. The stage workflow is `workflow_call`-only for Production execution; there is no manual `workflow_dispatch` entrypoint.
6. The called stage verifies that `caller_run_id` equals the current GitHub run ID and that the current run is the exact first-attempt `workflow_run` caller on the authorized SHA with the canonical validation job already successful.

This design adds no `actions: write` permission and performs no GitHub REST workflow-dispatch POST.

The source wiring does not itself authorize or execute a Production LINE Login stage. A fresh exact Owner command on Issue #26 remains required after any future merge, and candidate promotion remains a separate Owner approval boundary.
