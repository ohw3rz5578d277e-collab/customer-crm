# Member runtime implementation gate

Status: source-level backend sequence Steps 1 through 9 complete. Not Production activated.

## Backend sequence

1. Implement exact Member, Prospect, CRM Customer and family identity bindings.
2. Implement one-time hashed invite validation and atomic redemption.
3. Implement Prospect registration and append-only consent evidence.
4. Implement profile review queue before Master updates.
5. Implement server-side Google sync, idempotency, bounded retries and reconciliation.
6. Enforce D1 profile cache TTL of 7 days and hard PII purge deadline of 30 days.
7. Implement exact Prospect to CRM Customer promotion preserving Member identity and history.
8. Integrate family-scoped memory counts and durable BLACK read evidence.
9. Add integration and security tests, then run Node 22 test suite on exact HEAD.

## Current release boundary

The source-level sequence is complete only when `member-runtime-step9-final-gate.test.mjs` passes on the exact candidate SHA under Node 22 and the normal exact-HEAD Member CI remains successful.

The immediate next release gate is **Owner-authorized read-only Member Production runtime readiness**. That gate must fail closed unless the exact authorized current-main SHA first passes the Step 9 final gate. Only after that source prerequisite may it observe GitHub Production release receipts, Production Worker secret names, and Production D1 schema/migration state.

Merging source/test/docs for the readiness gate does not authorize its `workflow_dispatch`. Production reads still require a fresh exact-SHA Owner authorization.

## Release gates

No automatic BLACK award, shooting-fee discount, LINE send, CRM customer creation outside CRM, or production activation. Production D1, Google, R2, secrets, routes, migrations, and deployment require separate Owner approval. Require exact SHA, successful CI, review, rollback and health checks. UI is handled separately.

Production runtime-readiness observation is not Production activation and must not mutate Production. Any later secret change, migration/schema apply, deploy, Worker activation, route activation, LINE Login activation, private/public storage change, Customer/Family/Member/Prospect mutation, BLACK/MEMORY write, commerce activation, or paid spend remains a separate fresh Owner gate.
