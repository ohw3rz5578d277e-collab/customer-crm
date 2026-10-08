# Member runtime implementation gate

Status: source-only planning. Not production ready.

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

## Release gates

No automatic BLACK award, shooting-fee discount, LINE send, CRM customer creation outside CRM, or production activation. Production D1, Google, R2, secrets, routes, migrations, and deployment require separate Owner approval. Require exact SHA, successful CI, review, rollback and health checks. UI is handled separately.
