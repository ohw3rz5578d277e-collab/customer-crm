# Member profile change review queue contract

## Purpose

Identity, version, binding, or Prospect-promotion ambiguity must never overwrite Customer/Prospect Master automatically.

The customer-facing response may acknowledge receipt without exposing internal identity mismatch details. The submitted change is held for administrator review.

## Review reasons

- identity_mismatch
- version_conflict
- promotion_collision
- binding_mismatch

## Safety

A queued item is not permission to mutate Customer Master.

Approval requires fresh identity re-verification and latest-version verification. Approval creates a new auditable sync event rather than replaying an uncertain write.

Rejection leaves Master unchanged and records an audit decision.

The normal administrator surface is Customer CRM. Direct Google Sheet editing is not the review workflow.

Review Queue persistence and any PII payload storage require a later schema/privacy decision. The current source planner stores only a digest in its durable candidate shape and marks submitted profile content ephemeral.

## Authorization boundary

No Production queue write, Customer/Prospect Master write, CRM mutation, Google operation, Customer ID mutation, LINE send, deploy, D1 migration/write, route/secret/R2/UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
