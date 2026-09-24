# MIZUNO PHOTO MEMBER — Phase 1 Identity Contract

Baseline: 2026-09-24 JST

## Purpose

Define the CRM-side family/household identity contract needed by the future MIZUNO PHOTO MEMBER app without changing Production data or interfering with the current LINE-history / sales reconciliation work.

The Member app is a separate customer-facing product. Customer CRM remains the canonical source of customer identity.

## Identity chain

```
LINE Login / Member session
        |
        v
exact LINE identity verification
        |
        v
canonical Customer ID
        |
        v
explicit Customer <-> Family link
        |
        v
Family ID
        |
        +--> Member Core
        +--> MEMORIES
        +--> CREATE
        +--> FAMILY PASS
        +--> SHOP
```

## Non-negotiable safety rules

1. A Family ID is never inferred from name, address, phone, email, LINE display name, child name, or fuzzy similarity.
2. Customer ID remains the existing canonical CRM identity.
3. Existing `customer_family_members` rows describe profile/family-member information and do not themselves establish household identity.
4. Two customers share a Member family only after an explicit link exists.
5. One canonical customer may have at most one active Member-family link at a time.
6. Historical family links use soft delete; identity history is not silently overwritten.
7. The initial foundation is read-only. No family creation/link endpoint is wired to Production.
8. Production D1 schema application and writes require separate Owner authorization.

## Additive schema

- `customer_family_groups`
  - stable `family_id`
  - display name
  - active/inactive state
- `customer_family_customer_links`
  - exact `customer_id`
  - `family_id`
  - relationship/access role
  - soft delete

No change is made to the `customers` table.

## Initial API contract

A future internal Member API may call:

`GET /api/internal/member-family/customer/:customer_id`

The current module implements this read model but is intentionally not wired into the Production entry point.

Expected states:

- `linked`
- `unlinked`
- `schema_not_applied`
- invalid Customer ID -> rejected

## Relationship to the customer-facing app

The customer-facing app should never request a Family ID by name or by a user-supplied Customer ID.

Expected flow:

1. Verify LINE/member session server-side.
2. Resolve the canonical CRM Customer ID.
3. Resolve the explicit Family ID through the internal contract.
4. Issue a Member session bound to that Family ID.
5. Every family-scoped resource verifies the session Family ID before returning data.

This provides the authorization boundary for personalized HOME, MEMORIES, CREATE, SHOP, MY and later BLACK experiences.

## Current scope

Included:
- managed migration source
- read-only family resolver
- regression tests
- contract documentation

Not included:
- Production schema apply
- Production D1 write
- family auto-creation
- LINE Login implementation
- Member Core DB
- photo storage
- customer-facing UI
- deploy

## Production gates

Before activation:

1. current CRM identity-reconciliation work must remain unaffected;
2. migration review and local verification must pass;
3. Member login contract must be implemented;
4. cross-family access tests must prove family A cannot access family B;
5. fresh Owner approval is required for Production schema apply;
6. fresh Owner approval is required for any Production write path.
