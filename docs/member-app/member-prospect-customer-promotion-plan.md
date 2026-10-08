# Prospect to Customer promotion plan

## Trigger

Promotion becomes relevant only after the real customer lifecycle reaches the point where Customer CRM has created a canonical Customer ID through its controlled process, typically the first reservation.

Member/FAMILY PASS never creates that Customer ID.

## Required evidence

A promotion candidate must verify:

- valid Prospect ID
- stable Member Identity
- persisted Prospect ID <-> Member Identity binding exact match
- canonical Customer ID exists and source is Customer CRM
- target Customer ID is not already bound to another Member
- Prospect is still in promotable Prospect state
- consent history will be preserved
- acquisition attribution will be preserved
- signup-benefit state will be preserved

Any mismatch/collision routes to review; no fuzzy auto-merge is allowed.

## Result

Successful execution later will preserve Member Identity while linking the canonical Customer ID and marking the Prospect promoted. It must not create a second Member account.

This planner does not execute the promotion.

## Authorization boundary

No Customer ID generation/update/delete/merge, CRM write, Production D1 write, Google Master write, LINE send, deploy, migration, route/secret/R2/UI change, benefit redemption, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
