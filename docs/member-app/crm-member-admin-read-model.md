# Customer CRM Member administration read-model contract

## Purpose

Customer CRM is the administrator-facing control surface for Member/FAMILY PASS customer information. This phase defines a read-only aggregation model only; it does not implement UI.

## Customer view

For an existing Customer, CRM may aggregate:

- canonical Customer ID
- Member registration/binding status
- consent status/version summary
- invitation status: unissued / active / used / expired / revoked
- pending Review Queue count
- benefit status
- reservation/member lifecycle status

## Prospect view

For a pre-booking Prospect, CRM may aggregate:

- Prospect ID
- stable Member Identity reference
- registration status/date in later persistence layer
- acquisition source
- consent status
- pending Review Queue count
- signup benefit status
- reservation status
- promotion/link status

Prospect rows do not receive canonical Customer IDs until Customer CRM's separately controlled real-customer creation flow creates one.

## Mutation boundary

This read model cannot:

- generate Customer IDs
- approve/reject Review Queue items
- issue/revoke invitations
- promote Prospect to Customer
- write Google Customer/Prospect Master
- send LINE
- activate benefits

Those actions require separate command planners/executors and Owner/runtime gates.

## UI boundary

No CRM UI is changed by this phase. A later dedicated UI phase may render this model.

## Authorization boundary

No Production read/write, migration, deploy, Google/GAS operation, CRM mutation, Customer ID generation, LINE send, route/secret/R2/UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
