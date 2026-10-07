# Member new-registration benefit foundation

## Purpose

A public FAMILY PASS Prospect registration may later receive a one-time signup benefit intended to encourage the first reservation.

This foundation models the entitlement only. It does not choose the commercial offer.

## Issue

A signup benefit candidate requires:

- canonical server-generated Member Identity
- canonical Prospect ID
- completed registration
- current required consent
- persisted evidence that no signup benefit was previously issued

Only one signup entitlement may exist for the Member/Prospect registration lifecycle.

## States

issued -> available -> reserved -> used

Controlled terminal/alternate states:

- expired
- revoked
- reserved -> available when a reservation is legitimately released/cancelled under a later policy

Used/expired/revoked are not automatically reissued.

## Redemption

A transition to used requires:

- exact authenticated Member Identity match
- canonical Customer ID already created by Customer CRM
- reservation identifier
- persisted evidence that prior redemption count is zero

The entitlement does not create Customer IDs and does not itself apply a discount or mutate commerce.

## Prospect to Customer continuity

Promotion preserves the same entitlement and state. It does not reset, duplicate, or reissue the benefit.

Thus a benefit issued as a Prospect remains attributable to the original acquisition/registration lifecycle after the Member becomes a Customer.

## Commercial definition

The actual benefit is intentionally unset in this phase. Future configuration may define an approved photo addition, photo-goods benefit, weekday/seasonal offer, or another bounded first-booking benefit.

Commercial value, expiration policy, eligibility details, stacking rules, Square/WooCommerce behavior, and customer-facing copy require separate approval.

## Authorization boundary

No Production entitlement issue/transition/redemption, automatic discount, Square/WooCommerce write, Customer ID generation, CRM mutation, Google write, D1 write/migration, LINE send, deploy, route/secret/R2/UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
