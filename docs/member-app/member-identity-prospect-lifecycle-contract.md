# Member identity and prospect lifecycle contract

Baseline: 2026-10-07 JST

## Purpose

Define the source-only identity contract for the next FAMILY PASS / MEMBER stage without changing Production runtime, UI, D1 schema, Google storage, LINE behavior, or Customer ID ownership.

## Two customer entry paths

### Existing customer: first login

"First login" is reserved for a person who already has a canonical CRM Customer ID because a reservation or shoot already exists.

- The customer does not need to know or type the Customer ID.
- An administrator selects the canonical customer in Customer CRM and issues a customer-specific one-time invitation.
- The public invitation URL must contain only an opaque, cryptographically strong random token. It must not expose the Customer ID.
- The server resolves the token to exactly one canonical Customer ID.
- The invitation is single-use, expires, and a replacement invitation invalidates the older active invitation.
- Successful registration creates/binds a Member Identity without generating or changing the Customer ID.

### Prospect: new registration

"New registration" is reserved for a person who has not yet booked and therefore does not yet have a canonical Customer ID.

- Public acquisition may originate from Instagram, the website, advertising, or another campaign.
- Registration creates a Prospect ID and a stable Member Identity.
- Prospect registration must not generate a Customer ID.
- Customer ID remains owned exclusively by Customer CRM.
- When the prospect later makes a first reservation, Customer CRM may create the canonical Customer ID through its separately controlled customer-creation flow.
- Prospect-to-customer promotion binds that Customer ID to the existing Member Identity; it must not create a second Member account.

## Member Identity

Member Identity is a server-generated internal identity separate from Customer ID and Prospect ID.

- It must be generated from cryptographically strong randomness.
- It is stable across Prospect -> Customer promotion.
- A client-supplied Customer ID, Prospect ID, or Member Identity must never determine the write target.
- Raw authentication secrets must not be stored in Google Sheets.
- A Member Identity must not be inferred from name, address, phone number, email address, LINE display name, or fuzzy matching.

## Required first-registration profile

Registration requires the customer to review/provide the current profile, including at minimum:

- name
- phone number
- postal/address information
- email address

For an existing customer, already-known profile values may be prefilled for review/editing. Missing values are collected during registration. For a prospect, the profile is newly collected.

Profile data is not an identity proof by itself.

## Consent

Registration is not complete until required Terms of Service and Privacy Policy consent is recorded.

Consent records must retain at least:

- Member Identity reference
- terms version
- privacy-policy version
- accepted timestamps

Consent history is append-only. A later policy version may require a new consent record without deleting prior consent evidence.

## Customer data ownership

The target long-term data model is:

- Google Customer Master: long-term canonical PII profile
- Customer History: append-only profile-change audit history
- Cloudflare: authentication, identity binding, FAMILY PASS/runtime state, sync control, and bounded PII cache
- Customer CRM app: administrator-facing profile view/edit/review surface

Administrators must edit customer information through Customer CRM rather than directly editing the backing Google Sheet.

## Cache and retention contract

- Customer-facing profile reads may use a seven-day cache to avoid visible Google/GAS latency.
- Google refresh should be prefetched/backgrounded when possible.
- A confirmed profile edit invalidates or refreshes the cache immediately; it does not wait seven days.
- Seven-day cache refresh must not extend the absolute D1 PII retention window indefinitely.
- D1 PII has a hard absolute maximum retention of 30 days.
- Before day 30, normal purge is fail-closed: purge only after durable Google synchronization, identity correspondence, and latest-version equality are verified.
- At the 30-day deadline, cached PII must be deleted or made cryptographically inaccessible even when Google synchronization or verification is still unavailable. The system may retain only non-PII retry/audit state needed to reconcile the failed sync later.
- A failed or unverified sync at the deadline must enter an explicit recovery/review state; it must not silently extend PII retention beyond 30 days.
- Runtime identity/state needed for Customer ID, Family, Member, FAMILY PASS, MEMORIES, sync status, and audit control is not automatically deleted with PII.

## Profile-write identity gate and review queue

A normal customer profile update requires server-derived authenticated session identity plus exact canonical identity binding.

If identity evidence is inconsistent:

- do not overwrite Customer Master;
- do not overwrite another customer's profile;
- preserve the submitted change in a separate review queue;
- allow the customer-facing flow to acknowledge receipt without exposing internal identity mismatch details;
- require a human administrator to approve/reject the pending change in Customer CRM;
- record the administrative decision in audit history.

A pending/review state must never expose another customer's PII, MEMORIES, private media, or FAMILY PASS state.

## Prospect promotion safety

Prospect -> Customer promotion must be explicit and collision-safe.

- Customer CRM remains the only Customer ID issuer.
- No name/phone/email/fuzzy match may silently merge a prospect into a customer.
- Ambiguous promotion is review-required.
- Existing Member Identity, consent history, acquisition attribution, and eligible unused registration benefits survive a successful promotion.
- No automatic Customer merge/delete/update is authorized by this contract.

## Registration benefit boundary

A future new-registration benefit may be associated with a Prospect/Member and survive promotion to Customer.

This contract does not define the commercial benefit value and does not authorize automatic discounts, WooCommerce/Square mutation, commerce activation, or paid spend.

## Existing FAMILY PASS invariants

This contract does not change existing Family, MEMORIES, Passport, TODAY'S MEMORY, NEXT MEMORY, or BLACK semantics.

In particular, BLACK remains a separately controlled entitlement and this contract does not authorize BLACK writes or historical MEMORY backfill.

## Authorization boundary

This source-only contract does **not** authorize:

- Production deploy or Worker activation
- Production D1 read/write or migration apply
- Google Sheets/GAS creation, read, write, deployment, or credential changes
- CRM customer create/update/delete/merge
- Customer ID generation
- Member/Prospect Production writes
- invitation issuance in Production
- LINE send or LINE Login activation
- R2 binding/object access
- Member Production route activation
- private-media route activation
- secret or security-policy changes
- BLACK entitlement writes
- historical MEMORY writes
- commerce activation or paid spend
- UI changes

Every Production mutation remains separately Owner-gated.
