# Member one-time invitation validation and redemption source contract

Baseline main: `cf6f206df0badc51fb321ea854452d4a7e28927c`

Status: source/test/docs only. Not Production-ready.

## Purpose

Implement backend sequence step 2 from `member-runtime-release-gates.md`: validate a customer-specific one-time invitation from its opaque raw token without exposing Customer ID in the URL, then describe an atomic single-use redemption operation without executing it.

## Validation contract

The public invitation carries only a high-entropy opaque raw token. The server:

1. accepts only the canonical token shape;
2. computes SHA-256 server-side;
3. looks up the persisted invitation by the digest and requires an exact-one match;
4. verifies that the returned row itself is trusted persisted evidence;
5. derives canonical Customer ID only from that persisted row, never from client input;
6. requires persisted `consumed_at` and `invalidated_at` evidence to be present explicitly; omitted evidence is not treated as NULL;
7. rejects revoked, already-consumed, expired, malformed, duplicate, mismatched or ambiguously-scoped evidence;
8. requires the customer's active invitation cardinality to be exactly one and scopes that count to the same canonical Customer ID.

All textual IDs/digests/timestamps are strict scalar strings. Arrays, objects and other non-scalar shapes do not coerce into accepted evidence. Count evidence accepts only safe non-negative integer numbers or canonical decimal strings.

## Time contract

`now`, `expires_at`, and non-null consumed/invalidated timestamps use canonical millisecond UTC form:

`YYYY-MM-DDTHH:mm:ss.sssZ`

Invalid or normalized-away calendar dates fail closed. An invitation is expired when `now >= expires_at`.

## Raw-token handling

The planner may receive the raw token only as transient input required to calculate SHA-256. It does not return the raw token and marks raw-token retention/output as false.

The persisted invitation record stores `token_sha256`, not the raw token.

## Atomic redemption contract

A validated invitation produces a source-only compare-and-set statement equivalent to:

```sql
UPDATE member_customer_invitations
SET consumed_at = ?
WHERE invitation_id = ?
  AND canonical_customer_id = ?
  AND token_sha256 = ?
  AND expires_at = ?
  AND consumed_at IS NULL
  AND invalidated_at IS NULL
  AND expires_at > ?
```

Redemption succeeds only if the eventual execution reports exactly one affected row.

Zero affected rows means stale/racing evidence or prior redemption/revocation/expiry. The caller must not retry the same mutation blindly; it must re-read and revalidate first.

This compare-and-set prevents two concurrent requests from both successfully consuming the same invitation.

## Identity boundary

Invitation validation/redemption does not:

- accept a client-selected Customer ID;
- generate or modify Customer ID;
- create or merge a Customer;
- generate Family ID;
- mutate Member Identity;
- perform fuzzy identity matching;
- grant FAMILY PASS, BLACK, MEMORIES or other entitlement;
- perform profile writes.

Those remain separate gates.

## Authorization boundary

This source-only stage does **not** authorize:

- Production deploy or Worker activation;
- Production D1 read/write;
- migration apply;
- actual invitation issuance, revocation or redemption in Production;
- CRM write/mutation;
- Customer ID or Family ID generation/update/delete/merge;
- LINE send or LINE Login activation;
- Google network send or Customer Master mutation;
- R2 object access;
- route activation;
- secret/token changes;
- security-policy changes;
- commerce activation or paid spend.

`planOneTimeInvitationRedemption(...)` therefore returns `write_allowed:false`, `execute:false`, `production_write_authorized:false`, and `execution_requires_separate_gate:true` even when validation is ready.
