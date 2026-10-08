# Member Step 8 runtime read hardening

Baseline: current canonical main after PR #232, `b90d185eccc3bd84b08eb1b829231b9eedb79c10`.

## Purpose

PR #232 completed the source-level Step 8 lifecycle integration evidence gate. This follow-up hardens the existing server-side `member-family-pass-read-model.mjs` read path so its raw D1 evidence follows the same fail-closed rules before it feeds FAMILY PASS / BLACK reads.

## MEMORY count evidence

The server reader accepts the `COUNT(*)` result only when it is a non-negative JavaScript safe integer. Numeric strings, booleans, objects, arrays, bigint values, and unsafe integers are not coerced.

The count remains exact-Family scoped and includes only published, non-deleted `member_memories` rows. The read result exposes the exact Family ID used for the count and `count_verified=true`.

Malformed count evidence returns `invalid_memory_count_evidence` and does not produce a tier.

## Durable BLACK evidence

When `member_family_pass_entitlements` exists, a returned row is trusted only when all of these are exact:

- row `family_id` equals the authorized Family;
- `black_lifetime` is numeric integer `1`;
- `black_achieved_at` is canonical millisecond UTC (`YYYY-MM-DDTHH:mm:ss.sssZ`);
- `qualifying_memory_count` is a non-negative safe integer and at least `10`;
- `achievement_source` is exactly `published-member-memories`.

A malformed durable row returns `invalid_durable_black_evidence`. It is not silently downgraded to a count-only tier.

A valid read exposes the exact Family query scope and whether zero or one durable row was observed.

## Locked product rules

- BLACK threshold remains exactly 10 MEMORIES.
- A valid persisted BLACK remains lifetime BLACK even if current MEMORY count later drops below 10.
- BLACK benefit remains PHOTO GOODS 10% OFF FOREVER only.
- Shooting fees are not discounted.
- Automatic BLACK award/backfill remains disabled.
- Automatic discount enforcement remains disabled.

## Safety boundary

Source/test/docs only. This change does not read Production D1 during review, write or delete Production D1, apply a migration, award BLACK, mutate MEMORY, change Customer/Family/Member identity, call Google/CRM/LINE/R2, deploy or activate a Worker/route, change secrets/security policy, activate Commerce, change UI, or spend money.
