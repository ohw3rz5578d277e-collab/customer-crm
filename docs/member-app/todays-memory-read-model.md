# MIZUNO PHOTO MEMBER — TODAY'S MEMORY Read Model

Baseline: 2026-09-24 JST

## Purpose

TODAY'S MEMORY is a source-only Member read model for surfacing past Family MEMORIES around today's calendar date.

The customer experience should distinguish:

- an exact calendar anniversary: **この日の思い出**
- a nearby seasonal fallback: **この頃の思い出**

This phase does not activate the HOME module, send messages, create reservations, or modify data.

## Source

TODAY'S MEMORY reuses:

`readMemberMemoriesForSession`

Therefore it inherits the approved MEMORIES boundaries:

- server-verified Member session
- exact canonical Customer ID
- explicit Family link
- published MEMORIES only
- deleted MEMORIES hidden
- private media storage keys not exposed

No new D1 schema is introduced.

## Exact anniversary rule

An exact anniversary requires:

- MEMORY shoot date has the same month and day as the current Member date
- MEMORY shoot year is strictly earlier than the current year

Example:

Current date:
`2026-09-24`

Eligible exact anniversaries:

- `2025-09-24`
- `2024-09-24`
- `2023-09-24`

The newest prior-year match becomes primary.

Up to 3 exact matches may be returned for preview.

The customer-facing headline is:

`この日の思い出`

## Seasonal fallback

If there is no exact anniversary, the model may choose one nearby past MEMORY only when:

- it is in the same calendar month
- its day-of-month is within ±7 days
- it is from an earlier year

Selection order:

1. smallest calendar-day distance
2. newest shoot date
3. stable MEMORY ID tie-break

The headline is:

`この頃の思い出`

This wording deliberately avoids presenting a nearby date as if it happened exactly today.

The fallback never crosses into another month.

## No-match behavior

If there is:

- no exact anniversary, and
- no same-month MEMORY within ±7 days

then:

`today_memory = null`

The system does not invent a MEMORY.

## Current-year exclusion

A MEMORY from the current year is not treated as nostalgia/anniversary evidence.

TODAY'S MEMORY is intended to recall prior Family history, not show a shoot from earlier this same calendar year as an anniversary.

## Leap-day behavior

February 29 is treated conservatively.

Exact February 29 anniversary:

- only matches when the current date is also February 29.

On February 28 in a non-leap year:

- a past February 29 MEMORY may appear only as the seasonal nearby fallback;
- it is not labeled as an exact `この日の思い出`.

## Invalid dates

Invalid calendar dates are ignored.

Examples:

- `2025-02-29`
- `2026-09-31`

An invalid deterministic internal `as_of` value fails closed in the pure builder.

## Bounded source note

The current MEMORIES list reader is bounded.

Therefore TODAY'S MEMORY explicitly reports:

- `bounded_source_window=true`
- `database_total_claim=false`

This phase does not claim it searched an unbounded lifetime database history beyond the approved MEMORIES reader contract.

A future optimized anniversary query could be introduced separately if the Family history grows beyond the current read window.

## Output

When available, TODAY'S MEMORY returns:

- as-of date
- mode
- headline
- primary MEMORY
- preview MEMORY list
- exact match count
- seasonal fallback flag

Each MEMORY includes anniversary metadata:

- mode
- years ago
- calendar distance in days

The underlying MEMORY card remains the approved safe Member MEMORY list shape.

## Conceptual HTTP contract

Source-only endpoint:

`GET /api/internal/member/todays-memory`

The endpoint:

- requires server Member session
- is GET-only
- does not accept Customer ID from the request
- does not accept Family ID from the request
- does not accept client-controlled `as_of`
- is not Production route-wired

The source-level read function may accept deterministic `as_of` only for tests/internal verification.

## HOME boundary

This phase intentionally does **not** update the HOME composition model.

HOME continues to report:

`today_memory_active=false`

Activation in HOME should be a separately reviewed source change after this read model is accepted.

## Current exclusions

Not included:

- HOME activation
- final TODAY'S MEMORY UI
- push notifications
- LINE draft/send
- automatic contact
- reservation creation
- favorites
- Creative generation
- Shop integration
- Production route wiring
- Production D1 schema apply
- Production D1 write
- Customer/Family write
- Member session issuance
- Production deploy

## Safety status

Current implementation is:

- read-only
- server Member session only
- exact Family authorization inherited from MEMORIES
- published/non-deleted MEMORY source
- exact anniversary prioritized
- honest nearby fallback wording
- no cross-month fallback
- current-year MEMORY excluded
- conservative leap-day handling
- HOME activation = 0
- automatic contact = 0
- LINE send = 0
- Production route wiring = 0
- Production write = 0
