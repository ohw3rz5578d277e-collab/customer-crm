# MIZUNO PHOTO MEMBER — NEXT MEMORY Read Model

Baseline: 2026-09-24 JST

## Purpose

NEXT MEMORY is a read-only Member model that turns canonical managed child profile data and Family MEMORY history into a small set of future photography candidates for later HOME / MY experiences.

Initial candidate families include:

- Half Birthday
- 1st Birthday
- ordinary birthday
- 七五三
- 入学
- 卒業
- 成人記念

This phase does not send messages, make reservations, change CRM data, apply pricing, or render final customer UI.

## Identity boundary

The reader accepts identity only from the server-verified Member session:

- canonical 8-digit Customer ID
- explicit Family ID

It revalidates the current Customer -> Family link.

Only canonical Customer IDs already explicitly linked to the same Family may contribute child-profile evidence.

It never expands Family identity using:

- customer name
- address
- phone
- email
- LINE display name
- child name
- birthdate similarity
- fuzzy matching

## Managed child source

Child evidence is read only from:

`customer_family_members`

Required conditions:

- exact linked canonical `customer_id`
- `relation='child'`
- non-deleted row
- exact child row ID

The Member model may aggregate managed child rows from multiple canonical Customer IDs only when those Customer IDs are already explicitly linked to the same Family.

No legacy `child1_name / child1_birthdate` fallback is used by this Member model.

## Child identity rule

Exact child row ID is the only child identity key used here.

The model does not deduplicate two different child IDs because they happen to share:

- the same name
- the same birthdate
- the same school stage

That avoids accidentally merging siblings or twins.

If the same exact child ID appears under different linked Customer IDs with conflicting source Customer, name, birthdate, or school stage, the model fails closed with:

`ambiguous_child_identity`

and requires review.

## Birthdate-based candidate logic

The model reuses the existing pure date/opportunity functions in:

`crm-customer360-marketing-engine.mjs`

for child events that can be directly derived from a managed birthdate.

Member-visible date-based candidates are limited to:

- birthday
- Half Birthday
- 1st Birthday
- 七五三
- 成人記念

CRM marketing priority, dormancy, LTV, campaign classes, and LINE draft generation are not used.

## School events are explicit-stage only

The shared CRM marketing engine also contains age-based school timing estimates.

Those estimates are deliberately **not** used by NEXT MEMORY.

NEXT MEMORY does not conclude that:

- age 6 means school entrance
- age 12 means elementary-school graduation
- age 15 means junior-high graduation
- age 18 means high-school graduation

Instead, school candidates are created only from explicit managed `school_stage` evidence.

Examples:

- `年長` -> 小学校入学候補
- `小学6年` / `小6` -> 小学校卒業候補
- `中学3年` / `中3` -> 中学校卒業候補
- `高校3年` / `高3` -> 高校卒業候補

A school-stage candidate may exist even when birthdate is unavailable.

No target date is invented for these stage-only candidates.

## Generic birthday suppression

A generic birthday candidate is removed when the same exact child and target date already has a more specific milestone.

Examples:

- 1st Birthday + generic birthday on the same date -> keep 1st Birthday
- 3rd birthday + 七五三 on the same date -> keep 七五三

This avoids showing duplicate calls to action for one event.

## Family MEMORY context

Past photography history is read only from:

`member_memories`

and only rows where:

- exact Family ID
- `published=1`
- non-deleted

The current MEMORY schema does not contain canonical child ID.

Therefore NEXT MEMORY explicitly reports:

- `family_history_is_child_specific=false`
- `child_memory_link_available=false`

Past Family genres may annotate a candidate as already present somewhere in Family history, but they do not prove that the same child has already completed that event.

Family history therefore does not automatically suppress a new child candidate.

## Conservative genre evidence

Family history matching is exact after Unicode/case normalization.

Examples:

- `七五三` matches 七五三 history
- `七五三後のファミリー` does not
- generic `バースデー` can annotate an ordinary birthday
- generic `バースデー` does not prove 1st Birthday history

The genre evidence remains informational only.

## Candidate output

Each candidate may include:

- event type
- label
- target date, when deterministically known
- days until, when deterministically known
- exact child row ID
- child display name
- age from managed birthdate when available
- source: birthdate or explicit school stage
- Family-level genre-history match
- `history_scope='family_not_child'`
- `candidate_only=true`
- `automatic_contact=false`

Candidates with real upcoming dates are ordered primarily by nearest date.

Undated explicit school-stage candidates remain available without inventing dates.

The first ranked candidate becomes:

`next_memory`

## LINE consultation CTA contract

NEXT MEMORY may expose a future customer-facing CTA contract:

- channel: LINE
- intent: consultation
- automatic send: false

This is metadata only.

It does not send LINE, create a LINE draft, or make a reservation.

## No child evidence

If no canonical managed child rows exist:

- no child is inferred from names or photos;
- no age is inferred from MEMORY titles;
- no recommendation is invented from LINE messages or free-text notes.

`next_memory` may be null and candidate list may be empty.

## Conceptual HTTP contract

Source-only endpoint:

`GET /api/internal/member/next-memory`

The endpoint:

- requires server Member session
- is GET-only
- does not accept Customer ID from the request
- does not accept Family ID from the request
- does not accept client-controlled `as_of`
- is not Production route-wired

The source-level read function accepts deterministic `as_of` only for internal tests and verification.

## Current exclusions

Not included:

- final HOME UI
- final MY UI
- child-specific MEMORY linkage
- MEMORY -> child write
- birthdate-based school timing inference
- automatic contact
- marketing priority score
- LINE draft generation
- LINE send
- reservation creation
- coupon creation
- price changes
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
- canonical Customer ID only
- explicit Family links only
- exact child IDs only
- no child-name identity inference
- no fuzzy child deduplication
- Family-scoped published MEMORY history only
- Family history not claimed as child-specific
- school milestones explicit-stage only
- birthdate-based school timing inference = 0
- consultation CTA only
- automatic contact = 0
- LINE send = 0
- Production route wiring = 0
- Production write = 0
