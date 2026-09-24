# MIZUNO PHOTO MEMBER — NEXT MEMORY Read Model

Baseline: 2026-09-24 JST

## Purpose

NEXT MEMORY is the read-only Member model that turns canonical child profile dates and Family MEMORY history into a small set of future photography candidates.

Its purpose is to support later HOME / MY experiences such as:

- 1st Birthday
- Half Birthday
- 七五三
- 入学・卒業
- 成人記念
- ordinary birthday

This phase does not send messages, book a shoot, modify CRM data, or render the final customer UI.

## Identity boundary

The reader accepts identity only from the server-verified Member session:

- canonical 8-digit Customer ID
- explicit Family ID

It then revalidates the current Customer -> Family link.

Only canonical Customer IDs explicitly linked to that Family may contribute child-profile evidence.

It never expands the Family using:

- customer name
- address
- phone
- email
- LINE display name
- child name
- fuzzy similarity

## Child profile source

Canonical child profile evidence is read from:

`customer_family_members`

Required conditions:

- exact canonical linked `customer_id`
- `relation='child'`
- non-deleted row
- exact child row ID

The Member model may aggregate child rows from multiple canonical Customer IDs only when those Customer IDs are already explicitly linked to the same Family.

No Family membership is inferred.

## Duplicate child IDs

A child row ID is treated as an exact identity key.

If the same child ID appears under more than one linked Customer with conflicting:

- source Customer ID
- name
- birthdate
- school stage

the model fails closed with:

`ambiguous_child_identity`

It does not deduplicate by child name or birthdate.

## Shared date/opportunity logic

The model reuses the existing pure date/opportunity logic from:

`crm-customer360-marketing-engine.mjs`

This avoids creating a second incompatible age/calendar implementation.

Supported Member candidate types are limited to:

- birthday
- half birthday
- first birthday
- shichigosan
- school entry candidate
- graduation candidate
- coming-of-age candidate

CRM marketing-only concepts such as high-LTV, dormancy, priority scoring, or LINE drafts are not used.

## Duplicate event suppression

A generic birthday should not compete with a more specific milestone on the same child and target date.

Examples:

- 1st Birthday + generic birthday on the same date -> keep 1st Birthday
- 3rd birthday + 七五三 on the same date -> keep 七五三

This keeps the customer-facing candidate list focused.

## Family MEMORY evidence

Historical photography evidence comes only from:

`member_memories`

and only rows where:

- exact Family ID
- `published=1`
- non-deleted

The current MEMORY schema does not contain canonical `child_id`.

Therefore the model explicitly reports:

- `family_history_is_child_specific=false`
- `child_memory_link_available=false`

Past genres can annotate a candidate as already seen somewhere in the Family history, but they do not prove that the same child has already had that milestone photographed.

For that reason Family history does not automatically suppress a child candidate.

Example:

A Family has a past 1st Birthday MEMORY, but there are two children. Without a MEMORY -> child identity link, the model cannot know whether the current 1st Birthday candidate belongs to the same child.

## Candidate output

Each candidate may include:

- event type
- customer-facing label
- target date
- days until
- exact child row ID
- child display name
- computed age
- evidence source
- Family-level genre-history match
- `history_scope='family_not_child'`
- `candidate_only=true`
- `automatic_contact=false`

The first sorted candidate becomes:

`next_memory`

Candidate sorting is primarily by earliest upcoming timing, with more specific milestone types preferred over generic birthday when timing is equal.

## Current Family history matching

Genre history matching is conservative and exact after Unicode/case normalization.

Examples:

- `七五三` matches 七五三
- `七五三後のファミリー` does not
- `バースデー` can count as generic birthday history
- `バースデー` does not count as 1st Birthday history

This is only informational Family-level evidence.

## Missing child profile evidence

If the Family has no canonical child rows or no valid birthdates:

- no age-based recommendation is invented;
- `next_memory` may be null;
- candidate list may be empty.

The system does not guess child age from:

- customer age
- MEMORY title
- photo content
- child name
- LINE messages
- reservation notes

## School stage

Existing shared logic can use explicit `school_stage`, for example 年長, together with canonical child profile evidence.

The Member model does not infer school stage from age alone beyond the already existing shared opportunity logic.

## Conceptual HTTP contract

Source-only endpoint:

`GET /api/internal/member/next-memory`

The endpoint:

- requires server Member session
- is GET-only
- does not accept Customer ID
- does not accept Family ID
- does not accept client-controlled `as_of`
- is not Production route-wired

The source-level read function accepts a deterministic `as_of` option for tests and internal verification only.

## No messaging / booking automation

NEXT MEMORY is a suggestion model only.

This phase does not:

- send LINE
- create a LINE draft
- contact the customer
- make a reservation
- generate a coupon
- change a price
- trigger a campaign

The later customer UI may offer a contextual CTA to consultation/LINE, but activation of that route remains separate.

## Current exclusions

Not included:

- final HOME UI
- final MY UI
- child-specific MEMORY linkage
- MEMORY -> child write
- automatic contact
- marketing priority score
- LINE draft generation
- LINE send
- reservation creation
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
- no child-name inference
- Family-scoped published MEMORY history only
- no claim that Family history is child-specific
- no automatic contact
- LINE send = 0
- Production route wiring = 0
- Production write = 0
