# MIZUNO PHOTO MEMBER — Browser Page and Public Asset Delivery

Baseline: 2026-09-25 JST

## Purpose

This phase adds the source-only browser delivery foundation that was missing from the Production route wiring preflight.

It adds source contracts for:

- `GET /member`
- `GET /member/home`
- `GET /member/memories`
- `GET /member/create`
- `GET /member/shop`
- `GET /member/my`
- `GET /member-assets/member-app.css`
- `GET /member-assets/member-app.js`
- published catalog assets under `/member-assets/...`

None of these routes are wired into the Production Worker entry in this phase.

## Browser page behavior

The browser page verifies the existing signed Member session.

If a valid Member session is absent, the page renders a local login screen whose only login target is:

`/api/member/login/line/start?return_to=<local member path>`

It does not redirect to an arbitrary URL and does not expose Customer ID or Family ID.

When the signed session is valid, the server reads the current Member read models and server-renders the canonical five tabs:

1. HOME
2. MEMORIES
3. CREATE
4. SHOP
5. MY

The browser page does not auto-write, auto-contact, send LINE messages, create a Customer ID, or expose private storage keys.

## Identity conflict behavior

If any loaded Member component reports an identity/security conflict such as:

- family access denied;
- ambiguous Family identity;
- incomplete Family identity;
- ambiguous child identity;
- component identity mismatch;

the whole Member browser page fails closed instead of mixing partial family data.

## Browser asset policy

Two built-in app assets are exact allowlist entries:

- `/member-assets/member-app.css`
- `/member-assets/member-app.js`

The JavaScript currently binds only local five-tab navigation and HOME/MY tab CTAs.

It performs:

- fetch = 0
- write = 0
- external navigation = 0
- storage access = 0

MEMORIES detail interaction, SHOP filtering, CREATE execution, Favorites mutation, and private-media binary rendering remain separately reviewed capabilities.

## Published catalog assets

Other `/member-assets/...` paths are not served merely because a path exists.

Delivery requires:

1. a syntactically safe local path;
2. an exact published, non-deleted row in `member_public_assets`;
3. an allowed registry MIME type;
4. an explicitly injected trusted public asset adapter;
5. adapter output whose MIME exactly matches the registry;
6. a bounded object size.

The source delivery layer never discovers an asset binding from `env`.

The adapter contract is conceptually:

`asset_adapter.get(local_path)`

and returns body, size, and content_type.

## Public versus private media

This layer is only for public presentation assets such as approved SHOP or CREATE imagery.

It does not deliver customer private photos.

Private MEMORY photo delivery remains exclusively behind the separate signed-session → short-lived grant → reauthorization → trusted private storage adapter chain.

## Security headers

Member browser HTML uses no-store, noindex, DENY framing, same-origin opener/resource policy, restrictive CSP, and external same-origin CSS/JS only.

Catalog asset delivery uses nosniff, same-origin resource policy, no external redirects, and no Range support in this foundation.

## Production status

This source phase performs:

- Production Worker entry modification = 0
- Production browser route activation = 0
- Production asset route activation = 0
- Production public asset binding = 0
- Production public asset fetch = 0
- Production private media binding/fetch = 0
- Production deploy = 0
- Production schema apply = 0
- Production D1 write = 0
- CRM write = 0
- LINE send = 0
- Customer ID generation = 0

The Production route wiring preflight should continue to report browser page and asset Production routes as missing until the canonical Worker is explicitly changed under a fresh Owner gate.
