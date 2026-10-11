# Member Production LINE Login stage plan

## Current Production baseline

The Member route-only Worker version is already the active Production version at 100% traffic. The active route candidate intentionally keeps:

- `MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE=disabled`;
- trusted caller `line_login_approved=false`;
- private-media content route disabled;
- Favorites mutation/write disabled;
- MEMORY write disabled;
- FAMILY PASS / BLACK entitlement write disabled.

The Member session foundation and required Member runtime secrets already exist. Session issuance itself is not a separate runtime mode. A browser can receive a Member session only after a trusted login flow has resolved an existing canonical Customer and explicit Family link.

## Purpose

This source-only planner defines the only permitted delta for a future LINE Login candidate.

The future immutable candidate must be created from the exact canonical source and may change only the activation surface needed for LINE Login:

1. ephemeral `MEMBER_PRODUCTION_OWNER_APPROVED=true` in the candidate entry;
2. ephemeral `line_login_approved=true` in the trusted Production composition call;
3. candidate `MEMBER_PRODUCTION_ROUTE_MODE=enabled`;
4. candidate `MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE=enabled`.

All other high-risk or write-capable modes remain disabled.

## Mandatory disabled surfaces

A future LINE Login candidate must keep all of these disabled:

- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE`;
- `MEMBER_FAVORITES_MUTATION_ROUTE_MODE`;
- `MEMBER_FAVORITES_RATE_LIMIT_MODE`;
- `MEMBER_FAVORITES_WRITE_MODE`;
- `MEMBER_MEMORY_WRITE_MODE`;
- `MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE`.

The exact existing bindings must remain unchanged, including:

- `MEMBER_PRIVATE_MEDIA_BUCKET` -> `customer-crm-member-private-media`;
- `DB`;
- `LINE_SERVICE`;
- `RESERVATION_SERVICE`.

No candidate may create a new Customer, generate a Customer ID, create or automatically link a Family, write canonical CRM state, send LINE messages, activate Commerce, or automatically award BLACK benefits.

## Stage / promotion separation

This planner does not upload a Worker version and cannot change Production traffic.

The required release sequence remains fail-closed:

1. source-only planner and regression merge;
2. a separately reviewed Owner-gated immutable candidate stage implementation;
3. fresh exact Owner authorization to upload exactly one immutable LINE Login candidate at 0% traffic;
4. read-only candidate / active-version verification;
5. separate fresh Owner authorization for Production promotion.

A stage authorization must never implicitly authorize promotion.

## Current authorization boundary

This source-only planner does **not** authorize or execute:

- Production fetch;
- Production Worker upload, deploy, promotion, or traffic change;
- Production D1 read/write or migration apply;
- R2 object read/write;
- secret creation/change/value display;
- LINE Login external call;
- private-media content delivery;
- Customer / Prospect / Customer ID mutation;
- Family mutation;
- LINE send;
- Commerce activation;
- BLACK automatic award;
- paid spend.

The next boundary after this planner is merged is `OWNER_GATED_IMMUTABLE_LINE_LOGIN_CANDIDATE_STAGE`.
