# Customer360 Search / Filter Test Plan

- Global search: name, furigana, Customer ID, normalized phone, family name/furigana, city, zero result, same-name multiple results.
- Filters: LTV/AOV/shoot/dormant ranges, date ranges, family/child counts, child age, birth month, school stage, relation, event type/days, area, LINE, consent, photo status, source, campaign, genre, marketing class, AND combinations.
- Sort: every supported sort with deterministic Customer ID tie-break.
- Pagination: default 50, max 100, has_next.
- Privacy: list DTO excludes full address, exact birthdate, memo, reservations, LINE history, raw row.
- UI: search debounce/Enter/clear, slash focus, empty state, drawer, active chips, result count, sort, local saved views, Customer detail.
- Responsive: 1920x1080, 1440x900, 1024x768, 390x844, horizontal overflow 0.
- Existing P0 regression: Customer identity resolver/regression/canonical guards and Owner desktop layout.
- Build: Wrangler dry-run only with Cloudflare credentials intentionally empty.
