# Customer 360 test matrix

Automated foundation tests cover:

- unlimited family rows
- legacy child1-child3 compatibility
- JST age and next birthday
- birthday / half birthday / first birthday
- 七五三 candidate
- school candidate with school_stage priority
- coming-of-age candidate
- RFM and derived marketing classes
- high-LTV / dormant opportunities
- same-name family members do not affect identity
- family birthdate does not affect identity
- marketing priority ordering
- address summary privacy
- consent unknown by default
- family/profile writes fail closed without explicit feature gate
- authenticated Customer 360 API boundary
- existing campaign/score tables are not duplicated
- no customer identity-row writes from the new runtime
- responsive browser checks at 1920x1080, 1440x900, 1024x768, 390x844
- horizontal overflow 0
- exact family birthdates hidden from list and visible only in Customer 360
- LINE output is draft-only
- Wrangler dry-run build only
