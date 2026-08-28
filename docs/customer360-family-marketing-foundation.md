# Customer 360 / Family / Marketing Foundation

Task: `TASK-CRM-CUSTOMER360-FAMILY-MARKETING-FOUNDATION-01`

## Identity safety

- Customer ID owner remains Customer CRM.
- Family member name/birthdate are profile and marketing metadata only.
- Family data never creates, merges, resolves, or matches Customer IDs.
- LINE identity remains separate.
- Existing `customer_rank` is read only; marketing classes are derived separately.

## LTV terminology

`realized_ltv = customers.total_revenue`.

The UI labels this value as `実績LTV`. It is not predictive LTV.

## Family compatibility

`customer_family_members` is the future managed source for unlimited family members.
Legacy `child1_*` through `child3_*` fields remain untouched and are projected into Customer 360 when an equivalent managed row does not exist.

## Marketing profile

`customer_marketing_profiles` provides normalized address fields and future contact preference/opt-out fields. Existing `customers.address` remains untouched. Missing consent data is `unknown`, never implicit opt-in.

## Writes and Production activation

Family/profile POST APIs are disabled unless `CRM_CUSTOMER360_WRITE_ENABLED=1` is explicitly configured. This task does not configure it.

The managed migration is committed as source only. It is not applied to Production in this task.

No LINE send endpoint is implemented. `next_line` is draft text only.

## Existing campaign foundation reused

The implementation keeps the existing `crm_marketing_campaigns`, `crm_marketing_scores`, `crm_marketing_line_batches`, `crm_line_response_logs`, and `crm_marketing_ops_logs` model. No duplicate campaign/score table is introduced.

## Release gate

Production activation, managed migration application, environment write-gate enablement, and any LINE send capability require a separate PROJECT HQ task.
