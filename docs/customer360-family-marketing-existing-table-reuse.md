# Existing marketing table reuse

This foundation intentionally reuses the existing CRM marketing model:

- `crm_marketing_campaigns`
- `crm_marketing_scores`
- `crm_marketing_line_batches`
- `crm_line_response_logs`
- `crm_marketing_ops_logs`

`crm_marketing_scores.reasons_json`, `next_offer`, and `next_line` remain the extension points for future persistence of derived family/event recommendations.

This task keeps opportunity computation read-only and does not write derived scores to Production.
