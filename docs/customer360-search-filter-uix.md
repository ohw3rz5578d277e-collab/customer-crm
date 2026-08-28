# Customer360 Search / Filter UIX

TASK: TASK-CRM-CUSTOMER360-SEARCH-FILTER-UIX-02

## Contract

- Search is UI discovery only. It never participates in Customer identity resolution, merge, Customer ID allocation, or LINE identity.
- `/api/customer360/customers` returns a privacy-safe paginated list DTO only.
- List payload excludes full street address, exact family birthdates, private memo, reservations, LINE history, and raw customer rows.
- `/api/customer360/customer/{customer_id}` remains the authenticated detail endpoint for authorized private detail.
- `/api/customer360/marketing-home` returns KPI, top opportunities, and facets; it does not return the full customer collection.
- Free-text `q` is sent to the search endpoint but is not persisted to the browser URL or saved views.
- Saved views are browser-local only and do not add Production schema.
- `CRM_CUSTOMER360_WRITE_ENABLED` is unchanged and automatic LINE send remains disabled.

## Production safety

This task adds no migration and authorizes no Production D1 write, Customer/Family write, Worker deploy, Pages Production deploy, or merge.
