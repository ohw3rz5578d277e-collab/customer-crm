# Customer360 Search / Filter Performance Evidence

The search/filter test reports:

- serialized full internal Customer360 view bytes
- privacy-safe list response bytes
- reduction percentage

The browser UI also records list response payload bytes and render milliseconds in a secondary diagnostic line.

The browser contract never requires the complete Customer360 raw collection. Pagination is fixed at a default 50 rows with an allowed maximum of 100 rows.
