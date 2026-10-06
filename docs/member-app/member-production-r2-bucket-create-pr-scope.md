# Member R2 bucket create gate — PR scope

This branch only adds a gated source path for a future, separately authorized one-bucket R2 create operation.

It does not create a bucket, mutate existing buckets, bind R2 to Production, access R2 objects, deploy Production, change traffic, write D1/CRM/LINE, generate Customer IDs, change secrets, or activate routes.

The canonical future target remains `customer-crm-member-private-media` in jurisdiction `default` with Standard storage class and no public access enablement.
