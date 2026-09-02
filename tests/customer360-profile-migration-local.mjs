import assert from 'node:assert/strict';
import fs from 'node:fs';
import { DatabaseSync } from 'node:sqlite';

const sql=fs.readFileSync('migrations_managed/20260903_customer360_profile_auto_enrichment.sql','utf8');
assert.ok(!/\bDROP\s+(TABLE|COLUMN)\b/i.test(sql));
assert.ok(!/ALTER\s+TABLE\s+customers/i.test(sql));
assert.ok(!/customer_identity_(sequence|registry)/i.test(sql));

const db=new DatabaseSync(':memory:');
db.exec(`
CREATE TABLE customers(customer_id TEXT PRIMARY KEY,name TEXT);
CREATE TABLE customer_family_members(id TEXT PRIMARY KEY,customer_id TEXT,relation TEXT,name TEXT);
INSERT INTO customers(customer_id,name) VALUES('26000123','既存 顧客');
INSERT INTO customer_family_members(id,customer_id,relation,name) VALUES('fm_existing','26000123','child','既存 子');
`);
const beforeCustomer=db.prepare("SELECT * FROM customers WHERE customer_id='26000123'").get();
const beforeFamily=db.prepare("SELECT * FROM customer_family_members WHERE id='fm_existing'").get();
db.exec(sql);
db.exec(sql);
for(const table of ['customer_profile_enrichment','customer_family_member_metadata','customer_field_evidence','customer_notes_history']){
  assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_master WHERE type='table' AND name=?").get(table).n,1,table);
}
assert.deepEqual(db.prepare("SELECT * FROM customers WHERE customer_id='26000123'").get(),beforeCustomer);
assert.deepEqual(db.prepare("SELECT * FROM customer_family_members WHERE id='fm_existing'").get(),beforeFamily);
const indexes=db.prepare("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_customer_%' ORDER BY name").all().map(x=>x.name);
for(const name of ['idx_customer_profile_enrichment_lead','idx_customer_profile_enrichment_contact','idx_customer_profile_enrichment_referrer','idx_customer_family_member_metadata_order','idx_customer_field_evidence_dedupe','idx_customer_field_evidence_pending','idx_customer_field_evidence_field','idx_customer_notes_history_customer'])assert.ok(indexes.includes(name),name);
console.log('PROFILE_MIGRATION_LOCAL_APPLY=PASS');
console.log('PROFILE_MIGRATION_IDEMPOTENT=PASS');
console.log('EXISTING_CUSTOMERS_PRESERVED=PASS');
console.log('EXISTING_FAMILY_MEMBERS_PRESERVED=PASS');
console.log('DESTRUCTIVE_SCHEMA_CHANGES=0');
