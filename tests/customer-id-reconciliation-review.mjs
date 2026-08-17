import fs from 'node:fs';
const src=fs.readFileSync('src/crm-reconciliation-review.mjs','utf8');
const entry=fs.readFileSync('src/production-index-crm-browser-root-entry.js','utf8');
const migration=fs.readFileSync('migrations/20260817_customer_id_reconciliation_reviews.sql','utf8');
const workflow=fs.readFileSync('.github/workflows/apply-customer-id-reconciliation-review.yml','utf8');
const required=[
  'SAME_PERSON','DIFFERENT_PERSON','DEFERRED','UNREVIEWED',
  '/admin/customer-id-reconciliation',
  '/api/customer-id-reconciliation/reviews',
  'customer_id_reconciliation_review_audit',
  'RESERVATION_SERVICE','CRM_INTERNAL_TOKEN',
  '全'+"'"+'+c.total+',
  '@media(max-width:760px)',
  'merge/customer_id変更は行いません'
];
for(const x of required) if(!src.includes(x)) throw new Error('missing '+x);
if(!entry.includes('handleReconciliationReview')) throw new Error('entry handler missing');
if(!entry.includes('顧客ID照合')) throw new Error('admin navigation link missing');
for(const bad of [
  /UPDATE\s+customers\s+SET/i,
  /UPDATE\s+customer_reservations\s+SET/i,
  /DELETE\s+FROM\s+customers/i,
  /INSERT\s+INTO\s+customers/i,
  /UPDATE\s+app_reservations/i
]) if(bad.test(src)) throw new Error('forbidden customer mutation '+bad);
if(!/UPDATE customer_id_reconciliation_reviews SET decision=/i.test(src)) throw new Error('review decision write missing');
if(!/INSERT INTO customer_id_reconciliation_review_audit/i.test(src)) throw new Error('audit write missing');
for(const bad of [/\bDROP\b/i,/\bALTER\b/i,/\bDELETE\b/i,/\bUPDATE\b/i,/\bINSERT\b/i,/\bREPLACE\b/i,/\bTRUNCATE\b/i]) if(bad.test(migration)) throw new Error('forbidden migration statement '+bad);
for(const x of ['CREATE TABLE IF NOT EXISTS customer_id_reconciliation_reviews','CREATE TABLE IF NOT EXISTS customer_id_reconciliation_review_audit']) if(!migration.includes(x)) throw new Error('migration missing '+x);
if(!workflow.includes('expected 20')) throw new Error('fixed 20 candidate guard missing');
if(!workflow.includes('values intentionally not logged')) throw new Error('PII log guard marker missing');
if(!workflow.includes('INSERT OR IGNORE INTO customer_id_reconciliation_reviews')) throw new Error('seed is not review-storage-only');
console.log('Customer ID reconciliation review: safety + UI invariants PASS');
