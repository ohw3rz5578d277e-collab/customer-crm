import fs from 'node:fs';
import assert from 'node:assert/strict';

const entry=fs.readFileSync('src/production-index-crm-browser-root-entry.js','utf8');
const wrangler=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
const core=fs.readFileSync('src/index.js','utf8');
const secure=fs.readFileSync('src/secure-index.js','utf8');
const lineContext=fs.readFileSync('src/crm-line-context-events.mjs','utf8');
const draftCsv=fs.readFileSync('src/production-index-line-pending-csv.js','utf8');
const resolver=fs.readFileSync('src/customer-identity-resolver.mjs','utf8');
const registry=fs.readFileSync('migrations_managed/20260818_customer_identity_registry.sql','utf8');
const sequence=fs.readFileSync('migrations_managed/20260818_customer_identity_sequence.sql','utf8');

assert.equal(wrangler.main,'src/production-index-crm-browser-root-entry.js');
assert.match(entry,/production-index-crm-delivery-deadline-alerts-entry/);
assert.match(entry,/handleCustomerIdentityResolver/);assert.match(entry,/handleLineContextEvents/);assert.match(entry,/handleInternalCustomerDetail/);assert.match(entry,/handleReconciliationReview/);
assert.match(core,/customer_id TEXT PRIMARY KEY/);assert.match(core,/ON CONFLICT\(customer_id\) DO UPDATE SET/);
assert.match(secure,/customer_id LIKE \?/);assert.match(secure,/decodeURIComponent\(path\.replace\("\/api\/customers\/"/);
assert.match(lineContext,/line-context-events/);assert.match(draftCsv,/customer_line_draft_logs/);assert.match(draftCsv,/line-message-logs\/pending\.csv/);
assert.match(resolver,/WHERE line_user_id=\?/);assert.doesNotMatch(resolver,/customer_name\s*=\s*\?/);assert.doesNotMatch(resolver,/Math\.random|randomUUID|crypto\.randomUUID/);
assert.doesNotMatch(resolver,/"C"\s*\+\s*String/);assert.match(resolver,/YY\+6digit-global-sequence/);assert.match(resolver,/Asia\/Tokyo/);assert.match(resolver,/UPDATE customer_identity_sequence[\s\S]*RETURNING last_value/);
assert.match(registry,/line_user_id TEXT NOT NULL UNIQUE/);assert.match(registry,/idempotency_key TEXT NOT NULL UNIQUE/);assert.match(registry,/customer_id TEXT UNIQUE/);
assert.match(sequence,/customer_identity_sequence/);assert.match(sequence,/last_value INTEGER NOT NULL/);assert.match(sequence,/SUBSTR\(customer_id, 3, 6\)/);assert.doesNotMatch(sequence,/\b(?:DROP|DELETE|UPDATE|REPLACE|TRUNCATE)\b/i);
console.log('customer identity CRM compatibility/static regression PASS');
