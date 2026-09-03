import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
const canonical='https://customer-crm-api.ohw3rz5578d277e.workers.dev';

assert.ok(workflow.includes(canonical),'canonical Production Worker URL missing from release verifier');
assert.ok(workflow.includes('PRODUCTION_BASE_URL'),'Production health verifier must use an explicit canonical base URL');
assert.ok(!workflow.includes('DEPLOYMENT_URL: ${{ steps.deploy_worker.outputs.deployment-url }}'),'health verifier must not treat wrangler deployment-url output as canonical Worker root');
assert.match(workflow,/CF_ACCESS_CLIENT_ID:\s*\$\{\{ secrets\.CF_ACCESS_CLIENT_ID \}\}/);
assert.match(workflow,/CF_ACCESS_CLIENT_SECRET:\s*\$\{\{ secrets\.CF_ACCESS_CLIENT_SECRET \}\}/);
assert.match(workflow,/ADMIN_TOKEN:\s*\$\{\{ secrets\.ADMIN_TOKEN \}\}/);
assert.ok(workflow.includes('-H "CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID"'));
assert.ok(workflow.includes('-H "CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET"'));
assert.ok(workflow.includes('-H "x-admin-token: $ADMIN_TOKEN"'));
assert.ok(!/echo[^\n]*(CF_ACCESS_CLIENT_SECRET|ADMIN_TOKEN)\s*=\s*\$/i.test(workflow),'release verifier must not echo secret values');
for(const migration of ['20260828_customer360_family_marketing_foundation.sql','20260903_customer360_profile_auto_enrichment.sql'])assert.ok(workflow.includes(migration),`explicit migration allowlist missing: ${migration}`);
assert.ok(!/migrations_managed\/\*\.sql/.test(workflow),'migration allowlist must not become globbed');

console.log('CUSTOMER360_PRODUCTION_HEALTH_CANONICAL_URL=PASS');
console.log('CUSTOMER360_PRODUCTION_HEALTH_SERVER_AUTH=PASS');
