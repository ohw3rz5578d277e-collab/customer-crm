import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/customer360-production-safe-read.yml','utf8');

assert.ok(workflow.includes("await page.route(base+'/**'"),'Production browser verifier must authenticate through the Node-side route proxy');
assert.ok(workflow.includes('serverAuth'),'server-side authentication boundary missing');
assert.ok(workflow.includes('browserSecretHeaders'),'browser secret-header guard missing');
assert.ok(workflow.includes('PRODUCTION_BROWSER_SECRET_HEADERS=0'),'browser secret evidence marker missing');
assert.ok(!workflow.includes('extraHTTPHeaders:auth'),'Production secrets must not be installed as browser extraHTTPHeaders');
assert.ok(!workflow.includes('extraHTTPHeaders:serverAuth'),'Production secrets must not be installed as browser extraHTTPHeaders');
assert.ok(workflow.includes("if(req.method()!=='GET'&&req.method()!=='HEAD')"),'browser write fail-closed guard missing');
assert.ok(workflow.includes("route.abort('blockedbyclient')"),'browser non-read request must be blocked before Production');
for(const header of ['cf-access-client-id','cf-access-client-secret','x-admin-token','authorization','x-internal-token'])assert.ok(workflow.includes(header),`browser secret-header detector missing: ${header}`);

console.log('CUSTOMER360_PRODUCTION_BROWSER_SECRET_BOUNDARY=PASS');
console.log('PRODUCTION_BROWSER_WRITES=0_CONTRACT=PASS');
