import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/production-browser-smoke-auth.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-production-deploy-from-issue.yml','utf8');
const legacyTrigger=fs.readFileSync('.github/workflows/trigger-production-browser-smoke-temp.yml','utf8');

assert.match(workflow,/workflow_dispatch:\s*\n\s*inputs:\s*\n\s*expected_sha:/,'auth smoke must require expected_sha');
assert.match(workflow,/required:\s*true/,'expected_sha must be required');
assert.ok(workflow.includes("if [[ ! \"$RAW_EXPECTED_SHA\" =~ ^[0-9a-f]{40}$ ]]"),'expected SHA format must fail closed');
assert.ok(workflow.includes('Checkout exact authorized SHA'),'auth smoke must checkout exact SHA');
assert.ok(workflow.includes('ref: ${{ steps.authorized_sha.outputs.sha }}'),'checkout must use validated SHA');
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'),'auth smoke must read fresh main');
assert.ok(workflow.includes('MAIN_DRIFT expected=$EXPECTED_SHA current=$remote_main'),'main drift must fail closed');
assert.ok(workflow.includes('EXPECTED_SHA_GUARD=PASS'),'exact-SHA gate marker missing');
assert.match(workflow,/concurrency:\s*\n\s*group: customer-crm-production-deploy/,'auth smoke must share Production deploy lock');
assert.ok(workflow.includes("const HEALTH_PATHS = ['/health','/api/crm-health-check'];"),'both owned health paths must be probed');
assert.ok(workflow.includes('maxRedirects: 0'),'health probe must not follow redirects');
assert.ok(workflow.includes("res.headers()['location'] || ''"),'redirect location must be inspected');
assert.ok(workflow.includes("redirectTarget = u.origin + u.pathname"),'redirect logging must strip query values');
assert.ok(workflow.includes('ACCESS_AUTH_FAILED'),'Cloudflare Access redirect must be classified');
assert.ok(workflow.includes('CF-Access-Client-Id'),'service-token client id header missing');
assert.ok(workflow.includes('CF-Access-Client-Secret'),'service-token client secret header missing');
assert.ok(workflow.includes("'x-admin-token': ADMIN_TOKEN"),'admin auth header missing');
assert.ok(workflow.includes("const res = await request.get"),'health smoke must remain GET-only');
assert.ok(workflow.includes('decision POST: 0'),'write safety evidence missing');
assert.ok(workflow.includes('CRM write: 0'),'CRM write safety evidence missing');
assert.ok(workflow.includes('Reservation write: 0'),'Reservation write safety evidence missing');
assert.ok(workflow.includes('LINE write: 0'),'LINE write safety evidence missing');
assert.doesNotMatch(workflow,/\bwrangler\s+deploy\b/i,'auth smoke must not deploy Worker');
assert.doesNotMatch(workflow,/\bd1\s+migrations\s+apply\b/i,'auth smoke must not apply migrations');
assert.doesNotMatch(workflow,/\bd1\s+execute\b/i,'auth smoke must not execute D1');
assert.doesNotMatch(workflow,/request\.(post|put|patch|delete)\s*\(/i,'auth smoke must not issue mutation API requests');

assert.ok(bridge.includes("auth_smoke_re='^/crm-production-auth-smoke sha=([0-9a-f]{40})$'"),'bridge auth-smoke command must be exact');
assert.ok(bridge.includes("release_mode='auth-smoke'"),'bridge must classify auth smoke separately');
assert.ok(bridge.includes("dispatch_workflow='production-browser-smoke-auth.yml'"),'bridge must target read-only auth smoke workflow');
assert.ok(bridge.includes("inputs={'expected_sha':sha}"),'auth smoke dispatch must carry exact SHA only');
assert.ok(!/auth-smoke[\s\S]{0,500}confirm_production=true/.test(bridge),'auth smoke must not inherit deploy confirmation semantics');
assert.ok(legacyTrigger.includes('EXPECTED_SHA: ${{ github.sha }}'),'legacy trigger must pin the triggering main SHA');
assert.ok(legacyTrigger.includes('-f "inputs[expected_sha]=$EXPECTED_SHA"'),'legacy trigger must pass expected_sha to auth smoke workflow');
assert.ok(legacyTrigger.includes('^[[0-9a-f]{40}$')===false,'legacy trigger contract must not use malformed SHA regex text');
assert.ok(legacyTrigger.includes('^[0-9a-f]{40}$'),'legacy trigger must validate lowercase 40-hex SHA');

console.log('PRODUCTION_ACCESS_AUTH_SMOKE_EXACT_SHA=PASS');
console.log('PRODUCTION_ACCESS_AUTH_SMOKE_NO_FOLLOW_REDIRECT=PASS');
console.log('PRODUCTION_ACCESS_AUTH_SMOKE_READ_ONLY=PASS');
console.log('PRODUCTION_ACCESS_AUTH_SMOKE_BRIDGE_ROUTING=PASS');
