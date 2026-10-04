import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-storage-preflight.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-storage-preflight-from-issue.yml','utf8');

assert.match(workflow,/workflow_dispatch:\s*\n\s*inputs:\s*\n\s*expected_sha:/);
assert.ok(workflow.includes("mode:\n        description: Inventory account R2 buckets or verify one explicit candidate"));
assert.ok(workflow.includes("candidate_bucket_name:"));
assert.ok(workflow.includes("CURRENT_MAIN_EXACT_GATE=PASS"));
assert.ok(workflow.includes("CANONICAL_R2_BINDING_DECLARED=NO"));
assert.ok(workflow.includes("public_asset_adapter:null"));
assert.ok(workflow.includes("private_media_storage_adapter:null"));
assert.ok(workflow.includes("MEMBER_PRODUCTION_ROUTE_MODE_ENABLED"));
assert.ok(workflow.includes("PRIVATE_MEDIA_ROUTE_MODE_ENABLED"));
assert.ok(workflow.includes('wrangler@4.131.2'));
assert.ok(workflow.includes('wrangler deployments status --name customer-crm-api --json'));
assert.ok(workflow.includes('/r2/buckets?per_page=1000&order=name&direction=asc'));
assert.ok(workflow.includes('R2_BUCKET_INVENTORY_SHA256='));
assert.ok(workflow.includes('CANDIDATE_R2_BUCKET_NAME_SHA256='));
assert.ok(workflow.includes('CANDIDATE_R2_BUCKET_EXISTS=PASS'));
assert.ok(workflow.includes('R2_OBJECT_READ=0'));
assert.ok(workflow.includes('R2_WRITE=0'));
assert.ok(workflow.includes('PRODUCTION_STORAGE_BINDING_CHANGE=0'));
assert.ok(workflow.includes('PRODUCTION_STORAGE_FETCH=0'));
assert.ok(workflow.includes('PRODUCTION_DEPLOY=0'));
assert.ok(workflow.includes('PRODUCTION_TRAFFIC_CHANGE=0'));
assert.ok(workflow.includes('PRODUCTION_D1_WRITE=0'));
assert.ok(workflow.includes('ACTIVE_PRODUCTION_VERSION_STABLE=PASS'));
assert.ok(workflow.includes('CURRENT_MAIN_STABLE=PASS'));
assert.ok(workflow.includes('R2_BUCKET_INVENTORY_PAGINATION_UNSUPPORTED'));
assert.ok(workflow.includes('INVENTORY_MODE_FORBIDS_CANDIDATE_BUCKET'));
assert.ok(workflow.includes('INVALID_CANDIDATE_BUCKET_NAME'));

assert.doesNotMatch(workflow,/wrangler\s+r2\s+object\s+(get|put|delete)/i);
assert.doesNotMatch(workflow,/wrangler\s+r2\s+bucket\s+(create|delete)/i);
assert.doesNotMatch(workflow,/wrangler\s+(deploy|versions\s+deploy)/i);
assert.doesNotMatch(workflow,/wrangler\s+d1\s+execute/i);
assert.doesNotMatch(workflow,/client\/v4\/accounts\/\$CLOUDFLARE_ACCOUNT_ID\/r2\/buckets\/[^?"\s]+\/objects/i);
assert.doesNotMatch(workflow,/--request\s+(POST|PUT|PATCH|DELETE)/i);

assert.ok(bridge.includes("run-name: Production read-only bridge: storage-preflight"));
assert.ok(bridge.includes("github.event.issue.number == 26"));
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("/member-production-storage-preflight "));
assert.ok(bridge.includes("mode=(inventory|verify)"));
assert.ok(bridge.includes("candidate_bucket="));
assert.ok(bridge.includes("MEMBER_STORAGE_PREFLIGHT_AUTHORIZED_SHA="));
assert.ok(bridge.includes("MAIN_DRIFT"));
assert.ok(bridge.includes("member-production-storage-preflight.yml/dispatches"));
assert.ok(bridge.includes("PRODUCTION_STORAGE_BINDING_CHANGE=0"));
assert.ok(bridge.includes("PRODUCTION_STORAGE_FETCH=0"));
assert.ok(bridge.includes("R2_OBJECT_READ=0"));
assert.ok(bridge.includes("R2_WRITE=0"));
assert.ok(bridge.includes("PRODUCTION_DEPLOY=0"));
assert.ok(bridge.includes("PRODUCTION_TRAFFIC_CHANGE=0"));
assert.ok(bridge.includes("PRODUCTION_D1_WRITE=0"));

console.log('MEMBER_PRODUCTION_STORAGE_PREFLIGHT_CONTRACT=PASS');
