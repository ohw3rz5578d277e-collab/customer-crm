import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-storage-preflight.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-storage-preflight-from-issue.yml','utf8');

assert.match(workflow,/workflow_dispatch:\s*\n\s*inputs:\s*\n\s*expected_sha:/);
assert.ok(workflow.includes("mode:\n        description: Inventory account R2 buckets or verify one explicit candidate digest"));
assert.ok(workflow.includes("candidate_bucket_sha256:"));
assert.doesNotMatch(workflow,/candidate_bucket_name:\s*\n/);
assert.ok(workflow.includes("CURRENT_MAIN_EXACT_GATE=PASS"));
assert.ok(workflow.includes("CANONICAL_R2_BINDING_DECLARED=NO"));
assert.ok(workflow.includes("public_asset_adapter:null"));
assert.ok(workflow.includes("private_media_storage_adapter:null"));
assert.ok(workflow.includes("MEMBER_PRODUCTION_ROUTE_MODE_ENABLED"));
assert.ok(workflow.includes("PRIVATE_MEDIA_ROUTE_MODE_ENABLED"));
assert.ok(workflow.includes('wrangler@4.131.2'));
assert.ok(workflow.includes('wrangler deployments status --name customer-crm-api --json'));
assert.ok(workflow.includes('jurisdictions=(default eu us fedramp fedramp-high)'));
assert.ok(workflow.includes('cf-r2-jurisdiction: $jurisdiction'));
assert.ok(workflow.includes('/r2/buckets?per_page=1000&order=name&direction=asc'));
assert.ok(workflow.includes("const jurisdictions=['default','eu','us','fedramp','fedramp-high'];"));
assert.ok(workflow.includes('R2_JURISDICTIONS_SCANNED='));
assert.ok(workflow.includes('R2_BUCKET_INVENTORY_SHA256='));
assert.ok(workflow.includes('CANDIDATE_R2_BUCKET_NAME_SHA256='));
assert.ok(workflow.includes('CANDIDATE_R2_BUCKET_EXISTS=PASS'));
assert.ok(workflow.includes('CANDIDATE_R2_BUCKET_DIGEST_NOT_FOUND'));
assert.ok(workflow.includes('CANDIDATE_R2_BUCKET_DIGEST_NOT_UNIQUE_ACROSS_JURISDICTIONS'));
assert.ok(workflow.includes('INVENTORY_MODE_FORBIDS_CANDIDATE_BUCKET_DIGEST'));
assert.ok(workflow.includes('INVALID_CANDIDATE_BUCKET_SHA256'));
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

// Both R2 auth and inventory shells must be syntax-checked by PR CI.
assert.ok(workflow.includes('- name: Verify R2 auth and inventory shell syntax'));
assert.ok(workflow.includes('MEMBER_STORAGE_PREFLIGHT_R2_SHELL_SYNTAX=PASS'));
assert.ok(workflow.includes('bash -n /tmp/member-storage-r2-step-0.sh'));
assert.ok(workflow.includes('bash -n /tmp/member-storage-r2-step-1.sh'));
assert.doesNotMatch(workflow,/error_codes="\$\(node - "\$response_file" <<'NODE'/);
assert.ok(workflow.includes("error_codes=\"$(node -e 'const fs=require(\"fs\");"));

// R2 inventory must use a dedicated least-privilege Cloudflare REST API token,
// never the Worker-management token or R2 S3 credentials.
assert.ok(workflow.includes('- name: Verify dedicated R2 REST API token'));
assert.ok(workflow.includes('CLOUDFLARE_R2_READ_API_TOKEN: ${{ secrets.CLOUDFLARE_R2_READ_API_TOKEN }}'));
assert.ok(workflow.includes('CLOUDFLARE_R2_READ_API_TOKEN_MISSING'));
assert.ok(workflow.includes('R2_READ_REST_TOKEN_FORMAT_INVALID_WHITESPACE'));
assert.ok(workflow.includes('https://api.cloudflare.com/client/v4/user/tokens/verify'));
assert.ok(workflow.includes('R2_READ_REST_TOKEN_VERIFY_HTTP_${code}_CF_CODES_${error_codes}'));
assert.ok(workflow.includes('R2_READ_REST_TOKEN_NOT_ACTIVE'));
assert.ok(workflow.includes('R2_READ_REST_TOKEN_AUTH=PASS'));
assert.ok(workflow.includes('R2_READ_REST_TOKEN_VALUE_PRINTED=NO'));
assert.ok(workflow.includes('R2_AUTH_EXPECTED_CREDENTIAL=Cloudflare REST API token (Bearer), not R2 S3 Access Key ID/Secret Access Key'));
assert.ok(workflow.includes('Authorization: Bearer $CLOUDFLARE_R2_READ_API_TOKEN'));
assert.ok(workflow.includes('R2_AUTH_SECRET=CLOUDFLARE_R2_READ_API_TOKEN'));
const inventoryStep=workflow.slice(
  workflow.indexOf('- name: Read every R2 jurisdiction inventory without object access'),
  workflow.indexOf('- name: Verify active Production version and main did not drift'),
);
assert.doesNotMatch(inventoryStep,/Authorization: Bearer \$CLOUDFLARE_API_TOKEN/);

// Failure diagnostics expose Cloudflare numeric error codes only; no response messages/body are printed.
assert.ok(workflow.includes('CF_CODES_${error_codes}'));
assert.ok(workflow.includes('value?.code'));
assert.doesNotMatch(workflow,/console\.log\([^\n]*payload\?\.errors[^\n]*message/);
assert.doesNotMatch(workflow,/cat\s+[^\n]*(r2-token-verify|r2-buckets)/i);

// Postflight Worker/main drift verification must still execute after an R2 auth or inventory failure.
assert.ok(workflow.includes("if: ${{ always() && steps.active_before.outcome == 'success' }}"));
assert.ok(workflow.includes("R2_AUTH_OUTCOME: ${{ steps.r2_auth.outcome }}"));
assert.ok(workflow.includes("INVENTORY_OUTCOME: ${{ steps.inventory.outcome }}"));
assert.ok(workflow.includes('MEMBER_PRODUCTION_STORAGE_PREFLIGHT=FAIL_CLOSED'));

assert.doesNotMatch(workflow,/wrangler\s+r2\s+object\s+(get|put|delete)\b/i);
assert.doesNotMatch(workflow,/wrangler\s+r2\s+bucket\s+(create|delete)\b/i);
assert.doesNotMatch(workflow,/wrangler\s+(?:deploy\b|versions\s+deploy\b)/i);
assert.doesNotMatch(workflow,/wrangler\s+d1\s+execute\b/i);
assert.doesNotMatch(workflow,/client\/v4\/accounts\/\$CLOUDFLARE_ACCOUNT_ID\/r2\/buckets\/[^?"\s]+\/objects/i);
assert.doesNotMatch(workflow,/--request\s+(POST|PUT|PATCH|DELETE)\b/i);

assert.ok(bridge.includes("run-name: 'Production read-only bridge: storage-preflight'"));
assert.ok(bridge.includes("github.event.issue.number == 26"));
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("/member-production-storage-preflight "));
assert.ok(bridge.includes("mode=(inventory|verify)"));
assert.ok(bridge.includes("candidate_bucket_sha256=([0-9a-f]{64})"));
assert.doesNotMatch(bridge,/candidate_bucket=\(\[a-z0-9\]/);
assert.ok(bridge.includes("CANDIDATE_BUCKET_NAME_EXPOSED=NO"));
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
