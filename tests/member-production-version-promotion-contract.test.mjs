import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-version-promotion.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-version-promotion-from-issue.yml','utf8');
const foundation=fs.readFileSync('.github/workflows/member-app-foundation.yml','utf8');

for(const exact of [
  '41059eb0ca192f29f790abfd4563552581b1a6b8',
  '6dd49589-f01d-473f-876a-034563023b0e',
  '36285531724',
  'PROMOTE_MEMBER_STAGED_VERSION'
]){
  assert.ok(workflow.includes(exact), 'workflow missing exact gate: '+exact);
}
for(const input of ['expected_sha','staged_version_id','staging_run_id','owner_comment_id','confirmation']){
  assert.ok(workflow.includes(input+':'), 'missing workflow input: '+input);
}
assert.ok(workflow.includes('MAIN_DRIFT current=$current_main expected=$EXPECTED_SHA'));
assert.ok(workflow.includes('git merge-base --is-ancestor "$STAGING_SOURCE_SHA" "$EXPECTED_SHA"'));
assert.ok(workflow.includes('OWNER_PROMOTION_AUTHORIZATION_COMMENT=PASS'));
assert.ok(workflow.includes("'.github/workflows/member-production-runtime-secret-stage.yml'"));
assert.ok(workflow.includes("r.get('conclusion')=='success'"));
assert.ok(workflow.includes('MEMBER_RUNTIME_SECRET_STAGED_VERSION_ID=$STAGED_VERSION_ID'));
assert.ok(workflow.includes('MEMBER_STAGED_VERSION_LINEAGE_RECEIPT=PASS'));
assert.ok(workflow.includes('ACTIVE_PRODUCTION_DEPLOYMENT_FRESH_SNAPSHOT=PASS'));
assert.ok(workflow.includes('ACTIVE_PRODUCTION_DEPLOYMENT_DRIFTED_BEFORE_PROMOTION'));
assert.ok(workflow.includes('ACTIVE_PRODUCTION_DEPLOYMENT_STABLE_BEFORE_PROMOTION=PASS'));
assert.ok(workflow.includes('STAGED_VERSION_ALREADY_PRESENT_IN_ACTIVE_DEPLOYMENT'));
assert.ok(workflow.includes('npx wrangler versions deploy "$STAGED_VERSION_ID@100%"'));
assert.ok(workflow.includes('--name customer-crm-api'));
assert.ok(workflow.includes('--yes'));
assert.ok(workflow.includes('PROMOTED_VERSION_NOT_PRESENT_IN_ACTIVE_DEPLOYMENT'));

const deployMatches=workflow.match(/\bnpx wrangler versions deploy\b/g)||[];
assert.equal(deployMatches.length,1,'versions deploy must appear exactly once');
assert.doesNotMatch(workflow,/\bnpx wrangler deploy\b/);
assert.doesNotMatch(workflow,/\bwrangler d1\b/i);
assert.doesNotMatch(workflow,/\bwrangler secret (put|bulk|delete)\b/i);
assert.doesNotMatch(workflow,/\bwrangler versions secret (put|bulk|delete)\b/i);

assert.ok(bridge.includes("github.event.issue.number == 26"));
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("github.event.comment.user.login == 'ohw3rz5578d277e-collab'"));
assert.ok(bridge.includes("staged_version=(6dd49589-f01d-473f-876a-034563023b0e)"));
assert.ok(bridge.includes("staging_run=(36285531724)"));
assert.ok(bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha'));
assert.ok(bridge.includes('member-production-version-promotion.yml/dispatches'));
assert.doesNotMatch(bridge,/wrangler\s+versions\s+deploy/i);
assert.doesNotMatch(bridge,/wrangler\s+deploy/i);

for(const path of [
  '.github/workflows/member-production-version-promotion.yml',
  '.github/workflows/dispatch-member-production-version-promotion-from-issue.yml',
  'tests/member-production-version-promotion-contract.test.mjs'
]){
  assert.ok(foundation.includes(path), 'Member foundation scope missing: '+path);
}

console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_CONTRACT=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_OWNER_GATE=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_FRESH_SNAPSHOT_GATE=PASS');
console.log('MEMBER_PRODUCTION_VERSION_PROMOTION_SOURCE_ONLY_PR_GATE=PASS');
