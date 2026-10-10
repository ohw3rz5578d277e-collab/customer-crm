import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
const guard=fs.readFileSync('scripts/assert-customer-identity-sequence-monotonic.mjs','utf8');

const classifyStep='- name: Classify remote pending migrations and read-only state';
const predeployStep='- name: Assert identity sequence monotonic before deploy';
const deployStep='- name: Deploy customer-crm-api';
const postdeployStep='- name: Assert D1 verification results';
const guardInvocation='node scripts/assert-customer-identity-sequence-monotonic.mjs';

const classifyIndex=workflow.indexOf(classifyStep);
const predeployIndex=workflow.indexOf(predeployStep);
const deployIndex=workflow.indexOf(deployStep);
const postdeployIndex=workflow.indexOf(postdeployStep);
assert.ok(classifyIndex>=0,'classification step missing');
assert.ok(predeployIndex>classifyIndex,'predeploy identity guard must follow classification');
assert.ok(deployIndex>predeployIndex,'Worker deploy must remain after predeploy identity guard');
assert.ok(postdeployIndex>deployIndex,'post-deploy identity verification must remain after Worker deploy');
const between=workflow.slice(predeployIndex,deployIndex);
assert.ok(between.includes(guardInvocation),'late current-main guard script must execute immediately before Worker deploy');
assert.ok(workflow.includes("RELEASE_MODE: ${{ inputs.mode }}"),'release mode must remain available to late deploy guard');
assert.ok(workflow.includes('echo "EXPECTED_SHA=$normalized_sha" >> "$GITHUB_ENV"'),'exact authorized SHA must remain available to late deploy guard');

assert.ok(guard.includes("execFileSync('git', ['rev-parse', 'HEAD']"),'late guard must re-read exact checkout SHA');
assert.ok(guard.includes("execFileSync('git', ['ls-remote', 'origin', 'refs/heads/main']"),'late guard must fresh-read remote main');
assert.ok(guard.includes('BLOCKED_FINAL_CHECKOUT_SHA_MISMATCH'),'late guard must fail closed on checkout drift');
assert.ok(guard.includes('BLOCKED_FINAL_CURRENT_MAIN_SHA_MISMATCH'),'late guard must fail closed on main drift');
assert.ok(guard.includes("mode !== 'deploy'"),'late remote-main lookup must remain deploy-mode-only');
assert.ok(guard.includes('classification_invocation'),'classification pass must not consume the final deploy drift check');
assert.ok(guard.includes('FINAL_CURRENT_MAIN_GUARD_DONE'),'successful predeploy check must persist a one-job completion marker');
assert.ok(guard.includes('already_checked_predeploy'),'post-deploy sequence verification must skip the drift check');
assert.ok(guard.includes('FINAL_CURRENT_MAIN_GUARD=PASS'),'successful late guard must emit an auditable receipt marker');

console.log('DEPLOY_FINAL_CURRENT_MAIN_DRIFT_GUARD_CONTRACT=PASS');
