import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
const guard=fs.readFileSync('scripts/assert-customer-identity-sequence-monotonic.mjs','utf8');

const predeployStep='- name: Assert identity sequence monotonic before deploy';
const deployStep='- name: Deploy customer-crm-api';
const guardInvocation='node scripts/assert-customer-identity-sequence-monotonic.mjs';

const predeployIndex=workflow.indexOf(predeployStep);
const deployIndex=workflow.indexOf(deployStep);
assert.ok(predeployIndex>=0,'predeploy identity guard step missing');
assert.ok(deployIndex>predeployIndex,'Worker deploy must remain after predeploy identity guard');
const between=workflow.slice(predeployIndex,deployIndex);
assert.ok(between.includes(guardInvocation),'late current-main guard script must execute immediately before Worker deploy');
assert.ok(workflow.includes("RELEASE_MODE: ${{ inputs.mode }}"),'release mode must remain available to late deploy guard');
assert.ok(workflow.includes('echo "EXPECTED_SHA=$normalized_sha" >> "$GITHUB_ENV"'),'exact authorized SHA must remain available to late deploy guard');

assert.ok(guard.includes("execFileSync('git', ['rev-parse', 'HEAD']"),'late guard must re-read exact checkout SHA');
assert.ok(guard.includes("execFileSync('git', ['ls-remote', 'origin', 'refs/heads/main']"),'late guard must fresh-read remote main');
assert.ok(guard.includes('BLOCKED_FINAL_CHECKOUT_SHA_MISMATCH'),'late guard must fail closed on checkout drift');
assert.ok(guard.includes('BLOCKED_FINAL_CURRENT_MAIN_SHA_MISMATCH'),'late guard must fail closed on main drift');
assert.ok(guard.includes("mode !== 'deploy'"),'late remote-main lookup must remain deploy-mode-only');
assert.ok(guard.includes('FINAL_CURRENT_MAIN_GUARD=PASS'),'successful late guard must emit an auditable receipt marker');

console.log('DEPLOY_FINAL_CURRENT_MAIN_DRIFT_GUARD_CONTRACT=PASS');
