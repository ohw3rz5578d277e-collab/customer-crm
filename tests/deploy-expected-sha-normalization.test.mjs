import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
const SHA='87c9c31f595b408d158a9e75f9ca48db91ceac7c';

function normalizeExpectedSha(raw){
  const normalized=String(raw ?? '').trim().toLowerCase();
  if(!/^[0-9a-f]{40}$/.test(normalized)) throw new Error('INVALID_EXPECTED_SHA_FORMAT');
  return normalized;
}
function exactGate(expected,checkout,currentMain){
  if(checkout!==expected) throw new Error('BLOCKED_CHECKOUT_SHA_MISMATCH');
  if(currentMain!==expected) throw new Error('BLOCKED_CURRENT_MAIN_SHA_MISMATCH');
  return true;
}

// A-E: valid input normalization.
assert.equal(normalizeExpectedSha(SHA),SHA);
assert.equal(normalizeExpectedSha(`   ${SHA}`),SHA);
assert.equal(normalizeExpectedSha(`${SHA}   `),SHA);
assert.equal(normalizeExpectedSha(`\t  ${SHA} \t\n`),SHA);
assert.equal(normalizeExpectedSha(SHA.toUpperCase()),SHA);

// F-J: malformed input remains fail-closed. Internal whitespace is never removed.
for(const bad of [
  SHA.slice(0,39),
  `${SHA}0`,
  `${SHA.slice(0,39)}g`,
  `${SHA.slice(0,8)} ${SHA.slice(8)}`,
  '',
]) assert.throws(()=>normalizeExpectedSha(bad),/INVALID_EXPECTED_SHA_FORMAT/);

// K-L: normalized authorization still requires exact checkout and live-main equality.
assert.equal(exactGate(SHA,SHA,SHA),true);
assert.throws(()=>exactGate(SHA,`0${SHA.slice(1)}`,SHA),/BLOCKED_CHECKOUT_SHA_MISMATCH/);
assert.throws(()=>exactGate(SHA,SHA,`0${SHA.slice(1)}`),/BLOCKED_CURRENT_MAIN_SHA_MISMATCH/);

// Workflow contract: raw dispatch input is read only through an env boundary.
assert.ok(workflow.includes('- name: Normalize and validate authorized SHA'),'normalization must be the first release step');
assert.ok(workflow.includes('id: authorized_sha'));
assert.ok(workflow.includes('RAW_EXPECTED_SHA: ${{ inputs.expected_sha }}'));
assert.ok(workflow.includes("sys.stdin.read().strip().lower()"),'boundary whitespace trim/lowercase normalization missing');
assert.ok(workflow.includes('^[0-9a-f]{40}$'),'exact 40-hex validation missing');
assert.ok(workflow.includes("::error::INVALID_EXPECTED_SHA_FORMAT"));
assert.ok(workflow.includes("::error::INVALID_RELEASE_MODE"));
assert.ok(workflow.includes('echo "EXPECTED_SHA=$normalized_sha" >> "$GITHUB_ENV"'));
assert.ok(workflow.includes('ref: ${{ steps.authorized_sha.outputs.sha }}'),'checkout must use normalized output');
assert.ok(!workflow.includes('ref: ${{ inputs.expected_sha }}'),'raw input must never be a checkout ref');
assert.ok(!workflow.includes('EXPECTED_SHA: ${{ inputs.expected_sha }}'),'raw input must not remain the canonical expected SHA');

const releaseStart=workflow.indexOf('  release:');
const normalizeStep=workflow.indexOf('- name: Normalize and validate authorized SHA',releaseStart);
const checkoutStep=workflow.indexOf('- name: Checkout exact authorized SHA',releaseStart);
assert.ok(releaseStart>=0 && normalizeStep>releaseStart && checkoutStep>normalizeStep,'normalization must occur before checkout');

for(const token of [
  'AUTHORIZED_SHA=$EXPECTED_SHA',
  'CHECKOUT_SHA=$actual_sha',
  'CURRENT_MAIN_SHA=$remote_main',
  'RELEASE_MODE=$RELEASE_MODE',
  'BLOCKED_CHECKOUT_SHA_MISMATCH',
  'BLOCKED_CURRENT_MAIN_SHA_MISMATCH',
  'git rev-parse HEAD',
  'git ls-remote origin refs/heads/main',
]) assert.ok(workflow.includes(token),`release diagnostic/guard missing: ${token}`);

assert.match(workflow,/preflight\|deploy/,'release mode allow-list must remain exact');
assert.ok(workflow.includes("inputs.mode == 'preflight'"),'preflight semantics changed');
assert.ok(workflow.includes("inputs.mode == 'deploy'"),'deploy semantics changed');

console.log('EXPECTED_SHA_NORMALIZATION_CASE_A_E=PASS');
console.log('INVALID_EXPECTED_SHA_CASE_F_J=FAIL_CLOSED_PASS');
console.log('EXACT_SHA_MISMATCH_CASE_K_L=FAIL_CLOSED_PASS');
console.log('EXPECTED_SHA_WORKFLOW_CONTRACT=PASS');
