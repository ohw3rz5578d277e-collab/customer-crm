import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow = fs.readFileSync('.github/workflows/deploy-cloudflare.yml', 'utf8');
const oneRowQuery = "SELECT s.sequence_key,s.last_value,(SELECT COALESCE(MAX(CAST(SUBSTR(customer_id,3,6) AS INTEGER)),0) FROM customers WHERE customer_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]') AS existing_numeric_suffix_max FROM customer_identity_sequence s WHERE s.sequence_key='canonical_customer_id';";

function pos(token) {
  const i = workflow.indexOf(token);
  assert.notEqual(i, -1, `missing token: ${token}`);
  return i;
}

const apply = pos('Apply managed remote D1 migrations');
const preVerify = pos('Verify identity sequence before deploy (READ ONLY)');
const preAssert = pos('Assert identity sequence monotonic before deploy');
const deploy = pos('Deploy customer-crm-api');
const postVerify = pos('Verify identity sequence state (READ ONLY)');
const postAssert = pos('Assert D1 verification results');
const http = pos('Verify deployed Worker safety (READ ONLY HTTP)');

assert.ok(apply < preVerify, 'predeploy verify must run after migration apply');
assert.ok(preVerify < preAssert, 'predeploy assert must run after predeploy verify');
assert.ok(preAssert < deploy, 'predeploy assert must run before Worker deploy');
assert.ok(deploy < postVerify, 'postdeploy verify must run after Worker deploy');
assert.ok(postVerify < postAssert, 'postdeploy assert must run after postdeploy verify');
assert.ok(postAssert < http, 'postdeploy assert must run before HTTP health verification');

const helperInvocations = workflow.match(/node scripts\/assert-customer-identity-sequence-monotonic\.mjs/g) || [];
assert.equal(helperInvocations.length, 2, 'helper must be invoked exactly twice');

const queryMatches = workflow.match(/SELECT s\.sequence_key,s\.last_value,\(SELECT COALESCE\(MAX\(CAST\(SUBSTR\(customer_id,3,6\) AS INTEGER\)\),0\).*?existing_numeric_suffix_max FROM customer_identity_sequence s WHERE s\.sequence_key='canonical_customer_id';/g) || [];
assert.equal(queryMatches.length, 2, 'one-row sequence query must appear exactly twice');
assert.ok(workflow.includes(oneRowQuery), 'expected one-row query text must be present');
assert.ok(!workflow.includes('SELECT sequence_key,last_value FROM customer_identity_sequence'), 'legacy split sequence query must be removed');

console.log('deploy identity sequence workflow contract tests PASS');
