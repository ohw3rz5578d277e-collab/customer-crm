import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
function pos(token){const i=workflow.indexOf(token);assert.notEqual(i,-1,`missing token: ${token}`);return i;}

const preflightRead=pos('Read identity sequence state');
const classify=pos('Classify remote pending migrations and read-only state');
const preflightStop=pos('Preflight safety stop');
const apply=pos('Apply Customer360 managed remote D1 migration');
const preVerify=pos('Verify identity sequence before deploy (READ ONLY)');
const preAssert=pos('Assert identity sequence monotonic before deploy');
const deploy=pos('Deploy customer-crm-api');
const postVerify=pos('Verify identity sequence state (READ ONLY)');
const postAssert=pos('Assert D1 verification results');
const http=pos('Assert Production Customer360 health contract');

assert.ok(preflightRead<classify,'read-only sequence state must be available before classification');
assert.ok(classify<preflightStop,'classification must precede successful preflight stop');
assert.ok(preflightStop<apply,'deploy-only mutation steps must follow the preflight stop section');
assert.ok(apply<preVerify,'deploy pre-sequence verify must run after optional migration apply');
assert.ok(preVerify<preAssert,'predeploy assert must follow predeploy read');
assert.ok(preAssert<deploy,'identity sequence must be healthy before Worker deploy');
assert.ok(deploy<postVerify,'postdeploy verify must follow Worker deploy');
assert.ok(postVerify<postAssert,'postdeploy assert must follow postdeploy read');
assert.ok(postAssert<http,'postdeploy identity verification must precede HTTP smoke');

const helpers=workflow.match(/node scripts\/assert-customer-identity-sequence-monotonic\.mjs/g)||[];
assert.equal(helpers.length,3,'sequence monotonic helper must guard preflight, predeploy and postdeploy');
const queries=workflow.match(/existing_numeric_suffix_max/g)||[];
assert.ok(queries.length>=3,'one-row sequence readback must exist in preflight, predeploy and postdeploy paths');
assert.ok(!workflow.includes('SELECT sequence_key,last_value FROM customer_identity_sequence'),'legacy split sequence query must remain removed');
console.log('deploy identity sequence workflow contract tests PASS');
