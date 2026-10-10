import assert from 'node:assert/strict';
import {
  assertCustomerIdentitySequenceMonotonic,
  assertDeployCurrentMainStable
} from '../scripts/assert-customer-identity-sequence-monotonic.mjs';

function pass(name, payload) {
  const out = assertCustomerIdentitySequenceMonotonic(JSON.stringify(payload));
  assert.equal(out.ok, true, name);
  return out;
}

function fail(name, payload, pattern = /identity_sequence_/) {
  assert.throws(() => {
    const raw = typeof payload === 'string' ? payload : JSON.stringify(payload);
    assertCustomerIdentitySequenceMonotonic(raw);
  }, pattern, name);
}

const row = (last, max) => ({ sequence_key: 'canonical_customer_id', last_value: last, existing_numeric_suffix_max: max });

pass('CASE 1 last=0 max=0 PASS', row(0, 0));
{
  const out=pass('CASE 2 last=123 max=123 PASS', row(123, 123));
  assert.equal(out.ahead_by,0);
  assert.equal(out.ahead_of_existing_max,false);
}
{
  const out=pass('CASE 3 last=500 max=499 PASS', row(500, 499));
  assert.equal(out.ahead_by,1);
  assert.equal(out.ahead_of_existing_max,true);
  assert.equal(out.monotonic,true);
}
fail('CASE 4 last=499 max=500 FAIL', row(499, 500), /behind/);
fail('CASE 5 canonical row missing FAIL', { results: [{ sequence_key: 'other', last_value: 1, existing_numeric_suffix_max: 1 }] }, /canonical_row_missing/);
fail('CASE 6 last_value missing FAIL', { sequence_key: 'canonical_customer_id', existing_numeric_suffix_max: 1 }, /canonical_row_missing|last_value_missing/);
fail('CASE 7 existing max missing FAIL', { sequence_key: 'canonical_customer_id', last_value: 1 }, /canonical_row_missing|existing_numeric_suffix_max_missing/);
fail('CASE 8 malformed JSON FAIL', '{not-json', /malformed_json/);
fail('CASE 9 non-numeric value FAIL', row('abc', 1), /not_integer/);
fail('CASE 10 negative last FAIL', row(-1, 0), /negative/);
fail('CASE 11 last > 999999 FAIL', row(1000000, 0), /above_max/);
pass('CASE 12 nested wrangler result shape PASS', {
  result: [
    {
      results: [
        row('42', '41')
      ]
    }
  ]
});
fail('CASE 13 multiple conflicting canonical rows FAIL', {
  results: [row(500, 499), row(499, 500)]
}, /multiple_canonical_rows/);

const sha='9096da7f66a37b1f79ada9b287d220c5cfbcef12';
const other='1daa40e4f4eefd2a31628ffd780a1fe9d184ae8a';
{
  const out=assertDeployCurrentMainStable({
    releaseMode:'preflight',
    expectedSha:'not-required',
    checkoutSha:'not-required',
    remoteMainOutput:'not-required'
  });
  assert.equal(out.checked,false,'preflight must not invoke late deploy main guard');
}
{
  const out=assertDeployCurrentMainStable({
    releaseMode:'deploy',
    expectedSha:sha,
    checkoutSha:sha,
    remoteMainOutput:`${sha}\trefs/heads/main\n`
  });
  assert.equal(out.checked,true);
  assert.equal(out.expected_sha,sha);
  assert.equal(out.checkout_sha,sha);
  assert.equal(out.current_main_sha,sha);
}
assert.throws(() => assertDeployCurrentMainStable({
  releaseMode:'deploy',
  expectedSha:sha,
  checkoutSha:sha,
  remoteMainOutput:`${other}\trefs/heads/main\n`
}), /BLOCKED_FINAL_CURRENT_MAIN_SHA_MISMATCH/,'deploy must fail closed when main drifts after the initial gate');
assert.throws(() => assertDeployCurrentMainStable({
  releaseMode:'deploy',
  expectedSha:sha,
  checkoutSha:other,
  remoteMainOutput:`${sha}\trefs\/heads\/main\n`
}), /BLOCKED_FINAL_CHECKOUT_SHA_MISMATCH/,'deploy must fail closed if checkout no longer matches authorization');
assert.throws(() => assertDeployCurrentMainStable({
  releaseMode:'deploy',
  expectedSha:'BAD',
  checkoutSha:sha,
  remoteMainOutput:`${sha}\trefs/heads/main\n`
}), /deploy_current_main_expected_sha_invalid/,'deploy expected SHA must remain exact lowercase 40-hex');
assert.throws(() => assertDeployCurrentMainStable({
  releaseMode:'deploy',
  expectedSha:sha,
  checkoutSha:sha,
  remoteMainOutput:''
}), /deploy_current_main_remote_main_sha_invalid/,'missing remote main evidence must fail closed');

console.log('deploy identity sequence monotonic guard tests PASS');
