import assert from 'node:assert/strict';
import { assertCustomerIdentitySequenceMonotonic } from '../scripts/assert-customer-identity-sequence-monotonic.mjs';

function pass(name, payload) {
  const out = assertCustomerIdentitySequenceMonotonic(JSON.stringify(payload));
  assert.equal(out.ok, true, name);
}

function fail(name, payload, pattern = /identity_sequence_/) {
  assert.throws(() => {
    const raw = typeof payload === 'string' ? payload : JSON.stringify(payload);
    assertCustomerIdentitySequenceMonotonic(raw);
  }, pattern, name);
}

const row = (last, max) => ({ sequence_key: 'canonical_customer_id', last_value: last, existing_numeric_suffix_max: max });

pass('CASE 1 last=0 max=0 PASS', row(0, 0));
pass('CASE 2 last=123 max=123 PASS', row(123, 123));
pass('CASE 3 last=500 max=499 PASS', row(500, 499));
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

console.log('deploy identity sequence monotonic guard tests PASS');
