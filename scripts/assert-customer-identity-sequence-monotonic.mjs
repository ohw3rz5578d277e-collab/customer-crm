const MAX_SEQUENCE = 999999;
const SEQUENCE_KEY = 'canonical_customer_id';

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function parseOutput(raw) {
  const text = raw == null ? '' : String(raw).trim();
  if (!text) throw new Error('identity_sequence_output_empty');
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`identity_sequence_output_malformed_json: ${error.message}`);
  }
}

function numericInteger(value, name) {
  const n = typeof value === 'number' ? value : (typeof value === 'string' && value.trim() !== '' ? Number(value) : NaN);
  if (!Number.isFinite(n) || !Number.isInteger(n)) throw new Error(`identity_sequence_${name}_not_integer`);
  return n;
}

function walk(value, visit) {
  if (Array.isArray(value)) {
    for (const item of value) walk(item, visit);
    return;
  }
  if (!isPlainObject(value)) return;
  visit(value);
  for (const item of Object.values(value)) walk(item, visit);
}

export function findCanonicalSequenceRows(parsed) {
  const rows = [];
  walk(parsed, (obj) => {
    if (obj.sequence_key === SEQUENCE_KEY &&
        Object.prototype.hasOwnProperty.call(obj, 'last_value') &&
        Object.prototype.hasOwnProperty.call(obj, 'existing_numeric_suffix_max')) {
      rows.push(obj);
    }
  });
  return rows;
}

export function assertCustomerIdentitySequenceMonotonic(rawOutput) {
  const parsed = typeof rawOutput === 'string' ? parseOutput(rawOutput) : rawOutput;
  const rows = findCanonicalSequenceRows(parsed);
  if (rows.length === 0) throw new Error('identity_sequence_canonical_row_missing');
  if (rows.length > 1) throw new Error('identity_sequence_multiple_canonical_rows');
  const row = rows[0];
  if (row.sequence_key !== SEQUENCE_KEY) throw new Error('identity_sequence_key_mismatch');

  if (!Object.prototype.hasOwnProperty.call(row, 'last_value')) throw new Error('identity_sequence_last_value_missing');
  if (!Object.prototype.hasOwnProperty.call(row, 'existing_numeric_suffix_max')) throw new Error('identity_sequence_existing_numeric_suffix_max_missing');

  const lastValue = numericInteger(row.last_value, 'last_value');
  const existingMax = numericInteger(row.existing_numeric_suffix_max, 'existing_numeric_suffix_max');

  if (lastValue < 0) throw new Error('identity_sequence_last_value_negative');
  if (lastValue > MAX_SEQUENCE) throw new Error('identity_sequence_last_value_above_max');
  if (existingMax < 0) throw new Error('identity_sequence_existing_numeric_suffix_max_negative');
  if (lastValue < existingMax) {
    throw new Error(`identity_sequence_behind_existing_customer_ids: last_value=${lastValue}, existing_numeric_suffix_max=${existingMax}`);
  }

  return {
    ok: true,
    sequence_key: SEQUENCE_KEY,
    last_value: lastValue,
    existing_numeric_suffix_max: existingMax,
    monotonic: true
  };
}

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf8');
}

if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const input = await readStdin();
    const result = assertCustomerIdentitySequenceMonotonic(input);
    console.log(`Customer identity sequence monotonic guard passed: last_value=${result.last_value}, existing_numeric_suffix_max=${result.existing_numeric_suffix_max}`);
  } catch (error) {
    console.error(error && error.message ? error.message : String(error));
    process.exit(1);
  }
}
