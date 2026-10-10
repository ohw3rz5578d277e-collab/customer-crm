import { execFileSync } from 'node:child_process';

const MAX_SEQUENCE = 999999;
const SEQUENCE_KEY = 'canonical_customer_id';
const SHA_RE = /^[0-9a-f]{40}$/;

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

function normalizeSha(value, label) {
  const sha = String(value == null ? '' : value).trim().toLowerCase();
  if (!SHA_RE.test(sha)) throw new Error(`deploy_current_main_${label}_sha_invalid`);
  return sha;
}

function remoteMainShaFromLsRemote(raw) {
  const line = String(raw == null ? '' : raw).trim().split(/\r?\n/).find(Boolean) || '';
  const sha = line.split(/\s+/)[0] || '';
  return normalizeSha(sha, 'remote_main');
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
    ahead_by: lastValue-existingMax,
    ahead_of_existing_max: lastValue>existingMax,
    monotonic: true
  };
}

export function assertDeployCurrentMainStable({ releaseMode, expectedSha, checkoutSha, remoteMainOutput } = {}) {
  const mode = String(releaseMode == null ? '' : releaseMode).trim();
  if (mode !== 'deploy') return { checked: false, mode };

  const expected = normalizeSha(expectedSha, 'expected');
  const checkout = normalizeSha(checkoutSha, 'checkout');
  const remoteMain = remoteMainShaFromLsRemote(remoteMainOutput);

  if (checkout !== expected) {
    throw new Error(`BLOCKED_FINAL_CHECKOUT_SHA_MISMATCH:current=${checkout}:expected=${expected}`);
  }
  if (remoteMain !== expected) {
    throw new Error(`BLOCKED_FINAL_CURRENT_MAIN_SHA_MISMATCH:current=${remoteMain}:expected=${expected}`);
  }

  return {
    checked: true,
    mode,
    expected_sha: expected,
    checkout_sha: checkout,
    current_main_sha: remoteMain
  };
}

function verifyDeployCurrentMainFromGit() {
  const mode = String(process.env.RELEASE_MODE || '').trim();
  if (mode !== 'deploy') return { checked: false, mode };

  let checkoutSha;
  let remoteMainOutput;
  try {
    checkoutSha = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
    remoteMainOutput = execFileSync('git', ['ls-remote', 'origin', 'refs/heads/main'], { encoding: 'utf8' });
  } catch (error) {
    const detail = error && error.message ? error.message : String(error);
    throw new Error(`deploy_current_main_git_query_failed: ${detail}`);
  }

  return assertDeployCurrentMainStable({
    releaseMode: mode,
    expectedSha: process.env.EXPECTED_SHA,
    checkoutSha,
    remoteMainOutput
  });
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
    const deployMain = verifyDeployCurrentMainFromGit();
    if (deployMain.checked) {
      console.log(`FINAL_DEPLOY_CHECKOUT_SHA=${deployMain.checkout_sha}`);
      console.log(`FINAL_DEPLOY_CURRENT_MAIN_SHA=${deployMain.current_main_sha}`);
      console.log(`FINAL_DEPLOY_EXPECTED_SHA=${deployMain.expected_sha}`);
      console.log('FINAL_CURRENT_MAIN_GUARD=PASS');
    }
    console.log(`Customer identity sequence monotonic guard passed: last_value=${result.last_value}, existing_numeric_suffix_max=${result.existing_numeric_suffix_max}, ahead_by=${result.ahead_by}`);
  } catch (error) {
    console.error(error && error.message ? error.message : String(error));
    process.exit(1);
  }
}
