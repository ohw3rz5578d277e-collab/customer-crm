import fs from 'node:fs';

const NO_WRITE_FILES = [
  'src/crm-line-history-recovery-next-phase.mjs',
  'src/crm-line-history-recovery-discovery.mjs',
  'src/crm-line-history-owner-decision-plan.mjs',
  'src/crm-line-history-owner-review-queue.mjs',
  'scripts/discover-line-history-recovery-artifacts.mjs',
  'scripts/inspect-line-history-recovery-status.mjs',
  'scripts/plan-line-history-owner-decisions.mjs',
  'scripts/run-line-history-owner-authorization-prep.sh'
];

const IDENT = '(?:"[^"]+"|`[^`]+`|\\[[^\\]]+\\]|[A-Za-z_][A-Za-z0-9_$]*)';
const QUALIFIED_IDENT = `${IDENT}(?:\\s*\\.\\s*${IDENT})?`;
const OR_CONFLICT = '(?:OR\\s+(?:ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)\\s+)?';

const MUTATION_PATTERNS = [
  new RegExp(`\\bINSERT\\s+${OR_CONFLICT}INTO\\s+${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bUPDATE\\s+${OR_CONFLICT}${QUALIFIED_IDENT}\\s+SET\\b`, 'i'),
  new RegExp(`\\bDELETE\\s+FROM\\s+${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bCREATE\\s+(?:TEMP(?:ORARY)?\\s+)?(?:TABLE|INDEX|TRIGGER|VIEW)\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bALTER\\s+TABLE\\s+${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bDROP\\s+(?:TABLE|INDEX|TRIGGER|VIEW)\\s+(?:IF\\s+EXISTS\\s+)?${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bREPLACE\\s+(?:INTO\\s+)?${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bTRUNCATE\\s+(?:TABLE\\s+)?${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bUPSERT\\s+${QUALIFIED_IDENT}`, 'i')
];

const PRODUCTION_EXECUTION_PATTERNS = [
  /\bwrangler\b/i,
  /\bd1\s+execute\b/i,
  /--execute-production-write\b/i
];

function fail(code, detail = '') {
  console.error(detail ? `STOP: ${code}=${detail}` : `STOP: ${code}`);
  process.exit(1);
}

function readRequired(path) {
  try {
    return fs.readFileSync(path, 'utf8');
  } catch {
    fail('REQUIRED_FILE_MISSING', path);
  }
}

function mutationMatch(text) {
  return MUTATION_PATTERNS.find((pattern) => pattern.test(text)) || null;
}

function executionMatch(text) {
  return PRODUCTION_EXECUTION_PATTERNS.find((pattern) => pattern.test(text)) || null;
}

for (const path of NO_WRITE_FILES) {
  const text = readRequired(path);
  if (executionMatch(text)) fail('NO_WRITE_RUNTIME_CONTAINS_PRODUCTION_EXECUTION_TOKEN', path);
  if (mutationMatch(text)) fail('NO_WRITE_RUNTIME_CONTAINS_MUTATION_SQL', path);
}

const mutationSamples = [
  'INSERT INTO t VALUES (1)',
  'INSERT OR IGNORE INTO t VALUES (1)',
  'INSERT OR ABORT INTO "customers" VALUES (1)',
  'INSERT OR REPLACE INTO `customers` VALUES (1)',
  'UPDATE t SET x=1',
  'UPDATE "customers" SET x=1',
  'UPDATE OR FAIL [customers] SET x=1',
  'UPDATE\ncustomers SET x=1',
  'DELETE FROM t',
  'DELETE\nFROM [customers]',
  'CREATE TABLE t(x INTEGER)',
  'CREATE TEMP TABLE t(x INTEGER)',
  'CREATE TEMPORARY TABLE "t"(x INTEGER)',
  'CREATE TABLE IF NOT EXISTS [t](x INTEGER)',
  'ALTER TABLE t ADD COLUMN y TEXT',
  'DROP TABLE t',
  'DROP TABLE IF EXISTS "t"',
  'REPLACE INTO t VALUES (1)',
  'TRUNCATE TABLE t',
  'UPSERT t'
];

for (const sample of mutationSamples) {
  if (!mutationMatch(sample)) fail('SQL_DENY_LIST_SELF_TEST_MISSED', JSON.stringify(sample));
}

const allowedSamples = [
  'tmp_path.replace(receipt_path)',
  'value.replace(/x/g, y)',
  'createHash("sha256")',
  'const status = "UPDATE_REQUIRED";',
  'const note = "create local receipt only";'
];

for (const sample of allowedSamples) {
  if (mutationMatch(sample)) fail('SQL_DENY_LIST_FALSE_POSITIVE', JSON.stringify(sample));
}

const requiredMarkers = new Map([
  ['scripts/run-line-history-owner-authorization-prep.sh', [
    'customer-crm-line-history-no-write-completion-v1',
    'PRODUCTION_D1_READ=0',
    'PRODUCTION_D1_WRITE=0',
    'AUTHORIZATION_GRANTED=NO'
  ]],
  ['scripts/run-line-history-recovery-operator.sh', [
    'STATUS_ARGS+=(--main-sha "$LOCAL_HEAD")'
  ]],
  ['scripts/plan-line-history-owner-decisions.mjs', [
    'PROPOSED_WRITE_ACTIONS=0',
    'AUTHORIZATION_GRANTED=NO',
    'PRODUCTION_D1_WRITE=0'
  ]]
]);

for (const [path, markers] of requiredMarkers) {
  const text = readRequired(path);
  for (const marker of markers) {
    if (!text.includes(marker)) fail('REQUIRED_NO_WRITE_MARKER_MISSING', `${path}:${marker}`);
  }
}

console.log(`SQL_MUTATION_DENY_LIST_SELF_TEST=${mutationSamples.length}/${mutationSamples.length}_PASS`);
console.log(`SQL_MUTATION_DENY_LIST_FALSE_POSITIVE_SELF_TEST=${allowedSamples.length}/${allowedSamples.length}_PASS`);
console.log('SQL_MUTATION_DENY_LIST_MULTILINE_SELF_TEST=PASS');
console.log('SQL_MUTATION_DENY_LIST_QUOTED_IDENTIFIER_SELF_TEST=PASS');
console.log('SQL_MUTATION_DENY_LIST_SQLITE_MODIFIER_SELF_TEST=PASS');
console.log('DECISION_PLAN_NO_WRITE_CONTRACT=PASS');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('PRODUCTION_MUTATION_PATH_ADDED=0');
console.log('LINE_HISTORY_NO_WRITE_SAFETY=PASS');
