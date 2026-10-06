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

const IDENT = '(?:"(?:""|[^"])+"|`(?:``|[^`])+`|\'(?:\'\'|[^\'])+\'|\\[(?:\\]\\]|[^\\]])+\\]|[A-Za-z_][A-Za-z0-9_$]*)';
const QUALIFIED_IDENT = `${IDENT}(?:\\s*\\.\\s*${IDENT}){0,2}`;
const OR_CONFLICT = '(?:OR\\s+(?:ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)\\s+)?';
const UPDATE_QUALIFIERS = `(?:(?:\\s+AS\\s+${IDENT})|(?:\\s+INDEXED\\s+BY\\s+${IDENT})|(?:\\s+NOT\\s+INDEXED))*`;
const CREATE_MODIFIERS = '(?:(?:TEMP|TEMPORARY|UNIQUE|VIRTUAL)\\s+)*';

const MUTATION_PATTERNS = [
  new RegExp(`\\bINSERT\\s+${OR_CONFLICT}INTO\\s+${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bUPDATE\\s+${OR_CONFLICT}${QUALIFIED_IDENT}${UPDATE_QUALIFIERS}\\s+SET\\b`, 'i'),
  new RegExp(`\\bDELETE\\s+FROM\\s+${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bCREATE\\s+${CREATE_MODIFIERS}(?:TABLE|INDEX|TRIGGER|VIEW)\\b`, 'i'),
  new RegExp(`\\bALTER\\s+TABLE\\s+${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bDROP\\s+(?:TABLE|INDEX|TRIGGER|VIEW)\\s+(?:IF\\s+EXISTS\\s+)?${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bREPLACE\\s+(?:INTO\\s+)?${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bTRUNCATE\\s+(?:TABLE\\s+)?${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bUPSERT\\s+${QUALIFIED_IDENT}`, 'i'),
  /\bPRAGMA\b/i,
  /\bVACUUM\b/i,
  /\bREINDEX\b/i,
  /\bANALYZE\b/i,
  /\bATTACH\s+(?:DATABASE\s+)?/i,
  /\bDETACH\s+(?:DATABASE\s+)?/i
];

const NETWORK_MODULE_SPECIFIER = /(?:\bfrom\s*|\brequire\s*\(\s*|\bimport\s*\(\s*|\bimport\s*)['"](?:(?:node:)?(?:http|https|http2|net|tls|dgram|dns(?:\/promises)?)|cloudflare:sockets|undici|axios|got|node-fetch|ws)['"]/i;
const PRODUCTION_EXECUTION_PATTERNS = [
  /\bwrangler\b/i,
  /\bd1\s+execute\b/i,
  /--execute-production-write\b/i,
  NETWORK_MODULE_SPECIFIER,
  /\bprocess\.getBuiltinModule\s*\(/i,
  /\bcreateRequire\s*\(/i,
  /\bfetch\s*\(/i,
  /\bWebSocket\s*\(/i,
  /\bEventSource\s*\(/i,
  /\b(?:curl|wget)\b/i,
  /child_process/i,
  /https?:\/\//i,
  /wss?:\/\//i
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

function normalizeSqlSource(text) {
  let out = String(text ?? '');
  for (let i = 0; i < 2; i++) {
    out = out
      .replace(/\\"/g, '"')
      .replace(/\\'/g, "'")
      .replace(/\\`/g, '`')
      .replace(/\\\[/g, '[')
      .replace(/\\\]/g, ']')
      .replace(/\\n/g, '\n')
      .replace(/\\r/g, '\r')
      .replace(/\\t/g, '\t');
  }
  return out
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/--[^\r\n]*(?:\r?\n|$)/g, '\n');
}

function mutationMatch(text) {
  const normalized = normalizeSqlSource(text);
  return MUTATION_PATTERNS.find((pattern) => pattern.test(normalized)) || null;
}

function executionMatch(text) {
  return PRODUCTION_EXECUTION_PATTERNS.find((pattern) => pattern.test(text)) || null;
}

for (const path of NO_WRITE_FILES) {
  const text = readRequired(path);
  if (executionMatch(text)) fail('NO_WRITE_RUNTIME_CONTAINS_PRODUCTION_OR_NETWORK_EXECUTION_TOKEN', path);
  if (mutationMatch(text)) fail('NO_WRITE_RUNTIME_CONTAINS_MUTATION_SQL', path);
}

const mutationSamples = [
  'INSERT INTO t VALUES (1)',
  'INSERT OR IGNORE INTO t VALUES (1)',
  'INSERT OR ABORT INTO "customers" VALUES (1)',
  'INSERT OR REPLACE INTO `customers` VALUES (1)',
  'INSERT INTO "customer""history" VALUES (1)',
  'INSERT/**/INTO t VALUES (1)',
  'UPDATE t SET x=1',
  'UPDATE "customers" SET x=1',
  'UPDATE "customer""history" SET x=1',
  'UPDATE `customer``history` SET x=1',
  "UPDATE 'customer''history' SET x=1",
  'UPDATE OR FAIL [customers] SET x=1',
  'UPDATE main.customers AS c SET x=1',
  'UPDATE customers INDEXED BY idx SET x=1',
  'UPDATE\ncustomers SET x=1',
  'UPDATE/*comment*/ "customers" SET x=1',
  'UPDATE--comment\ncustomers SET x=1',
  String.raw`const sql = "UPDATE \"customer\"\"history\" SET x=1";`,
  String.raw`const sql = "UPDATE\ncustomers SET x=1";`,
  'DELETE FROM t',
  'DELETE/**/FROM [customers]',
  'DELETE\nFROM [customers]',
  'CREATE TABLE t(x INTEGER)',
  'CREATE/**/TABLE t(x INTEGER)',
  'CREATE TABLE "customer""history"(x INTEGER)',
  'CREATE TEMP TABLE t(x INTEGER)',
  'CREATE TEMPORARY TABLE "t"(x INTEGER)',
  'CREATE TABLE IF NOT EXISTS [t](x INTEGER)',
  'CREATE UNIQUE INDEX idx ON t(x)',
  'CREATE TEMP UNIQUE INDEX idx2 ON t(x)',
  'CREATE VIRTUAL TABLE v USING fts5(x)',
  String.raw`const sql = "CREATE UNIQUE INDEX idx ON t(x)";`,
  'ALTER/**/TABLE t ADD COLUMN y TEXT',
  'ALTER TABLE t ADD COLUMN y TEXT',
  'DROP TABLE t',
  'DROP/**/TABLE IF EXISTS "t"',
  'DROP TABLE IF EXISTS "t"',
  'REPLACE INTO t VALUES (1)',
  'TRUNCATE TABLE t',
  'UPSERT t',
  'PRAGMA user_version=1',
  'VACUUM',
  'REINDEX idx',
  'ANALYZE',
  "ATTACH/**/DATABASE 'other.db' AS other",
  "ATTACH DATABASE 'other.db' AS other",
  'DETACH/**/DATABASE other',
  'DETACH DATABASE other'
];

for (const sample of mutationSamples) {
  if (!mutationMatch(sample)) fail('SQL_DENY_LIST_SELF_TEST_MISSED', JSON.stringify(sample));
}

const allowedSamples = [
  'tmp_path.replace(receipt_path)',
  'value.replace(/x/g, y)',
  'createHash("sha256")',
  'const status = "UPDATE_REQUIRED";',
  'const note = "create local receipt only";',
  'function analyzeSalesHistory(records) {}',
  'const pragmaLabel = "metadata";',
  'const reindexRequired = false;',
  'const comment = "UPDATE documentation only";'
];

for (const sample of allowedSamples) {
  if (mutationMatch(sample)) fail('SQL_DENY_LIST_FALSE_POSITIVE', JSON.stringify(sample));
}

const executionSamples = [
  "import https from 'https'",
  "import 'node:http2'",
  "const http2 = require('http2')",
  "await import('node:dgram')",
  "import {lookup} from 'dns/promises'",
  "import tls from 'node:tls'",
  "import {connect} from 'cloudflare:sockets'",
  "import {request} from 'undici'",
  "process.getBuiltinModule('node:https')",
  "createRequire(import.meta.url)('node:http2')",
  "fetch(destination)",
  "new WebSocket(destination)",
  "curl $DESTINATION"
];
for (const sample of executionSamples) {
  if (!executionMatch(sample)) fail('NETWORK_DENY_LIST_SELF_TEST_MISSED', JSON.stringify(sample));
}

const executionAllowedSamples = [
  "const httpsLabel='offline';",
  "const networkStatus='DISABLED';",
  "const http2Enabled=false;",
  "const fetchRequired=false;"
];
for (const sample of executionAllowedSamples) {
  if (executionMatch(sample)) fail('NETWORK_DENY_LIST_FALSE_POSITIVE', JSON.stringify(sample));
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
console.log(`NETWORK_DENY_LIST_SELF_TEST=${executionSamples.length}/${executionSamples.length}_PASS`);
console.log(`NETWORK_DENY_LIST_FALSE_POSITIVE_SELF_TEST=${executionAllowedSamples.length}/${executionAllowedSamples.length}_PASS`);
console.log('SQL_MUTATION_DENY_LIST_SOURCE_ESCAPE_NORMALIZATION=PASS');
console.log('SQL_MUTATION_DENY_LIST_COMMENT_SEPARATOR_NORMALIZATION=PASS');
console.log('SQL_MUTATION_DENY_LIST_MULTILINE_SELF_TEST=PASS');
console.log('SQL_MUTATION_DENY_LIST_QUOTED_IDENTIFIER_SELF_TEST=PASS');
console.log('SQL_MUTATION_DENY_LIST_ESCAPED_IDENTIFIER_SELF_TEST=PASS');
console.log('SQL_MUTATION_DENY_LIST_SQLITE_CREATE_MODIFIER_SELF_TEST=PASS');
console.log('SQL_MUTATION_DENY_LIST_SQLITE_CONTROL_STATEMENT_SELF_TEST=PASS');
console.log('NO_WRITE_NETWORK_EXECUTION_SURFACE=0');
console.log('DECISION_PLAN_NO_WRITE_CONTRACT=PASS');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('PRODUCTION_MUTATION_PATH_ADDED=0');
console.log('LINE_HISTORY_NO_WRITE_SAFETY=PASS');
