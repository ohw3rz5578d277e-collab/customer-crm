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

const BLOCK_COMMENT = '/\\*(?:[^*]|\\*(?!\\/))*\\*/';
const SQL_SEPARATOR_ATOM = `(?:\\s|${BLOCK_COMMENT}|--[^\\r\\n]*(?:\\r\\n|\\r|\\n|$))`;
const SQL_SEP = `(?:${SQL_SEPARATOR_ATOM})+`;
const SQL_GAP = `(?:${SQL_SEPARATOR_ATOM})*`;
const IDENT = '(?:"(?:""|[^"])+"|`(?:``|[^`])+`|\'(?:\'\'|[^\'])+\'|\\[(?:\\]\\]|[^\\]])+\\]|[A-Za-z_][A-Za-z0-9_$]*)';
const QUALIFIED_IDENT = `${IDENT}(?:${SQL_GAP}\\.${SQL_GAP}${IDENT}){0,2}`;
const OR_CONFLICT = `(?:OR${SQL_SEP}(?:ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)${SQL_SEP})?`;
const UPDATE_QUALIFIERS = `(?:(?:${SQL_SEP}AS${SQL_SEP}${IDENT})|(?:${SQL_SEP}INDEXED${SQL_SEP}BY${SQL_SEP}${IDENT})|(?:${SQL_SEP}NOT${SQL_SEP}INDEXED))*`;
const CREATE_MODIFIERS = `(?:(?:TEMP|TEMPORARY|UNIQUE|VIRTUAL)${SQL_SEP})*`;

const MUTATION_PATTERNS = [
  new RegExp(`\\bINSERT${SQL_SEP}${OR_CONFLICT}INTO${SQL_SEP}${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bUPDATE${SQL_SEP}${OR_CONFLICT}${QUALIFIED_IDENT}${UPDATE_QUALIFIERS}${SQL_SEP}SET\\b`, 'i'),
  new RegExp(`\\bDELETE${SQL_SEP}FROM${SQL_SEP}${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bCREATE${SQL_SEP}${CREATE_MODIFIERS}(?:TABLE|INDEX|TRIGGER|VIEW)\\b`, 'i'),
  new RegExp(`\\bALTER${SQL_SEP}TABLE${SQL_SEP}${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bDROP${SQL_SEP}(?:TABLE|INDEX|TRIGGER|VIEW)${SQL_SEP}(?:IF${SQL_SEP}EXISTS${SQL_SEP})?${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bREPLACE${SQL_SEP}(?:INTO${SQL_SEP})?${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bTRUNCATE${SQL_SEP}(?:TABLE${SQL_SEP})?${QUALIFIED_IDENT}`, 'i'),
  new RegExp(`\\bUPSERT${SQL_SEP}${QUALIFIED_IDENT}`, 'i'),
  /\bPRAGMA\b/i,
  /\bVACUUM\b/i,
  /\bREINDEX\b/i,
  /\bANALYZE\b/i,
  new RegExp(`\\bATTACH${SQL_SEP}(?:DATABASE${SQL_SEP})?`, 'i'),
  new RegExp(`\\bDETACH${SQL_SEP}(?:DATABASE${SQL_SEP})?`, 'i')
];

const JS_GAP = `(?:\\s|${BLOCK_COMMENT}|//[^\\r\\n]*(?:\\r?\\n|$))*`;
const NETWORK_MODULE = '(?:(?:node:)?(?:http|https|http2|net|tls|dgram|dns(?:/promises)?)|cloudflare:sockets|undici|axios|got|node-fetch|ws)';
const NETWORK_MODULE_SPECIFIER = new RegExp(`(?:\\bfrom${JS_GAP}|\\brequire${JS_GAP}\\(${JS_GAP}|\\bimport${JS_GAP}\\(${JS_GAP}|\\bimport${JS_GAP})(['"\\x60])${NETWORK_MODULE}\\1`, 'i');
const PRODUCTION_EXECUTION_PATTERNS = [
  /\bwrangler\b/i,
  /\bd1\s+execute\b/i,
  /--execute-production-write\b/i,
  NETWORK_MODULE_SPECIFIER,
  new RegExp(`\\bprocess${JS_GAP}\\.${JS_GAP}getBuiltinModule${JS_GAP}\\(`, 'i'),
  new RegExp(`\\bcreateRequire${JS_GAP}\\(`, 'i'),
  new RegExp(`\\bfetch${JS_GAP}\\(`, 'i'),
  new RegExp(`\\bWebSocket${JS_GAP}\\(`, 'i'),
  new RegExp(`\\bEventSource${JS_GAP}\\(`, 'i'),
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
  out = out.replace(/\\(?:\r\n|\r|\n|\u2028|\u2029)/g, '');
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
  return out;
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
  'const marker = "--"; const sql = "UPDATE customers SET x=1";',
  'const marker = "/*"; const sql = "CREATE TABLE t(x INTEGER)";',
  'const marker = "*/"; const sql = "DELETE FROM t";',
  String.raw`const sql = "UPDATE \"customer\"\"history\" SET x=1";`,
  String.raw`const sql = "UPDATE\ncustomers SET x=1";`,
  'const sql = "UPDATE ' + '\\' + '\n' + 'customers SET x=1";',
  'const sql = "UPDATE ' + '\\' + String.fromCharCode(0x2028) + 'customers SET x=1";',
  'const sql = "UPDATE ' + '\\' + String.fromCharCode(0x2029) + 'customers SET x=1";',
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
  'const comment = "UPDATE documentation only";',
  'const marker = "--";',
  'const marker = "/*";',
  'const marker = "*/";'
];

for (const sample of allowedSamples) {
  if (mutationMatch(sample)) fail('SQL_DENY_LIST_FALSE_POSITIVE', JSON.stringify(sample));
}

const backtrackingProbe = `DELETE${' '.repeat(8192)}NOT_FROM customers`;
const backtrackingStartedAt = Date.now();
if (mutationMatch(backtrackingProbe)) fail('SQL_DENY_LIST_BACKTRACKING_PROBE_FALSE_POSITIVE');
const backtrackingElapsedMs = Date.now() - backtrackingStartedAt;
if (backtrackingElapsedMs > 1000) fail('SQL_DENY_LIST_BACKTRACKING_PROBE_SLOW', String(backtrackingElapsedMs));

const repeatedCommentProbe = `DELETE ${'/**/'.repeat(28)} NOT_FROM customers`;
const repeatedCommentStartedAt = Date.now();
if (mutationMatch(repeatedCommentProbe)) fail('SQL_DENY_LIST_REPEATED_COMMENT_PROBE_FALSE_POSITIVE');
const repeatedCommentElapsedMs = Date.now() - repeatedCommentStartedAt;
if (repeatedCommentElapsedMs > 2000) fail('SQL_DENY_LIST_REPEATED_COMMENT_PROBE_SLOW', String(repeatedCommentElapsedMs));

const executionSamples = [
  "import https from 'https'",
  "import 'node:http2'",
  "const http2 = require('http2')",
  "const http2 = require /* comment */ ('node:http2')",
  "await import('node:dgram')",
  "await import /* comment */ ('node:dgram')",
  "import /* comment */ 'node:https'",
  'await import(`node:http2`)',
  'const https = require(`node:https`)',
  'import `node:dgram`',
  "import {lookup} from 'dns/promises'",
  "import tls from 'node:tls'",
  "import {connect} from 'cloudflare:sockets'",
  "import {request} from 'undici'",
  "process /* comment */ . getBuiltinModule /* comment */ ('node:https')",
  "createRequire /* comment */ (import.meta.url)('node:http2')",
  "fetch /* comment */ (destination)",
  "new WebSocket /* comment */ (destination)",
  "curl $DESTINATION"
];
for (const sample of executionSamples) {
  if (!executionMatch(sample)) fail('NETWORK_DENY_LIST_SELF_TEST_MISSED', JSON.stringify(sample));
}

const executionAllowedSamples = [
  "const httpsLabel='offline';",
  "const networkStatus='DISABLED';",
  "const http2Enabled=false;",
  "const fetchRequired=false;",
  "const marker='require /* comment */';"
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
console.log('SQL_MUTATION_DENY_LIST_JS_LINE_CONTINUATION_NORMALIZATION=PASS');
console.log('SQL_MUTATION_DENY_LIST_JS_UNICODE_LINE_CONTINUATION_NORMALIZATION=PASS');
console.log('SQL_MUTATION_DENY_LIST_COMMENT_SEPARATOR_NORMALIZATION=PASS');
console.log('SQL_MUTATION_DENY_LIST_QUOTED_COMMENT_MARKER_PRESERVATION=PASS');
console.log('SQL_MUTATION_DENY_LIST_COMMENT_TOKENIZATION=PASS');
console.log('SQL_MUTATION_DENY_LIST_BOUNDED_BACKTRACKING=PASS');
console.log('SQL_MUTATION_DENY_LIST_MULTILINE_SELF_TEST=PASS');
console.log('SQL_MUTATION_DENY_LIST_QUOTED_IDENTIFIER_SELF_TEST=PASS');
console.log('SQL_MUTATION_DENY_LIST_ESCAPED_IDENTIFIER_SELF_TEST=PASS');
console.log('SQL_MUTATION_DENY_LIST_SQLITE_CREATE_MODIFIER_SELF_TEST=PASS');
console.log('SQL_MUTATION_DENY_LIST_SQLITE_CONTROL_STATEMENT_SELF_TEST=PASS');
console.log('NETWORK_MODULE_COMMENT_SEPARATOR_SELF_TEST=PASS');
console.log('NETWORK_MODULE_TEMPLATE_LITERAL_SELF_TEST=PASS');
console.log('NO_WRITE_NETWORK_EXECUTION_SURFACE=0');
console.log('DECISION_PLAN_NO_WRITE_CONTRACT=PASS');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('PRODUCTION_MUTATION_PATH_ADDED=0');
console.log('LINE_HISTORY_NO_WRITE_SAFETY=PASS');
