import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {parseProductionSnapshotText} from '../src/crm-sales-snapshot.mjs';

const root=fs.mkdtempSync(path.join(os.tmpdir(),'crm-production-snapshot-'));
const sha='a'.repeat(40);
const canaries=[
  '26999101',
  '26999102',
  'U_SNAPSHOT_PRIVATE_CANARY_2222',
  'PRIVATE_NAME_CANARY_ALPHA',
  'PRIVATE_NAME_CANARY_BETA',
  'PRIVATE_NAME_CANARY_DUPLICATE',
  '2099-12-31T23:59:59Z'
];

function run(inputValue,name='case'){
  const input=path.join(root,`${name}.raw`);
  const output=path.join(root,`${name}.json`);
  fs.writeFileSync(input,typeof inputValue==='string'?inputValue:JSON.stringify(inputValue),'utf8');
  const result=spawnSync(process.execPath,[
    'scripts/build-production-identity-snapshot.mjs',
    '--input',input,
    '--output',output,
    '--source-sha',sha
  ],{encoding:'utf8'});
  return {result,output};
}

function assertNoCanaryOutput(result,label){
  const combined=`${result.stdout||''}\n${result.stderr||''}`;
  for(const canary of canaries){
    assert.equal(combined.includes(canary),false,`${label} leaked private canary ${canary}`);
  }
}

const BLOCK_COMMENT='/\\*(?:[^*]|\\*(?!\\/))*\\*/';
const SQL_SEPARATOR_ATOM=`(?:\\s|${BLOCK_COMMENT}|--[^\\r\\n]*(?:\\r\\n|\\r|\\n|$))`;
const SQL_SEP=`(?:${SQL_SEPARATOR_ATOM})+`;
const SQL_GAP=`(?:${SQL_SEPARATOR_ATOM})*`;
const IDENT="(?:\"(?:\"\"|[^\"])+\"|`(?:``|[^`])+`|'(?:''|[^'])+'|\\[(?:\\]\\]|[^\\]])+\\]|[A-Za-z_][A-Za-z0-9_$]*)";
const QUALIFIED_IDENT=`${IDENT}(?:${SQL_GAP}\\.${SQL_GAP}${IDENT}){0,2}`;
const OR_CONFLICT=`(?:OR${SQL_SEP}(?:ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)${SQL_SEP})?`;
const UPDATE_QUALIFIERS=`(?:(?:${SQL_SEP}AS${SQL_SEP}${IDENT})|(?:${SQL_SEP}INDEXED${SQL_SEP}BY${SQL_SEP}${IDENT})|(?:${SQL_SEP}NOT${SQL_SEP}INDEXED))*`;
const CREATE_MODIFIERS=`(?:(?:TEMP|TEMPORARY|UNIQUE|VIRTUAL)${SQL_SEP})*`;
const SQL_MUTATION_PATTERNS=[
  new RegExp(`\\bINSERT${SQL_SEP}${OR_CONFLICT}INTO${SQL_SEP}${QUALIFIED_IDENT}`,'i'),
  new RegExp(`\\bUPDATE${SQL_SEP}${OR_CONFLICT}${QUALIFIED_IDENT}${UPDATE_QUALIFIERS}${SQL_SEP}SET\\b`,'i'),
  new RegExp(`\\bDELETE${SQL_SEP}FROM${SQL_SEP}${QUALIFIED_IDENT}`,'i'),
  new RegExp(`\\bCREATE${SQL_SEP}${CREATE_MODIFIERS}(?:TABLE|INDEX|TRIGGER|VIEW)\\b`,'i'),
  new RegExp(`\\bALTER${SQL_SEP}TABLE${SQL_SEP}${QUALIFIED_IDENT}`,'i'),
  new RegExp(`\\bDROP${SQL_SEP}(?:TABLE|INDEX|TRIGGER|VIEW)${SQL_SEP}(?:IF${SQL_SEP}EXISTS${SQL_SEP})?${QUALIFIED_IDENT}`,'i'),
  new RegExp(`\\bREPLACE${SQL_SEP}(?:INTO${SQL_SEP})?${QUALIFIED_IDENT}`,'i'),
  new RegExp(`\\bTRUNCATE${SQL_SEP}(?:TABLE${SQL_SEP})?${QUALIFIED_IDENT}`,'i'),
  new RegExp(`\\bUPSERT${SQL_SEP}${QUALIFIED_IDENT}`,'i'),
  /\bPRAGMA\b/i,
  /\bVACUUM\b/i,
  /\bREINDEX\b/i,
  /\bANALYZE\b/i,
  new RegExp(`\\bATTACH${SQL_SEP}(?:DATABASE${SQL_SEP})?`,'i'),
  new RegExp(`\\bDETACH${SQL_SEP}(?:DATABASE${SQL_SEP})?`,'i')
];
function normalizeSqlForScan(value){
  let out=String(value??'');
  out=out.replace(/\\(?:\r\n|\r|\n|\u2028|\u2029)/g,'');
  for(let i=0;i<2;i++){
    out=out
      .replace(/\\\"/g,'\"')
      .replace(/\\'/g,"'")
      .replace(/\\`/g,'`')
      .replace(/\\\[/g,'[')
      .replace(/\\\]/g,']')
      .replace(/\\n/g,'\n')
      .replace(/\\r/g,'\r')
      .replace(/\\t/g,'\t');
  }
  return out;
}
const hasMutationSql=value=>SQL_MUTATION_PATTERNS.some(re=>re.test(normalizeSqlForScan(value)));

const JS_GAP='(?:(?:\\s)|(?:/\\*[\\s\\S]*?\\*/)|(?://[^\\r\\n]*(?:\\r?\\n|$)))*';
const NETWORK_BUILTIN='(?:http|https|http2|net|tls|dns(?:/promises)?|dgram)';
const NETWORK_SPECIFIER=`(?:(?:node:)?${NETWORK_BUILTIN}|cloudflare:sockets|undici|axios|got|node-fetch)`;
const NETWORK_MODULE_RE=new RegExp(`(?:\\bfrom${JS_GAP}|\\brequire${JS_GAP}\\(${JS_GAP}|\\bimport${JS_GAP}\\(${JS_GAP}|\\bimport${JS_GAP})(['\"\\x60])${NETWORK_SPECIFIER}\\1`,'i');
const hasNetworkSurface=value=>[
  NETWORK_MODULE_RE,
  new RegExp(`\\bprocess${JS_GAP}\\.${JS_GAP}getBuiltinModule${JS_GAP}\\(`,'i'),
  new RegExp(`\\bcreateRequire${JS_GAP}\\(`,'i'),
  new RegExp(`\\bfetch${JS_GAP}\\(`,'i'),
  new RegExp(`\\bWebSocket${JS_GAP}\\(`,'i'),
  new RegExp(`\\bEventSource${JS_GAP}\\(`,'i'),
  /https?:\/\//i,
  /\b(?:curl|wget)\b/i,
  /child_process/i
].some(re=>re.test(value));

try{
  const rows=[
    {customer_id:'26999102',line_user_id:'U_SNAPSHOT_PRIVATE_CANARY_2222',name:'PRIVATE_NAME_CANARY_BETA',deleted_at:''},
    {customer_id:'26999101',line_user_id:'',name:'PRIVATE_NAME_CANARY_ALPHA',deleted_at:'2099-12-31T23:59:59Z'}
  ];

  const wrapped=run([{results:rows,success:true}], 'wrapped');
  assert.equal(wrapped.result.status,0,wrapped.result.stderr||wrapped.result.stdout);
  assertNoCanaryOutput(wrapped.result,'wrapped');
  const body=fs.readFileSync(wrapped.output,'utf8');
  const parsed=JSON.parse(body);
  assert.equal(parsed.snapshot_format,'customer-crm-production-identity-snapshot-v1');
  assert.equal(parsed.complete,true);
  assert.equal(parsed.query_scope,'all_customer_identities');
  assert.equal(parsed.source_main_sha,sha);
  assert.equal(parsed.customer_count,2);
  assert.deepEqual(parsed.customers.map(x=>x.customer_id),['26999101','26999102']);
  assert.equal(parsed.customers[0].deleted_at,'2099-12-31T23:59:59Z');
  assert.deepEqual(parseProductionSnapshotText(body).map(x=>x.customer_id),['26999101','26999102']);
  assert.match(wrapped.result.stdout,/PRIVATE_CUSTOMER_VALUES_PRINTED=0/);
  assert.match(wrapped.result.stdout,/PRODUCTION_D1_WRITE=0/);

  const objectShape=run({results:rows},'object');
  assert.equal(objectShape.result.status,0,objectShape.result.stderr||objectShape.result.stdout);
  assertNoCanaryOutput(objectShape.result,'object');

  const directRows=run(rows,'direct');
  assert.equal(directRows.result.status,0,directRows.result.stderr||directRows.result.stdout);
  assertNoCanaryOutput(directRows.result,'direct');

  const bomWrapped=run(`\uFEFF${JSON.stringify([{results:rows}])}`,'bom');
  assert.equal(bomWrapped.result.status,0,bomWrapped.result.stderr||bomWrapped.result.stdout);
  assertNoCanaryOutput(bomWrapped.result,'bom');

  const failedWrapper=run([
    {results:[],success:false,error:'query failed'},
    {results:rows,success:true}
  ],'failed-wrapper');
  assert.notEqual(failedWrapper.result.status,0);
  assert.match(failedWrapper.result.stderr,/STOP_WRAPPER_ENTRY_ERROR_PRESENT|STOP_WRAPPER_ENTRY_UNSUCCESSFUL/);
  assertNoCanaryOutput(failedWrapper.result,'failed-wrapper');

  const malformedWrapper=run([
    {unexpected:true},
    {results:rows,success:true}
  ],'malformed-wrapper');
  assert.notEqual(malformedWrapper.result.status,0);
  assert.match(malformedWrapper.result.stderr,/STOP_WRAPPER_ENTRY_RESULTS_REQUIRED/);
  assertNoCanaryOutput(malformedWrapper.result,'malformed-wrapper');

  const errorWrapperCases=[
    [{results:rows,error:'query failed'}],
    [{results:rows,success:true,error:null}],
    {results:rows,error:''},
    {result:{results:rows,success:true,error:'contradictory'}}
  ];
  for(const [index,value] of errorWrapperCases.entries()){
    const errorWrapper=run(value,`error-wrapper-${index}`);
    assert.notEqual(errorWrapper.result.status,0,`error-bearing wrapper ${index} should fail closed`);
    assert.match(errorWrapper.result.stderr,/STOP_WRAPPER_ENTRY_ERROR_PRESENT/);
    assertNoCanaryOutput(errorWrapper.result,`error-wrapper-${index}`);
  }

  const outerWrapperFailureCases=[
    {success:false,error:'query failed',result:{results:rows,success:true}},
    {error:'query failed',result:{results:rows,success:true}},
    {success:false,result:{results:rows,success:true}},
    {success:true,error:null,result:{results:rows,success:true}}
  ];
  for(const [index,value] of outerWrapperFailureCases.entries()){
    const outerFailure=run(value,`outer-wrapper-failure-${index}`);
    assert.notEqual(outerFailure.result.status,0,`outer wrapper failure ${index} should fail closed`);
    assert.match(outerFailure.result.stderr,/STOP_WRAPPER_ENTRY_ERROR_PRESENT|STOP_WRAPPER_ENTRY_UNSUCCESSFUL/);
    assertNoCanaryOutput(outerFailure.result,`outer-wrapper-failure-${index}`);
  }

  for(const field of ['customer_id','line_user_id','name','deleted_at']){
    const incomplete={...rows[0]};
    delete incomplete[field];
    const missingField=run([{results:[incomplete],success:true}],`missing-${field}`);
    assert.notEqual(missingField.result.status,0,`${field} omission should fail closed`);
    assert.match(missingField.result.stderr,/STOP_CUSTOMER_ROW_IDENTITY_FIELDS_REQUIRED/);
    assertNoCanaryOutput(missingField.result,`missing-${field}`);
  }

  const duplicate=run([{results:[rows[0],{...rows[0],name:'PRIVATE_NAME_CANARY_DUPLICATE'}]}],'duplicate');
  assert.notEqual(duplicate.result.status,0);
  assert.match(duplicate.result.stderr,/STOP_DUPLICATE_CUSTOMER_ID/);
  assertNoCanaryOutput(duplicate.result,'duplicate');

  const empty=run([{results:[]}],'empty');
  assert.notEqual(empty.result.status,0);
  assert.match(empty.result.stderr,/STOP_CUSTOMER_ROWS_EMPTY/);

  const prefixedGarbage=run(`transport log line\n${JSON.stringify([{results:rows}])}`,'prefixed-garbage');
  assert.notEqual(prefixedGarbage.result.status,0);
  assert.match(prefixedGarbage.result.stderr,/STOP_INPUT_JSON_INVALID/);
  assertNoCanaryOutput(prefixedGarbage.result,'prefixed-garbage');

  const suffixedGarbage=run(`${JSON.stringify([{results:rows}])}\nnon-json trailer`,'suffixed-garbage');
  assert.notEqual(suffixedGarbage.result.status,0);
  assert.match(suffixedGarbage.result.stderr,/STOP_INPUT_JSON_INVALID/);
  assertNoCanaryOutput(suffixedGarbage.result,'suffixed-garbage');

  const badShaInput=path.join(root,'bad-sha.raw');
  const badShaOutput=path.join(root,'bad-sha.json');
  fs.writeFileSync(badShaInput,JSON.stringify([{results:rows}]),'utf8');
  const badSha=spawnSync(process.execPath,[
    'scripts/build-production-identity-snapshot.mjs',
    '--input',badShaInput,
    '--output',badShaOutput,
    '--source-sha','not-a-sha'
  ],{encoding:'utf8'});
  assert.notEqual(badSha.status,0);
  assert.match(badSha.stderr,/STOP_SOURCE_SHA_INVALID/);
  assertNoCanaryOutput(badSha,'bad-sha');

  const source=fs.readFileSync('scripts/build-production-identity-snapshot.mjs','utf8');
  assert.equal(hasNetworkSurface(source),false,'normalizer contains network/process execution surface');
  assert.doesNotMatch(source,/\bwrangler\b/i);
  assert.doesNotMatch(source,/d1\s+execute/i);
  assert.doesNotMatch(source,/\bprocess\.env\b/i);
  assert.equal(hasMutationSql(source),false,'normalizer contains mutation SQL context');
  assert.match(source,/\.replace\(/,'local string replace remains allowed');
  assert.match(source,/customer-crm-production-identity-snapshot-v1/);
  assert.match(source,/all_customer_identities/);
  assert.match(source,/parseProductionSnapshotText/);
  assert.match(source,/PRIVATE_CUSTOMER_VALUES_PRINTED=0/);
  assert.match(source,/PRODUCTION_D1_WRITE=0/);

  const networkFixtures=[
    "import https from 'https'",
    "import 'https'",
    "import/*comment*/'node:http'",
    "import {} from/*comment*/'node:http2'",
    "import http from 'node:http'",
    "import {connect} from 'node:http2'",
    "await import('http2')",
    "await import/*comment*/('node:http2')",
    'await import(`node:https`)',
    'const netTemplate = require(`node:net`)',
    "const net = require('net')",
    "const tls = require/*comment*/('node:tls')",
    "await import('node:tls')",
    "import {lookup} from 'dns'",
    "import {resolve} from 'node:dns/promises'",
    "const dnsPromises = require('dns/promises')",
    "import dgram from 'node:dgram'",
    "import {connect} from 'cloudflare:sockets'",
    "import {request} from 'undici'",
    "import 'node-fetch'",
    "process.getBuiltinModule('https')",
    "process/*comment*/.getBuiltinModule/*comment*/('node:http2')",
    "createRequire(import.meta.url)('node:https')",
    "createRequire/*comment*/(import.meta.url)('node:https')",
    "fetch/*comment*/('https://example.invalid')",
    "new WebSocket('wss://example.invalid')",
    "curl https://example.invalid"
  ];
  for(const fixture of networkFixtures){
    assert.equal(hasNetworkSurface(fixture),true,`network guard missed ${fixture}`);
  }
  const networkAllowedFixtures=[
    "const httpsLabel='offline';",
    "const networkStatus='DISABLED';",
    "const fetchRequired=false;",
    "const createRequireLabel='disabled';",
    "const marker='import/*comment*/';"
  ];
  for(const fixture of networkAllowedFixtures){
    assert.equal(hasNetworkSurface(fixture),false,`network guard false-positive ${fixture}`);
  }

  const mutationFixtures=[
    'INSERT INTO customers(customer_id) VALUES(1)',
    'INSERT OR ABORT INTO main.customers(customer_id) VALUES(1)',
    'UPDATE customers SET name=1',
    "UPDATE/**/customers/**/SET name='new'",
    "UPDATE--comment\ncustomers--comment\nSET name='new'",
    String.raw`const sql="UPDATE--x\ncustomers--x\nSET name='new'";`,
    'UPDATE main.customers SET name=1',
    'UPDATE customers AS c SET name=1',
    'UPDATE customers INDEXED BY idx SET name=1',
    'DELETE FROM main.customers',
    'DELETE/**/FROM/**/main.customers',
    'DELETE--comment\nFROM--comment\nmain.customers',
    String.raw`const sql="DELETE--x\nFROM--x\nmain.customers";`,
    'CREATE TABLE t(x INTEGER)',
    'CREATE/**/UNIQUE/**/INDEX idx ON customers(customer_id)',
    'CREATE--comment\nUNIQUE--comment\nINDEX idx ON customers(customer_id)',
    String.raw`const sql="CREATE--x\nUNIQUE--x\nINDEX idx ON customers(customer_id)";`,
    'CREATE UNIQUE INDEX idx ON customers(customer_id)',
    'CREATE VIRTUAL TABLE v USING fts5(x)',
    'ALTER TABLE customers ADD COLUMN x TEXT',
    'DROP TABLE IF EXISTS customers',
    'REPLACE INTO customers(customer_id) VALUES(1)',
    'TRUNCATE TABLE customers',
    'UPSERT customers',
    'PRAGMA user_version=1',
    'VACUUM',
    'REINDEX idx',
    'ANALYZE',
    "ATTACH DATABASE 'x.db' AS x",
    'DETACH DATABASE x'
  ];
  for(const fixture of mutationFixtures){
    assert.equal(hasMutationSql(fixture),true,`SQL guard missed ${fixture}`);
  }
  const mutationAllowedFixtures=[
    'tmp_path.replace(receipt_path)',
    'const status="UPDATE_REQUIRED";',
    'createHash("sha256")',
    'function analyzeSalesHistory(){}'
  ];
  for(const fixture of mutationAllowedFixtures){
    assert.equal(hasMutationSql(fixture),false,`SQL guard false-positive ${fixture}`);
  }

  console.log('PRODUCTION_IDENTITY_SNAPSHOT_WRAPPED_JSON=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_OBJECT_JSON=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_DIRECT_ROWS=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_BOM_TOLERANCE=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_WRAPPER_INTEGRITY_FAIL_CLOSED=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_WRAPPER_ERROR_FIELD_FAIL_CLOSED=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_OUTER_WRAPPER_FAIL_CLOSED=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_REQUIRED_IDENTITY_FIELDS=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_DELETED_IDENTITY_PRESERVED=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_DUPLICATE_FAIL_CLOSED=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_EMPTY_FAIL_CLOSED=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_GARBAGE_FAIL_CLOSED=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_SHA_BOUND=PASS');
  console.log(`PRODUCTION_IDENTITY_SNAPSHOT_NETWORK_GUARD=${networkFixtures.length}/${networkFixtures.length}_PASS`);
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_NETWORK_COMMENT_SEPARATOR=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_NETWORK_TEMPLATE_LITERAL=PASS');
  console.log(`PRODUCTION_IDENTITY_SNAPSHOT_SQL_GUARD=${mutationFixtures.length}/${mutationFixtures.length}_PASS`);
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_SQL_COMMENT_SEPARATOR=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_SQL_LINE_COMMENT_SEPARATOR=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_SQL_ESCAPED_NEWLINE_SOURCE=PASS');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_PRIVATE_CANARIES=0');
  console.log('PRODUCTION_IDENTITY_SNAPSHOT_LOCAL_ONLY=PASS');
  console.log('PRODUCTION_D1_READ=0');
  console.log('PRODUCTION_D1_WRITE=0');
  console.log('NETWORK_ACCESS=0');
}finally{
  fs.rmSync(root,{recursive:true,force:true});
}
