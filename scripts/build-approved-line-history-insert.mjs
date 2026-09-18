import fs from 'node:fs';
import { buildApprovedLineHistoryInsertSql } from '../src/crm-line-history-owner-approved-insert.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}

const packetPath=arg('--packet');
const previewResultPath=arg('--preview-result');
const privateRowsPath=arg('--private-rows');
const approvalFile=arg('--approval-file');
const mainSha=arg('--main-sha');
const outSql=arg('--out-sql')||'owner-approved-line-history-insert.sql';
const outManifest=arg('--out-manifest')||'owner-approved-line-history-insert-manifest.json';

if(!packetPath||!previewResultPath||!privateRowsPath||!approvalFile||!mainSha){
  console.error(
    'Usage: node scripts/build-approved-line-history-insert.mjs '+
    '--packet write-authorization-packet.json '+
    '--preview-result owner-backfill-preview-result.json '+
    '--private-rows owner-backfill-selected-private.json '+
    '--approval-file owner-exact-approval.txt '+
    '--main-sha <40hex> '+
    '[--out-sql approved.sql] [--out-manifest manifest.json]'
  );
  process.exit(2);
}

const packetBytes=fs.readFileSync(packetPath);
const previewBytes=fs.readFileSync(previewResultPath);
const rowsBytes=fs.readFileSync(privateRowsPath);
const approvalText=fs.readFileSync(approvalFile,'utf8');

const packet=JSON.parse(packetBytes.toString('utf8'));
const previewResult=JSON.parse(previewBytes.toString('utf8'));
const privateRows=JSON.parse(rowsBytes.toString('utf8'));

const result=buildApprovedLineHistoryInsertSql({
  packet,
  packetBytes,
  previewResult,
  previewResultBytes:previewBytes,
  privateRows,
  privateRowsBytes:rowsBytes,
  approvalText,
  mainSha
});

const manifest={
  planner:result.planner,
  packet_sha256:result.packet_sha256,
  exact_physical_insert_rows:result.exact_physical_insert_rows,
  source_statement_rows:Number(result.source_statement_rows||0),
  sql_sha256:result.sql_sha256,
  ready:result.ready,
  authorization_granted:result.authorization_granted,
  blockers:result.blockers,
  safety:result.safety
};

fs.writeFileSync(outManifest,JSON.stringify(manifest,null,2)+'\n');

if(!result.ready){
  if(fs.existsSync(outSql))fs.unlinkSync(outSql);
  console.log('RESULT=APPROVED_LINE_HISTORY_INSERT_BLOCKED');
  console.log('BLOCKER_COUNT='+result.blockers.length);
  console.log('AUTHORIZATION_GRANTED=NO');
  console.log('WRITE_SQL_GENERATED=0');
  console.log('PRODUCTION_D1_WRITE=0');
  console.log('MANIFEST='+outManifest);
  process.exit(3);
}

fs.writeFileSync(outSql,result.sql,'utf8');

console.log('RESULT=APPROVED_LINE_HISTORY_INSERT_SQL_READY');
console.log('EXACT_PHYSICAL_INSERT_ROWS='+result.exact_physical_insert_rows);
console.log('SOURCE_STATEMENT_ROWS='+result.source_statement_rows);
console.log('SQL_SHA256='+result.sql_sha256);
console.log('AUTHORIZATION_GRANTED=YES');
console.log('WRITE_SQL_GENERATED=YES');
console.log('SQL_EXECUTED=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_TABLE_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('CUSTOMER_MERGE=0');
console.log('LINE_SEND=0');
console.log('PRIVATE_VALUES_PRINTED_TO_TERMINAL=0');
console.log('SQL_FILE='+outSql);
console.log('MANIFEST='+outManifest);
