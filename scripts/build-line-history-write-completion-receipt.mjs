import fs from 'node:fs';
import { buildLineHistoryWriteCompletionReceipt } from '../src/crm-line-history-write-completion-receipt.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}

const packetPath=arg('--packet');
const manifestPath=arg('--insert-manifest');
const writeResultPath=arg('--write-result');
const postPreviewPath=arg('--post-preview-result');
const mainSha=arg('--main-sha');
const outPath=arg('--out')||'line-history-write-completion-receipt.json';

if(!packetPath||!manifestPath||!writeResultPath||!postPreviewPath||!mainSha){
  console.error(
    'Usage: node scripts/build-line-history-write-completion-receipt.mjs '+
    '--packet write-authorization-packet.json '+
    '--insert-manifest approved-insert-manifest.json '+
    '--write-result write-result.json '+
    '--post-preview-result post-preview-result.json '+
    '--main-sha <40hex> [--out completion-receipt.json]'
  );
  process.exit(2);
}

const packetBytes=fs.readFileSync(packetPath);
const manifestBytes=fs.readFileSync(manifestPath);
const writeBytes=fs.readFileSync(writeResultPath);
const postBytes=fs.readFileSync(postPreviewPath);

const result=buildLineHistoryWriteCompletionReceipt({
  packet:JSON.parse(packetBytes.toString('utf8')),
  packetBytes,
  insertManifest:JSON.parse(manifestBytes.toString('utf8')),
  insertManifestBytes:manifestBytes,
  writeResult:JSON.parse(writeBytes.toString('utf8')),
  writeResultBytes:writeBytes,
  postPreviewResult:JSON.parse(postBytes.toString('utf8')),
  postPreviewResultBytes:postBytes,
  mainSha
});

fs.writeFileSync(outPath,JSON.stringify(result,null,2)+'\n');

console.log('RESULT='+(result.complete?'LINE_HISTORY_WRITE_COMPLETION_RECEIPT_READY':'LINE_HISTORY_WRITE_COMPLETION_RECEIPT_BLOCKED'));
console.log('COMPLETE='+(result.complete?'YES':'NO'));
console.log('EXACT_PHYSICAL_INSERT_ROWS='+result.exact_physical_insert_rows);
console.log('CHANGE_METADATA_FOUND='+(result.change_metadata_found?'YES':'NO'));
console.log('REPORTED_CHANGE_ROWS='+result.reported_change_rows);
console.log('POST_PREVIEW_WOULD_INSERT_ROWS='+result.post_preview_would_insert_rows);
console.log('BLOCKER_COUNT='+result.blocker_count);
console.log('PACKET_SHA256='+result.packet_sha256);
console.log('INSERT_MANIFEST_SHA256='+result.insert_manifest_sha256);
console.log('WRITE_RESULT_SHA256='+result.write_result_sha256);
console.log('POST_PREVIEW_RESULT_SHA256='+result.post_preview_result_sha256);
console.log('APPROVED_INSERT_SQL_SHA256='+result.approved_insert_sql_sha256);
console.log('PRIVATE_VALUES_PRINTED_TO_TERMINAL=0');
console.log('OUTPUT='+outPath);

if(!result.complete)process.exit(3);
