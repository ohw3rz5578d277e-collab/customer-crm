import fs from 'node:fs';
import { buildLineHistoryOwnerAuthorizationPacket } from '../src/crm-line-history-owner-authorization-packet.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}

const planPath=arg('--plan');
const previewResultPath=arg('--preview-result');
const mainSha=arg('--main-sha');
const outPath=arg('--out')||'line-history-owner-write-authorization-packet.json';

if(!planPath||!previewResultPath||!mainSha){
  console.error(
    'Usage: node scripts/build-line-history-owner-authorization-packet.mjs '+
    '--plan decision-plan-private.json '+
    '--preview-result owner-backfill-preview-result.json '+
    '--main-sha <40hex> [--out authorization-packet.json]'
  );
  process.exit(2);
}

const planRaw=fs.readFileSync(planPath);
const previewRaw=fs.readFileSync(previewResultPath);
const plan=JSON.parse(planRaw.toString('utf8'));
const previewResult=JSON.parse(previewRaw.toString('utf8'));

const packet=buildLineHistoryOwnerAuthorizationPacket({
  plan,
  sourcePlanBytes:planRaw,
  previewResult,
  sourcePreviewResultBytes:previewRaw,
  mainSha
});

fs.writeFileSync(outPath,JSON.stringify(packet,null,2)+'\n');

console.log('RESULT=LINE_HISTORY_OWNER_AUTHORIZATION_PACKET_READY');
console.log('PACKET_READY='+(packet.packet_ready?'YES':'NO'));
console.log('AUTHORIZATION_REQUIRED='+(packet.authorization_required?'YES':'NO'));
console.log('AUTHORIZATION_GRANTED=NO');
console.log('BACKFILL_IDENTITY_ACTIONS='+packet.backfill_identity_actions);
console.log('ALREADY_PRESENT_ROWS='+packet.already_present_rows);
console.log('BATCH_DUPLICATE_ROWS='+packet.batch_duplicate_rows);
console.log('EXACT_PHYSICAL_INSERT_ROWS='+packet.exact_physical_insert_rows);
console.log('VALIDATION_ERROR_COUNT='+packet.validation_error_count);
console.log('BLOCKER_COUNT='+packet.blockers.length);
console.log('SOURCE_PLAN_SHA256='+packet.source_plan_sha256);
console.log('SOURCE_PREVIEW_RESULT_SHA256='+packet.source_preview_result_sha256);
console.log('SOURCE_PREVIEW_SQL_SHA256='+packet.source_preview_sql_sha256);
console.log('SOURCE_MAIN_SHA='+packet.source_main_sha);
if(packet.approval_text)console.log('APPROVAL_TEXT='+packet.approval_text);
console.log('OUTPUT='+outPath);
console.log('PRIVATE_ACTION_VALUES_PRINTED_TO_TERMINAL=0');
console.log('SQL_GENERATED=0');
console.log('SQL_EXECUTED=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('CUSTOMER_MERGE=0');
console.log('LINE_SEND=0');
console.log('WORKER_DEPLOY=0');
console.log('PRODUCTION_DEPLOY=0');

if(!packet.packet_ready)process.exit(3);
