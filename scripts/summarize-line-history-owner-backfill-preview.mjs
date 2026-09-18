import fs from 'node:fs';
import { summarizeLineHistoryOwnerBackfillPreview } from '../src/crm-line-history-owner-backfill-preview-result.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}

const summaryPath=arg('--preview-summary');
const rawPath=arg('--d1-raw');
const outPath=arg('--out')||'owner-backfill-preview-result.json';

if(!summaryPath||!rawPath){
  console.error(
    'Usage: node scripts/summarize-line-history-owner-backfill-preview.mjs '+
    '--preview-summary owner-backfill-preview-summary.json '+
    '--d1-raw owner-backfill-preview-d1.raw.json '+
    '[--out owner-backfill-preview-result.json]'
  );
  process.exit(2);
}

const previewSummary=JSON.parse(fs.readFileSync(summaryPath,'utf8'));
const d1Payload=JSON.parse(fs.readFileSync(rawPath,'utf8'));

const result=summarizeLineHistoryOwnerBackfillPreview({previewSummary,d1Payload});
fs.writeFileSync(outPath,JSON.stringify(result,null,2)+'\n');

console.log('RESULT='+(result.preview_pass?'LINE_HISTORY_OWNER_BACKFILL_D1_PREVIEW_PASS':'LINE_HISTORY_OWNER_BACKFILL_D1_PREVIEW_BLOCKED'));
console.log('CHUNK_COUNT='+result.chunk_count);
console.log('CANDIDATE_ROWS='+result.candidate_rows);
console.log('BATCH_DUPLICATE_ROWS='+result.batch_duplicate_rows);
console.log('TARGET_MISSING_ROWS='+result.target_missing_rows);
console.log('TARGET_LINE_CONFLICT_ROWS='+result.target_line_conflict_rows);
console.log('ALREADY_PRESENT_ROWS='+result.already_present_rows);
console.log('WOULD_INSERT_ROWS='+result.would_insert_rows);
console.log('EXACT_PHYSICAL_INSERT_ROWS='+result.exact_physical_insert_rows);
console.log('VALIDATION_ERROR_COUNT='+result.validation_error_count);
console.log('AUTHORIZATION_GRANTED=NO');
console.log('OUTPUT='+outPath);
console.log('PRIVATE_VALUES_PRINTED_TO_TERMINAL=0');
console.log('WRITE_SQL_GENERATED=0');
console.log('PRODUCTION_D1_READ=YES');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('CUSTOMER_MERGE=0');
console.log('LINE_SEND=0');

if(!result.preview_pass)process.exit(3);
