import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { buildLineHistoryOwnerBackfillReadonlyPreview } from '../src/crm-line-history-owner-backfill-preview.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}
function sha256Bytes(v){return createHash('sha256').update(v).digest('hex')}
function readCandidates(file){
  const raw=JSON.parse(fs.readFileSync(file,'utf8'));
  if(Array.isArray(raw))return raw;
  if(raw&&Array.isArray(raw.candidates))return raw.candidates;
  if(raw&&Array.isArray(raw.rows))return raw.rows;
  if(raw&&Array.isArray(raw.results))return raw.results;
  throw new Error('unsupported_candidate_snapshot_shape');
}

const planPath=arg('--plan');
const candidatesPath=arg('--candidates');
const outDir=arg('--out-dir')||'line-history-owner-backfill-preview';

if(!planPath||!candidatesPath){
  console.error(
    'Usage: node scripts/prepare-line-history-owner-backfill-preview.mjs '+
    '--plan decision-plan-private.json --candidates candidate-snapshot.json '+
    '[--out-dir DIR]'
  );
  process.exit(2);
}

const planRaw=fs.readFileSync(planPath);
const candidateRaw=fs.readFileSync(candidatesPath);
const plan=JSON.parse(planRaw.toString('utf8'));
const candidates=readCandidates(candidatesPath);
const result=buildLineHistoryOwnerBackfillReadonlyPreview({plan,candidates});

fs.mkdirSync(outDir,{recursive:true});

const privateRowsPath=path.join(outDir,'owner-backfill-selected-private.json');
const previewSqlPath=path.join(outDir,'owner-backfill-preview.sql');
const summaryPath=path.join(outDir,'owner-backfill-preview-summary.json');

const privateRowsBytes=Buffer.from(JSON.stringify(result.private_rows,null,2)+'\n');
const previewSqlBytes=Buffer.from(result.preview_sql||'');

fs.writeFileSync(privateRowsPath,privateRowsBytes);
fs.writeFileSync(previewSqlPath,previewSqlBytes);

const summary={
  planner:result.planner,
  source_plan_sha256:sha256Bytes(planRaw),
  source_candidate_snapshot_sha256:sha256Bytes(candidateRaw),
  selected_private_rows_sha256:sha256Bytes(privateRowsBytes),
  preview_sql_sha256:sha256Bytes(previewSqlBytes),
  source_identity_actions:result.source_identity_actions,
  selected_candidate_rows:result.selected_candidate_rows,
  batch_duplicate_rows:result.batch_duplicate_rows,
  preview_candidate_rows:result.preview_candidate_rows,
  validation_error_count:result.validation_error_count,
  validation_errors:result.validation_errors,
  preview_ready:result.preview_ready,
  safety:result.safety
};

fs.writeFileSync(summaryPath,JSON.stringify(summary,null,2)+'\n');

console.log('RESULT='+(result.preview_ready?'LINE_HISTORY_OWNER_BACKFILL_PREVIEW_READY':'LINE_HISTORY_OWNER_BACKFILL_PREVIEW_BLOCKED'));
console.log('SOURCE_IDENTITY_ACTIONS='+result.source_identity_actions);
console.log('SELECTED_CANDIDATE_ROWS='+result.selected_candidate_rows);
console.log('BATCH_DUPLICATE_ROWS='+result.batch_duplicate_rows);
console.log('PREVIEW_CANDIDATE_ROWS='+result.preview_candidate_rows);
console.log('VALIDATION_ERROR_COUNT='+result.validation_error_count);
console.log('SOURCE_PLAN_SHA256='+summary.source_plan_sha256);
console.log('SOURCE_CANDIDATE_SNAPSHOT_SHA256='+summary.source_candidate_snapshot_sha256);
console.log('SELECTED_PRIVATE_ROWS_SHA256='+summary.selected_private_rows_sha256);
console.log('PREVIEW_SQL_SHA256='+summary.preview_sql_sha256);
console.log('SUMMARY='+summaryPath);
console.log('PREVIEW_SQL='+previewSqlPath);
console.log('PRIVATE_ROWS='+privateRowsPath);
console.log('PRIVATE_VALUES_PRINTED_TO_TERMINAL=0');
console.log('PREVIEW_SQL_READ_ONLY=YES');
console.log('WRITE_SQL_GENERATED=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('CUSTOMER_MERGE=0');
console.log('LINE_SEND=0');

if(!result.preview_ready)process.exit(3);
