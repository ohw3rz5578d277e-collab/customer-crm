import fs from 'node:fs';
import assert from 'node:assert/strict';
import { summarizeLineHistoryOwnerBackfillPreview } from '../src/crm-line-history-owner-backfill-preview-result.mjs';

const previewSummary={
  planner:'line_history_owner_backfill_readonly_preview_v1',
  source_plan_sha256:'a'.repeat(64),
  source_candidate_snapshot_sha256:'b'.repeat(64),
  selected_private_rows_sha256:'c'.repeat(64),
  preview_sql_sha256:'d'.repeat(64),
  selected_candidate_rows:5,
  batch_duplicate_rows:1,
  validation_error_count:0,
  preview_ready:true
};

const d1Payload=[
  {
    results:[
      {
        chunk_no:1,
        candidate_rows:3,
        batch_duplicate_rows:1,
        target_missing_rows:0,
        target_line_conflict_rows:0,
        already_present_rows:1,
        would_insert_rows:1
      }
    ],
    success:true
  },
  {
    results:[
      {
        chunk_no:2,
        candidate_rows:2,
        batch_duplicate_rows:0,
        target_missing_rows:0,
        target_line_conflict_rows:0,
        already_present_rows:0,
        would_insert_rows:2
      }
    ],
    success:true
  }
];

const result=summarizeLineHistoryOwnerBackfillPreview({previewSummary,d1Payload});

assert.equal(result.planner,'line_history_owner_backfill_preview_result_v1');
assert.equal(result.chunk_count,2);
assert.equal(result.candidate_rows,5);
assert.equal(result.batch_duplicate_rows,1);
assert.equal(result.target_missing_rows,0);
assert.equal(result.target_line_conflict_rows,0);
assert.equal(result.already_present_rows,1);
assert.equal(result.would_insert_rows,3);
assert.equal(result.exact_physical_insert_rows,3);
assert.equal(result.validation_error_count,0);
assert.equal(result.preview_pass,true);
assert.equal(result.write_required,true);
assert.equal(result.authorization_granted,false);
assert.equal(result.safety.production_d1_read,1);
assert.equal(result.safety.production_d1_write,0);
assert.equal(result.safety.write_sql_generated,false);
assert.equal(result.safety.write_sql_executed,false);

const missingTarget=summarizeLineHistoryOwnerBackfillPreview({
  previewSummary,
  d1Payload:[{
    results:[{
      chunk_no:1,
      candidate_rows:5,
      batch_duplicate_rows:1,
      target_missing_rows:1,
      target_line_conflict_rows:0,
      already_present_rows:0,
      would_insert_rows:3
    }]
  }]
});
assert.equal(missingTarget.preview_pass,false);
assert.ok(missingTarget.validation_errors.includes('TARGET_CUSTOMER_MISSING'));
assert.equal(missingTarget.exact_physical_insert_rows,0);

const lineConflict=summarizeLineHistoryOwnerBackfillPreview({
  previewSummary,
  d1Payload:[{
    results:[{
      chunk_no:1,
      candidate_rows:5,
      batch_duplicate_rows:1,
      target_missing_rows:0,
      target_line_conflict_rows:1,
      already_present_rows:0,
      would_insert_rows:3
    }]
  }]
});
assert.equal(lineConflict.preview_pass,false);
assert.ok(lineConflict.validation_errors.includes('TARGET_LINE_CONFLICT'));

const countMismatch=summarizeLineHistoryOwnerBackfillPreview({
  previewSummary,
  d1Payload:[{
    results:[{
      chunk_no:1,
      candidate_rows:4,
      batch_duplicate_rows:1,
      target_missing_rows:0,
      target_line_conflict_rows:0,
      already_present_rows:0,
      would_insert_rows:3
    }]
  }]
});
assert.equal(countMismatch.preview_pass,false);
assert.ok(countMismatch.validation_errors.includes('PREVIEW_CANDIDATE_COUNT_MISMATCH'));

const cli=fs.readFileSync('scripts/summarize-line-history-owner-backfill-preview.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.match(cli,/EXACT_PHYSICAL_INSERT_ROWS/);
assert.match(cli,/AUTHORIZATION_GRANTED=NO/);
assert.match(cli,/PRODUCTION_D1_WRITE=0/);

const runner=fs.readFileSync('scripts/run-line-history-owner-backfill-readonly-preview.sh','utf8');
assert.match(runner,/MAIN_SHA_GUARD=PASS/);
assert.match(runner,/PREVIEW_SQL_READ_ONLY=PASS/);
assert.match(runner,/SELECT 1 AS auth_probe/);
assert.match(runner,/--file "\$SQL"/);
assert.match(runner,/summarize-line-history-owner-backfill-preview\.mjs/);
assert.match(runner,/PRODUCTION_D1_READ=YES/);
assert.match(runner,/PRODUCTION_D1_WRITE=0/);
assert.match(runner,/WRITE_SQL_GENERATED=0/);
assert.doesNotMatch(runner,/wrangler_clean\s+d1\s+execute[^\n]*--command\s+"(?:INSERT|UPDATE|DELETE)/i);
assert.doesNotMatch(runner,/wrangler\s+deploy/i);
assert.doesNotMatch(runner,/d1\s+migrations\s+apply/i);

console.log('LINE_HISTORY_OWNER_BACKFILL_D1_PREVIEW_AGGREGATE=PASS');
console.log('LINE_HISTORY_OWNER_BACKFILL_D1_TARGET_GUARDS=PASS');
console.log('LINE_HISTORY_OWNER_BACKFILL_D1_COUNT_GUARD=PASS');
console.log('LINE_HISTORY_OWNER_BACKFILL_D1_RUNNER_READ_ONLY=PASS');
console.log('WRITE_SQL_GENERATED=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
