import fs from 'node:fs';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { buildApprovedLineHistoryInsertSql } from '../src/crm-line-history-owner-approved-insert.mjs';

const sha256=v=>createHash('sha256').update(v).digest('hex');
const mainSha='f'.repeat(40);
const line='U1234567890abcdef1234567890abcdef';
const privateRows=[
  {
    message_key:'m1',
    target_customer_id:'26000001',
    line_user_id:line,
    direction:'inbound',
    message_type:'text',
    message_text:'hello',
    sender_name:'Customer',
    sent_at:'2026-01-01T10:00:00+09:00',
    source:'sheet',
    source_row:2,
    queue_id:'q1',
    batch_duplicate:false
  },
  {
    message_key:'m2',
    target_customer_id:'26000001',
    line_user_id:line,
    direction:'outbound',
    message_type:'text',
    message_text:'reply',
    sender_name:'Owner',
    sent_at:'2026-01-01T10:01:00+09:00',
    source:'sheet',
    source_row:3,
    queue_id:'q1',
    batch_duplicate:false
  },
  {
    message_key:'m3',
    target_customer_id:'26000001',
    line_user_id:line,
    direction:'inbound',
    message_type:'text',
    message_text:'duplicate',
    sender_name:'Customer',
    sent_at:'2026-01-01T10:02:00+09:00',
    source:'sheet',
    source_row:4,
    queue_id:'q1',
    batch_duplicate:true
  }
];

const rowsBytes=Buffer.from(JSON.stringify(privateRows,null,2)+'\n');
const previewResult={
  planner:'line_history_owner_backfill_preview_result_v1',
  preview_pass:true,
  validation_error_count:0,
  target_missing_rows:0,
  target_line_conflict_rows:0,
  exact_physical_insert_rows:2,
  authorization_granted:false
};
const previewBytes=Buffer.from(JSON.stringify(previewResult,null,2)+'\n');
const packetBase={
  planner:'line_history_owner_write_authorization_packet_v2',
  packet_ready:true,
  authorization_required:true,
  authorization_granted:false,
  authorization_scope:'CUSTOMER_LINE_MESSAGES_INSERT_ONLY',
  source_main_sha:mainSha,
  source_preview_result_sha256:sha256(previewBytes),
  selected_private_rows_sha256:sha256(rowsBytes),
  exact_physical_insert_rows:2
};
const approval='PLAN_SHA a / PREVIEW_SHA b / MAIN_SHA '+mainSha+' / ROWS 2 のcustomer_line_messages INSERT-only Production D1 writeを承認します';
const packet={...packetBase,approval_text:approval};
const packetBytes=Buffer.from(JSON.stringify(packet,null,2)+'\n');

const result=buildApprovedLineHistoryInsertSql({
  packet,
  packetBytes,
  previewResult,
  previewResultBytes:previewBytes,
  privateRows,
  privateRowsBytes:rowsBytes,
  approvalText:approval,
  mainSha
});

assert.equal(result.ready,true);
assert.equal(result.authorization_granted,true);
assert.equal(result.exact_physical_insert_rows,2);
assert.equal(result.source_statement_rows,2);
assert.match(result.sql,/WITH candidates\(/);
assert.match(result.sql,/INSERT OR IGNORE INTO customer_line_messages/);
assert.equal((result.sql.match(/INSERT OR IGNORE INTO customer_line_messages/g)||[]).length,1);
const sqlBody=result.sql.replace(/^--.*$/gm,'');
assert.doesNotMatch(sqlBody,/\bUPDATE\b/i);
assert.doesNotMatch(sqlBody,/\bDELETE\b/i);
assert.doesNotMatch(sqlBody,/INSERT\s+(?:OR\s+IGNORE\s+)?INTO\s+customers\b/i);
assert.doesNotMatch(result.sql,/\bm3\b/);
assert.match(result.sql,/m1/);
assert.match(result.sql,/m2/);
assert.equal(result.safety.target_table,'customer_line_messages');
assert.equal(result.safety.insert_only,true);
assert.equal(result.safety.customer_table_write,false);

const wrongApproval=buildApprovedLineHistoryInsertSql({
  packet,
  packetBytes,
  previewResult,
  previewResultBytes:previewBytes,
  privateRows,
  privateRowsBytes:rowsBytes,
  approvalText:'wrong',
  mainSha
});
assert.equal(wrongApproval.ready,false);
assert.equal(wrongApproval.authorization_granted,false);
assert.equal(wrongApproval.sql,'');
assert.ok(wrongApproval.blockers.includes('EXACT_APPROVAL_TEXT_MISMATCH'));

const wrongMain=buildApprovedLineHistoryInsertSql({
  packet,
  packetBytes,
  previewResult,
  previewResultBytes:previewBytes,
  privateRows,
  privateRowsBytes:rowsBytes,
  approvalText:approval,
  mainSha:'e'.repeat(40)
});
assert.equal(wrongMain.ready,false);
assert.ok(wrongMain.blockers.includes('MAIN_SHA_MISMATCH'));

const wrongPreviewBytes=Buffer.from(JSON.stringify({...previewResult,exact_physical_insert_rows:1})+'\n');
const wrongPreview=buildApprovedLineHistoryInsertSql({
  packet,
  packetBytes,
  previewResult:{...previewResult,exact_physical_insert_rows:1},
  previewResultBytes:wrongPreviewBytes,
  privateRows,
  privateRowsBytes:rowsBytes,
  approvalText:approval,
  mainSha
});
assert.equal(wrongPreview.ready,false);
assert.ok(wrongPreview.blockers.includes('PREVIEW_RESULT_SHA_MISMATCH'));
assert.ok(wrongPreview.blockers.includes('EXACT_ROWS_MISMATCH'));

const wrongRows=Buffer.from(JSON.stringify(privateRows.slice(0,1))+'\n');
const rowsMismatch=buildApprovedLineHistoryInsertSql({
  packet,
  packetBytes,
  previewResult,
  previewResultBytes:previewBytes,
  privateRows:privateRows.slice(0,1),
  privateRowsBytes:wrongRows,
  approvalText:approval,
  mainSha
});
assert.equal(rowsMismatch.ready,false);
assert.ok(rowsMismatch.blockers.includes('PRIVATE_ROWS_SHA_MISMATCH'));

const conflictPreview=buildApprovedLineHistoryInsertSql({
  packet,
  packetBytes,
  previewResult:{...previewResult,target_line_conflict_rows:1},
  previewResultBytes:previewBytes,
  privateRows,
  privateRowsBytes:rowsBytes,
  approvalText:approval,
  mainSha
});
assert.equal(conflictPreview.ready,false);
assert.ok(conflictPreview.blockers.includes('PREVIEW_TARGET_LINE_CONFLICT'));

const cli=fs.readFileSync('scripts/build-approved-line-history-insert.mjs','utf8');
assert.match(cli,/--approval-file/);
assert.match(cli,/EXACT_PHYSICAL_INSERT_ROWS/);
assert.match(cli,/WRITE_SQL_GENERATED=YES/);
assert.match(cli,/SQL_EXECUTED=0/);
assert.match(cli,/PRODUCTION_D1_WRITE=0/);
assert.match(cli,/CUSTOMER_TABLE_WRITE=0/);
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/d1\s+execute/i);

const runner=fs.readFileSync('scripts/run-line-history-owner-approved-insert.sh','utf8');
const approvalIndex=runner.indexOf('EXACT_OWNER_APPROVAL=PASS');
const freshPreviewIndex=runner.indexOf('Mandatory fresh Production D1 preview');
const buildSqlIndex=runner.indexOf('Generate exact-approved INSERT-only SQL');
const executeIndex=runner.indexOf('Execute exact-approved Production D1 INSERT-only write');
assert.ok(approvalIndex>0);
assert.ok(freshPreviewIndex>approvalIndex);
assert.ok(buildSqlIndex>freshPreviewIndex);
assert.ok(executeIndex>buildSqlIndex);
assert.match(runner,/STOP_EXACT_OWNER_APPROVAL_MISMATCH/);
assert.match(runner,/STOP_PACKET_MAIN_SHA_MISMATCH/);
assert.match(runner,/STOP_FRESH_PREVIEW_DRIFT/);
assert.match(runner,/STOP_FRESH_PREVIEW_EXACT_ROWS_DRIFT/);
assert.match(runner,/APPROVED_SQL_STATIC_AUDIT=PASS/);
assert.match(runner,/TARGET_TABLE=customer_line_messages/);
assert.match(runner,/CUSTOMER_TABLE_WRITE=0/);
assert.match(runner,/PRODUCTION_D1_WRITE=YES/);
assert.match(runner,/REMAINING_WOULD_INSERT_ROWS=0/);
assert.doesNotMatch(runner,/wrangler\s+deploy/i);
assert.doesNotMatch(runner,/d1\s+migrations\s+apply/i);
assert.doesNotMatch(runner,/api\.line\.me/i);

const executeSection=runner.slice(executeIndex);
assert.match(executeSection,/--file "\$OUT_DIR\/approved-insert\.sql"/);
assert.doesNotMatch(executeSection,/UPDATE\s+customers/i);
assert.doesNotMatch(executeSection,/DELETE\s+FROM\s+customers/i);

console.log('LINE_HISTORY_APPROVED_INSERT_EXACT_APPROVAL_GATE=PASS');
console.log('LINE_HISTORY_APPROVED_INSERT_MAIN_SHA_GATE=PASS');
console.log('LINE_HISTORY_APPROVED_INSERT_ARTIFACT_HASH_GATE=PASS');
console.log('LINE_HISTORY_APPROVED_INSERT_SINGLE_STATEMENT=PASS');
console.log('LINE_HISTORY_APPROVED_INSERT_CUSTOMER_LINE_MESSAGES_ONLY=PASS');
console.log('LINE_HISTORY_APPROVED_INSERT_FRESH_PREVIEW_BEFORE_WRITE=PASS');
console.log('LINE_HISTORY_APPROVED_INSERT_POST_WRITE_VERIFY=PASS');
console.log('CUSTOMER_TABLE_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('CUSTOMER_MERGE=0');
console.log('LINE_SEND=0');
