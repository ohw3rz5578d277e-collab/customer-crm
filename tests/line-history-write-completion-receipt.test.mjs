import fs from 'node:fs';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { buildLineHistoryWriteCompletionReceipt } from '../src/crm-line-history-write-completion-receipt.mjs';

const hash=v=>createHash('sha256').update(v).digest('hex');
const mainSha='8'.repeat(40);

const packet={
  planner:'line_history_owner_write_authorization_packet_v2',
  packet_ready:true,
  authorization_required:true,
  authorization_scope:'CUSTOMER_LINE_MESSAGES_INSERT_ONLY',
  source_main_sha:mainSha,
  exact_physical_insert_rows:3
};
const packetBytes=Buffer.from(JSON.stringify(packet,null,2)+'\n');

const manifest={
  planner:'line_history_owner_approved_insert_sql_v1',
  packet_sha256:hash(packetBytes),
  exact_physical_insert_rows:3,
  source_statement_rows:4,
  sql_sha256:'a'.repeat(64),
  ready:true,
  authorization_granted:true,
  blockers:[],
  safety:{
    target_table:'customer_line_messages',
    insert_only:true,
    customer_table_write:false
  }
};
const manifestBytes=Buffer.from(JSON.stringify(manifest,null,2)+'\n');

const writeResult={
  planner:'line_history_owner_approved_insert_result_v1',
  expected_physical_insert_rows:3,
  reported_change_rows:3,
  change_metadata_found:true,
  write_command_completed:true,
  exact_change_count_match:true
};
const writeBytes=Buffer.from(JSON.stringify(writeResult,null,2)+'\n');

const post={
  planner:'line_history_owner_backfill_preview_result_v1',
  preview_pass:true,
  validation_error_count:0,
  target_missing_rows:0,
  target_line_conflict_rows:0,
  already_present_rows:4,
  would_insert_rows:0
};
const postBytes=Buffer.from(JSON.stringify(post,null,2)+'\n');

const receipt=buildLineHistoryWriteCompletionReceipt({
  packet,
  packetBytes,
  insertManifest:manifest,
  insertManifestBytes:manifestBytes,
  writeResult,
  writeResultBytes:writeBytes,
  postPreviewResult:post,
  postPreviewResultBytes:postBytes,
  mainSha
});

assert.equal(receipt.planner,'line_history_owner_write_completion_receipt_v1');
assert.equal(receipt.complete,true);
assert.equal(receipt.exact_physical_insert_rows,3);
assert.equal(receipt.change_metadata_found,true);
assert.equal(receipt.reported_change_rows,3);
assert.equal(receipt.post_preview_would_insert_rows,0);
assert.equal(receipt.blocker_count,0);
assert.equal(receipt.authorization_scope,'CUSTOMER_LINE_MESSAGES_INSERT_ONLY');
assert.equal(receipt.packet_sha256,hash(packetBytes));
assert.equal(receipt.insert_manifest_sha256,hash(manifestBytes));
assert.equal(receipt.write_result_sha256,hash(writeBytes));
assert.equal(receipt.post_preview_result_sha256,hash(postBytes));
assert.equal(receipt.approved_insert_sql_sha256,'a'.repeat(64));

const serialized=JSON.stringify(receipt);
for(const secret of [
  '26001234',
  'U1234567890abcdef1234567890abcdef',
  'private message',
  'Private Customer'
]){
  assert.ok(!serialized.includes(secret),'private value leaked: '+secret);
}

const badPost=buildLineHistoryWriteCompletionReceipt({
  packet,
  packetBytes,
  insertManifest:manifest,
  insertManifestBytes:manifestBytes,
  writeResult,
  writeResultBytes:writeBytes,
  postPreviewResult:{...post,would_insert_rows:1},
  postPreviewResultBytes:postBytes,
  mainSha
});
assert.equal(badPost.complete,false);
assert.ok(badPost.blockers.includes('POST_PREVIEW_REMAINING_INSERTS'));

const badCount=buildLineHistoryWriteCompletionReceipt({
  packet,
  packetBytes,
  insertManifest:manifest,
  insertManifestBytes:manifestBytes,
  writeResult:{...writeResult,reported_change_rows:2,exact_change_count_match:false},
  writeResultBytes:writeBytes,
  postPreviewResult:post,
  postPreviewResultBytes:postBytes,
  mainSha
});
assert.equal(badCount.complete,false);
assert.ok(badCount.blockers.includes('WRITE_CHANGE_COUNT_MISMATCH'));
assert.ok(badCount.blockers.includes('WRITE_REPORTED_ROWS_MISMATCH'));

const badManifest=buildLineHistoryWriteCompletionReceipt({
  packet,
  packetBytes,
  insertManifest:{...manifest,packet_sha256:'0'.repeat(64)},
  insertManifestBytes:manifestBytes,
  writeResult,
  writeResultBytes:writeBytes,
  postPreviewResult:post,
  postPreviewResultBytes:postBytes,
  mainSha
});
assert.equal(badManifest.complete,false);
assert.ok(badManifest.blockers.includes('INSERT_MANIFEST_PACKET_SHA_MISMATCH'));

const badMain=buildLineHistoryWriteCompletionReceipt({
  packet,
  packetBytes,
  insertManifest:manifest,
  insertManifestBytes:manifestBytes,
  writeResult,
  writeResultBytes:writeBytes,
  postPreviewResult:post,
  postPreviewResultBytes:postBytes,
  mainSha:'7'.repeat(40)
});
assert.equal(badMain.complete,false);
assert.ok(badMain.blockers.includes('MAIN_SHA_MISMATCH'));

const cli=fs.readFileSync('scripts/build-line-history-write-completion-receipt.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/https?:\/\//i);
assert.match(cli,/PRIVATE_VALUES_PRINTED_TO_TERMINAL=0/);

const runner=fs.readFileSync('scripts/run-line-history-owner-approved-insert.sh','utf8');
const postIndex=runner.indexOf('Mandatory post-write READ ONLY verification');
const receiptIndex=runner.indexOf('Build privacy-safe completion receipt');
assert.ok(postIndex>0);
assert.ok(receiptIndex>postIndex);
assert.match(runner,/build-line-history-write-completion-receipt\.mjs/);
assert.match(runner,/COMPLETION_RECEIPT=\$OUT_DIR\/completion-receipt\.json/);

console.log('LINE_HISTORY_WRITE_RECEIPT_HASH_CHAIN=PASS');
console.log('LINE_HISTORY_WRITE_RECEIPT_EXACT_ROWS=PASS');
console.log('LINE_HISTORY_WRITE_RECEIPT_POST_VERIFY_REQUIRED=PASS');
console.log('LINE_HISTORY_WRITE_RECEIPT_PRIVATE_VALUES_HIDDEN=PASS');
console.log('LINE_HISTORY_WRITE_RECEIPT_RUNNER_ORDER=PASS');
