import { createHash } from 'node:crypto';

function text(v){return v==null?'':String(v).trim()}
function digest(v){return createHash('sha256').update(v).digest('hex')}

export function buildLineHistoryWriteCompletionReceipt({
  packet={},
  packetBytes='',
  insertManifest={},
  insertManifestBytes='',
  writeResult={},
  writeResultBytes='',
  postPreviewResult={},
  postPreviewResultBytes='',
  mainSha=''
}={}){
  const blockers=[];
  const sha=text(mainSha);
  const packetSha=digest(packetBytes);
  const manifestSha=digest(insertManifestBytes);
  const writeSha=digest(writeResultBytes);
  const postSha=digest(postPreviewResultBytes);

  if(text(packet.planner)!=='line_history_owner_write_authorization_packet_v2')blockers.push('PACKET_FORMAT_INVALID');
  if(packet.packet_ready!==true)blockers.push('PACKET_NOT_READY');
  if(packet.authorization_required!==true)blockers.push('PACKET_AUTHORIZATION_NOT_REQUIRED');
  if(text(packet.authorization_scope)!=='CUSTOMER_LINE_MESSAGES_INSERT_ONLY')blockers.push('PACKET_SCOPE_INVALID');
  if(text(packet.source_main_sha)!==sha||!/^[0-9a-f]{40}$/.test(sha))blockers.push('MAIN_SHA_MISMATCH');

  if(text(insertManifest.planner)!=='line_history_owner_approved_insert_sql_v1')blockers.push('INSERT_MANIFEST_FORMAT_INVALID');
  if(insertManifest.ready!==true)blockers.push('INSERT_MANIFEST_NOT_READY');
  if(insertManifest.authorization_granted!==true)blockers.push('INSERT_MANIFEST_NOT_AUTHORIZED');
  if(text(insertManifest.packet_sha256)!==packetSha)blockers.push('INSERT_MANIFEST_PACKET_SHA_MISMATCH');

  if(text(writeResult.planner)!=='line_history_owner_approved_insert_result_v1')blockers.push('WRITE_RESULT_FORMAT_INVALID');
  if(writeResult.write_command_completed!==true)blockers.push('WRITE_COMMAND_NOT_COMPLETED');

  if(text(postPreviewResult.planner)!=='line_history_owner_backfill_preview_result_v1')blockers.push('POST_PREVIEW_FORMAT_INVALID');
  if(postPreviewResult.preview_pass!==true)blockers.push('POST_PREVIEW_NOT_PASS');
  if(Number(postPreviewResult.validation_error_count||0)!==0)blockers.push('POST_PREVIEW_HAS_ERRORS');
  if(Number(postPreviewResult.target_missing_rows||0)!==0)blockers.push('POST_PREVIEW_TARGET_MISSING');
  if(Number(postPreviewResult.target_line_conflict_rows||0)!==0)blockers.push('POST_PREVIEW_TARGET_LINE_CONFLICT');
  if(Number(postPreviewResult.would_insert_rows||0)!==0)blockers.push('POST_PREVIEW_REMAINING_INSERTS');

  const packetRows=Number(packet.exact_physical_insert_rows);
  const manifestRows=Number(insertManifest.exact_physical_insert_rows);
  const expectedRows=Number(writeResult.expected_physical_insert_rows);
  const reportedRows=Number(writeResult.reported_change_rows);

  if(!Number.isInteger(packetRows)||packetRows<1)blockers.push('PACKET_EXACT_ROWS_INVALID');
  if(manifestRows!==packetRows)blockers.push('MANIFEST_EXACT_ROWS_MISMATCH');
  if(expectedRows!==packetRows)blockers.push('WRITE_EXPECTED_ROWS_MISMATCH');

  if(writeResult.change_metadata_found===true){
    if(writeResult.exact_change_count_match!==true)blockers.push('WRITE_CHANGE_COUNT_MISMATCH');
    if(reportedRows!==packetRows)blockers.push('WRITE_REPORTED_ROWS_MISMATCH');
  }

  const uniqueBlockers=[...new Set(blockers)];
  const complete=uniqueBlockers.length===0;

  return {
    planner:'line_history_owner_write_completion_receipt_v1',
    complete,
    source_main_sha:sha,
    packet_sha256:packetSha,
    insert_manifest_sha256:manifestSha,
    write_result_sha256:writeSha,
    post_preview_result_sha256:postSha,
    approved_insert_sql_sha256:text(insertManifest.sql_sha256),
    authorization_scope:text(packet.authorization_scope),
    exact_physical_insert_rows:complete?packetRows:0,
    change_metadata_found:writeResult.change_metadata_found===true,
    reported_change_rows:writeResult.change_metadata_found===true?reportedRows:0,
    post_preview_would_insert_rows:Number(postPreviewResult.would_insert_rows||0),
    post_preview_already_present_rows:Number(postPreviewResult.already_present_rows||0),
    blocker_count:uniqueBlockers.length,
    blockers:uniqueBlockers,
    safety:{
      private_customer_id_output:false,
      private_line_user_id_output:false,
      message_text_output:false,
      customer_name_output:false,
      line_send:0,
      customer_id_generation:0,
      customer_update:0,
      customer_delete:0,
      customer_merge:0,
      worker_deploy:0,
      production_deploy:0
    }
  };
}
