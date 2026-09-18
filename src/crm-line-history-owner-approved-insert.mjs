import { createHash } from 'node:crypto';

function text(v){return v==null?'':String(v).trim()}
function digest(v){return createHash('sha256').update(v).digest('hex')}
function sqlText(v){return "'"+String(v??'').replaceAll('\u0000','').replaceAll("'","''")+"'"}

function normalizedRows(rows){
  return (Array.isArray(rows)?rows:[])
    .filter(x=>x&&typeof x==='object')
    .map(x=>({
      message_key:text(x.message_key),
      target_customer_id:text(x.target_customer_id),
      line_user_id:text(x.line_user_id),
      direction:text(x.direction),
      message_type:text(x.message_type)||'text',
      message_text:String(x.message_text??''),
      sender_name:text(x.sender_name),
      sent_at:text(x.sent_at),
      source:text(x.source),
      source_row:Number(x.source_row||0)||0,
      queue_id:text(x.queue_id),
      batch_duplicate:x.batch_duplicate===true
    }));
}

export function buildApprovedLineHistoryInsertSql({
  packet={},
  packetBytes='',
  previewResult={},
  previewResultBytes='',
  privateRows=[],
  privateRowsBytes='',
  approvalText='',
  mainSha=''
}={}){
  const blockers=[];
  const sha=text(mainSha);
  const packetPlanner=text(packet.planner);
  const expectedApproval=text(packet.approval_text);
  const actualApproval=text(approvalText);

  if(packetPlanner!=='line_history_owner_write_authorization_packet_v2')blockers.push('PACKET_FORMAT_INVALID');
  if(packet.packet_ready!==true)blockers.push('PACKET_NOT_READY');
  if(packet.authorization_required!==true)blockers.push('PACKET_AUTHORIZATION_NOT_REQUIRED');
  if(packet.authorization_granted!==false)blockers.push('PACKET_AUTHORIZATION_STATE_INVALID');
  if(text(packet.authorization_scope)!=='CUSTOMER_LINE_MESSAGES_INSERT_ONLY')blockers.push('PACKET_SCOPE_INVALID');

  if(!/^[0-9a-f]{40}$/.test(sha)||text(packet.source_main_sha)!==sha)blockers.push('MAIN_SHA_MISMATCH');
  if(!expectedApproval||actualApproval!==expectedApproval)blockers.push('EXACT_APPROVAL_TEXT_MISMATCH');

  const previewDigest=digest(previewResultBytes);
  const rowsDigest=digest(privateRowsBytes);
  const packetDigest=digest(packetBytes);

  if(text(packet.source_preview_result_sha256)!==previewDigest)blockers.push('PREVIEW_RESULT_SHA_MISMATCH');
  if(text(packet.selected_private_rows_sha256)!==rowsDigest)blockers.push('PRIVATE_ROWS_SHA_MISMATCH');

  if(text(previewResult.planner)!=='line_history_owner_backfill_preview_result_v1')blockers.push('PREVIEW_RESULT_FORMAT_INVALID');
  if(previewResult.preview_pass!==true)blockers.push('PREVIEW_RESULT_NOT_PASS');
  if(Number(previewResult.validation_error_count||0)!==0)blockers.push('PREVIEW_RESULT_HAS_ERRORS');
  if(Number(previewResult.target_missing_rows||0)!==0)blockers.push('PREVIEW_TARGET_MISSING');
  if(Number(previewResult.target_line_conflict_rows||0)!==0)blockers.push('PREVIEW_TARGET_LINE_CONFLICT');
  if(previewResult.authorization_granted!==false)blockers.push('PREVIEW_AUTHORIZATION_STATE_INVALID');

  const expectedRows=Number(packet.exact_physical_insert_rows);
  const previewRows=Number(previewResult.exact_physical_insert_rows);
  if(!Number.isInteger(expectedRows)||expectedRows<1)blockers.push('PACKET_EXACT_ROWS_INVALID');
  if(!Number.isInteger(previewRows)||previewRows!==expectedRows)blockers.push('EXACT_ROWS_MISMATCH');

  const rows=normalizedRows(privateRows);
  const insertable=rows.filter(x=>!x.batch_duplicate);

  for(const row of insertable){
    if(!row.message_key||!/^\d{8}$/.test(row.target_customer_id)||!/^U[0-9a-fA-F]{20,}$/.test(row.line_user_id)||!row.direction||!row.sent_at||!row.message_text){
      blockers.push('PRIVATE_ROW_REQUIRED_FIELD_INVALID');
      break;
    }
  }

  const uniqueBlockers=[...new Set(blockers)];
  if(uniqueBlockers.length){
    return {
      planner:'line_history_owner_approved_insert_sql_v1',
      packet_sha256:packetDigest,
      exact_physical_insert_rows:expectedRows>0?expectedRows:0,
      sql:'',
      sql_sha256:'',
      ready:false,
      authorization_granted:false,
      blockers:uniqueBlockers,
      safety:{
        target_table:'customer_line_messages',
        insert_only:true,
        customer_table_write:false,
        customer_id_generation:0,
        customer_update:0,
        customer_delete:0,
        customer_merge:0,
        line_send:0,
        worker_deploy:0,
        production_deploy:0
      }
    };
  }

  const values=insertable.map(row=>{
    const raw=JSON.stringify({
      source:'line_history_owner_review_backfill_v1',
      queue_id:row.queue_id||null,
      source_kind:row.source||null,
      source_row:row.source_row||null
    });
    return '('+[
      sqlText(row.message_key),
      sqlText(row.target_customer_id),
      sqlText(row.line_user_id),
      sqlText(row.direction),
      sqlText(row.message_type),
      sqlText(row.message_text),
      row.sender_name?sqlText(row.sender_name):'NULL',
      sqlText(row.sent_at),
      sqlText(raw)
    ].join(',')+')';
  }).join(',\\n');

  const sql=[
    '-- APPROVED WRITE SQL. customer_line_messages INSERT-only.',
    '-- Exact approval, packet, preview result, private rows, and main SHA were validated before generation.',
    '-- Single-statement write. No customers INSERT/UPDATE/DELETE. No Customer ID generation. No LINE send.',
    `WITH candidates(message_key,target_customer_id,line_user_id,direction,message_type,message_text,sender_name,sent_at,raw_json) AS (VALUES
${values}
)
INSERT OR IGNORE INTO customer_line_messages
(message_key,customer_id,line_user_id,direction,message_type,message_text,sender_name,sent_at,raw_json,created_at)
SELECT
  candidates.message_key,
  c.customer_id,
  candidates.line_user_id,
  candidates.direction,
  candidates.message_type,
  candidates.message_text,
  candidates.sender_name,
  candidates.sent_at,
  candidates.raw_json,
  datetime('now')
FROM candidates
JOIN customers c
  ON c.customer_id=candidates.target_customer_id
WHERE COALESCE(c.deleted_at,'')=''
  AND (COALESCE(c.line_user_id,'')='' OR COALESCE(c.line_user_id,'')=candidates.line_user_id)
  AND NOT EXISTS (
    SELECT 1
    FROM customer_line_messages m
    WHERE m.message_key=candidates.message_key
       OR (
         m.customer_id=c.customer_id
         AND COALESCE(m.line_user_id,'')=candidates.line_user_id
         AND m.direction=candidates.direction
         AND m.sent_at=candidates.sent_at
         AND m.message_text=candidates.message_text
       )
  );`,
    ''
  ].join('\\n');

  const body=sql.replace(/^--.*$/gm,'');

  if(/\b(?:UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE|UPSERT)\b/i.test(body)){
    throw new Error('generated_sql_contains_forbidden_write');
  }
  if(/INSERT\s+(?:OR\s+IGNORE\s+)?INTO\s+customers\b/i.test(body)){
    throw new Error('generated_sql_writes_customers');
  }
  const insertTargets=[...body.matchAll(/INSERT\s+(?:OR\s+IGNORE\s+)?INTO\s+([A-Za-z0-9_]+)/gi)].map(m=>m[1]);
  if(insertTargets.some(x=>x!=='customer_line_messages')){
    throw new Error('generated_sql_targets_forbidden_table');
  }

  return {
    planner:'line_history_owner_approved_insert_sql_v1',
    packet_sha256:packetDigest,
    exact_physical_insert_rows:expectedRows,
    source_statement_rows:insertable.length,
    sql,
    sql_sha256:digest(Buffer.from(sql)),
    ready:true,
    authorization_granted:true,
    blockers:[],
    safety:{
      target_table:'customer_line_messages',
      insert_only:true,
      customer_table_write:false,
      customer_id_generation:0,
      customer_update:0,
      customer_delete:0,
      customer_merge:0,
      line_send:0,
      worker_deploy:0,
      production_deploy:0
    }
  };
}
