function number(v){
  const n=Number(v);
  return Number.isFinite(n)&&n>=0?n:NaN;
}

function collectResultRows(value,out=[]){
  if(Array.isArray(value)){
    for(const item of value)collectResultRows(item,out);
    return out;
  }
  if(value&&typeof value==='object'){
    if(Array.isArray(value.results)){
      for(const row of value.results){
        if(row&&typeof row==='object')out.push(row);
      }
    }
    for(const v of Object.values(value))collectResultRows(v,out);
  }
  return out;
}

export function summarizeLineHistoryOwnerBackfillPreview({
  previewSummary={},
  d1Payload=null
}={}){
  const rows=collectResultRows(d1Payload,[])
    .filter(r=>Object.prototype.hasOwnProperty.call(r,'chunk_no'));

  const errors=[];
  if(previewSummary?.planner!=='line_history_owner_backfill_readonly_preview_v1'){
    errors.push('PREVIEW_SUMMARY_FORMAT_INVALID');
  }
  if(previewSummary?.preview_ready!==true){
    errors.push('PREVIEW_SUMMARY_NOT_READY');
  }
  if(Number(previewSummary?.validation_error_count||0)!==0){
    errors.push('PREVIEW_SUMMARY_VALIDATION_ERRORS_PRESENT');
  }
  if(!rows.length)errors.push('D1_PREVIEW_ROWS_MISSING');

  const fields=[
    'candidate_rows',
    'batch_duplicate_rows',
    'target_missing_rows',
    'target_line_conflict_rows',
    'already_present_rows',
    'would_insert_rows'
  ];

  const sums=Object.fromEntries(fields.map(k=>[k,0]));
  const seenChunks=new Set();

  for(const row of rows){
    const chunk=number(row.chunk_no);
    if(!Number.isFinite(chunk)||chunk<1){
      errors.push('D1_PREVIEW_CHUNK_INVALID');
      continue;
    }
    if(seenChunks.has(chunk))errors.push('D1_PREVIEW_CHUNK_DUPLICATE');
    seenChunks.add(chunk);

    for(const field of fields){
      const n=number(row[field]);
      if(!Number.isFinite(n)){
        errors.push('D1_PREVIEW_'+field.toUpperCase()+'_INVALID');
      }else{
        sums[field]+=n;
      }
    }
  }

  if(
    Number.isFinite(Number(previewSummary?.selected_candidate_rows))&&
    sums.candidate_rows!==Number(previewSummary.selected_candidate_rows)
  ){
    errors.push('PREVIEW_CANDIDATE_COUNT_MISMATCH');
  }

  if(
    Number.isFinite(Number(previewSummary?.batch_duplicate_rows))&&
    sums.batch_duplicate_rows!==Number(previewSummary.batch_duplicate_rows)
  ){
    errors.push('PREVIEW_BATCH_DUPLICATE_COUNT_MISMATCH');
  }

  if(sums.target_missing_rows>0)errors.push('TARGET_CUSTOMER_MISSING');
  if(sums.target_line_conflict_rows>0)errors.push('TARGET_LINE_CONFLICT');

  const uniqueErrors=[...new Set(errors)];

  return {
    planner:'line_history_owner_backfill_preview_result_v1',
    source_plan_sha256:String(previewSummary?.source_plan_sha256||''),
    source_candidate_snapshot_sha256:String(previewSummary?.source_candidate_snapshot_sha256||''),
    selected_private_rows_sha256:String(previewSummary?.selected_private_rows_sha256||''),
    source_preview_sql_sha256:String(previewSummary?.preview_sql_sha256||''),
    chunk_count:seenChunks.size,
    candidate_rows:sums.candidate_rows,
    batch_duplicate_rows:sums.batch_duplicate_rows,
    target_missing_rows:sums.target_missing_rows,
    target_line_conflict_rows:sums.target_line_conflict_rows,
    already_present_rows:sums.already_present_rows,
    would_insert_rows:sums.would_insert_rows,
    validation_error_count:uniqueErrors.length,
    validation_errors:uniqueErrors,
    preview_pass:uniqueErrors.length===0,
    write_required:uniqueErrors.length===0&&sums.would_insert_rows>0,
    exact_physical_insert_rows:uniqueErrors.length===0?sums.would_insert_rows:0,
    authorization_granted:false,
    safety:{
      production_d1_read:1,
      production_d1_write:0,
      sql_executed_read_only:true,
      write_sql_generated:false,
      write_sql_executed:false,
      customer_id_generation:0,
      customer_update:0,
      customer_delete:0,
      customer_merge:0,
      line_send:0,
      worker_deploy:0,
      production_deploy:0,
      private_values_output:false
    }
  };
}
