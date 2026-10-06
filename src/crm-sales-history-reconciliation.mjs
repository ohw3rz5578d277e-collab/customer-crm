export const SALES_RECONCILIATION_BUILD='crm-sales-local-reconciliation-20261006-05';
export const CURRENT_CUSTOMER_ID_RE=/^\d{8}$/;
export const LINE_USER_ID_RE=/^U[0-9a-fA-F]{20,}$/;

function text(v){return v==null?'':String(v).trim()}
function excelDate(serial){const n=Number(serial);if(!Number.isInteger(n)||n<20000||n>80000)return'';return new Date(Date.UTC(1899,11,30)+n*86400000).toISOString().slice(0,10)}
export function normalizeReconciliationName(v){return text(v).normalize('NFKC').toLowerCase().replace(/[\s　・･.．,，、()（）\[\]［］【】「」『』]/g,'')}
export function normalizeReconciliationDate(v){
  const raw=text(v).normalize('NFKC');if(!raw)return'';
  if(/^\d{5}$/.test(raw)){const x=excelDate(raw);if(x)return x}
  const m=raw.match(/^(20\d{2})[\/\.\-年](\d{1,2})[\/\.\-月](\d{1,2})(?:日)?/);if(!m)return'';
  const y=Number(m[1]),mo=Number(m[2]),d=Number(m[3]),dt=new Date(Date.UTC(y,mo-1,d));
  if(dt.getUTCFullYear()!==y||dt.getUTCMonth()!==mo-1||dt.getUTCDate()!==d)return'';
  return `${String(y).padStart(4,'0')}-${String(mo).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
}
function uniqueByCustomerId(rows){const m=new Map();for(const r of rows||[]){const id=text(r?.customer_id);if(id&&!m.has(id))m.set(id,r)}return [...m.values()]}
function pushIndex(map,key,row){if(!key)return;if(!map.has(key))map.set(key,[]);map.get(key).push(row)}
function rawSalesRowHasData(raw={}){return [raw.name,raw.shoot_date,raw.genre,raw.status,raw.repeat_flag,raw.sales_customer_id,raw.line_user_id].some(v=>text(v)!=='')}

export function normalizeSalesRecord(raw={}){
  const name=text(raw.name);
  return {year:Number(raw.year)||0,source_row:Number(raw.source_row)||0,name,name_key:normalizeReconciliationName(name),shoot_date:normalizeReconciliationDate(raw.shoot_date),genre:text(raw.genre),status:text(raw.status),repeat_flag:text(raw.repeat_flag),sales_customer_id:text(raw.sales_customer_id),line_user_id:text(raw.line_user_id)};
}

export function analyzeSalesHistory(records=[]){
  const errors=[],valid=[];
  for(let i=0;i<records.length;i++){
    const raw=records[i]||{};
    if(!rawSalesRowHasData(raw))continue;
    const row=normalizeSalesRecord(raw);
    if(!row.name_key){errors.push({index:i,error:'sales_customer_name_required'});continue}
    if(!text(raw.shoot_date)){errors.push({index:i,error:'sales_shoot_date_required',name:row.name});continue}
    if(!row.shoot_date){errors.push({index:i,error:'sales_invalid_shoot_date',name:row.name,value:text(raw.shoot_date)});continue}
    valid.push(row);
  }

  const groups=new Map();let duplicateSameDayRows=0;
  for(const row of valid){
    let g=groups.get(row.name_key);
    if(!g){g={name_key:row.name_key,name:row.name,source_row_count:0,shoots:new Map(),sales_customer_ids:new Set(),line_user_ids:new Set(),sales_customer_id_present_rows:0,line_user_id_present_rows:0,repeat_flag_seen:false};groups.set(row.name_key,g)}
    g.source_row_count++;
    if(row.sales_customer_id){g.sales_customer_ids.add(row.sales_customer_id);g.sales_customer_id_present_rows++}
    if(row.line_user_id){g.line_user_ids.add(row.line_user_id);g.line_user_id_present_rows++}
    if(row.repeat_flag)g.repeat_flag_seen=true;
    const existing=g.shoots.get(row.shoot_date);
    if(existing){duplicateSameDayRows++;existing.duplicate_source_rows++;existing.source_rows.push(row.source_row);if(!existing.genre&&row.genre)existing.genre=row.genre;if(!existing.status&&row.status)existing.status=row.status}
    else g.shoots.set(row.shoot_date,{shoot_date:row.shoot_date,year:row.year,genre:row.genre,status:row.status,source_rows:[row.source_row],duplicate_source_rows:0});
  }

  const customers=[];
  for(const g of groups.values()){
    const shoots=[...g.shoots.values()].sort((a,b)=>a.shoot_date.localeCompare(b.shoot_date));
    const years=[...new Set(shoots.map(x=>Number(x.year)||Number(x.shoot_date.slice(0,4))))].filter(Boolean).sort();
    const ids=[...g.sales_customer_ids],lineIds=[...g.line_user_ids];
    const invalidIds=ids.filter(id=>!CURRENT_CUSTOMER_ID_RE.test(id)),invalidLines=lineIds.filter(id=>!LINE_USER_ID_RE.test(id));
    const customerPartial=g.sales_customer_id_present_rows>0&&g.sales_customer_id_present_rows<g.source_row_count;
    const linePartial=g.line_user_id_present_rows>0&&g.line_user_id_present_rows<g.source_row_count;
    const customerConflict=ids.length>1||invalidIds.length>0;
    const lineConflict=lineIds.length>1||invalidLines.length>0;
    customers.push({
      name_key:g.name_key,name:g.name,source_row_count:g.source_row_count,shoot_count:shoots.length,shoot_dates:shoots.map(x=>x.shoot_date),years,is_repeater:shoots.length>=2,repeat_flag_seen:g.repeat_flag_seen,
      duplicate_same_day_rows:shoots.reduce((n,x)=>n+Number(x.duplicate_source_rows||0),0),
      sales_customer_ids:ids,sales_customer_id_present_rows:g.sales_customer_id_present_rows,sales_customer_id_partial:customerPartial,sales_customer_id_conflict:customerConflict,sales_customer_id_invalid_values:invalidIds,
      line_user_ids:lineIds,line_user_id_present_rows:g.line_user_id_present_rows,line_user_id_partial:linePartial,line_user_id_conflict:lineConflict,line_user_id_invalid_values:invalidLines,
      source_identity_partial:customerPartial||linePartial,source_identity_conflict:customerConflict||lineConflict,shoots
    });
  }
  customers.sort((a,b)=>a.name.localeCompare(b.name,'ja-JP'));
  return {build:SALES_RECONCILIATION_BUILD,source_row_count:records.length,valid_row_count:valid.length,customer_count:customers.length,duplicate_same_day_rows:duplicateSameDayRows,repeater_count:customers.filter(x=>x.is_repeater).length,cross_year_repeater_count:customers.filter(x=>x.years.length>=2).length,errors,customers};
}

export function reconcileSalesHistory({salesAnalysis,customerMaster=[],productionCustomers=[],lineNameEvidence=[]}={}){
  if(!salesAnalysis||!Array.isArray(salesAnalysis.customers))throw new Error('sales_analysis_required');
  const production=(productionCustomers||[]).map(row=>({customer_id:text(row?.customer_id),name:text(row?.name),name_key:normalizeReconciliationName(row?.name),line_user_id:text(row?.line_user_id),current_customer_id:CURRENT_CUSTOMER_ID_RE.test(text(row?.customer_id))})).filter(x=>x.customer_id);
  const master=(customerMaster||[]).map(row=>({customer_id:text(row?.customer_id??row?.customerId),line_user_id:text(row?.line_user_id??row?.lineUserId),name:text(row?.name??row?.displayName),line_name:text(row?.line_name??row?.lineName??row?.line_display_name),name_key:normalizeReconciliationName(row?.name??row?.displayName),line_name_key:normalizeReconciliationName(row?.line_name??row?.lineName??row?.line_display_name),current_customer_id:CURRENT_CUSTOMER_ID_RE.test(text(row?.customer_id??row?.customerId)),valid_line_user_id:LINE_USER_ID_RE.test(text(row?.line_user_id??row?.lineUserId))})).filter(x=>x.customer_id||x.line_user_id||x.name||x.line_name);
  const evidence=(lineNameEvidence||[]).map(row=>({name_key:normalizeReconciliationName(row?.name_key??row?.name),line_user_id:text(row?.line_user_id??row?.lineUserId),source:text(row?.source)||'line_body_exact_full_name'})).filter(x=>x.name_key&&LINE_USER_ID_RE.test(x.line_user_id));

  const prodByName=new Map(),prodByLine=new Map(),prodById=new Map();
  for(const p of production){if(p.name_key)pushIndex(prodByName,p.name_key,p);if(p.line_user_id)pushIndex(prodByLine,p.line_user_id,p);prodById.set(p.customer_id,p)}
  const masterByName=new Map(),masterByLine=new Map();
  for(const m of master){for(const key of new Set([m.name_key,m.line_name_key].filter(Boolean)))pushIndex(masterByName,key,m);if(m.valid_line_user_id)pushIndex(masterByLine,m.line_user_id,m)}
  const evidenceByName=new Map();for(const e of evidence){if(!evidenceByName.has(e.name_key))evidenceByName.set(e.name_key,new Map());evidenceByName.get(e.name_key).set(e.line_user_id,e)}

  const rows=[];
  for(const sales of salesAnalysis.customers){
    const direct=uniqueByCustomerId(prodByName.get(sales.name_key)||[]),masterMatches=masterByName.get(sales.name_key)||[],lineEvidence=[...(evidenceByName.get(sales.name_key)?.values()||[])];
    const linked=new Map();
    for(const m of masterMatches){
      if(m.current_customer_id&&prodById.has(m.customer_id))linked.set(m.customer_id,{production:prodById.get(m.customer_id),via:'customer_id'});
      if(m.valid_line_user_id){const matches=uniqueByCustomerId(prodByLine.get(m.line_user_id)||[]);if(matches.length===1&&matches[0].current_customer_id)linked.set(matches[0].customer_id,{production:matches[0],via:'line_user_id'})}
    }

    let classification='UNMATCHED',targetCustomerId='',evidenceText='no_conservative_exact_identity_evidence',safeExistingTarget=false;
    const hasCustomerId=sales.sales_customer_id_present_rows>0,hasLineId=sales.line_user_id_present_rows>0;
    const customerId=hasCustomerId&&sales.sales_customer_ids.length===1?sales.sales_customer_ids[0]:'';
    const lineId=hasLineId&&sales.line_user_ids.length===1?sales.line_user_ids[0]:'';
    const customerTarget=customerId?prodById.get(customerId):null;
    const lineTargets=lineId?uniqueByCustomerId(prodByLine.get(lineId)||[]):[];

    if(sales.source_identity_conflict||sales.source_identity_partial){
      classification='SALES_SOURCE_IDENTITY_INCOMPLETE_OR_CONFLICT_REVIEW';
      evidenceText='source_identity_not_complete_and_consistent_on_every_grouped_sales_row;no_name_fallback';
    }else if(hasCustomerId){
      if(!customerTarget){classification='SALES_CUSTOMER_ID_NOT_FOUND_REVIEW';evidenceText='complete_consistent_sales_customer_id_not_found_in_snapshot;no_name_fallback'}
      else if(hasLineId&&(lineTargets.length!==1||lineTargets[0].customer_id!==customerTarget.customer_id)){classification='SALES_SOURCE_IDENTITY_CROSSCHECK_FAILED_REVIEW';evidenceText='complete_customer_id_and_line_user_id_do_not_resolve_to_same_unique_snapshot_customer;no_name_fallback'}
      else {classification='SALES_CUSTOMER_ID_TO_PRODUCTION_UNIQUE';targetCustomerId=customerTarget.customer_id;evidenceText='same_canonical_sales_customer_id_on_every_grouped_source_row'+(hasLineId?'+line_user_id_crosscheck':'');safeExistingTarget=true}
    }else if(hasLineId){
      if(lineTargets.length>1){classification='SALES_LINE_USER_ID_AMBIGUOUS';evidenceText='complete_consistent_sales_line_user_id_maps_multiple_snapshot_customers'}
      else if(lineTargets.length===0){classification='SALES_LINE_USER_ID_NOT_FOUND_REVIEW';evidenceText='complete_consistent_sales_line_user_id_not_found_in_snapshot;no_name_fallback'}
      else if(!lineTargets[0].current_customer_id){classification='SALES_LINE_USER_ID_TARGET_NOT_CANONICAL_REVIEW';evidenceText='complete_consistent_sales_line_user_id_maps_only_to_noncanonical_customer_id;no_name_fallback'}
      else {classification='SALES_LINE_USER_ID_TO_PRODUCTION_UNIQUE';targetCustomerId=lineTargets[0].customer_id;evidenceText='same_valid_sales_line_user_id_on_every_grouped_source_row+canonical_customer_id';safeExistingTarget=true}
    }else if(direct.length===1){classification='PRODUCTION_EXACT_NAME_REVIEW';targetCustomerId=direct[0].current_customer_id?direct[0].customer_id:'';evidenceText='production_snapshot_name_exact_unique;owner_confirmation_required'}
    else if(direct.length>1){classification='PRODUCTION_EXACT_NAME_AMBIGUOUS';evidenceText='multiple_production_snapshot_name_exact_matches'}
    else if(linked.size===1){const [id,detail]=[...linked.entries()][0];classification='MASTER_EXACT_NAME_TO_PRODUCTION_REVIEW';targetCustomerId=id;evidenceText='customer_master_exact_name_to_snapshot_'+detail.via+';owner_confirmation_required'}
    else if(linked.size>1){classification='MASTER_EXACT_NAME_TO_PRODUCTION_AMBIGUOUS';evidenceText='multiple_snapshot_targets_from_customer_master'}
    else if(masterMatches.length===1){classification='CUSTOMER_MASTER_EXACT_NAME_REVIEW';evidenceText='customer_master_exact_name_without_unique_canonical_snapshot_target;owner_confirmation_required'}
    else if(masterMatches.length>1){classification='CUSTOMER_MASTER_EXACT_NAME_AMBIGUOUS';evidenceText='multiple_customer_master_exact_name_matches'}
    else if(lineEvidence.length===1){
      const evidenceLine=lineEvidence[0].line_user_id,prodLine=uniqueByCustomerId(prodByLine.get(evidenceLine)||[]),masterLine=masterByLine.get(evidenceLine)||[];
      if(prodLine.length===1&&prodLine[0].current_customer_id){classification='LINE_BODY_EXACT_NAME_TO_PRODUCTION_REVIEW';targetCustomerId=prodLine[0].customer_id;evidenceText='line_body_exact_full_name+unique_line_user_id_to_canonical_snapshot_customer;human_review_required'}
      else if(prodLine.length===1){classification='LINE_BODY_EXACT_NAME_TO_NONCANONICAL_PRODUCTION_REVIEW';evidenceText='line_body_exact_full_name+unique_line_user_id_to_noncanonical_snapshot_customer;human_review_required'}
      else if(prodLine.length>1){classification='LINE_BODY_EXACT_NAME_AMBIGUOUS';evidenceText='line_body_exact_full_name_but_line_user_id_maps_multiple_snapshot_customers'}
      else if(masterLine.length===1){classification='LINE_BODY_EXACT_NAME_TO_MASTER_REVIEW';if(masterLine[0].current_customer_id&&prodById.has(masterLine[0].customer_id))targetCustomerId=masterLine[0].customer_id;evidenceText='line_body_exact_full_name+unique_line_user_id_to_customer_master;human_review_required'}
      else if(masterLine.length>1){classification='LINE_BODY_EXACT_NAME_AMBIGUOUS';evidenceText='line_body_exact_full_name_but_line_user_id_maps_multiple_customer_master_rows'}
      else {classification='LINE_BODY_EXACT_NAME_REVIEW';evidenceText='line_body_exact_full_name+unique_line_user_id_without_current_identity_target;human_review_required'}
    }else if(lineEvidence.length>1){classification='LINE_BODY_EXACT_NAME_AMBIGUOUS';evidenceText='same_exact_full_name_observed_on_multiple_line_user_ids'}

    rows.push({name:sales.name,name_key:sales.name_key,source_row_count:sales.source_row_count,shoot_count:sales.shoot_count,shoot_dates:sales.shoot_dates,years:sales.years,is_repeater:sales.is_repeater,duplicate_same_day_rows:sales.duplicate_same_day_rows,sales_customer_ids:sales.sales_customer_ids,sales_customer_id_present_rows:sales.sales_customer_id_present_rows,sales_customer_id_partial:sales.sales_customer_id_partial,sales_customer_id_conflict:sales.sales_customer_id_conflict,line_user_ids:sales.line_user_ids,line_user_id_present_rows:sales.line_user_id_present_rows,line_user_id_partial:sales.line_user_id_partial,line_user_id_conflict:sales.line_user_id_conflict,source_identity_partial:sales.source_identity_partial,source_identity_conflict:sales.source_identity_conflict,classification,target_customer_id:targetCustomerId,safe_existing_target:safeExistingTarget,production_exact_match_count:direct.length,customer_master_exact_match_count:masterMatches.length,line_body_exact_name_evidence_count:lineEvidence.length,evidence:evidenceText});
  }

  const counts={};for(const row of rows)counts[row.classification]=(counts[row.classification]||0)+1;
  return {build:SALES_RECONCILIATION_BUILD,mode:'LOCAL_READ_ONLY_RECONCILIATION',sales_customer_count:rows.length,customer_master_rows:master.length,production_customer_rows:production.length,line_name_evidence_rows:evidence.length,classification_counts:counts,safe_existing_target_count:rows.filter(x=>x.safe_existing_target).length,review_evidence_count:rows.filter(x=>x.classification.startsWith('LINE_BODY_EXACT_NAME_')).length,unresolved_count:rows.filter(x=>!x.safe_existing_target).length,production_duplicate_name_groups:[...prodByName.values()].filter(x=>uniqueByCustomerId(x).length>1).length,production_duplicate_line_user_id_groups:[...prodByLine.values()].filter(x=>uniqueByCustomerId(x).length>1).length,automatic_customer_creation:false,automatic_customer_merge:false,fuzzy_auto_link:false,line_body_auto_link:false,name_only_unmatched_auto_create:false,customer_id_generation:false,production_network_access:false,production_read:false,production_write:false,rows};
}

export function salesReconciliationHealth(){
  return {sales_reconciliation_build:SALES_RECONCILIATION_BUILD,sales_reconciliation_local_only:true,sales_reconciliation_read_only:true,sales_reconciliation_same_name_same_date_dedupe:true,sales_reconciliation_repeat_rule:'same_normalized_name_distinct_shoot_dates>=2',sales_reconciliation_safe_target_requires_identity_on_every_grouped_source_row:true,sales_reconciliation_source_identity_conflicts_fail_closed:true,sales_reconciliation_cross_identifier_consistency_required:true,sales_reconciliation_populated_nameless_rows_fail_closed:true,sales_reconciliation_sales_customer_id_exact_auto_target_only:true,sales_reconciliation_sales_line_user_id_exact_auto_target_only:true,sales_reconciliation_auto_target_requires_canonical_customer_id:true,sales_reconciliation_production_name_exact_review_only:true,sales_reconciliation_customer_master_exact_review_only:true,sales_reconciliation_line_body_exact_name_review_only:true,sales_reconciliation_line_body_auto_link:false,sales_reconciliation_fuzzy_auto_link:false,sales_reconciliation_unmatched_auto_create:false,sales_reconciliation_customer_merge:false,sales_reconciliation_customer_id_generation:false,sales_reconciliation_production_network_access:false,sales_reconciliation_production_read:false,sales_reconciliation_production_write:false};
}
