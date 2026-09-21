import { normalizeImportName, normalizeImportDate } from './crm-customer-csv-import.mjs';

export const SALES_RECONCILIATION_BUILD='crm-sales-readonly-reconciliation-20260921-02';
export const CURRENT_CUSTOMER_ID_RE=/^\d{8}$/;
export const LINE_USER_ID_RE=/^U[0-9a-fA-F]{20,}$/;

function text(v){return v==null?'':String(v).trim()}
function uniqueByCustomerId(rows){
  const map=new Map();
  for(const row of rows||[]){
    const id=text(row?.customer_id);
    if(!id)continue;
    if(!map.has(id))map.set(id,row);
  }
  return [...map.values()];
}
function pushIndex(map,key,row){
  if(!key)return;
  if(!map.has(key))map.set(key,[]);
  map.get(key).push(row);
}

export function normalizeSalesRecord(raw={}){
  const name=text(raw.name);
  const shootDate=normalizeImportDate(raw.shoot_date);
  return{
    year:Number(raw.year)||0,
    source_row:Number(raw.source_row)||0,
    name,
    name_key:normalizeImportName(name),
    shoot_date:shootDate,
    genre:text(raw.genre),
    status:text(raw.status),
    repeat_flag:text(raw.repeat_flag),
    sales_customer_id:text(raw.sales_customer_id),
    line_user_id:text(raw.line_user_id)
  };
}

export function analyzeSalesHistory(records=[]){
  const errors=[];
  const valid=[];
  for(let i=0;i<records.length;i++){
    const row=normalizeSalesRecord(records[i]);
    if(!row.name_key&&!text(records[i]?.shoot_date))continue;
    if(!row.name_key){
      errors.push({index:i,error:'sales_customer_name_required'});
      continue;
    }
    if(!text(records[i]?.shoot_date)){
      errors.push({index:i,error:'sales_shoot_date_required',name:row.name});
      continue;
    }
    if(!row.shoot_date){
      errors.push({index:i,error:'sales_invalid_shoot_date',name:row.name,value:text(records[i]?.shoot_date)});
      continue;
    }
    valid.push(row);
  }

  const groups=new Map();
  let duplicateSameDayRows=0;
  for(const row of valid){
    let group=groups.get(row.name_key);
    if(!group){
      group={
        name_key:row.name_key,
        name:row.name,
        shoots:new Map(),
        source_rows:[],
        sales_customer_ids:new Set(),
        line_user_ids:new Set(),
        repeat_flag_seen:false
      };
      groups.set(row.name_key,group);
    }
    group.source_rows.push(row.source_row);
    if(row.sales_customer_id)group.sales_customer_ids.add(row.sales_customer_id);
    if(row.line_user_id)group.line_user_ids.add(row.line_user_id);
    if(row.repeat_flag)group.repeat_flag_seen=true;

    const existing=group.shoots.get(row.shoot_date);
    if(existing){
      duplicateSameDayRows++;
      existing.duplicate_source_rows=(existing.duplicate_source_rows||0)+1;
      existing.source_rows=[...(existing.source_rows||[]),row.source_row];
      if(!existing.genre&&row.genre)existing.genre=row.genre;
      if(!existing.status&&row.status)existing.status=row.status;
    }else{
      group.shoots.set(row.shoot_date,{
        shoot_date:row.shoot_date,
        year:row.year,
        genre:row.genre,
        status:row.status,
        source_rows:[row.source_row],
        duplicate_source_rows:0
      });
    }
  }

  const customers=[];
  for(const group of groups.values()){
    const shoots=[...group.shoots.values()].sort((a,b)=>a.shoot_date.localeCompare(b.shoot_date));
    const years=[...new Set(shoots.map(x=>Number(x.year)||Number(x.shoot_date.slice(0,4))))].filter(Boolean).sort();
    const ids=[...group.sales_customer_ids];
    const lineIds=[...group.line_user_ids];
    customers.push({
      name_key:group.name_key,
      name:group.name,
      shoot_count:shoots.length,
      shoot_dates:shoots.map(x=>x.shoot_date),
      years,
      is_repeater:shoots.length>=2,
      repeat_flag_seen:group.repeat_flag_seen,
      duplicate_same_day_rows:shoots.reduce((n,x)=>n+Number(x.duplicate_source_rows||0),0),
      sales_customer_ids:ids,
      sales_customer_id_conflict:ids.length>1,
      line_user_ids:lineIds,
      line_user_id_conflict:lineIds.length>1,
      shoots
    });
  }
  customers.sort((a,b)=>a.name.localeCompare(b.name,'ja-JP'));

  return{
    build:SALES_RECONCILIATION_BUILD,
    source_row_count:records.length,
    valid_row_count:valid.length,
    customer_count:customers.length,
    duplicate_same_day_rows:duplicateSameDayRows,
    repeater_count:customers.filter(x=>x.is_repeater).length,
    cross_year_repeater_count:customers.filter(x=>x.years.length>=2).length,
    errors,
    customers
  };
}

export function reconcileSalesHistory({
  salesAnalysis,
  customerMaster=[],
  productionCustomers=[],
  lineNameEvidence=[]
}={}){
  if(!salesAnalysis||!Array.isArray(salesAnalysis.customers))throw new Error('sales_analysis_required');

  const production=(productionCustomers||[]).map(row=>({
    customer_id:text(row?.customer_id),
    name:text(row?.name),
    name_key:normalizeImportName(row?.name),
    line_user_id:text(row?.line_user_id),
    current_customer_id:CURRENT_CUSTOMER_ID_RE.test(text(row?.customer_id))
  })).filter(x=>x.customer_id);

  const master=(customerMaster||[]).map(row=>({
    customer_id:text(row?.customer_id??row?.customerId),
    line_user_id:text(row?.line_user_id??row?.lineUserId),
    name:text(row?.name??row?.displayName),
    line_name:text(row?.line_name??row?.lineName??row?.line_display_name),
    name_key:normalizeImportName(row?.name??row?.displayName),
    line_name_key:normalizeImportName(row?.line_name??row?.lineName??row?.line_display_name),
    current_customer_id:CURRENT_CUSTOMER_ID_RE.test(text(row?.customer_id??row?.customerId)),
    valid_line_user_id:LINE_USER_ID_RE.test(text(row?.line_user_id??row?.lineUserId))
  })).filter(x=>x.customer_id||x.line_user_id||x.name||x.line_name);

  const evidence=(lineNameEvidence||[]).map(row=>({
    name_key:normalizeImportName(row?.name_key??row?.name),
    line_user_id:text(row?.line_user_id??row?.lineUserId),
    source:text(row?.source)||'line_body_exact_full_name'
  })).filter(x=>x.name_key&&LINE_USER_ID_RE.test(x.line_user_id));

  const prodByName=new Map(),prodByLine=new Map(),prodById=new Map();
  for(const p of production){
    if(p.name_key)pushIndex(prodByName,p.name_key,p);
    if(p.line_user_id)pushIndex(prodByLine,p.line_user_id,p);
    prodById.set(p.customer_id,p);
  }

  const masterByName=new Map(),masterByLine=new Map();
  for(const m of master){
    const keys=[m.name_key,m.line_name_key].filter(Boolean);
    for(const key of new Set(keys))pushIndex(masterByName,key,m);
    if(m.valid_line_user_id)pushIndex(masterByLine,m.line_user_id,m);
  }

  const evidenceByName=new Map();
  for(const e of evidence){
    if(!evidenceByName.has(e.name_key))evidenceByName.set(e.name_key,new Map());
    evidenceByName.get(e.name_key).set(e.line_user_id,e);
  }

  const rows=[];
  for(const sales of salesAnalysis.customers){
    const direct=uniqueByCustomerId(prodByName.get(sales.name_key)||[]);
    const masterMatches=masterByName.get(sales.name_key)||[];
    const lineEvidence=[...(evidenceByName.get(sales.name_key)?.values()||[])];

    const linked=new Map();
    for(const m of masterMatches){
      if(m.current_customer_id&&prodById.has(m.customer_id)){
        linked.set(m.customer_id,{
          production:prodById.get(m.customer_id),
          via:'customer_id'
        });
      }
      if(m.valid_line_user_id){
        const lineMatches=uniqueByCustomerId(prodByLine.get(m.line_user_id)||[]);
        if(lineMatches.length===1){
          const p=lineMatches[0];
          linked.set(p.customer_id,{production:p,via:'line_user_id'});
        }
      }
    }

    let classification='UNMATCHED';
    let targetCustomerId='';
    let evidenceText='no_conservative_exact_identity_evidence';
    let safeExistingTarget=false;

    const salesCurrentIds=[...new Set((sales.sales_customer_ids||[]).filter(id=>CURRENT_CUSTOMER_ID_RE.test(id)))];
    const salesLineIds=[...new Set((sales.line_user_ids||[]).filter(id=>LINE_USER_ID_RE.test(id)))];
    const salesIdTargets=uniqueByCustomerId(salesCurrentIds.map(id=>prodById.get(id)).filter(Boolean));
    const salesLineTargets=uniqueByCustomerId(salesLineIds.flatMap(id=>prodByLine.get(id)||[]));

    if(salesCurrentIds.length===1&&salesIdTargets.length===1){
      classification='SALES_CUSTOMER_ID_TO_PRODUCTION_UNIQUE';
      targetCustomerId=salesIdTargets[0].customer_id;
      evidenceText='sales_source_current_customer_id_exact';
      safeExistingTarget=true;
    }else if(salesCurrentIds.length>1||salesIdTargets.length>1){
      classification='SALES_CUSTOMER_ID_AMBIGUOUS';
      evidenceText='sales_source_contains_multiple_current_customer_ids';
    }else if(salesLineIds.length===1&&salesLineTargets.length===1){
      classification='SALES_LINE_USER_ID_TO_PRODUCTION_UNIQUE';
      targetCustomerId=salesLineTargets[0].customer_id;
      evidenceText='sales_source_line_user_id_exact';
      safeExistingTarget=true;
    }else if(salesLineIds.length>1||salesLineTargets.length>1){
      classification='SALES_LINE_USER_ID_AMBIGUOUS';
      evidenceText='sales_source_line_user_id_not_unique';
    }else if(direct.length===1){
      classification='PRODUCTION_EXACT_NAME_REVIEW';
      targetCustomerId=direct[0].customer_id;
      evidenceText='production_name_exact_unique;owner_confirmation_required';
    }else if(direct.length>1){
      classification='PRODUCTION_EXACT_NAME_AMBIGUOUS';
      evidenceText='multiple_production_name_exact_matches';
    }else if(linked.size===1){
      const [id,detail]=[...linked.entries()][0];
      classification='MASTER_EXACT_NAME_TO_PRODUCTION_REVIEW';
      targetCustomerId=id;
      evidenceText='customer_master_exact_name_to_production_'+detail.via+';owner_confirmation_required';
    }else if(linked.size>1){
      classification='MASTER_EXACT_NAME_TO_PRODUCTION_AMBIGUOUS';
      evidenceText='multiple_production_targets_from_customer_master';
    }else if(masterMatches.length===1){
      classification='CUSTOMER_MASTER_EXACT_NAME_REVIEW';
      evidenceText='customer_master_exact_name_without_unique_current_production_target;owner_confirmation_required';
    }else if(masterMatches.length>1){
      classification='CUSTOMER_MASTER_EXACT_NAME_AMBIGUOUS';
      evidenceText='multiple_customer_master_exact_name_matches';
    }else if(lineEvidence.length===1){
      const lineId=lineEvidence[0].line_user_id;
      const prodLine=uniqueByCustomerId(prodByLine.get(lineId)||[]);
      const masterLine=masterByLine.get(lineId)||[];
      if(prodLine.length===1){
        classification='LINE_BODY_EXACT_NAME_TO_PRODUCTION_REVIEW';
        targetCustomerId=prodLine[0].customer_id;
        evidenceText='line_body_exact_full_name+unique_line_user_id_to_production;human_review_required';
      }else if(prodLine.length>1){
        classification='LINE_BODY_EXACT_NAME_AMBIGUOUS';
        evidenceText='line_body_exact_full_name_but_line_user_id_maps_multiple_production_customers';
      }else if(masterLine.length===1){
        classification='LINE_BODY_EXACT_NAME_TO_MASTER_REVIEW';
        if(masterLine[0].current_customer_id&&prodById.has(masterLine[0].customer_id)){
          targetCustomerId=masterLine[0].customer_id;
        }
        evidenceText='line_body_exact_full_name+unique_line_user_id_to_customer_master;human_review_required';
      }else if(masterLine.length>1){
        classification='LINE_BODY_EXACT_NAME_AMBIGUOUS';
        evidenceText='line_body_exact_full_name_but_line_user_id_maps_multiple_customer_master_rows';
      }else{
        classification='LINE_BODY_EXACT_NAME_REVIEW';
        evidenceText='line_body_exact_full_name+unique_line_user_id_without_current_identity_target;human_review_required';
      }
    }else if(lineEvidence.length>1){
      classification='LINE_BODY_EXACT_NAME_AMBIGUOUS';
      evidenceText='same_exact_full_name_observed_on_multiple_line_user_ids';
    }

    rows.push({
      name:sales.name,
      name_key:sales.name_key,
      shoot_count:sales.shoot_count,
      shoot_dates:sales.shoot_dates,
      years:sales.years,
      is_repeater:sales.is_repeater,
      duplicate_same_day_rows:sales.duplicate_same_day_rows,
      sales_customer_ids:sales.sales_customer_ids,
      sales_customer_id_conflict:sales.sales_customer_id_conflict,
      line_user_ids:sales.line_user_ids,
      line_user_id_conflict:sales.line_user_id_conflict,
      classification,
      target_customer_id:targetCustomerId,
      safe_existing_target:safeExistingTarget,
      production_exact_match_count:direct.length,
      customer_master_exact_match_count:masterMatches.length,
      line_body_exact_name_evidence_count:lineEvidence.length,
      evidence:evidenceText
    });
  }

  const counts={};
  for(const row of rows)counts[row.classification]=(counts[row.classification]||0)+1;

  const productionDuplicateNameGroups=[...prodByName.values()].filter(x=>uniqueByCustomerId(x).length>1).length;
  const productionDuplicateLineGroups=[...prodByLine.values()].filter(x=>uniqueByCustomerId(x).length>1).length;

  return{
    build:SALES_RECONCILIATION_BUILD,
    mode:'READ_ONLY_RECONCILIATION',
    sales_customer_count:rows.length,
    customer_master_rows:master.length,
    production_customer_rows:production.length,
    line_name_evidence_rows:evidence.length,
    classification_counts:counts,
    safe_existing_target_count:rows.filter(x=>x.safe_existing_target).length,
    review_evidence_count:rows.filter(x=>x.classification.startsWith('LINE_BODY_EXACT_NAME_')).length,
    unresolved_count:rows.filter(x=>!x.safe_existing_target).length,
    production_duplicate_name_groups:productionDuplicateNameGroups,
    production_duplicate_line_user_id_groups:productionDuplicateLineGroups,
    automatic_customer_creation:false,
    automatic_customer_merge:false,
    fuzzy_auto_link:false,
    line_body_auto_link:false,
    name_only_unmatched_auto_create:false,
    customer_id_generation:false,
    production_write:false,
    rows
  };
}

export function salesReconciliationHealth(){
  return{
    sales_reconciliation_build:SALES_RECONCILIATION_BUILD,
    sales_reconciliation_read_only:true,
    sales_reconciliation_same_name_same_date_dedupe:true,
    sales_reconciliation_repeat_rule:'same_normalized_name_distinct_shoot_dates>=2',
    sales_reconciliation_sales_customer_id_exact_auto_target_only:true,
    sales_reconciliation_sales_line_user_id_exact_auto_target_only:true,
    sales_reconciliation_production_name_exact_review_only:true,
    sales_reconciliation_customer_master_exact_review_only:true,
    sales_reconciliation_customer_master_line_id_exact:true,
    sales_reconciliation_line_body_exact_name_review_only:true,
    sales_reconciliation_line_body_auto_link:false,
    sales_reconciliation_fuzzy_auto_link:false,
    sales_reconciliation_unmatched_auto_create:false,
    sales_reconciliation_customer_merge:false,
    sales_reconciliation_customer_id_generation:false,
    sales_reconciliation_production_write:false
  };
}
