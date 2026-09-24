import fs from 'node:fs';
import path from 'node:path';
import {
  analyzeSalesHistory,
  reconcileSalesHistory,
  SALES_RECONCILIATION_BUILD
} from '../src/crm-sales-history-reconciliation.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}
function required(name){
  const value=arg(name);
  if(!value)throw new Error('missing_argument:'+name);
  return value;
}
function readJson(file){
  return JSON.parse(fs.readFileSync(file,'utf8'));
}
function unwrapRows(value){
  if(Array.isArray(value)){
    if(value.length===1&&value[0]&&Array.isArray(value[0].results))return value[0].results;
    return value;
  }
  if(value&&typeof value==='object'){
    for(const key of ['rows','results','customers','records']){
      if(Array.isArray(value[key]))return value[key];
    }
    if(value.result&&Array.isArray(value.result.results))return value.result.results;
  }
  return [];
}
function scanJsonObjects(text){
  const out=[];
  for(let start=0;start<text.length;start++){
    const ch=text[start];
    if(ch!=='{'&&ch!=='[')continue;
    let depth=0,inString=false,escaped=false;
    for(let i=start;i<text.length;i++){
      const c=text[i];
      if(inString){
        if(escaped)escaped=false;
        else if(c==='\\')escaped=true;
        else if(c==='"')inString=false;
        continue;
      }
      if(c==='"'){inString=true;continue}
      if(c==='{'||c==='[')depth++;
      else if(c==='}'||c===']'){
        depth--;
        if(depth===0){
          const raw=text.slice(start,i+1);
          try{
            out.push(JSON.parse(raw));
            start=i;
          }catch{}
          break;
        }
      }
    }
  }
  return out;
}
function collectCustomerRows(value,out=[]){
  if(Array.isArray(value)){
    for(const item of value)collectCustomerRows(item,out);
    return out;
  }
  if(!value||typeof value!=='object')return out;
  if(Object.prototype.hasOwnProperty.call(value,'customer_id')&&Object.prototype.hasOwnProperty.call(value,'name')){
    out.push(value);
  }
  if(Array.isArray(value.results))for(const row of value.results)collectCustomerRows(row,out);
  else for(const item of Object.values(value))collectCustomerRows(item,out);
  return out;
}
function csvCell(v){
  const s=Array.isArray(v)?v.join(' | '):String(v??'');
  return '"'+s.replaceAll('"','""')+'"';
}
function esc(v){
  return String(v??'')
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'",'&#39;');
}

const salesRecordsPath=required('--sales-records');
const customerMasterPath=required('--customer-master');
const productionCustomersPath=required('--production-customers');
const outDir=required('--out-dir');
const lineEvidencePath=arg('--line-evidence');

fs.mkdirSync(outDir,{recursive:true});

const salesPayload=readJson(salesRecordsPath);
const salesRecords=unwrapRows(salesPayload);
const customerMaster=unwrapRows(readJson(customerMasterPath));
const lineEvidence=lineEvidencePath?(readJson(lineEvidencePath).evidence_rows||[]):[];

const productionRaw=fs.readFileSync(productionCustomersPath,'utf8')
  .replace(/\x1b\[[0-9;?]*[ -/]*[@-~]/g,'');
const productionObjects=scanJsonObjects(productionRaw);
const productionCustomers=[];
const seenProduction=new Set();
for(const obj of productionObjects){
  for(const row of collectCustomerRows(obj)){
    const id=String(row?.customer_id||'').trim();
    if(!id||seenProduction.has(id))continue;
    seenProduction.add(id);
    productionCustomers.push(row);
  }
}

if(!salesRecords.length)throw new Error('sales_records_empty');
if(!customerMaster.length)throw new Error('customer_master_empty');
if(!productionCustomers.length)throw new Error('production_customers_empty');

const analysis=analyzeSalesHistory(salesRecords);
if(analysis.errors.length){
  const errorPath=path.join(outDir,'sales-analysis-errors.json');
  fs.writeFileSync(errorPath,JSON.stringify(analysis.errors,null,2)+'\n');
  console.log('RESULT=STOP_SALES_ANALYSIS_ERRORS');
  console.log('SALES_ANALYSIS_ERRORS='+analysis.errors.length);
  console.log('PRIVATE_VALUES_PRINTED=0');
  process.exit(2);
}

const result=reconcileSalesHistory({
  salesAnalysis:analysis,
  customerMaster,
  productionCustomers,
  lineNameEvidence:lineEvidence
});

const summary={
  build:SALES_RECONCILIATION_BUILD,
  mode:result.mode,
  source_sales_rows:analysis.source_row_count,
  valid_sales_rows:analysis.valid_row_count,
  unique_sales_customers:analysis.customer_count,
  duplicate_same_day_rows:analysis.duplicate_same_day_rows,
  repeater_count:analysis.repeater_count,
  cross_year_repeater_count:analysis.cross_year_repeater_count,
  customer_master_rows:result.customer_master_rows,
  production_customer_rows:result.production_customer_rows,
  line_name_evidence_rows:result.line_name_evidence_rows,
  line_review_evidence_count:result.review_evidence_count,
  safe_existing_target_count:result.safe_existing_target_count,
  unresolved_count:result.unresolved_count,
  production_duplicate_name_groups:result.production_duplicate_name_groups,
  production_duplicate_line_user_id_groups:result.production_duplicate_line_user_id_groups,
  classification_counts:result.classification_counts,
  automatic_customer_creation:false,
  automatic_customer_merge:false,
  fuzzy_auto_link:false,
  line_body_auto_link:false,
  name_only_unmatched_auto_create:false,
  customer_id_generation:false,
  production_d1_write:false
};

fs.writeFileSync(
  path.join(outDir,'summary.json'),
  JSON.stringify(summary,null,2)+'\n'
);

const headers=[
  'sales_name','shoot_count','shoot_dates','years','is_repeater',
  'duplicate_same_day_rows','classification','target_customer_id',
  'safe_existing_target','production_exact_match_count',
  'customer_master_exact_match_count','line_body_exact_name_evidence_count','evidence'
];
const csv=[headers.join(',')];
for(const row of result.rows){
  const record={
    sales_name:row.name,
    shoot_count:row.shoot_count,
    shoot_dates:row.shoot_dates,
    years:row.years,
    is_repeater:row.is_repeater?'YES':'NO',
    duplicate_same_day_rows:row.duplicate_same_day_rows,
    classification:row.classification,
    target_customer_id:row.target_customer_id,
    safe_existing_target:row.safe_existing_target?'YES':'NO',
    production_exact_match_count:row.production_exact_match_count,
    customer_master_exact_match_count:row.customer_master_exact_match_count,
    line_body_exact_name_evidence_count:row.line_body_exact_name_evidence_count,
    evidence:row.evidence
  };
  csv.push(headers.map(h=>csvCell(record[h])).join(','));
}
fs.writeFileSync(path.join(outDir,'review.csv'),'\ufeff'+csv.join('\n')+'\n');

const body=result.rows.map(row=>`
<tr>
<td>${esc(row.name)}</td>
<td>${row.shoot_count}</td>
<td>${esc(row.shoot_dates.join(' / '))}</td>
<td>${esc(row.years.join(', '))}</td>
<td>${row.is_repeater?'リピーター候補':'初回候補'}</td>
<td>${esc(row.classification)}</td>
<td>${esc(row.target_customer_id)}</td>
<td>${row.safe_existing_target?'既存顧客候補':'自動処理禁止'}</td>
<td>${row.line_body_exact_name_evidence_count}</td>
<td>${esc(row.evidence)}</td>
</tr>`).join('');

const classList=Object.entries(result.classification_counts)
  .sort(([a],[b])=>a.localeCompare(b))
  .map(([k,v])=>`<li><b>${esc(k)}</b>: ${v}</li>`)
  .join('');

const html=`<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>売上管理 × CRM READ ONLY照合</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Helvetica Neue",Arial,sans-serif;margin:24px;line-height:1.5;color:#171717}
h1{font-size:24px;margin:0 0 12px}
h2{font-size:18px;margin-top:28px}
.notice{border:1px solid #bbb;border-radius:10px;padding:12px 14px;background:#fafafa}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px;margin:14px 0}
.card{border:1px solid #ddd;border-radius:8px;padding:10px}
table{border-collapse:collapse;width:100%;font-size:12px}
th,td{border:1px solid #ddd;padding:6px;vertical-align:top}
th{position:sticky;top:0;background:white}
</style>
</head>
<body>
<h1>売上管理 × Customer Master × Production CRM</h1>
<div class="notice">
READ ONLY照合です。名前だけでの自動統合、新規顧客作成、Customer ID生成、顧客merge、Production D1 writeは行いません。
</div>
<div class="grid">
<div class="card">売上行<br><b>${analysis.source_row_count}</b></div>
<div class="card">売上顧客<br><b>${analysis.customer_count}</b></div>
<div class="card">同日重複行<br><b>${analysis.duplicate_same_day_rows}</b></div>
<div class="card">リピーター候補<br><b>${analysis.repeater_count}</b></div>
<div class="card">既存顧客安全候補<br><b>${result.safe_existing_target_count}</b></div>
<div class="card">LINE追加証拠<br><b>${result.review_evidence_count}</b></div>
<div class="card">未解決<br><b>${result.unresolved_count}</b></div>
</div>
<h2>分類</h2>
<ul>${classList}</ul>
<h2>顧客別</h2>
<table>
<thead><tr>
<th>売上管理名</th><th>撮影回数</th><th>撮影日</th><th>年</th><th>リピート</th>
<th>分類</th><th>既存Customer ID候補</th><th>処理</th><th>LINE証拠数</th><th>根拠</th>
</tr></thead>
<tbody>${body}</tbody>
</table>
</body>
</html>`;
fs.writeFileSync(path.join(outDir,'review.html'),html);

console.log('RESULT=SALES_CRM_READONLY_RECONCILIATION_COMPLETE');
console.log('SALES_ROWS='+analysis.source_row_count);
console.log('UNIQUE_SALES_CUSTOMERS='+analysis.customer_count);
console.log('DUPLICATE_SAME_DAY_ROWS='+analysis.duplicate_same_day_rows);
console.log('REPEATER_COUNT='+analysis.repeater_count);
console.log('CROSS_YEAR_REPEATER_COUNT='+analysis.cross_year_repeater_count);
console.log('CUSTOMER_MASTER_ROWS='+result.customer_master_rows);
console.log('PRODUCTION_CUSTOMER_ROWS='+result.production_customer_rows);
console.log('LINE_NAME_EVIDENCE_ROWS='+result.line_name_evidence_rows);
console.log('LINE_REVIEW_EVIDENCE='+result.review_evidence_count);
console.log('SAFE_EXISTING_TARGETS='+result.safe_existing_target_count);
console.log('UNRESOLVED='+result.unresolved_count);
for(const [key,value] of Object.entries(result.classification_counts).sort()){
  console.log('CLASS_'+key+'='+value);
}
console.log('PRODUCTION_DUPLICATE_NAME_GROUPS='+result.production_duplicate_name_groups);
console.log('PRODUCTION_DUPLICATE_LINE_USER_ID_GROUPS='+result.production_duplicate_line_user_id_groups);
console.log('AUTOMATIC_CUSTOMER_CREATION=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_MERGE=0');
console.log('FUZZY_AUTO_LINK=0');
console.log('LINE_BODY_AUTO_LINK=0');
console.log('NAME_ONLY_UNMATCHED_AUTO_CREATE=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('PRIVATE_VALUES_PRINTED=0');
