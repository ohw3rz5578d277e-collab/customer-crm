import fs from 'node:fs';
import assert from 'node:assert/strict';
import {
  analyzeSalesHistory,
  reconcileSalesHistory,
  salesReconciliationHealth,
  normalizeReconciliationName,
  normalizeReconciliationDate
} from '../src/crm-sales-history-reconciliation.mjs';
import { parseProductionSnapshotText } from '../src/crm-sales-snapshot.mjs';
import { safeCsvCell, escapeHtml } from '../src/crm-sales-output-safety.mjs';
import { normalizeImportName, normalizeImportDate } from '../src/crm-customer-csv-import.mjs';

let passed=0;
async function test(name,fn){await fn();passed++;console.log('PASS '+passed+': '+name)}
function sales(row={}){return analyzeSalesHistory([{year:2026,source_row:15,name:'顧客A',shoot_date:'2026/02/01',...row}])}

await test('same normalized name and same date dedupes',()=>{
  const a=analyzeSalesHistory([
    {year:2025,source_row:15,name:'山田 花子',shoot_date:'2025/01/05'},
    {year:2025,source_row:16,name:'山田　花子',shoot_date:'2025-01-05'}
  ]);
  assert.equal(a.customer_count,1);assert.equal(a.duplicate_same_day_rows,1);assert.equal(a.customers[0].shoot_count,1);assert.equal(a.customers[0].is_repeater,false);
});

await test('same normalized name on distinct dates is repeater',()=>{
  const a=analyzeSalesHistory([
    {year:2024,source_row:15,name:'佐藤 未来',shoot_date:'2024/03/01'},
    {year:2025,source_row:20,name:'佐藤未来',shoot_date:'2025/03/02'}
  ]);
  assert.equal(a.repeater_count,1);assert.equal(a.cross_year_repeater_count,1);assert.deepEqual(a.customers[0].shoot_dates,['2024-03-01','2025-03-02']);
});

await test('populated nameless sales row fails closed',()=>{
  const a=analyzeSalesHistory([{year:2026,source_row:15,name:'',shoot_date:'2026/02/01',genre:'七五三'}]);
  assert.equal(a.valid_row_count,0);assert.equal(a.errors.length,1);assert.equal(a.errors[0].error,'sales_customer_name_required');
});

await test('local pure normalizers stay parity-locked to canonical import normalizers',()=>{
  for(const value of ['山田 花子','山田　花子','ＡＢＣ・Ｄ',' Test（A） ','佐藤[未来]']){
    assert.equal(normalizeReconciliationName(value),normalizeImportName(value));
  }
  for(const value of ['2026/02/01','2026-2-1','2026年2月1日','46000','invalid']){
    assert.equal(normalizeReconciliationDate(value),normalizeImportDate(value));
  }
});

await test('strict Production snapshot accepts complete JSON envelope',()=>{
  const rows=parseProductionSnapshotText(JSON.stringify({result:[{results:[
    {customer_id:'26990001',name:'A',line_user_id:'U11111111111111111111'},
    {customer_id:'26990002',name:'B',line_user_id:'U22222222222222222222'}
  ]}]}));
  assert.equal(rows.length,2);assert.equal(rows[0].customer_id,'26990001');
});

await test('strict Production snapshot rejects truncated JSON instead of recovering fragments',()=>{
  const raw='[{"customer_id":"26990001","name":"A","line_user_id":"U11111111111111111111"},{"customer_id":"26990002"';
  assert.throws(()=>parseProductionSnapshotText(raw),/production_snapshot_invalid_json/);
});

await test('strict Production snapshot rejects duplicate customer IDs',()=>{
  const raw=JSON.stringify([
    {customer_id:'26990001',name:'A'},
    {customer_id:'26990001',name:'B'}
  ]);
  assert.throws(()=>parseProductionSnapshotText(raw),/production_snapshot_duplicate_customer_id/);
});

await test('strict Production snapshot rejects missing customer ID',()=>{
  const raw=JSON.stringify([{customer_id:'',name:'A'}]);
  assert.throws(()=>parseProductionSnapshotText(raw),/production_snapshot_customer_id_required/);
});

await test('CSV output neutralizes spreadsheet formulas including leading whitespace',()=>{
  for(const value of ['=1+1','+SUM(A1:A2)','-1+2','@cmd','   =HYPERLINK("https://example.invalid")','\t+1']){
    assert.match(safeCsvCell(value),/^"'/);
  }
  assert.equal(safeCsvCell('普通の名前'),'"普通の名前"');
  assert.equal(escapeHtml('<script>'), '&lt;script&gt;');
});

await test('unique Production snapshot exact name is review-only',()=>{
  const r=reconcileSalesHistory({salesAnalysis:sales({name:'鈴木 花子'}),productionCustomers:[{customer_id:'26990001',name:'鈴木花子',line_user_id:'U11111111111111111111'}]});
  assert.equal(r.rows[0].classification,'PRODUCTION_EXACT_NAME_REVIEW');assert.equal(r.rows[0].target_customer_id,'26990001');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('noncanonical Production exact-name target is never exposed',()=>{
  const r=reconcileSalesHistory({salesAnalysis:sales({name:'鈴木 花子'}),productionCustomers:[{customer_id:'legacy-1',name:'鈴木花子',line_user_id:'U11111111111111111111'}]});
  assert.equal(r.rows[0].classification,'PRODUCTION_EXACT_NAME_REVIEW');assert.equal(r.rows[0].target_customer_id,'');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('duplicate Production exact names are ambiguous',()=>{
  const r=reconcileSalesHistory({salesAnalysis:sales({name:'鈴木花子'}),productionCustomers:[
    {customer_id:'26990001',name:'鈴木花子',line_user_id:'U11111111111111111111'},
    {customer_id:'26990002',name:'鈴木 花子',line_user_id:'U22222222222222222222'}
  ]});
  assert.equal(r.rows[0].classification,'PRODUCTION_EXACT_NAME_AMBIGUOUS');assert.equal(r.rows[0].target_customer_id,'');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('Customer Master exact name plus LINE target remains review-only',()=>{
  const line='Uabcdefabcdefabcdefabcdefabcdefab';
  const r=reconcileSalesHistory({salesAnalysis:sales({name:'高橋未来'}),customerMaster:[{customer_id:'C-old-1',name:'高橋未来',line_user_id:line}],productionCustomers:[{customer_id:'26990003',name:'LINE表示名',line_user_id:line}]});
  assert.equal(r.rows[0].classification,'MASTER_EXACT_NAME_TO_PRODUCTION_REVIEW');assert.equal(r.rows[0].target_customer_id,'26990003');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('legacy Customer Master ID alone is never adopted',()=>{
  const r=reconcileSalesHistory({salesAnalysis:sales({name:'高橋未来'}),customerMaster:[{customer_id:'C-old-1',name:'高橋未来',line_user_id:''}]});
  assert.equal(r.rows[0].classification,'CUSTOMER_MASTER_EXACT_NAME_REVIEW');assert.equal(r.rows[0].target_customer_id,'');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('sales-source canonical Customer ID exact match may be safe target',()=>{
  const r=reconcileSalesHistory({salesAnalysis:sales({sales_customer_id:'26990006'}),productionCustomers:[{customer_id:'26990006',name:'別表記',line_user_id:'U44444444444444444444'}]});
  assert.equal(r.rows[0].classification,'SALES_CUSTOMER_ID_TO_PRODUCTION_UNIQUE');assert.equal(r.rows[0].target_customer_id,'26990006');assert.equal(r.rows[0].safe_existing_target,true);
});

await test('sales-source LINE UserID exact match targets only canonical Customer ID',()=>{
  const line='U55555555555555555555';
  const r=reconcileSalesHistory({salesAnalysis:sales({line_user_id:line}),productionCustomers:[{customer_id:'26990007',name:'LINE別名',line_user_id:line}]});
  assert.equal(r.rows[0].classification,'SALES_LINE_USER_ID_TO_PRODUCTION_UNIQUE');assert.equal(r.rows[0].target_customer_id,'26990007');assert.equal(r.rows[0].safe_existing_target,true);
});

await test('sales-source LINE UserID cannot auto-target noncanonical Customer ID',()=>{
  const line='U55555555555555555555';
  const r=reconcileSalesHistory({salesAnalysis:sales({line_user_id:line}),productionCustomers:[{customer_id:'legacy-7',name:'LINE別名',line_user_id:line}]});
  assert.equal(r.rows[0].classification,'SALES_LINE_USER_ID_TARGET_NOT_CANONICAL_REVIEW');assert.equal(r.rows[0].target_customer_id,'');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('sales-source LINE UserID is ambiguous across canonical and legacy targets',()=>{
  const line='U55555555555555555555';
  const r=reconcileSalesHistory({salesAnalysis:sales({line_user_id:line}),productionCustomers:[
    {customer_id:'26990007',name:'LINE別名',line_user_id:line},
    {customer_id:'legacy-7',name:'旧顧客',line_user_id:line}
  ]});
  assert.equal(r.rows[0].classification,'SALES_LINE_USER_ID_AMBIGUOUS');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('missing exact sales Customer ID blocks weaker name fallback',()=>{
  const r=reconcileSalesHistory({salesAnalysis:sales({name:'同名顧客',sales_customer_id:'26999999'}),productionCustomers:[{customer_id:'26990008',name:'同名顧客',line_user_id:'U66666666666666666666'}]});
  assert.equal(r.rows[0].classification,'SALES_CUSTOMER_ID_NOT_FOUND_REVIEW');assert.equal(r.rows[0].target_customer_id,'');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('missing exact sales LINE UserID blocks weaker name fallback',()=>{
  const r=reconcileSalesHistory({salesAnalysis:sales({name:'同名顧客',line_user_id:'U77777777777777777777'}),productionCustomers:[{customer_id:'26990009',name:'同名顧客',line_user_id:'U88888888888888888888'}]});
  assert.equal(r.rows[0].classification,'SALES_LINE_USER_ID_NOT_FOUND_REVIEW');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('LINE body evidence remains review-only',()=>{
  const line='Uabcdefabcdefabcdefabcdefabcdefab';
  const r=reconcileSalesHistory({salesAnalysis:sales({name:'中村未来'}),productionCustomers:[{customer_id:'26990005',name:'LINEニックネーム',line_user_id:line}],lineNameEvidence:[{name:'中村未来',line_user_id:line}]});
  assert.equal(r.rows[0].classification,'LINE_BODY_EXACT_NAME_TO_PRODUCTION_REVIEW');assert.equal(r.rows[0].safe_existing_target,false);assert.equal(r.line_body_auto_link,false);
});

await test('LINE body evidence never exposes noncanonical Production ID',()=>{
  const line='Uabcdefabcdefabcdefabcdefabcdefab';
  const r=reconcileSalesHistory({salesAnalysis:sales({name:'中村未来'}),productionCustomers:[{customer_id:'legacy-5',name:'LINEニックネーム',line_user_id:line}],lineNameEvidence:[{name:'中村未来',line_user_id:line}]});
  assert.equal(r.rows[0].classification,'LINE_BODY_EXACT_NAME_TO_NONCANONICAL_PRODUCTION_REVIEW');assert.equal(r.rows[0].target_customer_id,'');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('unmatched customer remains unmatched and no mutation is enabled',()=>{
  const r=reconcileSalesHistory({salesAnalysis:sales({name:'未登録顧客'}),productionCustomers:[{customer_id:'26990004',name:'別の顧客',line_user_id:'U33333333333333333333'}]});
  assert.equal(r.rows[0].classification,'UNMATCHED');assert.equal(r.automatic_customer_creation,false);assert.equal(r.customer_id_generation,false);assert.equal(r.production_read,false);assert.equal(r.production_write,false);
});

await test('health locks local-only exact-match behavior',()=>{
  const h=salesReconciliationHealth();
  assert.equal(h.sales_reconciliation_local_only,true);assert.equal(h.sales_reconciliation_read_only,true);
  assert.equal(h.sales_reconciliation_sales_customer_id_exact_auto_target_only,true);assert.equal(h.sales_reconciliation_sales_line_user_id_exact_auto_target_only,true);
  assert.equal(h.sales_reconciliation_auto_target_requires_canonical_customer_id,true);
  assert.equal(h.sales_reconciliation_production_name_exact_review_only,true);assert.equal(h.sales_reconciliation_customer_master_exact_review_only,true);
  assert.equal(h.sales_reconciliation_line_body_exact_name_review_only,true);assert.equal(h.sales_reconciliation_fuzzy_auto_link,false);
  assert.equal(h.sales_reconciliation_customer_id_generation,false);assert.equal(h.sales_reconciliation_production_network_access,false);
  assert.equal(h.sales_reconciliation_production_read,false);assert.equal(h.sales_reconciliation_production_write,false);
});

await test('all invoked local reconciliation files contain no network or mutation execution path',()=>{
  const files=[
    'src/crm-sales-history-reconciliation.mjs',
    'src/crm-sales-snapshot.mjs',
    'src/crm-sales-output-safety.mjs',
    'scripts/reconcile-photo-sales-readonly.mjs',
    'scripts/extract-photo-sales-xlsx.py',
    'scripts/extract-line-name-evidence-xlsx.py',
    'scripts/run-sales-crm-readonly-reconciliation.sh'
  ];
  const all=files.map(file=>fs.readFileSync(file,'utf8')).join('\n');
  const runner=fs.readFileSync('scripts/run-sales-crm-readonly-reconciliation.sh','utf8');
  assert.doesNotMatch(all,/\b(?:INSERT|UPDATE|DELETE|REPLACE|UPSERT)\s+/i);
  assert.doesNotMatch(all,/\b(?:wrangler|curl|wget|npx|ssh|scp|rsync)\b/i);
  assert.doesNotMatch(all,/\bfetch\s*\(|node:(?:http|https)|urllib|requests\.|api\.cloudflare\.com|api\.line\.me/i);
  assert.doesNotMatch(all,/CLOUDFLARE_(?:API_TOKEN|API_KEY|EMAIL|ACCOUNT_ID)/i);
  assert.doesNotMatch(all,/git\s+ls-remote|--remote\b/i);
  assert.match(runner,/<production-customers-snapshot\.json>/);
  assert.match(runner,/supplied as complete JSON/);
  assert.match(runner,/PRODUCTION_NETWORK_ACCESS=0/);
  assert.match(runner,/PRODUCTION_D1_READ=0/);
  assert.match(runner,/PRODUCTION_D1_WRITE=0/);
  assert.match(runner,/CUSTOMER_ID_GENERATION=0/);
  assert.match(runner,/CUSTOMER_MERGE=0/);
});

console.log('SALES_CRM_LOCAL_RECONCILIATION='+passed+'/'+passed+' PASS');
