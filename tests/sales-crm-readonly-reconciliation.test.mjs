import fs from 'node:fs';
import assert from 'node:assert/strict';
import {
  analyzeSalesHistory,
  reconcileSalesHistory,
  salesReconciliationHealth
} from '../src/crm-sales-history-reconciliation.mjs';

let passed=0;
async function test(name,fn){await fn();passed++;console.log('PASS '+passed+': '+name)}

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

await test('unique Production snapshot exact name is review-only',()=>{
  const sales=analyzeSalesHistory([{year:2025,source_row:15,name:'鈴木 花子',shoot_date:'2025/04/01'}]);
  const r=reconcileSalesHistory({salesAnalysis:sales,productionCustomers:[{customer_id:'26990001',name:'鈴木花子',line_user_id:'U11111111111111111111'}]});
  assert.equal(r.rows[0].classification,'PRODUCTION_EXACT_NAME_REVIEW');assert.equal(r.rows[0].target_customer_id,'26990001');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('duplicate Production snapshot exact names are ambiguous',()=>{
  const sales=analyzeSalesHistory([{year:2025,source_row:15,name:'鈴木花子',shoot_date:'2025/04/01'}]);
  const r=reconcileSalesHistory({salesAnalysis:sales,productionCustomers:[
    {customer_id:'26990001',name:'鈴木花子',line_user_id:'U11111111111111111111'},
    {customer_id:'26990002',name:'鈴木 花子',line_user_id:'U22222222222222222222'}
  ]});
  assert.equal(r.rows[0].classification,'PRODUCTION_EXACT_NAME_AMBIGUOUS');assert.equal(r.rows[0].target_customer_id,'');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('Customer Master exact name plus LINE target remains review-only',()=>{
  const line='Uabcdefabcdefabcdefabcdefabcdefab';
  const sales=analyzeSalesHistory([{year:2025,source_row:15,name:'高橋未来',shoot_date:'2025/05/01'}]);
  const r=reconcileSalesHistory({salesAnalysis:sales,customerMaster:[{customer_id:'C-old-1',name:'高橋未来',line_user_id:line}],productionCustomers:[{customer_id:'26990003',name:'LINE表示名',line_user_id:line}]});
  assert.equal(r.rows[0].classification,'MASTER_EXACT_NAME_TO_PRODUCTION_REVIEW');assert.equal(r.rows[0].target_customer_id,'26990003');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('legacy Customer Master ID alone is never adopted',()=>{
  const sales=analyzeSalesHistory([{year:2025,source_row:15,name:'高橋未来',shoot_date:'2025/05/01'}]);
  const r=reconcileSalesHistory({salesAnalysis:sales,customerMaster:[{customer_id:'C-old-1',name:'高橋未来',line_user_id:''}]});
  assert.equal(r.rows[0].classification,'CUSTOMER_MASTER_EXACT_NAME_REVIEW');assert.equal(r.rows[0].target_customer_id,'');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('sales-source canonical Customer ID exact match may be safe target',()=>{
  const sales=analyzeSalesHistory([{year:2026,source_row:15,name:'顧客A',shoot_date:'2026/02/01',sales_customer_id:'26990006'}]);
  const r=reconcileSalesHistory({salesAnalysis:sales,productionCustomers:[{customer_id:'26990006',name:'別表記',line_user_id:'U44444444444444444444'}]});
  assert.equal(r.rows[0].classification,'SALES_CUSTOMER_ID_TO_PRODUCTION_UNIQUE');assert.equal(r.rows[0].target_customer_id,'26990006');assert.equal(r.rows[0].safe_existing_target,true);
});

await test('sales-source LINE UserID exact match may be safe target',()=>{
  const line='U55555555555555555555';
  const sales=analyzeSalesHistory([{year:2026,source_row:15,name:'顧客B',shoot_date:'2026/02/02',line_user_id:line}]);
  const r=reconcileSalesHistory({salesAnalysis:sales,productionCustomers:[{customer_id:'26990007',name:'LINE別名',line_user_id:line}]});
  assert.equal(r.rows[0].classification,'SALES_LINE_USER_ID_TO_PRODUCTION_UNIQUE');assert.equal(r.rows[0].safe_existing_target,true);
});

await test('missing sales-source Customer ID blocks weaker name fallback',()=>{
  const sales=analyzeSalesHistory([{year:2026,source_row:15,name:'同名顧客',shoot_date:'2026/02/03',sales_customer_id:'26999999'}]);
  const r=reconcileSalesHistory({salesAnalysis:sales,productionCustomers:[{customer_id:'26990008',name:'同名顧客',line_user_id:'U66666666666666666666'}]});
  assert.equal(r.rows[0].classification,'SALES_CUSTOMER_ID_NOT_FOUND_REVIEW');assert.equal(r.rows[0].target_customer_id,'');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('missing sales-source LINE UserID blocks weaker name fallback',()=>{
  const sales=analyzeSalesHistory([{year:2026,source_row:15,name:'同名顧客',shoot_date:'2026/02/04',line_user_id:'U77777777777777777777'}]);
  const r=reconcileSalesHistory({salesAnalysis:sales,productionCustomers:[{customer_id:'26990009',name:'同名顧客',line_user_id:'U88888888888888888888'}]});
  assert.equal(r.rows[0].classification,'SALES_LINE_USER_ID_NOT_FOUND_REVIEW');assert.equal(r.rows[0].safe_existing_target,false);
});

await test('LINE body evidence remains review-only',()=>{
  const line='Uabcdefabcdefabcdefabcdefabcdefab';
  const sales=analyzeSalesHistory([{year:2026,source_row:15,name:'中村未来',shoot_date:'2026/03/01'}]);
  const r=reconcileSalesHistory({salesAnalysis:sales,productionCustomers:[{customer_id:'26990005',name:'LINEニックネーム',line_user_id:line}],lineNameEvidence:[{name:'中村未来',line_user_id:line}]});
  assert.equal(r.rows[0].classification,'LINE_BODY_EXACT_NAME_TO_PRODUCTION_REVIEW');assert.equal(r.rows[0].safe_existing_target,false);assert.equal(r.line_body_auto_link,false);
});

await test('unmatched customer remains unmatched and no mutation is enabled',()=>{
  const sales=analyzeSalesHistory([{year:2026,source_row:15,name:'未登録顧客',shoot_date:'2026/02/01'}]);
  const r=reconcileSalesHistory({salesAnalysis:sales,productionCustomers:[{customer_id:'26990004',name:'別の顧客',line_user_id:'U33333333333333333333'}]});
  assert.equal(r.rows[0].classification,'UNMATCHED');assert.equal(r.automatic_customer_creation,false);assert.equal(r.customer_id_generation,false);assert.equal(r.production_read,false);assert.equal(r.production_write,false);
});

await test('health locks local-only exact-match behavior',()=>{
  const h=salesReconciliationHealth();
  assert.equal(h.sales_reconciliation_local_only,true);assert.equal(h.sales_reconciliation_read_only,true);
  assert.equal(h.sales_reconciliation_sales_customer_id_exact_auto_target_only,true);assert.equal(h.sales_reconciliation_sales_line_user_id_exact_auto_target_only,true);
  assert.equal(h.sales_reconciliation_production_name_exact_review_only,true);assert.equal(h.sales_reconciliation_customer_master_exact_review_only,true);
  assert.equal(h.sales_reconciliation_line_body_exact_name_review_only,true);assert.equal(h.sales_reconciliation_fuzzy_auto_link,false);
  assert.equal(h.sales_reconciliation_customer_id_generation,false);assert.equal(h.sales_reconciliation_production_network_access,false);
  assert.equal(h.sales_reconciliation_production_read,false);assert.equal(h.sales_reconciliation_production_write,false);
});

await test('runner has no Production or network execution path',()=>{
  const files=[
    'src/crm-sales-history-reconciliation.mjs',
    'scripts/reconcile-photo-sales-readonly.mjs',
    'scripts/extract-photo-sales-xlsx.py',
    'scripts/extract-line-name-evidence-xlsx.py',
    'scripts/run-sales-crm-readonly-reconciliation.sh'
  ];
  const all=files.map(file=>fs.readFileSync(file,'utf8')).join('\n');
  const runner=fs.readFileSync('scripts/run-sales-crm-readonly-reconciliation.sh','utf8');
  assert.doesNotMatch(all,/\b(?:INSERT|UPDATE|DELETE|REPLACE|UPSERT)\s+/i);
  assert.doesNotMatch(runner,/\b(?:wrangler|curl|wget|npx|ssh|scp|rsync)\b/i);
  assert.doesNotMatch(runner,/git\s+ls-remote/i);
  assert.doesNotMatch(runner,/CLOUDFLARE_(?:API_TOKEN|API_KEY|EMAIL|ACCOUNT_ID)/i);
  assert.doesNotMatch(runner,/--remote\b/i);
  assert.doesNotMatch(runner,/api\.cloudflare\.com|api\.line\.me/i);
  assert.match(runner,/<production-customers-snapshot\.json>/);
  assert.match(runner,/Production snapshot must be obtained separately under an authorized read-only flow/);
  assert.match(runner,/PRODUCTION_NETWORK_ACCESS=0/);
  assert.match(runner,/PRODUCTION_D1_READ=0/);
  assert.match(runner,/PRODUCTION_D1_WRITE=0/);
  assert.match(runner,/CUSTOMER_ID_GENERATION=0/);
  assert.match(runner,/CUSTOMER_MERGE=0/);
});

console.log('SALES_CRM_LOCAL_RECONCILIATION='+passed+'/'+passed+' PASS');
