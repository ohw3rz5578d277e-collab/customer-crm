import fs from 'node:fs';
import assert from 'node:assert/strict';
import {
  analyzeSalesHistory,
  reconcileSalesHistory,
  salesReconciliationHealth
} from '../src/crm-sales-history-reconciliation.mjs';

let passed=0;
async function test(name,fn){
  await fn();
  passed++;
  console.log('PASS '+passed+': '+name);
}

await test('same normalized name + same shoot date dedupes to one shoot',()=>{
  const a=analyzeSalesHistory([
    {year:2025,source_row:15,name:'山田 花子',shoot_date:'2025/01/05'},
    {year:2025,source_row:16,name:'山田　花子',shoot_date:'2025-01-05'}
  ]);
  assert.equal(a.customer_count,1);
  assert.equal(a.duplicate_same_day_rows,1);
  assert.equal(a.customers[0].shoot_count,1);
  assert.equal(a.customers[0].is_repeater,false);
});

await test('same normalized name on different dates is repeater',()=>{
  const a=analyzeSalesHistory([
    {year:2024,source_row:15,name:'佐藤 未来',shoot_date:'2024/03/01'},
    {year:2025,source_row:20,name:'佐藤未来',shoot_date:'2025/03/02'}
  ]);
  assert.equal(a.customer_count,1);
  assert.equal(a.repeater_count,1);
  assert.equal(a.cross_year_repeater_count,1);
  assert.deepEqual(a.customers[0].shoot_dates,['2024-03-01','2025-03-02']);
});

await test('unique Production exact name is safe existing target',()=>{
  const sales=analyzeSalesHistory([
    {year:2025,source_row:15,name:'鈴木 花子',shoot_date:'2025/04/01'}
  ]);
  const r=reconcileSalesHistory({
    salesAnalysis:sales,
    customerMaster:[],
    productionCustomers:[
      {customer_id:'26990001',name:'鈴木花子',line_user_id:'U11111111111111111111'}
    ]
  });
  assert.equal(r.rows[0].classification,'PRODUCTION_EXACT_UNIQUE');
  assert.equal(r.rows[0].target_customer_id,'26990001');
  assert.equal(r.rows[0].safe_existing_target,true);
});

await test('duplicate Production exact names are ambiguous and never auto-linked',()=>{
  const sales=analyzeSalesHistory([
    {year:2025,source_row:15,name:'鈴木花子',shoot_date:'2025/04/01'}
  ]);
  const r=reconcileSalesHistory({
    salesAnalysis:sales,
    customerMaster:[],
    productionCustomers:[
      {customer_id:'26990001',name:'鈴木花子',line_user_id:'U11111111111111111111'},
      {customer_id:'26990002',name:'鈴木 花子',line_user_id:'U22222222222222222222'}
    ]
  });
  assert.equal(r.rows[0].classification,'PRODUCTION_EXACT_AMBIGUOUS');
  assert.equal(r.rows[0].target_customer_id,'');
  assert.equal(r.rows[0].safe_existing_target,false);
});

await test('Customer Master exact name can resolve through exact LINE UserID',()=>{
  const sales=analyzeSalesHistory([
    {year:2025,source_row:15,name:'高橋未来',shoot_date:'2025/05/01'}
  ]);
  const line='Uabcdefabcdefabcdefabcdefabcdefab';
  const r=reconcileSalesHistory({
    salesAnalysis:sales,
    customerMaster:[
      {customer_id:'C-old-1',name:'高橋未来',line_user_id:line}
    ],
    productionCustomers:[
      {customer_id:'26990003',name:'LINE表示名',line_user_id:line}
    ]
  });
  assert.equal(r.rows[0].classification,'MASTER_TO_PRODUCTION_UNIQUE');
  assert.equal(r.rows[0].target_customer_id,'26990003');
  assert.equal(r.rows[0].safe_existing_target,true);
});

await test('legacy Customer Master ID alone is never adopted as current target',()=>{
  const sales=analyzeSalesHistory([
    {year:2025,source_row:15,name:'高橋未来',shoot_date:'2025/05/01'}
  ]);
  const r=reconcileSalesHistory({
    salesAnalysis:sales,
    customerMaster:[
      {customer_id:'C-old-1',name:'高橋未来',line_user_id:''}
    ],
    productionCustomers:[]
  });
  assert.equal(r.rows[0].classification,'CUSTOMER_MASTER_EXACT_ONLY');
  assert.equal(r.rows[0].target_customer_id,'');
  assert.equal(r.rows[0].safe_existing_target,false);
});

await test('unmatched sales customer remains unmatched and is never auto-created',()=>{
  const sales=analyzeSalesHistory([
    {year:2026,source_row:15,name:'未登録顧客',shoot_date:'2026/02/01'}
  ]);
  const r=reconcileSalesHistory({
    salesAnalysis:sales,
    customerMaster:[],
    productionCustomers:[
      {customer_id:'26990004',name:'別の顧客',line_user_id:'U33333333333333333333'}
    ]
  });
  assert.equal(r.rows[0].classification,'UNMATCHED');
  assert.equal(r.rows[0].safe_existing_target,false);
  assert.equal(r.automatic_customer_creation,false);
  assert.equal(r.customer_id_generation,false);
  assert.equal(r.production_write,false);
});

await test('conflicting sales customer IDs are surfaced but do not choose identity',()=>{
  const a=analyzeSalesHistory([
    {year:2025,source_row:15,name:'伊藤花子',shoot_date:'2025/01/01',sales_customer_id:'G123456'},
    {year:2025,source_row:16,name:'伊藤花子',shoot_date:'2025/02/01',sales_customer_id:'G654321'}
  ]);
  assert.equal(a.customers[0].sales_customer_id_conflict,true);
  assert.equal(a.customers[0].sales_customer_ids.length,2);
});

await test('health locks reconciliation to read-only exact-match behavior',()=>{
  const h=salesReconciliationHealth();
  assert.equal(h.sales_reconciliation_read_only,true);
  assert.equal(h.sales_reconciliation_same_name_same_date_dedupe,true);
  assert.equal(h.sales_reconciliation_fuzzy_auto_link,false);
  assert.equal(h.sales_reconciliation_unmatched_auto_create,false);
  assert.equal(h.sales_reconciliation_customer_merge,false);
  assert.equal(h.sales_reconciliation_customer_id_generation,false);
  assert.equal(h.sales_reconciliation_production_write,false);
});

await test('local reconciliation scripts contain no write path',()=>{
  const core=fs.readFileSync('src/crm-sales-history-reconciliation.mjs','utf8');
  const cli=fs.readFileSync('scripts/reconcile-photo-sales-readonly.mjs','utf8');
  const extractor=fs.readFileSync('scripts/extract-photo-sales-xlsx.py','utf8');
  const runner=fs.readFileSync('scripts/run-sales-crm-readonly-reconciliation.sh','utf8');

  assert.doesNotMatch(core,/\b(?:INSERT|UPDATE|DELETE|REPLACE|UPSERT)\s+/i);
  assert.doesNotMatch(cli,/\b(?:INSERT|UPDATE|DELETE|REPLACE|UPSERT)\s+/i);
  assert.doesNotMatch(runner,/\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE|UPSERT)\s+/i);
  assert.doesNotMatch(cli,/LINE_SERVICE|pushMessage|replyMessage|api\.line\.me/i);
  assert.doesNotMatch(runner,/LINE_SERVICE|pushMessage|replyMessage|api\.line\.me/i);
  assert.match(runner,/SELECT customer_id,name,line_user_id FROM customers/);
  assert.match(runner,/PRODUCTION_QUERY_STATIC_AUDIT=PASS/);
  assert.match(runner,/PRODUCTION_D1_WRITE=0/);
  assert.match(runner,/CUSTOMER_CREATION=0/);
  assert.match(runner,/CUSTOMER_ID_GENERATION=0/);
  assert.match(runner,/CUSTOMER_MERGE=0/);
  assert.match(cli,/PRODUCTION_D1_WRITE=0/);
  assert.match(cli,/AUTOMATIC_CUSTOMER_CREATION=0/);
  assert.match(extractor,/PRODUCTION_D1_WRITE=0/);
});

console.log('SALES_CRM_READONLY_RECONCILIATION='+passed+'/'+passed+' PASS');
