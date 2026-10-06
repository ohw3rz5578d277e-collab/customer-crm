import assert from 'node:assert/strict';
import { analyzeSalesHistory, reconcileSalesHistory, salesReconciliationHealth } from '../src/crm-sales-history-reconciliation.mjs';

let passed=0;
function test(name,fn){fn();passed++;console.log(`PASS ${passed}: ${name}`)}
const p=(customer_id,name='顧客A',line_user_id='')=>({customer_id,name,line_user_id});

function twoRows(a,b){
  return analyzeSalesHistory([
    {year:2026,source_row:15,name:'同名顧客',shoot_date:'2026/01/01',...a},
    {year:2026,source_row:16,name:'同名顧客',shoot_date:'2026/02/01',...b}
  ]);
}

test('partial Customer ID coverage across same-name rows is review-only',()=>{
  const analysis=twoRows({sales_customer_id:'26990001'},{});
  const group=analysis.customers[0];
  assert.equal(group.source_row_count,2);
  assert.equal(group.sales_customer_id_present_rows,1);
  assert.equal(group.sales_customer_id_partial,true);
  const r=reconcileSalesHistory({salesAnalysis:analysis,productionCustomers:[p('26990001','同名顧客')]});
  assert.equal(r.rows[0].classification,'SALES_SOURCE_IDENTITY_INCOMPLETE_OR_CONFLICT_REVIEW');
  assert.equal(r.rows[0].target_customer_id,'');
  assert.equal(r.rows[0].safe_existing_target,false);
});

test('partial LINE UserID coverage across same-name rows is review-only',()=>{
  const line='U11111111111111111111';
  const analysis=twoRows({line_user_id:line},{});
  assert.equal(analysis.customers[0].line_user_id_partial,true);
  const r=reconcileSalesHistory({salesAnalysis:analysis,productionCustomers:[p('26990001','同名顧客',line)]});
  assert.equal(r.rows[0].classification,'SALES_SOURCE_IDENTITY_INCOMPLETE_OR_CONFLICT_REVIEW');
  assert.equal(r.rows[0].safe_existing_target,false);
});

test('canonical plus legacy Customer IDs fail closed before filtering',()=>{
  const analysis=twoRows({sales_customer_id:'26990001'},{sales_customer_id:'legacy-7'});
  const group=analysis.customers[0];
  assert.equal(group.sales_customer_id_conflict,true);
  assert.deepEqual(new Set(group.sales_customer_ids),new Set(['26990001','legacy-7']));
  const r=reconcileSalesHistory({salesAnalysis:analysis,productionCustomers:[p('26990001','同名顧客')]});
  assert.equal(r.rows[0].classification,'SALES_SOURCE_IDENTITY_INCOMPLETE_OR_CONFLICT_REVIEW');
  assert.equal(r.rows[0].safe_existing_target,false);
});

test('valid plus malformed LINE UserIDs fail closed before filtering',()=>{
  const valid='U22222222222222222222';
  const analysis=twoRows({line_user_id:valid},{line_user_id:'bad-line-id'});
  assert.equal(analysis.customers[0].line_user_id_conflict,true);
  const r=reconcileSalesHistory({salesAnalysis:analysis,productionCustomers:[p('26990002','同名顧客',valid)]});
  assert.equal(r.rows[0].classification,'SALES_SOURCE_IDENTITY_INCOMPLETE_OR_CONFLICT_REVIEW');
  assert.equal(r.rows[0].safe_existing_target,false);
});

test('complete identical Customer ID on every row may target uniquely',()=>{
  const analysis=twoRows({sales_customer_id:'26990003'},{sales_customer_id:'26990003'});
  const r=reconcileSalesHistory({salesAnalysis:analysis,productionCustomers:[p('26990003','別表記')]});
  assert.equal(r.rows[0].classification,'SALES_CUSTOMER_ID_TO_PRODUCTION_UNIQUE');
  assert.equal(r.rows[0].target_customer_id,'26990003');
  assert.equal(r.rows[0].safe_existing_target,true);
});

test('complete identical LINE UserID on every row may target canonical customer',()=>{
  const line='U33333333333333333333';
  const analysis=twoRows({line_user_id:line},{line_user_id:line});
  const r=reconcileSalesHistory({salesAnalysis:analysis,productionCustomers:[p('26990004','別表記',line)]});
  assert.equal(r.rows[0].classification,'SALES_LINE_USER_ID_TO_PRODUCTION_UNIQUE');
  assert.equal(r.rows[0].target_customer_id,'26990004');
  assert.equal(r.rows[0].safe_existing_target,true);
});

test('complete Customer ID and LINE evidence must resolve to same snapshot customer',()=>{
  const line='U44444444444444444444';
  const analysis=twoRows(
    {sales_customer_id:'26990005',line_user_id:line},
    {sales_customer_id:'26990005',line_user_id:line}
  );
  const r=reconcileSalesHistory({salesAnalysis:analysis,productionCustomers:[
    p('26990005','顧客A','U55555555555555555555'),
    p('26990006','顧客B',line)
  ]});
  assert.equal(r.rows[0].classification,'SALES_SOURCE_IDENTITY_CROSSCHECK_FAILED_REVIEW');
  assert.equal(r.rows[0].target_customer_id,'');
  assert.equal(r.rows[0].safe_existing_target,false);
});

test('health contract locks row-level coverage and conflict rules',()=>{
  const h=salesReconciliationHealth();
  assert.equal(h.sales_reconciliation_safe_target_requires_identity_on_every_grouped_source_row,true);
  assert.equal(h.sales_reconciliation_source_identity_conflicts_fail_closed,true);
  assert.equal(h.sales_reconciliation_cross_identifier_consistency_required,true);
});

console.log(`SALES_CRM_SOURCE_IDENTITY_COVERAGE=${passed}/${passed} PASS`);
