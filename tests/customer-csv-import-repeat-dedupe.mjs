import fs from 'node:fs';
import assert from 'node:assert/strict';
import {
  parseCsvText,
  analyzeCsvImport,
  normalizeImportName,
  normalizeImportDate,
  findReservationCsvHeaderRow,
  customerCsvImportHealth,
  injectCustomerCsvImport,
  handleCustomerCsvImport
} from '../src/crm-customer-csv-import.mjs';

let passed=0;
async function test(name,fn){await fn();passed++;console.log('PASS '+passed+': '+name)}

await test('normalizes Japanese customer names with NFKC and whitespace removal',()=>{
  assert.equal(normalizeImportName(' 山田　花子 '),'山田花子');
  assert.equal(normalizeImportName('ﾔﾏﾀﾞ ﾊﾅｺ'),'ヤマダハナコ');
});

await test('normalizes supported shoot dates',()=>{
  assert.equal(normalizeImportDate('2026/1/2'),'2026-01-02');
  assert.equal(normalizeImportDate('2026年5月3日'),'2026-05-03');
  assert.equal(normalizeImportDate('2026-02-30'),'');
});

await test('CSV parser keeps commas and escaped quotes inside quoted fields',()=>{
  const rows=parseCsvText('顧客名,備考\n"山田 花子","大阪, 北区 ""テスト"""\n');
  assert.equal(rows.length,2);
  assert.equal(rows[1][0],'山田 花子');
  assert.equal(rows[1][1],'大阪, 北区 "テスト"');
});


await test('accepts the exact Reservation app CSV layout with preamble rows before the real header',()=>{
  const preamble=Array.from({length:13},(_,i)=>'予約管理情報'+(i+1)+',').join('\n');
  const csv=preamble+'\n'+[
    '名前,撮影日,撮影場所,ジャンル,撮影プラン,単価,交通費,オプション1単価,オプション2単価,Movie,その他費用,追加購入,総額,備考',
    '山田 花子,2026/01/10,大阪城,お宮参り,Normal,24800,2000,0,0,0,0,0,26800,初回',
    '山田花子,2026/05/03,万博公園,ファミリーフォト,Special,35000,1500,0,0,0,0,0,36500,2回目',
    '山田花子,2026/05/03,万博公園,ファミリーフォト,Special,35000,1500,0,0,0,0,0,36500,重複行'
  ].join('\n');
  const rows=parseCsvText(csv);
  assert.equal(findReservationCsvHeaderRow(rows),13);
  const a=analyzeCsvImport(csv);
  assert.equal(a.header_row,14);
  assert.equal(a.customer_count,1);
  assert.equal(a.repeater_count,1);
  assert.equal(a.duplicate_same_day_rows,1);
  assert.deepEqual(a.groups[0].shoot_dates,['2026-01-10','2026-05-03']);
  assert.equal(a.groups[0].shoots[0].total_amount,26800);
  assert.equal(a.groups[0].shoots[1].total_amount,36500);
});


await test('Reservation CSV fallback amount and genre normalization match Reservation importer rules',()=>{
  const csv=[
    '名前,撮影日,撮影場所,ジャンル,撮影プラン,単価,交通費,オプション1単価,オプション2単価,Movie,その他費用,追加購入,総額,備考',
    '佐藤 未来,2026/03/01,大阪,ファミリーフォト,(新) Normal,,2000,1000,0,0,500,0,,'
  ].join('\n');
  const a=analyzeCsvImport(csv);
  assert.equal(a.groups[0].shoots[0].genre,'家族');
  assert.equal(a.groups[0].shoots[0].total_amount,28300);
});

await test('same normalized name and same shoot date is one shoot, not a repeat',()=>{
  const csv=[
    '顧客名,撮影日,ジャンル,金額',
    '山田 花子,2026/01/10,お宮参り,25000',
    '山田花子,2026-01-10,お宮参り,25000'
  ].join('\n');
  const a=analyzeCsvImport(csv);
  assert.equal(a.customer_count,1);
  assert.equal(a.duplicate_same_day_rows,1);
  assert.equal(a.repeater_count,0);
  assert.equal(a.groups[0].shoot_count,1);
  assert.equal(a.groups[0].is_repeater,false);
});

await test('same normalized name on different shoot dates becomes repeater',()=>{
  const csv=[
    '顧客名,撮影日,ジャンル',
    '山田 花子,2026/01/10,お宮参り',
    '山田　花子,2026/05/03,ファミリー'
  ].join('\n');
  const a=analyzeCsvImport(csv);
  assert.equal(a.customer_count,1);
  assert.equal(a.duplicate_same_day_rows,0);
  assert.equal(a.repeater_count,1);
  assert.equal(a.groups[0].shoot_count,2);
  assert.deepEqual(a.groups[0].shoot_dates,['2026-01-10','2026-05-03']);
  assert.equal(a.groups[0].is_repeater,true);
});

await test('same-day duplicates collapse while different customers remain separate',()=>{
  const csv=[
    '顧客名,撮影日,料金',
    '山田 花子,2026-01-10,20000',
    '山田花子,2026-01-10,25000',
    '山田花子,2026-05-10,30000',
    '佐藤 未来,2026-02-01,18000'
  ].join('\n');
  const a=analyzeCsvImport(csv);
  assert.equal(a.valid_row_count,4);
  assert.equal(a.customer_count,2);
  assert.equal(a.duplicate_same_day_rows,1);
  assert.equal(a.repeater_count,1);
  const yamada=a.groups.find(x=>x.name_key==='山田花子');
  assert.equal(yamada.shoots[0].total_amount,25000,'same-day duplicate should not sum revenue');
});

await test('conflicting exact Customer IDs for same normalized name is rejected',()=>{
  const csv=[
    '顧客名,撮影日,顧客ID',
    '山田 花子,2026-01-10,26000001',
    '山田花子,2026-05-03,26000002'
  ].join('\n');
  const a=analyzeCsvImport(csv);
  assert.equal(a.errors.some(x=>x.error==='conflicting_customer_ids_for_same_name'),true);
});

await test('invalid Customer ID never silently creates or remaps identity',()=>{
  const csv=['顧客名,撮影日,顧客ID','山田花子,2026-01-10,G123'].join('\n');
  const a=analyzeCsvImport(csv);
  assert.equal(a.errors.some(x=>x.error==='invalid_customer_id'),true);
});

await test('name column is mandatory',()=>{
  assert.throws(()=>analyzeCsvImport('撮影日,ジャンル\n2026-01-01,七五三'),/csv_name_column_required/);
});

await test('Reservation CSV requires the shoot-date column',()=>{
  assert.throws(()=>analyzeCsvImport('顧客名,電話番号\n山田花子,09012345678'),/csv_shoot_date_column_required/);
});

await test('Reservation CSV rejects a named row with no shoot date',()=>{
  const a=analyzeCsvImport('顧客名,撮影日,撮影場所\n山田花子,,大阪');
  assert.equal(a.errors.some(x=>x.error==='shoot_date_required'),true);
});

await test('health declares exact-only import matching and no fuzzy merge',()=>{
  const h=customerCsvImportHealth();
  assert.equal(h.customer_csv_import,true);
  assert.equal(h.customer_csv_import_reservation_csv_compatible,true);
  assert.equal(h.customer_csv_import_reservation_csv_amount_rules,true);
  assert.equal(h.customer_csv_import_reservation_csv_genre_rules,true);
  assert.equal(h.customer_csv_import_same_day_dedupe,true);
  assert.equal(h.customer_csv_import_repeat_rule,'same_name_distinct_shoot_dates>=2');
  assert.equal(h.customer_csv_import_existing_customer_auto_name_merge,false);
  assert.equal(h.customer_csv_import_customer_id_owner,'customer-crm');
  assert.equal(h.customer_csv_import_line_send,false);
  assert.equal(h.customer_csv_import_fuzzy_match,false);
});

await test('commit dedupes against an existing CRM reservation on the same customer and shoot date',()=>{
  const src=fs.readFileSync('src/crm-customer-csv-import.mjs','utf8');
  assert.match(src,/WHERE customer_id=\? AND shoot_date=\? AND COALESCE\(deleted_at,''\)='' LIMIT 1/);
  assert.match(src,/deduped_by:'customer_id\+shoot_date'/);
});

await test('import implementation never reduces an existing repeat_count',()=>{
  const src=fs.readFileSync('src/crm-customer-csv-import.mjs','utf8');
  assert.match(src,/const count=Math\.max\(knownCount,dates\.length\)/);
});

await test('mobile UI supports UTF-8 and Shift_JIS CSV files',()=>{
  const html=injectCustomerCsvImport('<!doctype html><html><head></head><body></body></html>');
  assert.match(html,/UTF-8/);
  assert.match(html,/Shift_JIS/);
  assert.match(html,/TextDecoder/);
  assert.match(html,/arrayBuffer/);
});

await test('mobile UI explains dedupe/repeat behavior and requires preview before commit',()=>{
  const html=injectCustomerCsvImport('<!doctype html><html><head></head><body></body></html>');
  assert.match(html,/予約CSVから顧客取込/);
  assert.match(html,/予約管理アプリと同じ予約CSVをそのまま選べます/);
  assert.match(html,/同じ顧客名＋同じ撮影日は重複として1回/);
  assert.match(html,/同じ顧客名で撮影日が違えばリピーター/);
  assert.match(html,/内容を確認/);
  assert.match(html,/取込を確定/);
  assert.match(html,/preview_receipt/);
  assert.match(html,/customer-csv-import\/preview/);
  assert.match(html,/customer-csv-import\/commit/);
});

await test('Production entry wires owner-only CSV API and health',()=>{
  const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
  assert.match(entry,/handleCustomerCsvImport/);
  assert.match(entry,/customerCsvImportHealth/);
  assert.match(entry,/injectCustomerCsvImport/);
  assert.match(entry,/ownerEmail===OWNER_EMAIL/);
  assert.match(entry,/\/api\/customer-csv-import\/preview/);
  assert.match(entry,/\/api\/customer-csv-import\/commit/);
});

await test('CSV API fails closed when Origin header is missing',async()=>{
  const req=new Request('https://crm.example.test/api/customer-csv-import/preview',{
    method:'POST',
    headers:{'content-type':'application/json'},
    body:JSON.stringify({csv_text:['名前,撮影日,撮影場所','山田花子,2026-01-01,大阪'].join('\n')})
  });
  const res=await handleCustomerCsvImport(req,{},{authorized:true});
  assert.equal(res.status,403);
});

await test('CSV commit requires a matching signed preview receipt',async()=>{
  const req=new Request('https://crm.example.test/api/customer-csv-import/commit',{
    method:'POST',
    headers:{'content-type':'application/json','origin':'https://crm.example.test'},
    body:JSON.stringify({csv_text:['名前,撮影日,撮影場所','山田花子,2026-01-01,大阪'].join('\n')})
  });
  const res=await handleCustomerCsvImport(req,{DB:{prepare(){throw new Error('DB must not be touched before receipt validation')}}},{authorized:true});
  assert.equal(res.status,409);
  const body=await res.json();
  assert.equal(body.error,'valid_preview_receipt_required');
});

await test('new customer creation is transactional with the first CSV shoot',()=>{
  const src=fs.readFileSync('src/crm-customer-csv-import.mjs','utf8');
  assert.match(src,/typeof db\.batch!=='function'/);
  assert.match(src,/await db\.batch\(\[customerStmt,reservationStmt\]\)/);
  assert.match(src,/concurrent_csv_winner/);
  assert.match(src,/csv_customer_atomic_create_failed/);
});

await test('soft-deleted customers and reservation mappings are excluded from CSV resolution',()=>{
  const src=fs.readFileSync('src/crm-customer-csv-import.mjs','utf8');
  assert.match(src,/COALESCE\(deleted_at,''\)=''/);
  assert.match(src,/JOIN customers c ON c\.customer_id=r\.customer_id/);
  assert.match(src,/csv_event_key_soft_deleted_requires_review/);
});

await test('explicit mapping is constrained to same-name preview candidates',()=>{
  const src=fs.readFileSync('src/crm-customer-csv-import.mjs','utf8');
  assert.match(src,/mapped_customer_id_not_in_preview_candidates/);
  assert.match(src,/candidates\.get\(group\.name_key\)/);
});

await test('CSV module contains no fuzzy matching, LINE send, or Reservation-side Customer ID generation',()=>{
  const src=fs.readFileSync('src/crm-customer-csv-import.mjs','utf8');
  assert.doesNotMatch(src,/levenshtein|soundex|jaro|similarity\s*\(/i);
  assert.match(src,/customer_csv_import_fuzzy_match:false/);
  assert.doesNotMatch(src,/LINE_SERVICE|pushMessage|replyMessage|api\.line\.me/i);
  assert.match(src,/allocateCustomerId/);
  assert.doesNotMatch(src,/Math\.random|Date\.now\(\).*customer/i);
});

console.log('CUSTOMER_CSV_IMPORT_REPEAT_DEDUPE='+passed+'/'+passed+' PASS');
