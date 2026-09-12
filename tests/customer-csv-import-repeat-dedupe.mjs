import fs from 'node:fs';
import assert from 'node:assert/strict';
import {
  parseCsvText,
  analyzeCsvImport,
  normalizeImportName,
  normalizeImportDate,
  findReservationCsvHeaderRow,
  customerCsvImportHealth,
  injectCustomerCsvImport
} from '../src/crm-customer-csv-import.mjs';

let passed=0;
function test(name,fn){fn();passed++;console.log('PASS '+passed+': '+name)}

test('normalizes Japanese customer names with NFKC and whitespace removal',()=>{
  assert.equal(normalizeImportName(' 山田　花子 '),'山田花子');
  assert.equal(normalizeImportName('ﾔﾏﾀﾞ ﾊﾅｺ'),'ヤマダハナコ');
});

test('normalizes supported shoot dates',()=>{
  assert.equal(normalizeImportDate('2026/1/2'),'2026-01-02');
  assert.equal(normalizeImportDate('2026年5月3日'),'2026-05-03');
  assert.equal(normalizeImportDate('2026-02-30'),'');
});

test('CSV parser keeps commas and escaped quotes inside quoted fields',()=>{
  const rows=parseCsvText('顧客名,備考\n"山田 花子","大阪, 北区 ""テスト"""\n');
  assert.equal(rows.length,2);
  assert.equal(rows[1][0],'山田 花子');
  assert.equal(rows[1][1],'大阪, 北区 "テスト"');
});


test('accepts the exact Reservation app CSV layout with preamble rows before the real header',()=>{
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

test('same normalized name and same shoot date is one shoot, not a repeat',()=>{
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

test('same normalized name on different shoot dates becomes repeater',()=>{
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

test('same-day duplicates collapse while different customers remain separate',()=>{
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

test('conflicting exact Customer IDs for same normalized name is rejected',()=>{
  const csv=[
    '顧客名,撮影日,顧客ID',
    '山田 花子,2026-01-10,26000001',
    '山田花子,2026-05-03,26000002'
  ].join('\n');
  const a=analyzeCsvImport(csv);
  assert.equal(a.errors.some(x=>x.error==='conflicting_customer_ids_for_same_name'),true);
});

test('invalid Customer ID never silently creates or remaps identity',()=>{
  const csv=['顧客名,撮影日,顧客ID','山田花子,2026-01-10,G123'].join('\n');
  const a=analyzeCsvImport(csv);
  assert.equal(a.errors.some(x=>x.error==='invalid_customer_id'),true);
});

test('name column is mandatory',()=>{
  assert.throws(()=>analyzeCsvImport('撮影日,ジャンル\n2026-01-01,七五三'),/csv_name_column_required/);
});

test('CSV without shoot-date column stays customer-only and does not invent repeats',()=>{
  const a=analyzeCsvImport('顧客名,電話番号\n山田花子,09012345678\n山田花子,09012345678');
  assert.equal(a.customer_count,1);
  assert.equal(a.repeater_count,0);
  assert.equal(a.groups[0].shoot_count,0);
  assert.equal(a.warnings.some(x=>x.warning==='shoot_date_column_not_found'),true);
});

test('health declares exact-only import matching and no fuzzy merge',()=>{
  const h=customerCsvImportHealth();
  assert.equal(h.customer_csv_import,true);
  assert.equal(h.customer_csv_import_reservation_csv_compatible,true);
  assert.equal(h.customer_csv_import_same_day_dedupe,true);
  assert.equal(h.customer_csv_import_repeat_rule,'same_name_distinct_shoot_dates>=2');
  assert.equal(h.customer_csv_import_existing_customer_auto_name_merge,false);
  assert.equal(h.customer_csv_import_customer_id_owner,'customer-crm');
  assert.equal(h.customer_csv_import_line_send,false);
  assert.equal(h.customer_csv_import_fuzzy_match,false);
});

test('commit dedupes against an existing CRM reservation on the same customer and shoot date',()=>{
  const src=fs.readFileSync('src/crm-customer-csv-import.mjs','utf8');
  assert.match(src,/WHERE customer_id=\? AND shoot_date=\? LIMIT 1/);
  assert.match(src,/deduped_by:'customer_id\+shoot_date'/);
});

test('import implementation never reduces an existing repeat_count',()=>{
  const src=fs.readFileSync('src/crm-customer-csv-import.mjs','utf8');
  assert.match(src,/const count=Math\.max\(knownCount,dates\.length\)/);
});

test('mobile UI supports UTF-8 and Shift_JIS CSV files',()=>{
  const html=injectCustomerCsvImport('<!doctype html><html><head></head><body></body></html>');
  assert.match(html,/UTF-8/);
  assert.match(html,/Shift_JIS/);
  assert.match(html,/TextDecoder/);
  assert.match(html,/arrayBuffer/);
});

test('mobile UI explains dedupe/repeat behavior and requires preview before commit',()=>{
  const html=injectCustomerCsvImport('<!doctype html><html><head></head><body></body></html>');
  assert.match(html,/顧客CSV取込/);
  assert.match(html,/予約管理アプリと同じ予約CSVをそのまま選べます/);
  assert.match(html,/同じ顧客名＋同じ撮影日は重複として1回/);
  assert.match(html,/同じ顧客名で撮影日が違えばリピーター/);
  assert.match(html,/内容を確認/);
  assert.match(html,/取込を確定/);
  assert.match(html,/customer-csv-import\/preview/);
  assert.match(html,/customer-csv-import\/commit/);
});

test('Production entry wires owner-only CSV API and health',()=>{
  const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
  assert.match(entry,/handleCustomerCsvImport/);
  assert.match(entry,/customerCsvImportHealth/);
  assert.match(entry,/injectCustomerCsvImport/);
  assert.match(entry,/ownerEmail===OWNER_EMAIL/);
  assert.match(entry,/\/api\/customer-csv-import\/preview/);
  assert.match(entry,/\/api\/customer-csv-import\/commit/);
});

test('CSV module contains no fuzzy matching, LINE send, or Reservation-side Customer ID generation',()=>{
  const src=fs.readFileSync('src/crm-customer-csv-import.mjs','utf8');
  assert.doesNotMatch(src,/levenshtein|soundex|jaro|similarity\s*\(/i);
  assert.match(src,/customer_csv_import_fuzzy_match:false/);
  assert.doesNotMatch(src,/LINE_SERVICE|pushMessage|replyMessage|api\.line\.me/i);
  assert.match(src,/allocateCustomerId/);
  assert.doesNotMatch(src,/Math\.random|Date\.now\(\).*customer/i);
});

console.log('CUSTOMER_CSV_IMPORT_REPEAT_DEDUPE='+passed+'/'+passed+' PASS');
