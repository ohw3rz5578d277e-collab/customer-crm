import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { parseLineLogCsv, parseCustomerMasterCsv, resolveLineHistoryRows, normalizeSheetTimestamp, lineHistorySheetBackfillHealth } from '../src/crm-line-history-sheet-backfill.mjs';

const uid=n=>'U'+String(n).padStart(32,'a').slice(-32);
const modernLine=uid('1');
const legacyLine=uid('2');
const oldIdLine=uid('3');
const headers=['日時','顧客ID','名前','内容','種類','AI処理','発信者','LINE UserID','取得元','msgKey','csvFileName','csvRowNo','importedAt'];
function q(v){const s=String(v??'');return /[",\n]/.test(s)?'"'+s.replaceAll('"','""')+'"':s}
function row(values){return values.map(q).join(',')}
const huge='x'.repeat(7000);
const lineCsv=[
 row(headers),
 row(['2026/01/14 10:00:00','','現代顧客','こんにちは','text','FALSE','User',modernLine,'webhook','modern-key','','','2026/01/14 10:00:01']),
 row(['2024/12/17 19:54:06','','Arisa','ご連絡ありがとうございます','text','FALSE','Admin','','csv','legacy-admin','乾早香.csv','5','2026/01/27 20:12:18']),
 row(['2024/12/17 19:55:11','','乾早香','よろしくお願いします','text','FALSE','User','','csv','legacy-user','乾早香.csv','6','2026/01/27 20:12:19']),
 row(['2026/01/14 10:00:00','','現代顧客','こんにちは duplicate','text','FALSE','User',modernLine,'webhook','modern-key','','','2026/01/14 10:00:02']),
 row(['2025/01/01 00:00:00','','',''+huge,'text','FALSE','User','','','','','','']),
 row(['2025/01/02 00:00:00','','不明','unknown sender','text','FALSE','Mystery','','csv','unknown-key','不明.csv','1',''])
].join('\n');

const masterCsv=[
 row(['顧客ID','LINE UserID','名前','LINE名（本人設定）']),
 row(['26000001',modernLine,'現代顧客','Modern']),
 row(['26000002',legacyLine,'乾早香','さやか'])
].join('\n');

assert.equal(normalizeSheetTimestamp('2026/01/14 10:00:00'),'2026-01-14T10:00:00+09:00');
const parsed=parseLineLogCsv(lineCsv);
assert.equal(parsed.row_count,6);
assert.equal(parsed.rows.length,4);
assert.equal(parsed.skipped.filter(x=>x.reason==='legacy_conversation_blob').length,1);
assert.equal(parsed.skipped.filter(x=>x.reason==='unknown_direction').length,1);

const master=parseCustomerMasterCsv(masterCsv);
assert.equal(master.entries.length,2);
const dbCustomers=[
 {customer_id:'26000001',line_user_id:modernLine,name:'現代顧客',line_display_name:'Modern'},
 {customer_id:'26000002',line_user_id:legacyLine,name:'乾早香',line_display_name:'さやか'}
];
const resolved=await resolveLineHistoryRows(parsed,master,dbCustomers);
assert.equal(resolved.conflicts.length,0);
assert.equal(resolved.candidates.length,3);
assert.equal(resolved.candidates.find(x=>x.message_key==='modern-key').customer_id,'26000001');
assert.equal(resolved.candidates.find(x=>x.message_key==='legacy-admin').customer_id,'26000002');
assert.equal(resolved.candidates.find(x=>x.message_key==='legacy-admin').line_user_id,legacyLine);
assert.equal(resolved.candidates.find(x=>x.message_key==='legacy-admin').direction,'outbound');
assert.equal(resolved.candidates.find(x=>x.message_key==='legacy-user').direction,'inbound');
assert.equal(resolved.candidates.find(x=>x.message_key==='legacy-admin').match_source,'master_csv_file_name');
assert.equal(resolved.skipped.filter(x=>x.reason==='duplicate_input_message_key').length,1);

const conflictParsed={rows:[{line:2,sent_at:'2026-01-01T00:00:00+09:00',customer_id_hint:'26000002',name:'',content:'x',message_type:'text',sender:'User',direction:'inbound',line_user_id:modernLine,source:'webhook',message_key:'conflict-key',csv_file_name:'',csv_row_no:'',message_id:'',media_url:''}],skipped:[]};
const conflict=await resolveLineHistoryRows(conflictParsed,master,dbCustomers);
assert.equal(conflict.candidates.length,0);
assert.equal(conflict.conflicts[0].reason,'customer_id_line_user_conflict');

const source=fs.readFileSync('src/crm-line-history-sheet-backfill.mjs','utf8');
const planner=fs.readFileSync('scripts/prepare-line-history-sheet-backfill.mjs','utf8');
const wrangler=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
const deploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
assert.match(source,/CRM_LINE_HISTORY_BACKFILL_WRITE_ENABLED/);
assert.match(source,/preview_receipt_required_or_invalid/);
assert.match(source,/INSERT OR IGNORE INTO customer_line_messages/);
assert.doesNotMatch(source,/INSERT\s+(?:OR\s+\w+\s+)?INTO\s+customers\b/i);
assert.doesNotMatch(source,/UPDATE\s+customers\b/i);
assert.doesNotMatch(source,/DELETE\s+FROM\s+customers\b/i);
assert.doesNotMatch(source,/api\.line\.me\/v2\/bot\/message/i);
assert.doesNotMatch(source,/allocateCustomerId/);
assert.match(planner,/DO NOT RUN WITHOUT FRESH OWNER APPROVAL/);
assert.match(planner,/legacy_customer_id_ignored/);
assert.match(planner,/remote_line_user_resolution_rows/);
assert.match(planner,/identity_matches=1/);
assert.match(planner,/INSERT OR IGNORE INTO customer_line_messages/);
assert.doesNotMatch(planner,/INSERT\s+(?:OR\s+IGNORE\s+)?INTO\s+customers\b/i);
assert.doesNotMatch(planner,/UPDATE\s+customers\b/i);
assert.doesNotMatch(planner,/DELETE\s+FROM\s+customers\b/i);
assert.equal(wrangler.main,'src/production-index-crm-customer360-entry.js','canonical Production entry must stay unchanged');
assert.ok(!deploy.includes('CRM_LINE_HISTORY_BACKFILL_WRITE_ENABLED=1'),'canonical release must not auto-enable backfill writes');

const plannerLineCsv=lineCsv+'\n'+row(['2026/03/01 10:00:00','C342ee0c10d','みずき','古いC IDでもLINE IDで復元','text','FALSE','User',oldIdLine,'webhook','legacy-c-line-key','','','2026/03/01 10:00:01']);
const plannerMasterCsv=masterCsv+'\n'+row(['C342ee0c10d',oldIdLine,'みずき','みずき']);
const tmp=fs.mkdtempSync(path.join(os.tmpdir(),'crm-line-backfill-'));
const lineFile=path.join(tmp,'LINE_log.csv'),masterFile=path.join(tmp,'Customer_Master.csv'),outDir=path.join(tmp,'out');
fs.writeFileSync(lineFile,plannerLineCsv);fs.writeFileSync(masterFile,plannerMasterCsv);
const plannerOut=execFileSync(process.execPath,['scripts/prepare-line-history-sheet-backfill.mjs','--line-log',lineFile,'--customer-master',masterFile,'--out-dir',outDir],{encoding:'utf8'});
assert.match(plannerOut,/RESULT=LINE_HISTORY_BACKFILL_PLAN_READY/);
assert.match(plannerOut,/PRODUCTION_D1_WRITE=0/);
const summary=JSON.parse(fs.readFileSync(path.join(outDir,'line-history-backfill-summary.json'),'utf8'));
assert.equal(summary.planner_version,2);
assert.equal(summary.candidate_rows,4);
assert.equal(summary.remote_line_user_resolution_rows,1);
assert.equal(summary.legacy_customer_id_ignored_rows,1);
assert.equal(summary.conflict_rows,0);
assert.equal(summary.safety.production_d1_write,0);
assert.equal(summary.safety.remote_identity_requires_unique_match,true);
const previewSql=fs.readFileSync(path.join(outDir,'line-history-backfill-preview.sql'),'utf8').replace(/^--.*$/gm,'');
const commitSql=fs.readFileSync(path.join(outDir,'line-history-backfill-commit.sql'),'utf8').replace(/^--.*$/gm,'');
assert.doesNotMatch(previewSql,/\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE)\b/i);
assert.match(previewSql,/identity_ambiguous/);
assert.match(commitSql,/INSERT OR IGNORE INTO customer_line_messages/);
assert.match(commitSql,/SELECT COUNT\(\*\) FROM customers/);
assert.match(commitSql,new RegExp(oldIdLine));
assert.doesNotMatch(commitSql,/C342ee0c10d[^\n]*customer_id=/i);
assert.doesNotMatch(commitSql,/\b(?:UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE)\b/i);
fs.rmSync(tmp,{recursive:true,force:true});

const health=lineHistorySheetBackfillHealth({});
assert.equal(health.line_history_sheet_backfill,true);
assert.equal(health.line_history_sheet_backfill_write_enabled,false);
assert.equal(health.line_history_sheet_backfill_customer_id_generation,false);
assert.equal(health.line_history_sheet_backfill_customer_update,false);
assert.equal(health.line_history_sheet_backfill_delete,false);
assert.equal(health.line_history_sheet_backfill_line_send,false);

console.log('LINE_HISTORY_SHEET_PARSE=PASS');
console.log('LINE_HISTORY_SHEET_DIRECTION=PASS');
console.log('LINE_HISTORY_SHEET_CUSTOMER_MASTER_FALLBACK=PASS');
console.log('LINE_HISTORY_SHEET_LEGACY_C_ID_LINE_USER_RECOVERY=PASS');
console.log('LINE_HISTORY_SHEET_REMOTE_IDENTITY_UNIQUE_GUARD=PASS');
console.log('LINE_HISTORY_SHEET_IDENTITY_CONFLICT_BLOCK=PASS');
console.log('LINE_HISTORY_SHEET_DUPLICATE_INPUT=PASS');
console.log('LINE_HISTORY_SHEET_OFFLINE_PLAN=PASS');
console.log('LINE_HISTORY_SHEET_CUSTOMER_ID_GENERATION=0');
console.log('LINE_HISTORY_SHEET_CUSTOMER_UPDATE=0');
console.log('LINE_HISTORY_SHEET_DELETE=0');
console.log('LINE_HISTORY_SHEET_LINE_SEND=0');
