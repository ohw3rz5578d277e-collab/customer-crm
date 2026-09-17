import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { parseCsvText } from '../src/crm-customer-csv-import.mjs';
import { normalizeSheetTimestamp } from '../src/crm-line-history-sheet-backfill.mjs';

const CUSTOMER_ID_RE=/^\d{8}$/;
const LINE_ID_RE=/^U[0-9a-fA-F]{20,}$/;
const MAX_ROWS=12_000;

function arg(name){const i=process.argv.indexOf(name);return i>=0?String(process.argv[i+1]||''):''}
const linePath=arg('--line-log');
const masterPath=arg('--customer-master');
const outDir=arg('--out-dir')||'artifacts/line-history-backfill';
if(!linePath||!masterPath){console.error('Usage: node scripts/prepare-line-history-sheet-backfill.mjs --line-log LINE_log.csv --customer-master Customer_Master.csv [--out-dir DIR]');process.exit(2)}

function read(file){return fs.readFileSync(file,'utf8')}
function text(v){return v==null?'':String(v).trim()}
function normHeader(v){return text(v).normalize('NFKC').replace(/[\s　\r\n（）()]/g,'').toLowerCase()}
function normName(v){return text(v).normalize('NFKC').toLowerCase().replace(/\.csv$/i,'').replace(/[\s　・･.．,，、()（）\[\]［］【】「」『』]/g,'')}
function stripCsv(v){return text(v).replace(/^.*[\\/]/,'').replace(/\.csv$/i,'')}
function sqlText(v){return "'"+String(v??'').replaceAll('\u0000','').replaceAll("'","''")+"'"}
function sqlNullable(v){return text(v)?sqlText(v):'NULL'}
function sha(v){return createHash('sha256').update(String(v)).digest('hex')}
function direction(sender){const s=text(sender).normalize('NFKC').toLowerCase();if(/^(admin|out|outbound|sent|send|arisa|mizuno|owner|staff|スタッフ)$/.test(s))return'outbound';if(/^(user|in|inbound|customer|客|顧客)$/.test(s))return'inbound';return''}
function operatorName(name){return /^(arisa|mizuno|水野|ありさ)$/i.test(normName(name))}
function messageType(value,content,mediaUrl){const v=text(value).toLowerCase();if(v)return v;if(text(mediaUrl))return'image';if(/\(sticker\)|スタンプ/.test(text(content)))return'sticker';return'text'}
function indexes(headers,aliases){const wanted=new Set(aliases.map(normHeader)),out=[];for(let i=0;i<headers.length;i++)if(wanted.has(normHeader(headers[i])))out.push(i);return out}
function pick(cells,idxs){for(const i of idxs){const v=text(cells[i]);if(v)return v}return''}
function picker(headers,spec){const idx={};for(const [k,a] of Object.entries(spec))idx[k]=indexes(headers,a);return(cells,k)=>pick(cells,idx[k]||[])}
function addMulti(map,key,value){if(!key)return;if(!map.has(key))map.set(key,[]);map.get(key).push(value)}
function identityKey(e){return `${text(e.customer_id)}\u0000${text(e.line_user_id)}`}
function uniqueEntry(entries){const usable=(entries||[]).filter(x=>x.customer_id||x.line_user_id),ids=[...new Set(usable.map(identityKey))];return ids.length===1?usable[0]:null}
function countReasons(items){const out={};for(const x of items||[])out[x.reason]=(out[x.reason]||0)+1;return out}
function countMatchSources(items){const out={};for(const x of items||[])out[x.match_source]=(out[x.match_source]||0)+1;return out}

const LINE_HEADERS={
  sent_at:['日時','sent_at','sentat','timestamp'],customer_id:['顧客id','customerid','customer_id'],name:['名前','name','displayname'],
  content:['内容','text','message_text','messagetext'],message_type:['種類','msgtype','messagetype','message_type'],sender:['発信者','sender','direction'],
  line_user_id:['lineuserid','line_user_id','line user id'],source:['取得元','source'],message_key:['msgkey','messagekey','message_key'],
  csv_file_name:['csvfilename','csv_file_name'],csv_row_no:['csvrowno','csv_row_no'],message_id:['messageid','message_id'],media_url:['mediaurl','media_url']
};
const MASTER_HEADERS={customer_id:['顧客id','customerid','customer_id'],line_user_id:['lineuserid','line_user_id','line user id'],name:['名前','name','customername'],line_name:['line名本人設定','line名','displayname','line_display_name']};

function parseMaster(csvText){
  const matrix=parseCsvText(csvText);if(matrix.length<2)throw new Error('customer_master_csv_has_no_rows');if(matrix.length-1>MAX_ROWS)throw new Error('customer_master_row_limit_exceeded');
  const headers=matrix[0].map(text),get=picker(headers,MASTER_HEADERS),entries=[],skipped=[];
  for(let i=1;i<matrix.length;i++){
    const cells=matrix[i],rawCustomerId=get(cells,'customer_id'),rawLineId=get(cells,'line_user_id'),name=get(cells,'name'),lineName=get(cells,'line_name');
    if(!rawCustomerId&&!rawLineId&&!name)continue;
    const customerId=CUSTOMER_ID_RE.test(rawCustomerId)?rawCustomerId:'';
    const lineId=LINE_ID_RE.test(rawLineId)?rawLineId:'';
    if(rawLineId&&!lineId)skipped.push({line:i+1,reason:'invalid_master_line_user_id',value:rawLineId});
    if(!customerId&&!lineId){skipped.push({line:i+1,reason:'master_identity_unusable',legacy_customer_id:rawCustomerId,name});continue}
    entries.push({line:i+1,customer_id:customerId,legacy_customer_id:customerId?'':rawCustomerId,line_user_id:lineId,name,line_name:lineName,name_key:normName(name)});
  }
  return{row_count:matrix.length-1,entries,skipped};
}

function parseLineLog(csvText){
  const matrix=parseCsvText(csvText);if(matrix.length<2)throw new Error('line_log_csv_has_no_rows');if(matrix.length-1>MAX_ROWS)throw new Error('line_log_row_limit_exceeded');
  const headers=matrix[0].map(text),get=picker(headers,LINE_HEADERS),rows=[],skipped=[],warnings=[];
  for(let i=1;i<matrix.length;i++){
    const cells=matrix[i],content=get(cells,'content'),mediaUrl=get(cells,'media_url'),rawCustomerId=get(cells,'customer_id'),rawLineId=get(cells,'line_user_id');
    const row={line:i+1,sent_at:normalizeSheetTimestamp(get(cells,'sent_at')),customer_id_hint:CUSTOMER_ID_RE.test(rawCustomerId)?rawCustomerId:'',legacy_customer_id_hint:rawCustomerId&&!CUSTOMER_ID_RE.test(rawCustomerId)?rawCustomerId:'',name:get(cells,'name'),content:content||mediaUrl,message_type:messageType(get(cells,'message_type'),content,mediaUrl),sender:get(cells,'sender'),direction:direction(get(cells,'sender')),line_user_id:LINE_ID_RE.test(rawLineId)?rawLineId:'',source:get(cells,'source'),message_key:get(cells,'message_key'),csv_file_name:get(cells,'csv_file_name'),csv_row_no:get(cells,'csv_row_no'),message_id:get(cells,'message_id'),media_url:mediaUrl};
    if(!row.content){skipped.push({line:row.line,reason:'empty_message'});continue}
    if(!row.sent_at){skipped.push({line:row.line,reason:'invalid_timestamp'});continue}
    if(!row.direction){skipped.push({line:row.line,reason:'unknown_direction',sender:row.sender});continue}
    if(rawCustomerId&&!row.customer_id_hint)warnings.push({line:row.line,reason:'legacy_customer_id_ignored',value:rawCustomerId});
    if(rawLineId&&!row.line_user_id)warnings.push({line:row.line,reason:'invalid_line_user_id_ignored',value:rawLineId});
    if(!row.message_key&&!row.message_id&&!row.csv_file_name&&!row.source&&row.content.length>6000){skipped.push({line:row.line,reason:'legacy_conversation_blob'});continue}
    rows.push(row);
  }
  return{row_count:matrix.length-1,rows,skipped,warnings};
}

function buildCandidates(lineLog,master){
  const masterByLine=new Map(),masterByName=new Map();
  for(const e of master.entries){addMulti(masterByLine,e.line_user_id,e);addMulti(masterByName,e.name_key,e)}
  const candidates=[],skipped=[...lineLog.skipped],conflicts=[],seen=new Set();
  for(const row of lineLog.rows){
    let customerId=row.customer_id_hint,lineId=row.line_user_id,matchSource=customerId?(lineId?'line_log_customer_id_and_line_user_id':'line_log_customer_id'):(lineId?'line_log_line_user_id':'');
    let masterEntry=null;
    if(lineId)masterEntry=uniqueEntry(masterByLine.get(lineId));
    if(!masterEntry){const fileKey=normName(stripCsv(row.csv_file_name));if(fileKey)masterEntry=uniqueEntry(masterByName.get(fileKey));if(masterEntry&&!matchSource)matchSource='master_csv_file_name'}
    if(!masterEntry){const nameKey=!operatorName(row.name)?normName(row.name):'';if(nameKey)masterEntry=uniqueEntry(masterByName.get(nameKey));if(masterEntry&&!matchSource)matchSource='master_exact_name'}
    if(masterEntry){
      if(customerId&&masterEntry.customer_id&&customerId!==masterEntry.customer_id){conflicts.push({line:row.line,reason:'customer_id_master_conflict',customer_id:customerId,master_customer_id:masterEntry.customer_id});continue}
      if(lineId&&masterEntry.line_user_id&&lineId!==masterEntry.line_user_id){conflicts.push({line:row.line,reason:'line_user_master_conflict',line_user_id:lineId,master_line_user_id:masterEntry.line_user_id});continue}
      if(!customerId&&masterEntry.customer_id)customerId=masterEntry.customer_id;
      if(!lineId&&masterEntry.line_user_id)lineId=masterEntry.line_user_id;
      if(!matchSource)matchSource='master_identity';
    }
    if(!customerId&&!lineId){skipped.push({line:row.line,reason:'customer_unmatched',csv_file_name:row.csv_file_name,name:row.name});continue}
    if(!matchSource)matchSource=customerId?'customer_id_pending':'remote_line_user_id_pending';
    if(!customerId&&lineId&&matchSource==='line_log_line_user_id')matchSource='remote_line_user_id_pending';
    let key=text(row.message_key)||text(row.message_id);
    if(!key)key='sheet:v2:'+sha([customerId,lineId,row.direction,row.sent_at,row.message_type,row.content,row.csv_file_name,row.csv_row_no||row.line].join('\n'));
    if(seen.has(key)){skipped.push({line:row.line,reason:'duplicate_input_message_key',message_key:key});continue}seen.add(key);
    candidates.push({message_key:key,customer_id_hint:customerId,line_user_id:lineId,direction:row.direction,message_type:row.message_type||'text',message_text:row.content,sender_name:row.name||'',sent_at:row.sent_at,match_source:matchSource,source_row:row.line,csv_file_name:row.csv_file_name,csv_row_no:row.csv_row_no,source:row.source||'line_history_sheet_backfill',media_url:row.media_url||'',legacy_customer_id_hint:row.legacy_customer_id_hint||''});
  }
  return{candidates,skipped,conflicts,warnings:lineLog.warnings};
}

function identityCondition(alias='c',prefix='candidates.'){
  const hintExists=`EXISTS(SELECT 1 FROM customers cx WHERE cx.customer_id=${prefix}customer_id_hint)`;
  return `((${prefix}customer_id_hint<>'' AND ${hintExists} AND ${alias}.customer_id=${prefix}customer_id_hint AND (${prefix}line_user_id='' OR COALESCE(${alias}.line_user_id,'')=${prefix}line_user_id)) OR ((${prefix}customer_id_hint='' OR NOT ${hintExists}) AND ${prefix}line_user_id<>'' AND COALESCE(${alias}.line_user_id,'')=${prefix}line_user_id))`;
}
function literalIdentityCondition(x,alias='c'){
  if(x.customer_id_hint&&x.line_user_id){
    const hint=sqlText(x.customer_id_hint),line=sqlText(x.line_user_id);
    const hintExists=`EXISTS(SELECT 1 FROM customers cx WHERE cx.customer_id=${hint})`;
    return `((${hintExists} AND ${alias}.customer_id=${hint} AND COALESCE(${alias}.line_user_id,'')=${line}) OR (NOT ${hintExists} AND COALESCE(${alias}.line_user_id,'')=${line}))`;
  }
  if(x.customer_id_hint)return `${alias}.customer_id=${sqlText(x.customer_id_hint)}`;
  return `COALESCE(${alias}.line_user_id,'')=${sqlText(x.line_user_id)}`;
}

const lineCsv=read(linePath),masterCsv=read(masterPath),lineLog=parseLineLog(lineCsv),master=parseMaster(masterCsv),resolved=buildCandidates(lineLog,master);
if(resolved.conflicts.length){console.error('RESULT=LINE_HISTORY_BACKFILL_IDENTITY_CONFLICT');console.error(JSON.stringify(resolved.conflicts.slice(0,30),null,2));process.exit(3)}
if(!resolved.candidates.length){console.error('RESULT=LINE_HISTORY_BACKFILL_NO_CANDIDATES');process.exit(4)}

fs.mkdirSync(outDir,{recursive:true});
const previewPath=path.join(outDir,'line-history-backfill-preview.sql'),commitPath=path.join(outDir,'line-history-backfill-commit.sql'),summaryPath=path.join(outDir,'line-history-backfill-summary.json');

const preview=['-- READ ONLY. Generated by prepare-line-history-sheet-backfill.mjs','-- Resolves legacy C... and non-current Customer ID hints through customers.line_user_id only when the hint does not exist and exactly one current CRM customer matches.','-- Existing current Customer IDs remain authoritative and must agree with any supplied LINE UserID.','-- This file must not contain INSERT/UPDATE/DELETE/CREATE/ALTER/DROP.'];
const CHUNK=75,condition=identityCondition();
for(let offset=0;offset<resolved.candidates.length;offset+=CHUNK){
  const chunk=resolved.candidates.slice(offset,offset+CHUNK),values=chunk.map(x=>`(${sqlText(x.message_key)},${sqlText(x.customer_id_hint)},${sqlText(x.line_user_id)},${sqlText(x.direction)},${sqlText(x.sent_at)},${sqlText(x.message_text)})`).join(',\n');
  preview.push(`\nWITH candidates(message_key,customer_id_hint,line_user_id,direction,sent_at,message_text) AS (VALUES\n${values}\n),\nresolved AS (\n SELECT candidates.*,\n  (SELECT COUNT(*) FROM customers c WHERE ${condition}) AS identity_matches,\n  (SELECT c.customer_id FROM customers c WHERE ${condition} LIMIT 1) AS resolved_customer_id,\n  (SELECT COALESCE(c.line_user_id,'') FROM customers c WHERE ${condition} LIMIT 1) AS resolved_line_user_id\n FROM candidates\n)\nSELECT ${Math.floor(offset/CHUNK)+1} AS chunk_no,COUNT(*) AS candidate_rows,\n SUM(CASE WHEN identity_matches=1 THEN 1 ELSE 0 END) AS identity_unique,\n SUM(CASE WHEN identity_matches=0 THEN 1 ELSE 0 END) AS identity_missing,\n SUM(CASE WHEN identity_matches>1 THEN 1 ELSE 0 END) AS identity_ambiguous,\n SUM(CASE WHEN identity_matches=1 AND EXISTS(SELECT 1 FROM customer_line_messages m WHERE m.message_key=resolved.message_key OR (m.customer_id=resolved.resolved_customer_id AND COALESCE(m.line_user_id,'')=CASE WHEN resolved.line_user_id<>'' THEN resolved.line_user_id ELSE resolved.resolved_line_user_id END AND m.direction=resolved.direction AND m.sent_at=resolved.sent_at AND m.message_text=resolved.message_text)) THEN 1 ELSE 0 END) AS already_present,\n SUM(CASE WHEN identity_matches=1 AND NOT EXISTS(SELECT 1 FROM customer_line_messages m WHERE m.message_key=resolved.message_key OR (m.customer_id=resolved.resolved_customer_id AND COALESCE(m.line_user_id,'')=CASE WHEN resolved.line_user_id<>'' THEN resolved.line_user_id ELSE resolved.resolved_line_user_id END AND m.direction=resolved.direction AND m.sent_at=resolved.sent_at AND m.message_text=resolved.message_text)) THEN 1 ELSE 0 END) AS would_insert\nFROM resolved;`);
}
fs.writeFileSync(previewPath,preview.join('\n')+'\n');

const commit=['-- WRITE FILE. DO NOT RUN WITHOUT FRESH OWNER APPROVAL.','-- INSERT ONLY. Existing current Customer IDs are authoritative; a non-existent Customer ID hint may fall back to one unique current LINE UserID match.','-- No customer creation/update/delete. Idempotent by message_key + exact fingerprint.'];
for(const x of resolved.candidates){
  const cond=literalIdentityCondition(x,'c'),cond2=literalIdentityCondition(x,'c2');
  const raw=JSON.stringify({source:'line_history_sheet_backfill_v2',match_source:x.match_source,source_row:x.source_row,csv_file_name:x.csv_file_name||null,csv_row_no:x.csv_row_no||null,source_kind:x.source||null,media_url:x.media_url||null,legacy_customer_id_hint:x.legacy_customer_id_hint||null});
  commit.push(`INSERT OR IGNORE INTO customer_line_messages (message_key,customer_id,line_user_id,direction,message_type,message_text,sender_name,sent_at,raw_json,created_at)\nSELECT ${sqlText(x.message_key)},c.customer_id,CASE WHEN ${sqlText(x.line_user_id)}<>'' THEN ${sqlText(x.line_user_id)} ELSE NULLIF(COALESCE(c.line_user_id,''),'') END,${sqlText(x.direction)},${sqlText(x.message_type||'text')},${sqlText(x.message_text)},${sqlNullable(x.sender_name)},${sqlText(x.sent_at)},${sqlText(raw)},datetime('now')\nFROM customers c\nWHERE ${cond}\n  AND (SELECT COUNT(*) FROM customers c2 WHERE ${cond2})=1\n  AND NOT EXISTS (SELECT 1 FROM customer_line_messages m WHERE m.message_key=${sqlText(x.message_key)} OR (m.customer_id=c.customer_id AND COALESCE(m.line_user_id,'')=CASE WHEN ${sqlText(x.line_user_id)}<>'' THEN ${sqlText(x.line_user_id)} ELSE COALESCE(c.line_user_id,'') END AND m.direction=${sqlText(x.direction)} AND m.sent_at=${sqlText(x.sent_at)} AND m.message_text=${sqlText(x.message_text)}));`);
}
fs.writeFileSync(commitPath,commit.join('\n\n')+'\n');

const summary={generated_at:new Date().toISOString(),planner_version:3,source:{line_log:path.basename(linePath),customer_master:path.basename(masterPath)},line_log_rows:lineLog.row_count,parsed_message_rows:lineLog.rows.length,customer_master_rows:master.row_count,customer_master_usable_entries:master.entries.length,candidate_rows:resolved.candidates.length,remote_line_user_resolution_rows:resolved.candidates.filter(x=>!x.customer_id_hint&&x.line_user_id).length,legacy_customer_id_ignored_rows:resolved.warnings.filter(x=>x.reason==='legacy_customer_id_ignored').length,skipped_rows:resolved.skipped.length,skipped_reasons:countReasons(resolved.skipped),warning_reasons:countReasons(resolved.warnings),match_sources:countMatchSources(resolved.candidates),conflict_rows:resolved.conflicts.length,safety:{production_d1_write:0,customer_id_generation:0,customer_update:0,customer_delete:0,line_send:0,remote_identity_requires_unique_match:true,stale_customer_id_fallback_requires_nonexistent_hint:true,commit_requires_owner_approval:true},files:{preview_sql:previewPath,commit_sql:commitPath}};
fs.writeFileSync(summaryPath,JSON.stringify(summary,null,2)+'\n');

const previewSource=fs.readFileSync(previewPath,'utf8').replace(/^--.*$/gm,'');if(/\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE)\b/i.test(previewSource))throw new Error('preview_sql_not_read_only');
const commitSource=fs.readFileSync(commitPath,'utf8').replace(/^--.*$/gm,'');if(/\b(?:UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE|TRUNCATE)\b/i.test(commitSource))throw new Error('commit_sql_contains_forbidden_write');if(/INSERT\s+(?:OR\s+IGNORE\s+)?INTO\s+customers\b/i.test(commitSource))throw new Error('customer_insert_forbidden');

console.log(JSON.stringify(summary,null,2));
console.log('RESULT=LINE_HISTORY_BACKFILL_PLAN_READY');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('LINE_SEND=0');
