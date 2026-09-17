import { parseCsvText } from './crm-customer-csv-import.mjs';

const BUILD='crm-line-history-sheet-backfill-20260918-01';
const SOURCE='line_history_sheet_backfill';
const MAX_LINE_CSV_BYTES=15_000_000;
const MAX_MASTER_CSV_BYTES=8_000_000;
const MAX_ROWS=12_000;
const RECEIPT_SECONDS=15*60;
const CUSTOMER_ID_RE=/^\d{8}$/;
const LINE_ID_RE=/^U[0-9a-fA-F]{20,}$/;
const encoder=new TextEncoder();

function text(v){return v==null?'':String(v).trim()}
function json(data,status=200){return new Response(JSON.stringify(data,null,2),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store, no-cache, must-revalidate, max-age=0'}})}
function sameOrigin(request){const origin=text(request.headers.get('origin'));if(!origin)return false;try{return new URL(origin).origin===new URL(request.url).origin}catch{return false}}
function normalizeHeader(v){return text(v).normalize('NFKC').replace(/[\s　\r\n（）()]/g,'').toLowerCase()}
function normalizeName(v){return text(v).normalize('NFKC').toLowerCase().replace(/\.csv$/i,'').replace(/[\s　・･.．,，、()（）\[\]［］【】「」『』]/g,'')}
function stripCsvName(v){return text(v).replace(/^.*[\\/]/,'').replace(/\.csv$/i,'')}
function byteLength(v){return encoder.encode(String(v||'')).byteLength}
function b64url(bytes){let out='';for(const b of bytes)out+=String.fromCharCode(b);return btoa(out).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'')}
function fromB64url(value){const raw=String(value||'').replace(/-/g,'+').replace(/_/g,'/');const padded=raw+'='.repeat((4-raw.length%4)%4);try{const s=atob(padded),out=new Uint8Array(s.length);for(let i=0;i<s.length;i++)out[i]=s.charCodeAt(i);return out}catch{return new Uint8Array()}}
function safeEqual(a,b){if(a.length!==b.length)return false;let diff=0;for(let i=0;i<a.length;i++)diff|=a[i]^b[i];return diff===0}
async function sha256Hex(value){const data=encoder.encode(String(value));const out=new Uint8Array(await crypto.subtle.digest('SHA-256',data));return [...out].map(x=>x.toString(16).padStart(2,'0')).join('')}
async function hmac(value,secret){const key=await crypto.subtle.importKey('raw',encoder.encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);return new Uint8Array(await crypto.subtle.sign('HMAC',key,encoder.encode(value)))}
function receiptSecret(env){return text(env?.CRM_OWNER_SESSION_SECRET)||text(env?.ADMIN_TOKEN)}

const LINE_HEADERS={
  sent_at:['日時','sent_at','sentat','timestamp'],
  customer_id:['顧客id','customerid','customer_id'],
  name:['名前','name','displayname'],
  content:['内容','text','message_text','messagetext'],
  message_type:['種類','msgtype','messagetype','message_type'],
  sender:['発信者','sender','direction'],
  line_user_id:['lineuserid','line_user_id','line user id'],
  source:['取得元','source'],
  message_key:['msgkey','messagekey','message_key'],
  csv_file_name:['csvfilename','csv_file_name'],
  csv_row_no:['csvrowno','csv_row_no'],
  imported_at:['importedat','imported_at'],
  message_id:['messageid','message_id'],
  media_url:['mediaurl','media_url'],
  raw_json:['rawjson','raw_json']
};
const MASTER_HEADERS={
  customer_id:['顧客id','customerid','customer_id'],
  line_user_id:['lineuserid','line_user_id','line user id'],
  name:['名前','name','customername'],
  line_name:['line名本人設定','line名','displayname','line_display_name']
};
function indexes(headers,aliases){const wanted=new Set(aliases.map(normalizeHeader)),out=[];for(let i=0;i<headers.length;i++)if(wanted.has(normalizeHeader(headers[i])))out.push(i);return out}
function pick(cells,idxs){for(const i of idxs){const v=text(cells[i]);if(v)return v}return''}
function buildPicker(headers,spec){const map={};for(const [key,aliases] of Object.entries(spec))map[key]=indexes(headers,aliases);return(cells,key)=>pick(cells,map[key]||[])}

export function normalizeSheetTimestamp(value){
  const raw=text(value).normalize('NFKC');if(!raw)return'';
  if(/^\d{10,13}$/.test(raw)){let n=Number(raw);if(raw.length===10)n*=1000;const d=new Date(n);return Number.isNaN(d.getTime())?'':d.toISOString()}
  const m=raw.match(/^(20\d{2})[\/\-.年](\d{1,2})[\/\-.月](\d{1,2})(?:日)?(?:[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?/);
  if(m){const y=m[1],mo=String(m[2]).padStart(2,'0'),d=String(m[3]).padStart(2,'0'),hh=String(m[4]||0).padStart(2,'0'),mm=String(m[5]||0).padStart(2,'0'),ss=String(m[6]||0).padStart(2,'0');return `${y}-${mo}-${d}T${hh}:${mm}:${ss}+09:00`}
  const parsed=new Date(raw);return Number.isNaN(parsed.getTime())?'':parsed.toISOString();
}
function directionFrom(sender){const s=text(sender).normalize('NFKC').toLowerCase();if(/^(admin|out|outbound|sent|send|arisa|mizuno|owner|staff|スタッフ)$/.test(s))return'outbound';if(/^(user|in|inbound|customer|客|顧客)$/.test(s))return'inbound';return''}
function typeFrom(value,content,mediaUrl){const v=text(value).toLowerCase();if(v)return v;if(text(mediaUrl))return'image';if(/\(sticker\)|スタンプ/.test(text(content)))return'sticker';return'text'}
function legacyBlob(row){return !row.message_key&&!row.message_id&&!row.csv_file_name&&!row.source&&row.content.length>6000}

export function parseLineLogCsv(csvText){
  if(byteLength(csvText)>MAX_LINE_CSV_BYTES)throw new Error('line_log_csv_too_large');
  const matrix=parseCsvText(csvText);if(matrix.length<2)throw new Error('line_log_csv_has_no_rows');if(matrix.length-1>MAX_ROWS)throw new Error('line_log_row_limit_exceeded');
  const headers=matrix[0].map(text),get=buildPicker(headers,LINE_HEADERS);const rows=[],skipped=[];
  for(let i=1;i<matrix.length;i++){
    const cells=matrix[i],content=pick(cells,indexes(headers,LINE_HEADERS.content)),mediaUrl=get(cells,'media_url');
    const row={line:i+1,sent_at:normalizeSheetTimestamp(get(cells,'sent_at')),customer_id_hint:get(cells,'customer_id'),name:get(cells,'name'),content:content||mediaUrl,message_type:typeFrom(get(cells,'message_type'),content,mediaUrl),sender:get(cells,'sender'),direction:directionFrom(get(cells,'sender')),line_user_id:get(cells,'line_user_id'),source:get(cells,'source'),message_key:get(cells,'message_key'),csv_file_name:get(cells,'csv_file_name'),csv_row_no:get(cells,'csv_row_no'),imported_at:get(cells,'imported_at'),message_id:get(cells,'message_id'),media_url:mediaUrl,raw_json:get(cells,'raw_json')};
    if(!row.content&&!row.media_url){skipped.push({line:row.line,reason:'empty_message'});continue}
    if(!row.sent_at){skipped.push({line:row.line,reason:'invalid_timestamp'});continue}
    if(!row.direction){skipped.push({line:row.line,reason:'unknown_direction',sender:row.sender});continue}
    if(row.customer_id_hint&&!CUSTOMER_ID_RE.test(row.customer_id_hint)){skipped.push({line:row.line,reason:'invalid_customer_id_hint'});continue}
    if(row.line_user_id&&!LINE_ID_RE.test(row.line_user_id)){skipped.push({line:row.line,reason:'invalid_line_user_id'});continue}
    if(legacyBlob(row)){skipped.push({line:row.line,reason:'legacy_conversation_blob'});continue}
    rows.push(row);
  }
  return{headers,row_count:matrix.length-1,rows,skipped};
}

export function parseCustomerMasterCsv(csvText){
  if(!text(csvText))return{headers:[],row_count:0,entries:[],skipped:[]};
  if(byteLength(csvText)>MAX_MASTER_CSV_BYTES)throw new Error('customer_master_csv_too_large');
  const matrix=parseCsvText(csvText);if(matrix.length<2)throw new Error('customer_master_csv_has_no_rows');if(matrix.length-1>MAX_ROWS)throw new Error('customer_master_row_limit_exceeded');
  const headers=matrix[0].map(text),get=buildPicker(headers,MASTER_HEADERS);const entries=[],skipped=[];
  for(let i=1;i<matrix.length;i++){
    const cells=matrix[i],customerId=get(cells,'customer_id'),lineId=get(cells,'line_user_id'),name=get(cells,'name'),lineName=get(cells,'line_name');
    if(!customerId&&!lineId&&!name)continue;
    if(!CUSTOMER_ID_RE.test(customerId)){skipped.push({line:i+1,reason:'invalid_customer_id'});continue}
    if(lineId&&!LINE_ID_RE.test(lineId)){skipped.push({line:i+1,reason:'invalid_line_user_id'});continue}
    entries.push({line:i+1,customer_id:customerId,line_user_id:lineId,name,line_name:lineName,name_key:normalizeName(name)});
  }
  return{headers,row_count:matrix.length-1,entries,skipped};
}
function addMulti(map,key,value){if(!key)return;if(!map.has(key))map.set(key,[]);map.get(key).push(value)}
function uniqueCustomerId(items){const ids=[...new Set((items||[]).map(x=>text(x.customer_id)).filter(Boolean))];return ids.length===1?ids[0]:''}
function findMasterEntry(entries,customerId){return entries.find(x=>x.customer_id===customerId)||null}
function operatorName(name){return /^(arisa|mizuno|水野|ありさ)$/i.test(normalizeName(name))}

export async function resolveLineHistoryRows(parsed,master,dbCustomers){
  const existingById=new Map(),dbByLine=new Map(),masterByLine=new Map(),masterByName=new Map();
  for(const c of dbCustomers||[]){const id=text(c.customer_id),lineId=text(c.line_user_id);if(CUSTOMER_ID_RE.test(id))existingById.set(id,{...c,customer_id:id,line_user_id:lineId});if(LINE_ID_RE.test(lineId))addMulti(dbByLine,lineId,{...c,customer_id:id,line_user_id:lineId})}
  for(const e of master?.entries||[]){if(LINE_ID_RE.test(e.line_user_id))addMulti(masterByLine,e.line_user_id,e);if(e.name_key)addMulti(masterByName,e.name_key,e)}
  const candidates=[],skipped=[...(parsed?.skipped||[])],conflicts=[],seenKeys=new Set();
  for(const row of parsed?.rows||[]){
    let customerId='',matchSource='';const hinted=text(row.customer_id_hint);const lineId=text(row.line_user_id);
    const dbLineId=uniqueCustomerId(dbByLine.get(lineId));const masterLineId=uniqueCustomerId(masterByLine.get(lineId));
    if(hinted&&existingById.has(hinted)){customerId=hinted;matchSource='line_log_customer_id'}
    if(customerId&&dbLineId&&dbLineId!==customerId){conflicts.push({line:row.line,reason:'customer_id_line_user_conflict',customer_id:customerId,line_customer_id:dbLineId});continue}
    if(!customerId&&dbLineId){customerId=dbLineId;matchSource='crm_line_user_id'}
    if(!customerId&&masterLineId&&existingById.has(masterLineId)){customerId=masterLineId;matchSource='master_line_user_id'}
    const fileNameKey=normalizeName(stripCsvName(row.csv_file_name));
    const rowNameKey=!operatorName(row.name)?normalizeName(row.name):'';
    if(!customerId&&fileNameKey){const id=uniqueCustomerId(masterByName.get(fileNameKey));if(id&&existingById.has(id)){customerId=id;matchSource='master_csv_file_name'}}
    if(!customerId&&rowNameKey){const id=uniqueCustomerId(masterByName.get(rowNameKey));if(id&&existingById.has(id)){customerId=id;matchSource='master_exact_name'}}
    if(!customerId){skipped.push({line:row.line,reason:'customer_unmatched',line_user_id:lineId,csv_file_name:row.csv_file_name,name:row.name});continue}
    const customer=existingById.get(customerId);if(!customer){skipped.push({line:row.line,reason:'customer_not_in_crm',customer_id:customerId});continue}
    let finalLineId=lineId||text(customer.line_user_id);if(!finalLineId){const me=findMasterEntry(master?.entries||[],customerId);finalLineId=text(me?.line_user_id)}
    if(finalLineId&&!LINE_ID_RE.test(finalLineId)){skipped.push({line:row.line,reason:'resolved_line_user_invalid',customer_id:customerId});continue}
    let key=text(row.message_key)||text(row.message_id);
    if(!key)key='sheet:v1:'+await sha256Hex([customerId,finalLineId,row.direction,row.sent_at,row.message_type,row.content,row.csv_file_name,row.csv_row_no||row.line].join('\n'));
    if(seenKeys.has(key)){skipped.push({line:row.line,reason:'duplicate_input_message_key',message_key:key});continue}seenKeys.add(key);
    candidates.push({message_key:key,customer_id:customerId,line_user_id:finalLineId,direction:row.direction,message_type:row.message_type||'text',message_text:row.content,sender_name:row.name||'',sent_at:row.sent_at,match_source:matchSource,source_row:row.line,csv_file_name:row.csv_file_name,csv_row_no:row.csv_row_no,source:row.source||SOURCE,media_url:row.media_url||''});
  }
  return{candidates,skipped,conflicts};
}

async function loadCustomers(env){const r=await env.DB.prepare('SELECT customer_id,line_user_id,name,line_display_name FROM customers').all();return r?.results||[]}
async function loadExistingMessages(env){
  const r=await env.DB.prepare('SELECT message_key,customer_id,line_user_id,direction,sent_at,message_text FROM customer_line_messages').all();const rows=r?.results||[],keys=new Set(),fingerprints=new Set();
  for(const x of rows){if(text(x.message_key))keys.add(text(x.message_key));fingerprints.add([text(x.customer_id),text(x.line_user_id),text(x.direction),text(x.sent_at),text(x.message_text)].join('\u0000'))}
  return{count:rows.length,keys,fingerprints};
}
function fingerprint(x){return[text(x.customer_id),text(x.line_user_id),text(x.direction),text(x.sent_at),text(x.message_text)].join('\u0000')}
function countReasons(items){const out={};for(const x of items||[])out[x.reason]=(out[x.reason]||0)+1;return out}

export async function analyzeLineHistoryBackfill(env,lineCsv,masterCsv){
  const parsed=parseLineLogCsv(lineCsv),master=parseCustomerMasterCsv(masterCsv),customers=await loadCustomers(env),resolved=await resolveLineHistoryRows(parsed,master,customers),existing=await loadExistingMessages(env);const insertable=[],alreadyPresent=[];
  for(const c of resolved.candidates){if(existing.keys.has(c.message_key)||existing.fingerprints.has(fingerprint(c)))alreadyPresent.push(c);else insertable.push(c)}
  const matchSources={};for(const c of resolved.candidates)matchSources[c.match_source]=(matchSources[c.match_source]||0)+1;
  return{build:BUILD,line_log_rows:parsed.row_count,parsed_message_rows:parsed.rows.length,customer_master_rows:master.row_count,crm_customer_rows:customers.length,candidate_rows:resolved.candidates.length,insertable_rows:insertable.length,already_present_rows:alreadyPresent.length,skipped_rows:resolved.skipped.length,conflict_rows:resolved.conflicts.length,skipped_reasons:countReasons(resolved.skipped),match_sources:matchSources,conflicts:resolved.conflicts.slice(0,30),unmatched_sample:resolved.skipped.filter(x=>x.reason==='customer_unmatched').slice(0,30),insertable};
}
async function payloadHash(lineCsv,masterCsv){return sha256Hex(String(lineCsv||'')+'\n--CRM-MASTER-SPLIT--\n'+String(masterCsv||''))}
async function issueReceipt(env,lineCsv,masterCsv){const secret=receiptSecret(env);if(!secret)return'';const payload={v:1,build:BUILD,sha256:await payloadHash(lineCsv,masterCsv),exp:Math.floor(Date.now()/1000)+RECEIPT_SECONDS};const encoded=b64url(encoder.encode(JSON.stringify(payload))),sig=b64url(await hmac(encoded,secret));return encoded+'.'+sig}
async function verifyReceipt(env,lineCsv,masterCsv,receipt){const secret=receiptSecret(env);if(!secret)return false;const parts=text(receipt).split('.');if(parts.length!==2)return false;const [encoded,sig]=parts,expected=await hmac(encoded,secret);if(!safeEqual(fromB64url(sig),expected))return false;try{const p=JSON.parse(new TextDecoder().decode(fromB64url(encoded)));return p?.v===1&&p?.build===BUILD&&Number(p?.exp)>Math.floor(Date.now()/1000)&&p?.sha256===await payloadHash(lineCsv,masterCsv)}catch{return false}}
function compactAnalysis(a){const {insertable,...rest}=a;return rest}
function changedRows(result){return Number(result?.meta?.changes??result?.changes??0)||0}
async function insertMessages(env,rows){let inserted=0;for(let offset=0;offset<rows.length;offset+=50){const chunk=rows.slice(offset,offset+50),stmts=chunk.map(x=>env.DB.prepare(`INSERT OR IGNORE INTO customer_line_messages (message_key,customer_id,line_user_id,direction,message_type,message_text,sender_name,sent_at,raw_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))`).bind(x.message_key,x.customer_id,x.line_user_id||null,x.direction,x.message_type,x.message_text,x.sender_name||null,x.sent_at,JSON.stringify({source:SOURCE,match_source:x.match_source,source_row:x.source_row,csv_file_name:x.csv_file_name||null,csv_row_no:x.csv_row_no||null,source_kind:x.source,media_url:x.media_url||null})));if(env.DB.batch){const results=await env.DB.batch(stmts);inserted+=results.reduce((n,r)=>n+changedRows(r),0)}else{for(const s of stmts)inserted+=changedRows(await s.run())}}
  return inserted;
}

export async function handleLineHistorySheetBackfill(request,env,{authorized=false}={}){
  const url=new URL(request.url),preview=url.pathname==='/api/line-history-backfill/preview',commit=url.pathname==='/api/line-history-backfill/commit';if(!preview&&!commit)return null;
  if(request.method!=='POST')return json({ok:false,error:'method_not_allowed'},405);if(!authorized)return json({ok:false,error:'owner_auth_required'},401);if(!sameOrigin(request))return json({ok:false,error:'same_origin_required'},403);
  let body={};try{body=await request.json()}catch{return json({ok:false,error:'invalid_json'},400)}
  const lineCsv=String(body?.line_log_csv||''),masterCsv=String(body?.customer_master_csv||'');if(!lineCsv)return json({ok:false,error:'line_log_csv_required'},400);
  try{
    if(preview){const analysis=await analyzeLineHistoryBackfill(env,lineCsv,masterCsv),receipt=await issueReceipt(env,lineCsv,masterCsv);return json({ok:true,mode:'preview',...compactAnalysis(analysis),preview_receipt:receipt,receipt_expires_seconds:RECEIPT_SECONDS,production_write:false,customer_id_generation:false,customer_update:false,line_send:false,delete:false})}
    if(env?.CRM_LINE_HISTORY_BACKFILL_WRITE_ENABLED!=='1')return json({ok:false,error:'line_history_backfill_write_disabled'},403);
    if(!await verifyReceipt(env,lineCsv,masterCsv,body?.preview_receipt))return json({ok:false,error:'preview_receipt_required_or_invalid'},409);
    const analysis=await analyzeLineHistoryBackfill(env,lineCsv,masterCsv);if(analysis.conflict_rows)return json({ok:false,error:'identity_conflict',...compactAnalysis(analysis)},409);
    const inserted=await insertMessages(env,analysis.insertable);return json({ok:true,mode:'commit',inserted_rows:inserted,...compactAnalysis(analysis),customer_id_generation:false,customer_update:false,line_send:false,delete:false});
  }catch(e){return json({ok:false,error:text(e?.message||e)||'line_history_backfill_failed'},400)}
}

export function lineHistorySheetBackfillHealth(env={}){return{line_history_sheet_backfill:true,line_history_sheet_backfill_build:BUILD,line_history_sheet_backfill_preview_required:true,line_history_sheet_backfill_exact_line_user_primary:true,line_history_sheet_backfill_customer_master_fallback:true,line_history_sheet_backfill_csv_filename_fallback:true,line_history_sheet_backfill_insert_only:true,line_history_sheet_backfill_write_enabled:env?.CRM_LINE_HISTORY_BACKFILL_WRITE_ENABLED==='1',line_history_sheet_backfill_customer_id_generation:false,line_history_sheet_backfill_customer_update:false,line_history_sheet_backfill_delete:false,line_history_sheet_backfill_line_send:false}}
