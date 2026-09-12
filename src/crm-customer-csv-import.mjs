import { allocateCustomerId, jstYear } from './customer-identity-resolver.mjs';

const BUILD='crm-customer-csv-import-20260912-01';
const SOURCE='customer_csv_import';
const MAX_CSV_BYTES=2_500_000;
const MAX_ROWS=5000;
const PREVIEW_RECEIPT_SECONDS=10*60;
const CUSTOMER_ID_RE=/^\d{8}$/;
const encoder=new TextEncoder();

function text(v){return v==null?'':String(v).trim()}
function json(data,status=200){return new Response(JSON.stringify(data,null,2),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store, no-cache, must-revalidate, max-age=0'}})}
function sameOrigin(request){const origin=text(request.headers.get('origin'));if(!origin)return false;try{return new URL(origin).origin===new URL(request.url).origin}catch{return false}}
function normalizeHeader(v){return text(v).normalize('NFKC').replace(/[\s　\r\n]+/g,'').replace(/[①１]/g,'1').replace(/[②２]/g,'2').replace(/[③３]/g,'3').replace(/[（）()]/g,'').toLowerCase()}
export function normalizeImportName(v){return text(v).normalize('NFKC').toLowerCase().replace(/[\s　・･.．,，、()（）\[\]［］【】「」『』]/g,'')}
function displayName(v){return text(v).normalize('NFKC').replace(/[\s　]+/g,' ').trim()}
function toAmount(v){const n=Number(text(v).replace(/[,，円¥￥\s]/g,''));return Number.isFinite(n)?n:0}
function normalizeReservationGenre(value){
  const s=text(value);
  if(/^バースデ[イー]フォト$/.test(s))return'バースデーフォト';
  if(/^ニューボーンフォト$/.test(s))return'ニューボーン';
  if(/^マタニティフォト$/.test(s))return'マタニティ';
  if(/^ファミリーフォト$/.test(s))return'家族';
  return s;
}
function planAmountFromReservationLabel(label,memo){
  const raw=text(label);
  const explicit=raw.match(/[¥￥]\s*([\d,]{3,})/);
  if(explicit)return toAmount(explicit[1]);
  if(/\(新\).*special|special.*\(新\)/i.test(raw))return 35000;
  if(/\(新\).*normal|normal.*\(新\)/i.test(raw))return 24800;
  if(/otameshi|おためし|お試し/i.test(raw))return 15000;
  if(!raw){
    const memoAmount=text(memo).match(/^\s*[¥￥]\s*([\d,]{3,})\s*$/m);
    if(memoAmount)return toAmount(memoAmount[1]);
  }
  return 0;
}
function reservationCsvTotal(raw){
  const explicit=toAmount(raw.total_amount);
  if(explicit)return explicit;
  const plan=toAmount(raw.plan_amount)||planAmountFromReservationLabel(raw.plan_label,raw.memo);
  return plan+toAmount(raw.traffic_amount)+toAmount(raw.option1_amount)+toAmount(raw.option2_amount)+toAmount(raw.movie_amount)+toAmount(raw.other_amount)+toAmount(raw.additional_purchase);
}
function excelDate(serial){const n=Number(serial);if(!Number.isInteger(n)||n<20000||n>80000)return'';const d=new Date(Date.UTC(1899,11,30)+n*86400000);return d.toISOString().slice(0,10)}
export function normalizeImportDate(v){
  const raw=text(v).normalize('NFKC');
  if(!raw)return'';
  if(/^\d{5}$/.test(raw)){const x=excelDate(raw);if(x)return x}
  const m=raw.match(/^(20\d{2})[\/\.\-年](\d{1,2})[\/\.\-月](\d{1,2})(?:日)?/);
  if(!m)return'';
  const y=Number(m[1]),mo=Number(m[2]),d=Number(m[3]);
  const dt=new Date(Date.UTC(y,mo-1,d));
  if(dt.getUTCFullYear()!==y||dt.getUTCMonth()!==mo-1||dt.getUTCDate()!==d)return'';
  return `${String(y).padStart(4,'0')}-${String(mo).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
}

export function parseCsvText(csvText){
  const source=String(csvText||'').replace(/^\uFEFF/,'');
  const rows=[];let row=[],field='',quoted=false;
  for(let i=0;i<source.length;i++){
    const ch=source[i];
    if(quoted){
      if(ch==='"'&&source[i+1]==='"'){field+='"';i++;continue}
      if(ch==='"'){quoted=false;continue}
      field+=ch;continue;
    }
    if(ch==='"'){quoted=true;continue}
    if(ch===','){row.push(field);field='';continue}
    if(ch==='\n'){row.push(field);rows.push(row);row=[];field='';continue}
    if(ch==='\r'){if(source[i+1]==='\n')continue;row.push(field);rows.push(row);row=[];field='';continue}
    field+=ch;
  }
  if(quoted)throw new Error('csv_unclosed_quote');
  if(field!==''||row.length){row.push(field);rows.push(row)}
  return rows.filter(r=>r.some(v=>text(v)!==''));
}

const ALIASES={
  customer_id:['customerid','顧客id','顧客番号','顧客no','顧客番号id'],
  name:['name','customername','顧客名','氏名','名前','お名前'],
  furigana:['furigana','フリガナ','ふりがな','カナ','かな'],
  shoot_date:['shootdate','撮影日','撮影日付','撮影日時','日付','撮影年月日'],
  genre:['genre','ジャンル','撮影ジャンル','撮影内容'],
  phone:['phone','tel','電話','電話番号','携帯番号'],
  email:['email','mail','メール','メールアドレス'],
  address:['address','住所'],
  total_amount:['totalamount','amount','price','料金','金額','売上','総額','合計金額','撮影料金'],
  plan_label:['planlabel','撮影プラン','プラン'],
  plan_amount:['planamount','単価','プラン金額'],
  traffic_amount:['trafficamount','交通費'],
  option1_amount:['option1amount','オプション1単価','オプション①単価'],
  option2_amount:['option2amount','オプション2単価','オプション②単価'],
  movie_amount:['movie','movieamount'],
  other_amount:['otheramount','その他費用'],
  additional_purchase:['additionalpurchase','追加購入'],
  shoot_place:['shootplace','撮影場所','場所'],
  memo:['memo','note','備考','メモ']
};
const HEADER_KEY=new Map(Object.entries(ALIASES).flatMap(([key,values])=>values.map(v=>[normalizeHeader(v),key])));

export function findReservationCsvHeaderRow(rows){
  for(let i=0;i<Math.min(rows.length,50);i++){
    const map=new Set((rows[i]||[]).map(normalizeHeader));
    const hasName=['名前','顧客名','customer_name'].some(x=>map.has(normalizeHeader(x)));
    const hasDate=['撮影日','日付','shoot_date'].some(x=>map.has(normalizeHeader(x)));
    const hasPlace=['撮影場所','場所','shoot_place'].some(x=>map.has(normalizeHeader(x)));
    if(hasName&&hasDate&&hasPlace)return i;
  }
  return rows.length>=14?13:0;
}

function rowObject(headers,cells){
  const out={};
  for(let i=0;i<headers.length;i++){const key=HEADER_KEY.get(normalizeHeader(headers[i]));if(key&&out[key]===undefined)out[key]=text(cells[i])}
  return out;
}
function mergeSameShoot(a,b){
  const out={...a};
  for(const key of ['customer_id','name','furigana','genre','phone','email','address','memo'])if(!text(out[key])&&text(b[key]))out[key]=b[key];
  out.total_amount=Math.max(toAmount(a.total_amount),toAmount(b.total_amount));
  out._source_rows=[...(a._source_rows||[]),...(b._source_rows||[])];
  return out;
}

export function analyzeCsvImport(csvText){
  if(new TextEncoder().encode(String(csvText||'')).byteLength>MAX_CSV_BYTES)throw new Error('csv_too_large');
  const matrix=parseCsvText(csvText);
  if(matrix.length<2)throw new Error('csv_has_no_data_rows');
  const headerIndex=findReservationCsvHeaderRow(matrix);
  const dataRows=matrix.slice(headerIndex+1);
  if(dataRows.length>MAX_ROWS)throw new Error('csv_row_limit_exceeded');
  const headers=(matrix[headerIndex]||[]).map(text);
  const mappedHeaders=new Set(headers.map(h=>HEADER_KEY.get(normalizeHeader(h))).filter(Boolean));
  if(!mappedHeaders.has('name'))throw new Error('csv_name_column_required');
  if(!mappedHeaders.has('shoot_date'))throw new Error('csv_shoot_date_column_required');

  const groups=new Map(),errors=[],warnings=[];
  let duplicateRows=0,validRows=0;
  for(let offset=0;offset<dataRows.length;offset++){
    const raw=rowObject(headers,dataRows[offset]),line=headerIndex+offset+2;
    const name=displayName(raw.name),nameKey=normalizeImportName(name);
    const originalDate=text(raw.shoot_date);
    if(!nameKey&&!originalDate)continue;
    if(!nameKey){errors.push({line,error:'customer_name_required'});continue}
    if(!originalDate){errors.push({line,error:'shoot_date_required'});continue}
    const shootDate=normalizeImportDate(originalDate);
    if(!shootDate){errors.push({line,error:'invalid_shoot_date',value:originalDate});continue}
    const customerId=text(raw.customer_id);
    if(customerId&&!CUSTOMER_ID_RE.test(customerId)){errors.push({line,error:'invalid_customer_id',value:customerId});continue}

    validRows++;
    let group=groups.get(nameKey);
    if(!group){
      group={name_key:nameKey,name,customer_ids:new Set(),shoots:new Map(),customer_only_rows:[],first_row:line};
      groups.set(nameKey,group);
    }
    if(customerId)group.customer_ids.add(customerId);
    const normalized={...raw,name,shoot_date:shootDate,genre:normalizeReservationGenre(raw.genre),total_amount:reservationCsvTotal(raw),_source_rows:[line]};
    if(shootDate){
      if(group.shoots.has(shootDate)){duplicateRows++;group.shoots.set(shootDate,mergeSameShoot(group.shoots.get(shootDate),normalized))}
      else group.shoots.set(shootDate,normalized);
    }
  }

  const resultGroups=[];
  let repeaters=0;
  for(const group of groups.values()){
    const ids=[...group.customer_ids];
    if(ids.length>1)errors.push({line:group.first_row,error:'conflicting_customer_ids_for_same_name',name:group.name,customer_ids:ids});
    const shoots=[...group.shoots.values()].sort((a,b)=>a.shoot_date.localeCompare(b.shoot_date));
    if(shoots.length>=2)repeaters++;
    const allRows=[...shoots,...group.customer_only_rows];
    const firstValue=key=>text(allRows.find(x=>text(x[key]))?.[key]);
    resultGroups.push({
      name_key:group.name_key,
      name:group.name,
      csv_customer_id:ids[0]||'',
      shoot_count:shoots.length,
      shoot_dates:shoots.map(x=>x.shoot_date),
      is_repeater:shoots.length>=2,
      duplicate_same_day_rows:shoots.reduce((n,x)=>n+Math.max(0,(x._source_rows||[]).length-1),0),
      furigana:firstValue('furigana'),
      phone:firstValue('phone'),
      email:firstValue('email'),
      address:firstValue('address'),
      memo:firstValue('memo'),
      shoots,
      customer_only:shoots.length===0
    });
  }
  resultGroups.sort((a,b)=>a.name.localeCompare(b.name,'ja-JP'));


  return{
    headers,
    header_row:headerIndex+1,
    row_count:dataRows.length,
    valid_row_count:validRows,
    customer_count:resultGroups.length,
    duplicate_same_day_rows:duplicateRows,
    repeater_count:repeaters,
    errors,
    warnings,
    groups:resultGroups
  };
}

async function all(db,sql,...params){let q=db.prepare(sql);if(params.length)q=q.bind(...params);const r=await q.all();return r?.results||[]}
async function first(db,sql,...params){let q=db.prepare(sql);if(params.length)q=q.bind(...params);return await q.first()}
async function run(db,sql,...params){let q=db.prepare(sql);if(params.length)q=q.bind(...params);return await q.run()}
async function sha256Hex(value){const data=encoder.encode(String(value));const out=new Uint8Array(await crypto.subtle.digest('SHA-256',data));return [...out].map(x=>x.toString(16).padStart(2,'0')).join('')}
function b64url(bytes){let out='';for(const b of bytes)out+=String.fromCharCode(b);return btoa(out).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'')}
function fromB64url(value){const raw=String(value||'').replace(/-/g,'+').replace(/_/g,'/');const padded=raw+'='.repeat((4-raw.length%4)%4);try{const s=atob(padded),out=new Uint8Array(s.length);for(let i=0;i<s.length;i++)out[i]=s.charCodeAt(i);return out}catch{return new Uint8Array()}}
function safeEqual(a,b){if(a.length!==b.length)return false;let diff=0;for(let i=0;i<a.length;i++)diff|=a[i]^b[i];return diff===0}
async function hmac(value,secret){const key=await crypto.subtle.importKey('raw',encoder.encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);return new Uint8Array(await crypto.subtle.sign('HMAC',key,encoder.encode(value)))}
async function issuePreviewReceipt(env,csvText){
  const secret=text(env?.CRM_OWNER_SESSION_SECRET);if(!secret)return'';
  const payload={v:1,build:BUILD,csv_sha256:await sha256Hex(csvText),exp:Math.floor(Date.now()/1000)+PREVIEW_RECEIPT_SECONDS};
  const encoded=b64url(encoder.encode(JSON.stringify(payload))),sig=b64url(await hmac(encoded,secret));
  return encoded+'.'+sig;
}
async function verifyPreviewReceipt(env,csvText,receipt){
  const secret=text(env?.CRM_OWNER_SESSION_SECRET);if(!secret)return false;
  const parts=text(receipt).split('.');if(parts.length!==2)return false;
  const [encoded,sig]=parts;
  const expected=await hmac(encoded,secret);if(!safeEqual(fromB64url(sig),expected))return false;
  try{
    const payload=JSON.parse(new TextDecoder().decode(fromB64url(encoded)));
    if(payload?.v!==1||payload?.build!==BUILD||Number(payload?.exp)<=Math.floor(Date.now()/1000))return false;
    return payload.csv_sha256===await sha256Hex(csvText);
  }catch{return false}
}
async function importEventKey(nameKey,shootDate){return 'csv:v1:'+await sha256Hex(nameKey+'\n'+shootDate)}
function changedRows(r){return Number(r?.meta?.changes??r?.changes??r?.rowsAffected??0)}

async function customerById(db,id){return first(db,`SELECT customer_id,name FROM customers WHERE customer_id=? LIMIT 1`,id)}
async function existingNameCandidates(db){
  const rows=await all(db,`SELECT customer_id,name FROM customers WHERE customer_id IS NOT NULL AND trim(customer_id)<>'' LIMIT 20000`);
  const map=new Map();
  for(const row of rows){
    const key=normalizeImportName(row.name);if(!key)continue;
    if(!map.has(key))map.set(key,[]);
    map.get(key).push({customer_id:text(row.customer_id),name:text(row.name)});
  }
  return map;
}
async function priorCsvMappings(db){
  const rows=await all(db,`SELECT customer_id,customer_name FROM customer_reservations WHERE source=? AND customer_id IS NOT NULL AND trim(customer_id)<>'' LIMIT 20000`,SOURCE);
  const map=new Map();
  for(const row of rows){
    const key=normalizeImportName(row.customer_name);if(!key)continue;
    if(!map.has(key))map.set(key,new Set());
    map.get(key).add(text(row.customer_id));
  }
  return map;
}
async function enrichPreview(env,analysis){
  if(!env?.DB?.prepare)return analysis;
  const candidates=await existingNameCandidates(env.DB).catch(()=>new Map());
  const prior=await priorCsvMappings(env.DB).catch(()=>new Map());
  return{
    ...analysis,
    groups:analysis.groups.map(g=>{
      const priorIds=[...(prior.get(g.name_key)||[])].filter(Boolean);
      return{
        ...g,
        existing_candidates:candidates.get(g.name_key)||[],
        prior_csv_customer_ids:priorIds,
        prior_csv_mapping_ambiguous:priorIds.length>1
      };
    })
  };
}
function pickMetadata(group){
  const rows=[...group.shoots];
  const firstShoot=rows[0]||{};
  return{
    name:group.name,
    furigana:text(group.furigana||firstShoot.furigana),
    phone:text(group.phone||firstShoot.phone),
    email:text(group.email||firstShoot.email),
    address:text(group.address||firstShoot.address),
    memo:text(group.memo||firstShoot.memo)
  };
}
async function createCustomer(db,group){
  const allocation=await allocateCustomerId(db,jstYear());
  if(!allocation?.ok){const e=new Error(allocation?.error||'customer_id_allocation_failed');e.statusCode=allocation?.statusCode||409;throw e}
  const id=allocation.customer_id,meta=pickMetadata(group);
  await run(db,`INSERT INTO customers (customer_id,name,furigana,phone,address,email,memo,repeat_count,created_at,updated_at) VALUES (?,?,?,?,?,?,?,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`,
    id,meta.name,meta.furigana||null,meta.phone||null,meta.address||null,meta.email||null,meta.memo||null);
  return id;
}
async function fillBlankCustomerMetadata(db,customerId,group){
  const m=pickMetadata(group);
  await run(db,`UPDATE customers SET
    name=CASE WHEN trim(COALESCE(name,''))='' THEN ? ELSE name END,
    furigana=CASE WHEN trim(COALESCE(furigana,''))='' THEN ? ELSE furigana END,
    phone=CASE WHEN trim(COALESCE(phone,''))='' THEN ? ELSE phone END,
    email=CASE WHEN trim(COALESCE(email,''))='' THEN ? ELSE email END,
    address=CASE WHEN trim(COALESCE(address,''))='' THEN ? ELSE address END,
    memo=CASE WHEN trim(COALESCE(memo,''))='' THEN ? ELSE memo END,
    updated_at=CURRENT_TIMESTAMP
    WHERE customer_id=?`,
    m.name,m.furigana||null,m.phone||null,m.email||null,m.address||null,m.memo||null,customerId);
}
async function recalcRepeatStats(db,customerId){
  const [rows,current]=await Promise.all([
    all(db,`SELECT shoot_date,status FROM customer_reservations WHERE customer_id=? AND shoot_date IS NOT NULL AND trim(shoot_date)<>''`,customerId),
    first(db,`SELECT repeat_count,first_shoot_date,last_shoot_date FROM customers WHERE customer_id=? LIMIT 1`,customerId)
  ]);
  const dates=[...new Set(rows.filter(r=>!/(?:cancel|キャンセル)/i.test(text(r.status))).map(r=>normalizeImportDate(r.shoot_date)).filter(Boolean))].sort();
  const knownCount=Math.max(0,Number(current?.repeat_count||0)||0);
  const count=Math.max(knownCount,dates.length);
  const firstCandidates=[normalizeImportDate(current?.first_shoot_date),dates[0]||''].filter(Boolean).sort();
  const lastCandidates=[normalizeImportDate(current?.last_shoot_date),dates.at(-1)||''].filter(Boolean).sort();
  const firstShoot=firstCandidates[0]||null,lastShoot=lastCandidates.at(-1)||null;
  await run(db,`UPDATE customers SET repeat_count=?,first_shoot_date=?,last_shoot_date=?,updated_at=CURRENT_TIMESTAMP WHERE customer_id=?`,
    count,firstShoot,lastShoot,customerId);
  return{repeat_count:count,is_repeater:count>=2,first_shoot_date:firstShoot||'',last_shoot_date:lastShoot||''};
}
async function insertShoot(db,customerId,group,shoot){
  const eventKey=await importEventKey(group.name_key,shoot.shoot_date);
  const sameDate=await first(db,`SELECT event_key,source,customer_id FROM customer_reservations WHERE customer_id=? AND shoot_date=? LIMIT 1`,customerId,shoot.shoot_date);
  if(sameDate){
    if(text(sameDate.source)===SOURCE&&text(sameDate.event_key)===eventKey){
      const rawJson=JSON.stringify({import_build:BUILD,name_key:group.name_key,source_rows:shoot._source_rows||[],dedupe_rule:'same_normalized_name+same_shoot_date',raw:shoot});
      await run(db,`UPDATE customer_reservations SET customer_name=?,genre=COALESCE(NULLIF(?,''),genre),total_amount=CASE WHEN ?>COALESCE(total_amount,0) THEN ? ELSE total_amount END,status=COALESCE(NULLIF(status,''),'CSV取込'),raw_json=?,updated_at=CURRENT_TIMESTAMP WHERE event_key=? AND customer_id=?`,
        group.name,text(shoot.genre),toAmount(shoot.total_amount),toAmount(shoot.total_amount),rawJson,eventKey,customerId);
    }
    return{created:false,event_key:text(sameDate.event_key),deduped_by:'customer_id+shoot_date'};
  }
  const existing=await first(db,`SELECT customer_id FROM customer_reservations WHERE event_key=? LIMIT 1`,eventKey);
  if(existing&&text(existing.customer_id)!==customerId){
    const e=new Error('same_name_same_date_already_linked_to_different_customer');e.statusCode=409;e.event_key=eventKey;throw e;
  }
  const reservationId='CSV-'+(await sha256Hex(group.name_key+'\n'+shoot.shoot_date)).slice(0,20).toUpperCase();
  const rawJson=JSON.stringify({import_build:BUILD,name_key:group.name_key,source_rows:shoot._source_rows||[],dedupe_rule:'same_normalized_name+same_shoot_date',raw:shoot});
  if(existing)return{created:false,event_key:eventKey,deduped_by:'event_key'};
  await run(db,`INSERT INTO customer_reservations (event_key,reservation_id,customer_id,customer_name,genre,shoot_date,total_amount,status,source,raw_json,created_at,updated_at)
    VALUES (?,?,?,?,?,?,?,'CSV取込',?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`,
    eventKey,reservationId,customerId,group.name,text(shoot.genre)||null,shoot.shoot_date,toAmount(shoot.total_amount),SOURCE,rawJson);
  return{created:true,event_key:eventKey};
}
async function resolveGroupCustomerId(db,group,mappings,prior){
  if(group.csv_customer_id){
    const row=await customerById(db,group.csv_customer_id);
    if(!row){const e=new Error('csv_customer_id_not_found');e.statusCode=409;throw e}
    return{customer_id:group.csv_customer_id,resolution:'csv_customer_id'};
  }
  const mapped=text(mappings?.[group.name_key]);
  if(mapped){
    if(!CUSTOMER_ID_RE.test(mapped)){const e=new Error('mapped_customer_id_invalid');e.statusCode=400;throw e}
    const row=await customerById(db,mapped);
    if(!row){const e=new Error('mapped_customer_id_not_found');e.statusCode=409;throw e}
    return{customer_id:mapped,resolution:'owner_selected_existing'};
  }
  const priorIds=[...(prior.get(group.name_key)||[])].filter(Boolean);
  if(priorIds.length===1){
    const row=await customerById(db,priorIds[0]);
    if(row)return{customer_id:priorIds[0],resolution:'prior_csv_import'};
  }
  if(priorIds.length>1){const e=new Error('prior_csv_mapping_ambiguous');e.statusCode=409;throw e}
  return{customer_id:await createCustomer(db,group),resolution:'new_customer'};
}

async function commitImport(env,analysis,mappings={}){
  if(!env?.DB?.prepare)throw new Error('db_binding_missing');
  if(analysis.errors.length){const e=new Error('csv_validation_failed');e.statusCode=400;throw e}
  const prior=await priorCsvMappings(env.DB);
  const results=[];let createdCustomers=0,createdShoots=0,reusedShoots=0;const repeaterIds=new Set();
  for(const group of analysis.groups){
    try{
      const resolved=await resolveGroupCustomerId(env.DB,group,mappings,prior);
      if(resolved.resolution==='new_customer')createdCustomers++;
      await fillBlankCustomerMetadata(env.DB,resolved.customer_id,group);
      for(const shoot of group.shoots){
        const out=await insertShoot(env.DB,resolved.customer_id,group,shoot);
        if(out.created)createdShoots++;else reusedShoots++;
      }
      const stats=await recalcRepeatStats(env.DB,resolved.customer_id);
      if(stats.is_repeater)repeaterIds.add(resolved.customer_id);
      results.push({ok:true,name:group.name,name_key:group.name_key,customer_id:resolved.customer_id,resolution:resolved.resolution,shoot_count:group.shoot_count,...stats});
    }catch(error){
      results.push({ok:false,name:group.name,name_key:group.name_key,error:String(error?.message||error),statusCode:Number(error?.statusCode||500)});
    }
  }
  const failed=results.filter(x=>!x.ok);
  return{
    ok:failed.length===0,
    partial:failed.length>0&&failed.length<results.length,
    build:BUILD,
    source:SOURCE,
    customers_total:analysis.groups.length,
    customers_created:createdCustomers,
    shoots_created:createdShoots,
    shoots_reused:reusedShoots,
    duplicate_same_day_rows:analysis.duplicate_same_day_rows,
    repeater_customers:repeaterIds.size,
    failed_count:failed.length,
    results
  };
}

export async function handleCustomerCsvImport(request,env,{authorized=false}={}){
  const url=new URL(request.url);
  if(url.pathname!=='/api/customer-csv-import/preview'&&url.pathname!=='/api/customer-csv-import/commit')return null;
  if(request.method!=='POST')return json({ok:false,error:'method_not_allowed'},405);
  if(!authorized)return json({ok:false,error:'owner_auth_required'},401);
  if(!sameOrigin(request))return json({ok:false,error:'origin_mismatch'},403);
  const len=Number(request.headers.get('content-length')||0);if(len>MAX_CSV_BYTES*2)return json({ok:false,error:'request_too_large'},413);
  let body;try{body=await request.json()}catch{return json({ok:false,error:'invalid_json'},400)}
  const csvText=String(body?.csv_text||'');
  let analysis;try{analysis=analyzeCsvImport(csvText)}catch(error){return json({ok:false,error:String(error?.message||error)},400)}
  if(url.pathname.endsWith('/preview')){
    const preview=await enrichPreview(env,analysis);
    if(preview.errors.length)return json({ok:false,build:BUILD,source:SOURCE,...preview},400);
    const previewReceipt=await issuePreviewReceipt(env,csvText);
    if(!previewReceipt)return json({ok:false,error:'preview_receipt_unavailable'},503);
    return json({ok:true,build:BUILD,source:SOURCE,preview_receipt:previewReceipt,preview_receipt_expires_in:PREVIEW_RECEIPT_SECONDS,...preview});
  }
  if(!(await verifyPreviewReceipt(env,csvText,body?.preview_receipt)))return json({ok:false,error:'valid_preview_receipt_required'},409);
  const mappings=body?.mappings&&typeof body.mappings==='object'?body.mappings:{};
  try{
    const result=await commitImport(env,analysis,mappings);
    return json(result,result.ok?200:207);
  }catch(error){
    return json({ok:false,error:String(error?.message||error)},Number(error?.statusCode||500));
  }
}

export function customerCsvImportHealth(){
  return{
    customer_csv_import:true,
    customer_csv_import_build:BUILD,
    customer_csv_import_owner_only:true,
    customer_csv_import_preview_before_commit:true,
    customer_csv_import_preview_receipt_required:true,
    customer_csv_import_preview_receipt_seconds:PREVIEW_RECEIPT_SECONDS,
    customer_csv_import_name_match:'reservation_csv_normalized_name_exact_grouping',
    customer_csv_import_reservation_csv_compatible:true,
    customer_csv_import_reservation_csv_header_scan_rows:50,
    customer_csv_import_shoot_date_required:true,
    customer_csv_import_reservation_csv_amount_rules:true,
    customer_csv_import_reservation_csv_genre_rules:true,
    customer_csv_import_same_day_dedupe:true,
    customer_csv_import_repeat_rule:'same_name_distinct_shoot_dates>=2',
    customer_csv_import_existing_customer_auto_name_merge:false,
    customer_csv_import_customer_id_owner:'customer-crm',
    customer_csv_import_customer_id_sequence:'canonical_customer_id',
    customer_csv_import_line_send:false,
    customer_csv_import_fuzzy_match:false,
    customer_csv_import_max_rows:MAX_ROWS
  };
}

export function injectCustomerCsvImport(html){
  const source=String(html||'');
  if(!source||source.includes('crm-customer-csv-import-script'))return source;
  const style=`<style id="crm-customer-csv-import-style">
#crmCsvImportOpen{position:fixed;right:16px;bottom:88px;z-index:2147481200;border:0;border-radius:999px;background:#0f172a;color:#fff;padding:11px 15px;font:800 13px/1 -apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans JP",sans-serif;box-shadow:0 10px 28px rgba(15,23,42,.24)}
.crm-csv-sheet{position:fixed;inset:0;z-index:2147483000;display:none;background:rgba(15,23,42,.42);align-items:flex-end;justify-content:center}.crm-csv-sheet.open{display:flex}.crm-csv-panel{width:min(760px,100%);max-height:92dvh;overflow:auto;background:#fff;border-radius:24px 24px 0 0;padding:18px 18px calc(24px + env(safe-area-inset-bottom));font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans JP",sans-serif;color:#0f172a}.crm-csv-head{display:flex;align-items:center;justify-content:space-between;gap:12px}.crm-csv-head h2{margin:0;font-size:21px}.crm-csv-close{border:0;background:#f1f5f9;border-radius:999px;width:40px;height:40px;font-size:20px}.crm-csv-box{margin-top:14px;border:1px solid #e2e8f0;border-radius:16px;padding:13px}.crm-csv-file{width:100%;font-size:16px}.crm-csv-encoding{width:100%;min-height:42px;border:1px solid #cbd5e1;border-radius:11px;background:#fff;padding:0 10px;margin-top:9px}.crm-csv-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}.crm-csv-btn{border:0;border-radius:13px;min-height:44px;padding:0 14px;font-weight:850;background:#0f172a;color:#fff}.crm-csv-btn.secondary{background:#eef2f7;color:#0f172a}.crm-csv-btn:disabled{opacity:.45}.crm-csv-summary{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:12px}.crm-csv-stat{background:#f8fafc;border-radius:13px;padding:10px}.crm-csv-stat span{display:block;color:#64748b;font-size:11px;font-weight:750}.crm-csv-stat b{font-size:18px}.crm-csv-group{border-top:1px solid #e2e8f0;padding:10px 0}.crm-csv-group:first-child{border-top:0}.crm-csv-name{font-weight:900}.crm-csv-meta{color:#64748b;font-size:12px;margin-top:3px}.crm-csv-select{margin-top:7px;width:100%;min-height:42px;border:1px solid #cbd5e1;border-radius:11px;background:#fff;padding:0 10px}.crm-csv-repeat{color:#047857;font-weight:900}.crm-csv-warn{background:#fffbeb;border:1px solid #fde68a;border-radius:12px;padding:9px;margin-top:8px;color:#92400e;font-size:12px}.crm-csv-error{background:#fff1f2;border:1px solid #fecdd3;border-radius:12px;padding:9px;margin-top:8px;color:#9f1239;font-size:12px}@media(min-width:768px){.crm-csv-sheet{align-items:center;padding:24px}.crm-csv-panel{border-radius:24px;max-height:88vh}.crm-csv-summary{grid-template-columns:repeat(5,minmax(0,1fr))}}</style>`;
  const script=`<script id="crm-customer-csv-import-script">(()=>{if(window.__crmCustomerCsvImport)return;window.__crmCustomerCsvImport=1;
let csvText='',preview=null,csvBuffer=null,previewReceipt='';
function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function sheet(){return document.getElementById('crmCsvSheet')}
function status(html){const el=document.getElementById('crmCsvStatus');if(el)el.innerHTML=html}
function open(){sheet()?.classList.add('open')}function close(){sheet()?.classList.remove('open')}
function decodeCsv(){if(!csvBuffer)return'';const enc=document.getElementById('crmCsvEncoding')?.value||'utf-8';try{return new TextDecoder(enc).decode(csvBuffer)}catch(_){return new TextDecoder('utf-8').decode(csvBuffer)}}
async function api(path,body){const r=await fetch(path,{method:'POST',credentials:'same-origin',headers:{'content-type':'application/json'},body:JSON.stringify(body)});const j=await r.json().catch(()=>({ok:false,error:'invalid_response'}));if(!r.ok&&r.status!==207)throw Object.assign(new Error(j.error||'request_failed'),{data:j});return j}
function renderPreview(p){preview=p;previewReceipt=p.preview_receipt||'';const summary=document.getElementById('crmCsvSummary');summary.innerHTML='<div class="crm-csv-summary">'+[
['CSV行',p.row_count],['顧客',p.customer_count],['同日重複除外',p.duplicate_same_day_rows],['リピーター',p.repeater_count],['エラー',p.errors?.length||0]
].map(x=>'<div class="crm-csv-stat"><span>'+x[0]+'</span><b>'+x[1]+'</b></div>').join('')+'</div>';
const box=document.getElementById('crmCsvGroups');box.innerHTML=(p.groups||[]).map(g=>{const candidates=g.existing_candidates||[];const opts=['<option value="">新規顧客として取り込む</option>'].concat(candidates.map(c=>'<option value="'+esc(c.customer_id)+'">既存：'+esc(c.name)+' / '+esc(c.customer_id)+'</option>')).join('');return '<div class="crm-csv-group" data-name-key="'+esc(g.name_key)+'"><div class="crm-csv-name">'+esc(g.name)+(g.is_repeater?' <span class="crm-csv-repeat">リピーター</span>':'')+'</div><div class="crm-csv-meta">撮影 '+g.shoot_count+'回'+(g.shoot_dates?.length?' / '+g.shoot_dates.map(esc).join('・'):' / 撮影日なし')+(g.duplicate_same_day_rows?' / 同日重複 '+g.duplicate_same_day_rows+'行除外':'')+'</div>'+(g.csv_customer_id?'<div class="crm-csv-meta">CSV Customer ID：'+esc(g.csv_customer_id)+'</div>':(candidates.length?'<select class="crm-csv-select" data-map-key="'+esc(g.name_key)+'">'+opts+'</select>':''))+(g.prior_csv_mapping_ambiguous?'<div class="crm-csv-warn">過去CSV取込のCustomer IDが複数あります。既存顧客を選択してください。</div>':'')+'</div>'}).join('');
let msg='プレビュー完了。内容を確認してから「取込を確定」を押してください。';if(p.errors?.length)msg='<div class="crm-csv-error">'+p.errors.slice(0,8).map(e=>'行 '+esc(e.line||'-')+'：'+esc(e.error)).join('<br>')+'</div>';status(msg);document.getElementById('crmCsvCommit').disabled=!!(p.errors?.length)}
async function previewCsv(){csvText=decodeCsv();if(!csvText){status('<div class="crm-csv-warn">CSVファイルを選択してください。</div>');return}status('確認中…');try{renderPreview(await api('/api/customer-csv-import/preview',{csv_text:csvText}))}catch(e){status('<div class="crm-csv-error">'+esc(e.message)+'</div>')}}
async function commitCsv(){if(!preview||!csvText||!previewReceipt)return;const mappings={};document.querySelectorAll('[data-map-key]').forEach(el=>{if(el.value)mappings[el.dataset.mapKey]=el.value});const btn=document.getElementById('crmCsvCommit');btn.disabled=true;status('取り込み中…');try{const r=await api('/api/customer-csv-import/commit',{csv_text:csvText,preview_receipt:previewReceipt,mappings});const failures=(r.results||[]).filter(x=>!x.ok);status((r.ok?'<div class="crm-csv-warn" style="background:#ecfdf5;border-color:#a7f3d0;color:#065f46">取込完了：顧客 '+r.customers_total+'人 / 新規 '+r.customers_created+'人 / 撮影 '+r.shoots_created+'件 / リピーター '+r.repeater_customers+'人</div>':'<div class="crm-csv-error">一部取込に失敗しました：'+failures.map(x=>esc(x.name)+' '+esc(x.error)).join('<br>')+'</div>'));if(r.ok){window.dispatchEvent(new CustomEvent('crm-customers-changed'));setTimeout(()=>location.reload(),900)}}catch(e){status('<div class="crm-csv-error">'+esc(e.message)+'</div>');btn.disabled=false}}
function boot(){if(document.getElementById('crmCsvSheet'))return;const openBtn=document.createElement('button');openBtn.id='crmCsvImportOpen';openBtn.type='button';openBtn.textContent='予約CSV取込';openBtn.onclick=open;document.body.appendChild(openBtn);const modal=document.createElement('div');modal.id='crmCsvSheet';modal.className='crm-csv-sheet';modal.innerHTML='<div class="crm-csv-panel"><div class="crm-csv-head"><h2>予約CSVから顧客取込</h2><button class="crm-csv-close" type="button">×</button></div><div class="crm-csv-box"><b>CSVファイル</b><div class="crm-csv-meta">予約管理アプリと同じ予約CSVをそのまま選べます。同じ顧客名＋同じ撮影日は重複として1回、撮影日が違えばリピーターとして集計します。</div><input id="crmCsvFile" class="crm-csv-file" type="file" accept=".csv,text/csv"><select id="crmCsvEncoding" class="crm-csv-encoding"><option value="utf-8">UTF-8</option><option value="shift_jis">Shift_JIS（Excel等）</option></select><div class="crm-csv-actions"><button id="crmCsvPreview" class="crm-csv-btn secondary" type="button">内容を確認</button><button id="crmCsvCommit" class="crm-csv-btn" type="button" disabled>取込を確定</button></div><div id="crmCsvStatus" class="crm-csv-meta" style="margin-top:10px"></div><div id="crmCsvSummary"></div><div id="crmCsvGroups" style="margin-top:10px"></div></div></div>';modal.querySelector('.crm-csv-close').onclick=close;modal.addEventListener('click',e=>{if(e.target===modal)close()});document.body.appendChild(modal);document.getElementById('crmCsvFile').addEventListener('change',async e=>{const file=e.target.files?.[0];csvBuffer=file?await file.arrayBuffer():null;csvText='';preview=null;previewReceipt='';document.getElementById('crmCsvCommit').disabled=true;status(file?'選択：'+esc(file.name):'')});document.getElementById('crmCsvEncoding').addEventListener('change',()=>{if(csvBuffer){csvText=decodeCsv();preview=null;previewReceipt='';document.getElementById('crmCsvCommit').disabled=true;status('文字コードを変更しました。もう一度「内容を確認」を押してください。')}});document.getElementById('crmCsvPreview').onclick=previewCsv;document.getElementById('crmCsvCommit').onclick=commitCsv}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot);else boot();
})();<\/script>`;
  return source.includes('</head>')?source.replace('</head>',style+'</head>').replace('</body>',script+'</body>'):source+style+script;
}
