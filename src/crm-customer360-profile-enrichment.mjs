const BUILD='crm-customer360-profile-enrichment-20260903-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const LEAD_STATUS=new Set(['inquiry','scheduling','quoted','booked','completed','lost','cancelled']);
const LOST_REASON=new Set(['schedule_mismatch','price','competitor','no_response','postponed','other','unknown']);
const PUBLICATION=new Set(['unknown','allowed','partial','denied']);
const MARKETING_PERMISSION=new Set(['unknown','allowed','denied']);
const CORE_EDITABLE=new Set(['name','furigana','line_display_name','phone','address','email','memo']);
const EXT_EDITABLE=new Set(['wedding_anniversary','first_inquiry_at','last_contact_at','lead_status','lost_reason','referrer_customer_id','referrer_name','nps_score','nps_answered_at','nps_comment','publication_permission','marketing_contact_permission','notes']);
const COMPLETED_STATUSES=new Set(['撮影完了','completed','complete','done']);
const CANCEL_STATUSES=new Set(['キャンセル','cancelled','canceled']);

const text=v=>v==null?'':String(v).trim();
const dateOnly=v=>{const m=text(v).match(/^(\d{4}-\d{2}-\d{2})/);return m?m[1]:''};
const num=(v,f=0)=>{const n=Number(String(v??'').replace(/[,円¥\s]/g,''));return Number.isFinite(n)?n:f};
function json(data,status=200){return new Response(JSON.stringify(data),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-crm-customer360-profile-build':BUILD,'x-robots-tag':'noindex, nofollow','referrer-policy':'no-referrer'}})}
function accessEmail(request){return text(request.headers.get('cf-access-authenticated-user-email')||request.headers.get('Cf-Access-Authenticated-User-Email')||request.headers.get('cf-access-user-email'))}
function bearer(request){const h=text(request.headers.get('authorization'));return /^Bearer\s+/i.test(h)?h.replace(/^Bearer\s+/i,'').trim():''}
function internalAllowed(request,env){const supplied=text(request.headers.get('x-internal-token')||bearer(request)),expected=text(env?.CRM_INTERNAL_TOKEN);return !!expected&&supplied===expected}
function ownerAllowed(request,env){return env?.CRM_LOCAL_TEST_AUTH==='1'||!!accessEmail(request)}
async function all(env,sql,params=[]){let q=env.DB.prepare(sql);if(params.length)q=q.bind(...params);const r=await q.all();return r.results||[]}
async function first(env,sql,params=[]){let q=env.DB.prepare(sql);if(params.length)q=q.bind(...params);return (await q.first())||null}
async function tableExists(env,name){return !!(await first(env,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",[name]))}
async function columns(env,name){if(!(await tableExists(env,name)))return new Set();return new Set((await all(env,`PRAGMA table_info(${name})`)).map(x=>text(x.name)))}
function clipEvidence(v){const s=text(v).replace(/\s+/g,' ');return s.length<=120?s:s.slice(0,117)+'…'}
function normalizedStatus(v){return text(v).toLowerCase()}

async function exactCustomer(env,id){
  if(!CUSTOMER_ID_RE.test(id))return null;
  const c=await first(env,"SELECT * FROM customers WHERE CAST(customer_id AS TEXT)=? AND COALESCE(deleted_at,'')='' LIMIT 1",[id]);
  return c&&text(c.customer_id)===id?c:null;
}

async function extension(env,id){
  if(!(await tableExists(env,'customer_profile_enrichment')))return{};
  return (await first(env,'SELECT * FROM customer_profile_enrichment WHERE customer_id=? LIMIT 1',[id]))||{};
}

async function family(env,id){
  if(!(await tableExists(env,'customer_family_members')))return[];
  const meta=(await tableExists(env,'customer_family_member_metadata'))?await all(env,'SELECT member_id,birth_order FROM customer_family_member_metadata WHERE customer_id=?',[id]):[];
  const byId=new Map(meta.map(x=>[text(x.member_id),x.birth_order==null?null:Number(x.birth_order)]));
  const rows=await all(env,"SELECT * FROM customer_family_members WHERE customer_id=? AND COALESCE(deleted_at,'')='' ORDER BY created_at,id",[id]);
  let fallback=1;
  return rows.map(r=>{const order=r.relation==='child'?(byId.get(text(r.id))??fallback++):null;return{child_id:text(r.id),customer_id:id,relation:text(r.relation),birth_order:order,name:text(r.name),name_kana:text(r.furigana),birth_date:dateOnly(r.birthdate),gender_optional:text(r.gender),source:'crm',confidence:null,confirmed_at:'',updated_at:text(r.updated_at)}}).sort((a,b)=>(a.relation==='child'?0:1)-(b.relation==='child'?0:1)||(a.birth_order??999)-(b.birth_order??999));
}

async function reservationMetrics(env,id){
  if(!(await tableExists(env,'customer_reservations')))return{reservation_count:0,completed_shoot_count:0,cancel_count:0,repeat_count:0,customer_repeat_share:0,lifetime_revenue:0,average_shoot_value:0,shoot_genre_history:[],first_shoot_date:'',last_shoot_date:''};
  const rows=await all(env,"SELECT customer_id,genre,shoot_date,total_amount,status FROM customer_reservations WHERE CAST(customer_id AS TEXT)=? AND COALESCE(deleted_at,'')='' ORDER BY COALESCE(shoot_date,'') ASC",[id]);
  const exact=rows.filter(x=>text(x.customer_id)===id),completed=exact.filter(x=>COMPLETED_STATUSES.has(normalizedStatus(x.status))||COMPLETED_STATUSES.has(text(x.status))),cancelled=exact.filter(x=>CANCEL_STATUSES.has(normalizedStatus(x.status))||CANCEL_STATUSES.has(text(x.status)));
  const revenue=completed.reduce((s,x)=>s+Math.max(0,num(x.total_amount,0)),0),count=completed.length,genres=[...new Set(completed.map(x=>text(x.genre)).filter(Boolean))],dates=completed.map(x=>dateOnly(x.shoot_date)).filter(Boolean).sort();
  return{reservation_count:exact.length,completed_shoot_count:count,cancel_count:cancelled.length,repeat_count:Math.max(count-1,0),customer_repeat_share:count?Math.max(count-1,0)/count:0,lifetime_revenue:revenue,average_shoot_value:count?revenue/count:0,shoot_genre_history:genres,first_shoot_date:dates[0]||'',last_shoot_date:dates[dates.length-1]||''};
}

async function attributionView(env,customer,id){
  // Attribution remains reference/read-model data. Do not duplicate raw measurement rows here.
  const source=text(customer.acquisition_source);
  const out={first_touch_source:source||'unknown',last_touch_source:source||'unknown',campaign:'',adset:'',ad:'',landing_page:'',source:'customer-crm-reference'};
  for(const table of ['customer_attribution','crm_customer_attribution','attribution_visits']){
    if(!(await tableExists(env,table)))continue;
    const cols=await columns(env,table);if(!cols.has('customer_id'))continue;
    const rows=await all(env,`SELECT * FROM ${table} WHERE CAST(customer_id AS TEXT)=? ORDER BY COALESCE(created_at,'') ASC LIMIT 100`,[id]);
    if(!rows.length)continue;const a=rows[0],z=rows[rows.length-1];
    out.first_touch_source=text(a.source||a.acquisition_source)||out.first_touch_source;out.last_touch_source=text(z.source||z.acquisition_source)||out.last_touch_source;out.campaign=text(z.campaign||z.campaign_name);out.adset=text(z.adset||z.adset_name);out.ad=text(z.ad||z.ad_name);out.landing_page=text(z.landing_page||z.landing_url);out.source=table;break;
  }
  return out;
}

async function candidateRows(env,id){
  if(!(await tableExists(env,'customer_field_evidence')))return[];
  return all(env,"SELECT candidate_id,field_name,candidate_value,source,confidence,evidence_snippet,status,first_seen_at,last_seen_at,confirmed_by_human,confirmed_at FROM customer_field_evidence WHERE customer_id=? ORDER BY CASE status WHEN 'conflict' THEN 0 WHEN 'candidate' THEN 1 WHEN 'confirmed' THEN 2 ELSE 3 END, created_at DESC LIMIT 100",[id]);
}

async function profileView(env,id){
  const customer=await exactCustomer(env,id);if(!customer)return null;
  const ext=await extension(env,id),children=(await family(env,id)).filter(x=>x.relation==='child'),metrics=await reservationMetrics(env,id),attribution=await attributionView(env,customer,id),candidates=await candidateRows(env,id);
  return{customer_id:id,core:{name:text(customer.name),name_kana:text(customer.furigana),line_display_name:text(customer.line_display_name),phone:text(customer.phone),address:text(customer.address),email:text(customer.email),line_linked:!!text(customer.line_user_id)},family:{children,wedding_anniversary:dateOnly(ext.wedding_anniversary||customer.anniversary)},metrics,attribution,lifecycle:{first_inquiry_at:text(ext.first_inquiry_at),first_shoot_date:metrics.first_shoot_date,last_shoot_date:metrics.last_shoot_date,last_contact_at:text(ext.last_contact_at),lead_status:text(ext.lead_status)||'inquiry',lost_reason:text(ext.lost_reason)},referral:{referrer_customer_id:text(ext.referrer_customer_id),referrer_name:text(ext.referrer_name)},experience:{nps_score:ext.nps_score==null?null:Number(ext.nps_score),nps_answered_at:text(ext.nps_answered_at),nps_comment:text(ext.nps_comment),publication_permission:text(ext.publication_permission)||'unknown',marketing_contact_permission:text(ext.marketing_contact_permission)||'unknown'},notes:{current:text(ext.notes||customer.memo),updated_at:text(ext.notes_updated_at||customer.updated_at),updated_by:text(ext.notes_updated_by)},line_candidates:candidates,meta:{customer_identity_source:'crm_customer_id',customer_id_generation:false,derived_metrics_source:'customer_reservations',auto_apply:false,candidate_only:true}};
}

function validateExtensionPatch(p){
  if('lead_status'in p&&text(p.lead_status)&&!LEAD_STATUS.has(text(p.lead_status)))return'invalid_lead_status';
  if('lost_reason'in p&&text(p.lost_reason)&&!LOST_REASON.has(text(p.lost_reason)))return'invalid_lost_reason';
  if('publication_permission'in p&&!PUBLICATION.has(text(p.publication_permission)))return'invalid_publication_permission';
  if('marketing_contact_permission'in p&&!MARKETING_PERMISSION.has(text(p.marketing_contact_permission)))return'invalid_marketing_contact_permission';
  if('nps_score'in p&&p.nps_score!==null&&p.nps_score!==''&&(!Number.isInteger(Number(p.nps_score))||Number(p.nps_score)<0||Number(p.nps_score)>10))return'invalid_nps_score';
  if('referrer_customer_id'in p&&text(p.referrer_customer_id)&&!CUSTOMER_ID_RE.test(text(p.referrer_customer_id)))return'invalid_referrer_customer_id';return'';
}

async function recordConfirmed(env,id,field,value,actor,source='manual',snippet='Owner edit'){
  if(!(await tableExists(env,'customer_field_evidence')))return;
  const cid=`fe_${crypto.randomUUID()}`;await env.DB.prepare("INSERT INTO customer_field_evidence(candidate_id,customer_id,field_name,candidate_value,source,confidence,evidence_snippet,status,confirmed_by_human,confirmed_at,confirmed_by,created_at,updated_at) VALUES(?,?,?,?,?,1,?,'confirmed',1,datetime('now'),?,datetime('now'),datetime('now'))").bind(cid,id,field,text(value),source,clipEvidence(snippet),actor).run();
}

async function patchCustomer(request,env,id){
  if(env?.CRM_CUSTOMER360_WRITE_ENABLED!=='1')return json({ok:false,error:'customer360_write_disabled'},403);const customer=await exactCustomer(env,id);if(!customer)return json({ok:false,error:'customer_not_found'},404);
  const body=await request.json().catch(()=>null);if(!body||typeof body!=='object'||Array.isArray(body))return json({ok:false,error:'invalid_json'},400);if('customer_id'in body)return json({ok:false,error:'customer_id_readonly'},400);
  const changes=body.changes&&typeof body.changes==='object'?body.changes:body,actor=accessEmail(request)||'local-test';const bad=validateExtensionPatch(changes);if(bad)return json({ok:false,error:bad},400);
  const customerCols=await columns(env,'customers');let changed=0;
  for(const [k,v] of Object.entries(changes)){
    if(CORE_EDITABLE.has(k)){
      const col=k==='name_kana'?'furigana':k;if(!customerCols.has(col))continue;await env.DB.prepare(`UPDATE customers SET ${col}=?, updated_at=datetime('now') WHERE customer_id=?`).bind(text(v),id).run();await recordConfirmed(env,id,k,v,actor);changed++;continue;
    }
    if(EXT_EDITABLE.has(k)){
      if(!(await tableExists(env,'customer_profile_enrichment')))return json({ok:false,error:'profile_enrichment_schema_not_applied'},409);const val=k==='nps_score'?(v==null||v===''?null:Number(v)):text(v)||null;
      await env.DB.prepare(`INSERT INTO customer_profile_enrichment(customer_id,${k},created_at,updated_at,updated_by) VALUES(?,?,datetime('now'),datetime('now'),?) ON CONFLICT(customer_id) DO UPDATE SET ${k}=excluded.${k},updated_at=datetime('now'),updated_by=excluded.updated_by`).bind(id,val,actor).run();
      if(k==='notes'&&text(v)&&await tableExists(env,'customer_notes_history'))await env.DB.prepare("INSERT INTO customer_notes_history(note_id,customer_id,body,created_at,created_by) VALUES(?,?,?,datetime('now'),?)").bind(`note_${crypto.randomUUID()}`,id,text(v),actor).run();await recordConfirmed(env,id,k,v,actor);changed++;
    }
  }
  if(!changed)return json({ok:false,error:'no_editable_changes'},400);return json({ok:true,changed,customer:await profileView(env,id)});
}

async function saveChild(request,env,id){
  if(env?.CRM_CUSTOMER360_WRITE_ENABLED!=='1')return json({ok:false,error:'customer360_write_disabled'},403);if(!(await exactCustomer(env,id)))return json({ok:false,error:'customer_not_found'},404);if(!(await tableExists(env,'customer_family_members')))return json({ok:false,error:'family_schema_not_applied'},409);
  const body=await request.json().catch(()=>null);if(!body)return json({ok:false,error:'invalid_json'},400);const actor=accessEmail(request)||'local-test',childId=text(body.child_id)||`fm_${crypto.randomUUID()}`,birthOrder=Math.max(1,Number(body.birth_order||1)||1);const existing=await first(env,'SELECT customer_id FROM customer_family_members WHERE id=? LIMIT 1',[childId]);if(existing&&text(existing.customer_id)!==id)return json({ok:false,error:'family_member_customer_mismatch'},409);
  await env.DB.prepare("INSERT INTO customer_family_members(id,customer_id,relation,name,furigana,birthdate,gender,school_stage,memo,created_at,updated_at,updated_by,deleted_at) VALUES(?,?,'child',?,?,?,?,?,'',datetime('now'),datetime('now'),?,NULL) ON CONFLICT(id) DO UPDATE SET name=excluded.name,furigana=excluded.furigana,birthdate=excluded.birthdate,gender=excluded.gender,school_stage=excluded.school_stage,updated_at=datetime('now'),updated_by=excluded.updated_by,deleted_at=NULL").bind(childId,id,text(body.name),text(body.name_kana),dateOnly(body.birth_date),text(body.gender_optional)||null,text(body.school_stage)||null,actor).run();
  if(await tableExists(env,'customer_family_member_metadata'))await env.DB.prepare("INSERT INTO customer_family_member_metadata(member_id,customer_id,birth_order,created_at,updated_at,updated_by) VALUES(?,?,?,datetime('now'),datetime('now'),?) ON CONFLICT(member_id) DO UPDATE SET customer_id=excluded.customer_id,birth_order=excluded.birth_order,updated_at=datetime('now'),updated_by=excluded.updated_by").bind(childId,id,birthOrder,actor).run();await recordConfirmed(env,id,`child.${birthOrder}.name`,body.name,actor);if(dateOnly(body.birth_date))await recordConfirmed(env,id,`child.${birthOrder}.birth_date`,dateOnly(body.birth_date),actor);return json({ok:true,child_id:childId,customer:await profileView(env,id)});
}

function extractedCandidates(message){
  const msg=text(message),out=[];if(!msg)return out;const phone=msg.match(/(?:電話(?:番号)?(?:は|:|：)?\s*)?(0\d{1,4}[-ー‐]?\d{1,4}[-ー‐]?\d{3,4})/);if(phone)out.push({field:'phone',value:phone[1].replace(/[ー‐]/g,'-'),confidence:.99});const email=msg.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);if(email)out.push({field:'email',value:email[0],confidence:.99});
  const anniversary=msg.match(/(?:結婚記念日|入籍日)(?:は|が|:|：)?\s*(\d{4})[年\/-](\d{1,2})[月\/-](\d{1,2})日?/);if(anniversary)out.push({field:'wedding_anniversary',value:`${anniversary[1]}-${String(anniversary[2]).padStart(2,'0')}-${String(anniversary[3]).padStart(2,'0')}`,confidence:.98});
  const child=msg.match(/(?:第一子|第1子|長女|長男|娘|息子)(?:の名前)?(?:は|が|:|：)?\s*([一-龥々ぁ-んァ-ヶー]{1,12})(?:です|といいます|と言います|、|。|\s|$)/);if(child)out.push({field:'child.1.name',value:child[1],confidence:.9});
  const childBirth=msg.match(/(?:第一子|第1子|長女|長男|娘|息子).{0,24}?(\d{4})[年\/-](\d{1,2})[月\/-](\d{1,2})日?/);if(childBirth)out.push({field:'child.1.birth_date',value:`${childBirth[1]}-${String(childBirth[2]).padStart(2,'0')}-${String(childBirth[3]).padStart(2,'0')}`,confidence:.94});return out;
}

async function currentFieldValue(env,customer,id,field){
  if(CORE_EDITABLE.has(field))return text(customer[field==='name_kana'?'furigana':field]);const ext=await extension(env,id);if(EXT_EDITABLE.has(field))return text(ext[field]);if(field.startsWith('child.')){const [,ord,key]=field.split('.'),child=(await family(env,id)).find(x=>x.relation==='child'&&Number(x.birth_order)===Number(ord));return child?text(key==='birth_date'?child.birth_date:child[key]):''}return'';
}

async function extractFromLine(request,env,id){
  if(env?.CRM_CUSTOMER360_WRITE_ENABLED!=='1')return json({ok:false,error:'customer360_write_disabled'},403);const customer=await exactCustomer(env,id);if(!customer)return json({ok:false,error:'customer_not_found'},404);if(!(await tableExists(env,'customer_line_message_events')))return json({ok:false,error:'line_context_table_missing'},409);if(!(await tableExists(env,'customer_field_evidence')))return json({ok:false,error:'profile_enrichment_schema_not_applied'},409);const lineId=text(customer.line_user_id);if(!lineId)return json({ok:true,customer_id:id,extracted:0,candidates:[],reason:'line_not_linked'});
  const events=await all(env,"SELECT event_id,customer_id,line_user_id,message_text,occurred_at,created_at FROM customer_line_message_events WHERE CAST(customer_id AS TEXT)=? AND line_user_id=? AND direction='inbound' ORDER BY COALESCE(occurred_at,created_at,'') DESC LIMIT 50",[id,lineId]);let inserted=0;
  for(const event of events){if(text(event.customer_id)!==id||text(event.line_user_id)!==lineId)continue;for(const c of extractedCandidates(event.message_text)){const existing=await currentFieldValue(env,customer,id,c.field),status=existing&&existing!==text(c.value)?'conflict':'candidate',cid=`fe_${crypto.randomUUID()}`;try{await env.DB.prepare("INSERT INTO customer_field_evidence(candidate_id,customer_id,field_name,candidate_value,source,confidence,evidence_snippet,source_event_id,status,first_seen_at,last_seen_at,confirmed_by_human,created_at,updated_at) VALUES(?,?,?,?,'line',?,?,?, ?,datetime('now'),datetime('now'),0,datetime('now'),datetime('now'))").bind(cid,id,c.field,text(c.value),c.confidence,clipEvidence(event.message_text),text(event.event_id),status).run();inserted++}catch(_){}}
  }
  return json({ok:true,customer_id:id,extracted:inserted,candidates:await candidateRows(env,id),identity:{lookup_key:'customer_id',line_user_id_exact_match:true,fallback_used:false},auto_apply:false});
}

async function resolveCandidate(request,env,id,candidateId){
  if(env?.CRM_CUSTOMER360_WRITE_ENABLED!=='1')return json({ok:false,error:'customer360_write_disabled'},403);if(!(await exactCustomer(env,id)))return json({ok:false,error:'customer_not_found'},404);const body=await request.json().catch(()=>({})),action=text(body.action);if(!['confirm','reject'].includes(action))return json({ok:false,error:'invalid_action'},400);const row=await first(env,'SELECT * FROM customer_field_evidence WHERE candidate_id=? AND customer_id=? LIMIT 1',[candidateId,id]);if(!row)return json({ok:false,error:'candidate_not_found'},404);const actor=accessEmail(request)||'local-test';if(action==='reject'){await env.DB.prepare("UPDATE customer_field_evidence SET status='rejected',updated_at=datetime('now'),confirmed_by=? WHERE candidate_id=? AND customer_id=?").bind(actor,candidateId,id).run();return json({ok:true,status:'rejected'})}
  const field=text(row.field_name),value=text(row.candidate_value);if(field.startsWith('child.')){const [,ord,key]=field.split('.'),children=(await family(env,id)).filter(x=>x.relation==='child'),child=children.find(x=>Number(x.birth_order)===Number(ord));const payload={child_id:child?.child_id,birth_order:Number(ord),name:child?.name||'',name_kana:child?.name_kana||'',birth_date:child?.birth_date||''};if(key==='name')payload.name=value;if(key==='birth_date')payload.birth_date=value;const fake=new Request(request.url,{method:'POST',headers:request.headers,body:JSON.stringify(payload)});const res=await saveChild(fake,env,id);if(!res.ok)return res;}else{const fake=new Request(request.url,{method:'PATCH',headers:request.headers,body:JSON.stringify({changes:{[field]:value}})});const res=await patchCustomer(fake,env,id);if(!res.ok)return res;}
  await env.DB.prepare("UPDATE customer_field_evidence SET status='confirmed',confirmed_by_human=1,confirmed_at=datetime('now'),confirmed_by=?,updated_at=datetime('now') WHERE candidate_id=? AND customer_id=?").bind(actor,candidateId,id).run();return json({ok:true,status:'confirmed',customer:await profileView(env,id)});
}

export async function handleCustomerProfileEnrichmentRequest(request,env){
  const url=new URL(request.url),m=url.pathname.match(/^\/api\/customer360\/profile\/([0-9]{8})(?:\/([^/]+))?(?:\/([^/]+))?$/);if(!m)return null;const id=m[1],part=text(m[2]),sub=text(m[3]);const internal=internalAllowed(request,env);if(!ownerAllowed(request,env)&&!internal)return json({ok:false,error:'authentication_required'},401);
  if(request.method==='GET'&&!part){const data=await profileView(env,id);return data?json({ok:true,customer:data}):json({ok:false,error:'customer_not_found'},404)}
  if(request.method==='PATCH'&&!part){if(!ownerAllowed(request,env))return json({ok:false,error:'owner_auth_required'},403);return patchCustomer(request,env,id)}
  if(request.method==='POST'&&part==='children'){if(!ownerAllowed(request,env))return json({ok:false,error:'owner_auth_required'},403);return saveChild(request,env,id)}
  if(request.method==='POST'&&part==='extract-line'){if(!ownerAllowed(request,env))return json({ok:false,error:'owner_auth_required'},403);return extractFromLine(request,env,id)}
  if(request.method==='POST'&&part==='candidate'&&sub){if(!ownerAllowed(request,env))return json({ok:false,error:'owner_auth_required'},403);return resolveCandidate(request,env,id,decodeURIComponent(sub))}
  return json({ok:false,error:'not_found'},404);
}

export function customerProfileEnrichmentHealth(){return{customer360_profile_enrichment:true,customer360_profile_build:BUILD,customer_identity_crm_canonical_only:true,customer_id_generation:false,customer_line_extraction_exact_identity_only:true,customer_line_auto_apply:false,customer_line_extraction_mode:'candidate-only',human_confirmed_overwrite_by_line:false,derived_metrics_runtime_only:true,destructive_schema_changes:false,browser_secret_exposure:false};}

export const __test={extractedCandidates};
