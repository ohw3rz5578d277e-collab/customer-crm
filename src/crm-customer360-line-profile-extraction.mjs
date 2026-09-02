import { __test } from './crm-customer360-profile-enrichment.mjs';

const CUSTOMER_ID_RE=/^\d{8}$/;
const text=v=>v==null?'':String(v).trim();
function json(data,status=200){return new Response(JSON.stringify(data),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-robots-tag':'noindex, nofollow'}})}
function accessEmail(request){return text(request.headers.get('cf-access-authenticated-user-email')||request.headers.get('Cf-Access-Authenticated-User-Email')||request.headers.get('cf-access-user-email'))}
async function first(env,sql,params=[]){let q=env.DB.prepare(sql);if(params.length)q=q.bind(...params);return (await q.first())||null}
async function all(env,sql,params=[]){let q=env.DB.prepare(sql);if(params.length)q=q.bind(...params);const r=await q.all();return r.results||[]}
async function tableExists(env,name){return !!(await first(env,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",[name]))}
function clip(v){const s=text(v).replace(/\s+/g,' ');return s.length<=120?s:s.slice(0,117)+'…'}
async function existingValue(env,customer,id,field){if(field==='phone'||field==='email')return text(customer[field]);if(field==='wedding_anniversary'&&await tableExists(env,'customer_profile_enrichment'))return text((await first(env,'SELECT wedding_anniversary FROM customer_profile_enrichment WHERE customer_id=?',[id]))?.wedding_anniversary);return''}

export async function handleCustomer360LineProfileExtraction(request,env){
  const match=new URL(request.url).pathname.match(/^\/api\/customer360\/profile\/([0-9]{8})\/extract-line$/);if(!match)return null;
  if(request.method!=='POST')return json({ok:false,error:'method_not_allowed'},405);
  if(env?.CRM_LOCAL_TEST_AUTH!=='1'&&!accessEmail(request))return json({ok:false,error:'authentication_required'},401);
  if(env?.CRM_CUSTOMER360_WRITE_ENABLED!=='1')return json({ok:false,error:'customer360_write_disabled'},403);
  const id=match[1];if(!CUSTOMER_ID_RE.test(id))return json({ok:false,error:'invalid_customer_id'},400);
  const customer=await first(env,"SELECT customer_id,line_user_id,phone,email FROM customers WHERE CAST(customer_id AS TEXT)=? AND COALESCE(deleted_at,'')='' LIMIT 1",[id]);
  if(!customer||text(customer.customer_id)!==id)return json({ok:false,error:'customer_not_found'},404);
  const lineId=text(customer.line_user_id);if(!lineId)return json({ok:true,customer_id:id,extracted:0,candidates:[],reason:'line_not_linked',auto_apply:false});
  if(!(await tableExists(env,'customer_line_message_events')))return json({ok:false,error:'line_context_table_missing'},409);
  if(!(await tableExists(env,'customer_field_evidence')))return json({ok:false,error:'profile_enrichment_schema_not_applied'},409);
  const events=await all(env,"SELECT event_id,customer_id,line_user_id,message_text,occurred_at,created_at FROM customer_line_message_events WHERE CAST(customer_id AS TEXT)=? AND line_user_id=? AND direction='incoming' AND send_status='received' ORDER BY COALESCE(occurred_at,created_at,'') DESC LIMIT 50",[id,lineId]);
  let inserted=0;
  for(const event of events){
    if(text(event.customer_id)!==id||text(event.line_user_id)!==lineId)continue;
    for(const c of __test.extractedCandidates(event.message_text)){
      const existing=await existingValue(env,customer,id,c.field),status=existing&&existing!==text(c.value)?'conflict':'candidate';
      try{await env.DB.prepare("INSERT INTO customer_field_evidence(candidate_id,customer_id,field_name,candidate_value,source,confidence,evidence_snippet,source_event_id,status,first_seen_at,last_seen_at,confirmed_by_human,created_at,updated_at) VALUES(?,?,?,?,'line',?,?,?, ?,datetime('now'),datetime('now'),0,datetime('now'),datetime('now'))").bind(`fe_${crypto.randomUUID()}`,id,c.field,text(c.value),c.confidence,clip(event.message_text),text(event.event_id),status).run();inserted++}catch(_){}
    }
  }
  const candidates=await all(env,"SELECT candidate_id,field_name,candidate_value,source,confidence,evidence_snippet,status,first_seen_at,last_seen_at,confirmed_by_human,confirmed_at FROM customer_field_evidence WHERE customer_id=? ORDER BY created_at DESC LIMIT 100",[id]);
  return json({ok:true,customer_id:id,extracted:inserted,candidates,identity:{lookup_key:'customer_id',line_user_id_exact_match:true,fallback_used:false},auto_apply:false,ledger_direction:'incoming'});
}

export function customer360LineProfileExtractionHealth(){return{customer360_line_profile_extraction:true,line_event_direction:'incoming',line_event_receive_status:'received',exact_customer_id_and_line_user_id:true,line_profile_auto_apply:false};}
