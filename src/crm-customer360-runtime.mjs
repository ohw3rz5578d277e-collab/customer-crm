import {
  buildCustomerMarketingView,
  jstToday,
  mergeFamilyMembers,
  CUSTOMER360_HIGH_LTV_THRESHOLD
} from './crm-customer360-marketing-engine.mjs';
import {
  parseCustomerSearchParams,
  searchCustomerViews,
  buildFacets,
  listCustomerDto
} from './crm-customer360-search.mjs';
import { assignCanonicalCustomerIdToCustomerRef } from './crm-customer-id-autofill-runtime.mjs';
import { parsePeriodAnalyticsParams, buildPeriodAnalytics } from './crm-customer360-period-analytics.mjs';
import { parseApproachQueueParams, buildApproachQueue, approachContactState } from './crm-customer360-approach-queue.mjs';

const CUSTOMER_ID_RE=/^\d{8}$/;
const RELATIONS=new Set(['spouse','child','parent','grandparent','other']);
const text=v=>v==null?'':String(v).trim();
const num=(v,f=0)=>{const n=Number(String(v??'').replace(/[,円¥\s]/g,''));return Number.isFinite(n)?n:f};
const bool=v=>v===true||v===1||String(v).toLowerCase()==='true'||String(v)==='1';
const dateOnly=v=>{const m=text(v).match(/^(\d{4}-\d{2}-\d{2})/);return m?m[1]:''};

function json(data,status=200){return new Response(JSON.stringify(data),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-robots-tag':'noindex, nofollow','referrer-policy':'no-referrer'}})}
function accessEmail(request){return text(request.headers.get('cf-access-authenticated-user-email')||request.headers.get('Cf-Access-Authenticated-User-Email')||request.headers.get('cf-access-user-email'))}
function authorized(request,env){return env?.CRM_LOCAL_TEST_AUTH==='1'||!!accessEmail(request)}
async function safeAll(env,sql,params=[]){try{let s=env.DB.prepare(sql);if(params.length)s=s.bind(...params);const r=await s.all();return r.results||[]}catch(_){return[]}}
async function safeFirst(env,sql,params=[]){try{let s=env.DB.prepare(sql);if(params.length)s=s.bind(...params);return await s.first()}catch(_){return null}}
async function tableExists(env,name){return !!(await safeFirst(env,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",[name]))}
async function strictAll(env,sql,params=[]){let s=env.DB.prepare(sql);if(params.length)s=s.bind(...params);const r=await s.all();return r.results||[]}
async function strictFirst(env,sql,params=[]){let s=env.DB.prepare(sql);if(params.length)s=s.bind(...params);return await s.first()}
async function strictTableExists(env,name){return !!(await strictFirst(env,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",[name]))}
async function familyRows(env,id){if(!(await tableExists(env,'customer_family_members')))return[];return safeAll(env,"SELECT * FROM customer_family_members WHERE customer_id=? AND (deleted_at IS NULL OR deleted_at='') ORDER BY created_at ASC,id ASC",[id])}
async function profileRow(env,id){if(!(await tableExists(env,'customer_marketing_profiles')))return{};return (await safeFirst(env,"SELECT * FROM customer_marketing_profiles WHERE customer_id=? LIMIT 1",[id]))||{}}
async function activeCustomers(env){return (await safeAll(env,"SELECT rowid AS __customer_ref,* FROM customers")).filter(x=>!text(x.deleted_at))}
async function viewFor(env,c,onDate){return buildCustomerMarketingView(c,await familyRows(env,text(c.customer_id)),await profileRow(env,text(c.customer_id)),onDate)}

async function loadCustomerViews(env,onDate,{requireContactPermissions=false}={}){
  const customers=await activeCustomers(env);
  const managedFamily=await tableExists(env,'customer_family_members')?await safeAll(env,"SELECT * FROM customer_family_members WHERE deleted_at IS NULL OR deleted_at='' ORDER BY customer_id,created_at,id"):[];
  const profiles=await tableExists(env,'customer_marketing_profiles')?await safeAll(env,"SELECT * FROM customer_marketing_profiles"):[];
  let contactPermissions=[];
  if(requireContactPermissions){
    if(!(await strictTableExists(env,'customer_profile_enrichment')))throw new Error('contact_permission_unavailable');
    contactPermissions=await strictAll(env,"SELECT customer_id,marketing_contact_permission FROM customer_profile_enrichment");
  }else if(await tableExists(env,'customer_profile_enrichment')){
    contactPermissions=await safeAll(env,"SELECT customer_id,marketing_contact_permission FROM customer_profile_enrichment");
  }
  const familyByCustomer=new Map(),profileByCustomer=new Map(),contactPermissionByCustomer=new Map();
  for(const m of managedFamily){const id=text(m.customer_id);if(!familyByCustomer.has(id))familyByCustomer.set(id,[]);familyByCustomer.get(id).push(m)}
  for(const p of profiles)profileByCustomer.set(text(p.customer_id),p);
  for(const p of contactPermissions)contactPermissionByCustomer.set(text(p.customer_id),text(p.marketing_contact_permission));
  return customers.map(c=>{
    const id=text(c.customer_id),profile={...(profileByCustomer.get(id)||{})};
    if(contactPermissionByCustomer.get(id)==='denied')profile.marketing_opt_out=1;
    return buildCustomerMarketingView(c,familyByCustomer.get(id)||[],profile,onDate);
  });
}

async function facetData(env,views){
  const facets=buildFacets(views);
  if(await tableExists(env,'crm_marketing_campaigns')){
    const campaignRows=await safeAll(env,"SELECT name FROM crm_marketing_campaigns WHERE (deleted_at IS NULL OR deleted_at='') AND (status IS NULL OR status!='deleted') ORDER BY name");
    facets.campaigns=[...new Set([...facets.campaigns,...campaignRows.map(x=>text(x.name)).filter(Boolean)])].sort((a,b)=>a.localeCompare(b,'ja'));
  }
  return facets;
}

export async function marketingHomeData(env,onDate=jstToday()){
  let views,consentViews=null,contactCandidatesAvailable=true;
  try{
    consentViews=await loadCustomerViews(env,onDate,{requireContactPermissions:true});
    views=consentViews;
  }catch(_){
    contactCandidatesAvailable=false;
    views=await loadCustomerViews(env,onDate);
  }
  const approach=(consentViews||[]).filter(v=>v.recommendation.priority_score>0&&approachContactState(v).ready).sort((a,b)=>b.recommendation.priority_score-a.recommendation.priority_score||(a.next_opportunity?.days??99999)-(b.next_opportunity?.days??99999)||text(a.customer_id).localeCompare(text(b.customer_id)));
  const avg=views.length?Math.round(views.reduce((s,v)=>s+v.realized_ltv,0)/views.length):0;
  return {as_of:onDate,kpis:{customers: views.length,average_realized_ltv:avg,repeat_rate_pct:views.length?Math.round(views.filter(v=>v.shoot_count>=2).length/views.length*100):0,vip_high_ltv:views.filter(v=>v.realized_ltv>=CUSTOMER360_HIGH_LTV_THRESHOLD||v.marketing_classes.includes('VIP')).length,event_90d:views.filter(v=>v.opportunities.some(o=>o.days!=null&&o.days>=0&&o.days<=90)).length,dormant_180:views.filter(v=>num(v.raw.dormant_days)>=180).length,line_link_rate_pct:views.length?Math.round(views.filter(v=>v.line_linked).length/views.length*100):0,approach_this_month:approach.filter(v=>(v.next_opportunity?.days??99999)<=30||v.recommendation.priority_score>=650).length},top_opportunities:approach.slice(0,12).map(listCustomerDto),facets:await facetData(env,views),meta:{contact_candidates_available:contactCandidatesAvailable,contact_candidate_filter:'manual_contact_ready',contact_permission_fail_closed:true}};
}

export async function approachQueueData(env,searchParams,onDate=jstToday()){
  let params;
  try{params=parseApproachQueueParams(searchParams)}catch(error){return{error:text(error?.message)||'invalid_approach_queue_filter',status:400}}
  let views;
  try{views=await loadCustomerViews(env,onDate,{requireContactPermissions:true})}
  catch(_){return{error:'contact_permission_unavailable',status:503}}
  return buildApproachQueue(views,params);
}

export async function periodAnalyticsData(env,searchParams,onDate=jstToday()){
  let period;
  try{period=parsePeriodAnalyticsParams(searchParams,onDate)}catch(error){return{error:text(error?.message)||'invalid_period'}}
  let reservationsAvailable;
  try{reservationsAvailable=await strictTableExists(env,'customer_reservations')}
  catch(_){return{error:'analytics_read_unavailable',status:503}}
  if(!reservationsAvailable){
    return {
      available:false,
      period,
      current:{from:period.from,to:period.to,revenue:0,completed_shoots:0,unique_customers:0,average_order_value:0,repeat_customers_in_period:0,repeat_rate_pct:0,genres:[],monthly:[]},
      previous:{from:period.previous.from,to:period.previous.to,revenue:0,completed_shoots:0,unique_customers:0,average_order_value:0,repeat_customers_in_period:0,repeat_rate_pct:0,genres:[],monthly:[]},
      change_pct:{revenue:null,completed_shoots:null,unique_customers:null,average_order_value:null},
      meta:{read_only:true,identity_key:'customer_id',customer_id_generation:false,customer_write:false,line_send:false,table_available:false}
    };
  }
  let rows;
  try{rows=await strictAll(env,"SELECT customer_id,genre,shoot_date,total_amount,status FROM customer_reservations WHERE COALESCE(deleted_at,'')='' AND substr(COALESCE(shoot_date,''),1,10)>=? AND substr(COALESCE(shoot_date,''),1,10)<=? ORDER BY shoot_date ASC",[period.previous.from,period.to])}
  catch(_){return{error:'analytics_read_unavailable',status:503}}
  const out=buildPeriodAnalytics(rows,period);
  return {available:true,...out,meta:{...out.meta,table_available:true,rows_read:rows.length}};
}

export async function customer360ReadOnlyStatus(env){
  let dbOk=false;
  if(env?.DB?.prepare){
    try{
      const row=await env.DB.prepare('SELECT 1 AS ok').first();
      dbOk=Number(row?.ok||0)===1;
    }catch(_){dbOk=false}
  }
  return {
    ok:true,
    read_only:true,
    bindings:{
      DB:dbOk,
      RESERVATION_SERVICE:!!env?.RESERVATION_SERVICE,
      LINE_SERVICE:!!env?.LINE_SERVICE
    },
    customer360_marketing_foundation:true,
    customer360_status_read_only:true,
    d1_probe:'SELECT 1',
    d1_write:false,
    schema_repair:false,
    customer_write:false,
    customer_id_generation:false,
    line_send:false
  };
}

export async function customerListData(env,searchParams,onDate=jstToday()){
  const started=Date.now();
  let parsed;
  try{parsed=parseCustomerSearchParams(searchParams)}catch(error){return{error:text(error?.message)||'invalid_filter'}}
  parsed.on_date=parsed.on_date||onDate;
  const views=await loadCustomerViews(env,onDate),result=searchCustomerViews(views,parsed);
  return {...result,all_total:views.length,facets:await facetData(env,views),meta:{server_ms:Date.now()-started,privacy_safe_list_dto:true,identity_resolution_used:false,customer_ref_identity_source:false}};
}

export async function customer360Data(env,id,onDate=jstToday()){
  if(!CUSTOMER_ID_RE.test(id))return null;const c=await safeFirst(env,"SELECT * FROM customers WHERE customer_id=? LIMIT 1",[id]);if(!c||text(c.deleted_at))return null;
  const view=await viewFor(env,c,onDate);
  return {...view,reservations:await safeAll(env,"SELECT * FROM customer_reservations WHERE customer_id=? ORDER BY shoot_date DESC,created_at DESC LIMIT 50",[id]),line_history:await safeAll(env,"SELECT * FROM crm_line_response_logs WHERE customer_id=? ORDER BY created_at DESC LIMIT 30",[id]),marketing_history:await safeAll(env,"SELECT * FROM crm_marketing_ops_logs WHERE target_id=? ORDER BY created_at DESC LIMIT 50",[id])};
}

function validateMember(body){const relation=text(body.relation)||'other';if(!RELATIONS.has(relation))return{error:'invalid_relation'};return{member:{relation,name:text(body.name),furigana:text(body.furigana),birthdate:dateOnly(body.birthdate),gender:text(body.gender)||null,school_stage:text(body.school_stage)||null,memo:text(body.memo)}}}
async function familyWrite(request,env){
  if(env?.CRM_CUSTOMER360_WRITE_ENABLED!=='1')return json({ok:false,error:'family_write_disabled'},403);
  const body=await request.json().catch(()=>({})),customerId=text(body.customer_id);if(!CUSTOMER_ID_RE.test(customerId))return json({ok:false,error:'invalid_customer_id'},400);
  if(!(await safeFirst(env,"SELECT customer_id FROM customers WHERE customer_id=? LIMIT 1",[customerId])))return json({ok:false,error:'customer_not_found'},404);
  if(!(await tableExists(env,'customer_family_members')))return json({ok:false,error:'family_schema_not_applied'},409);
  if(text(body.action)==='delete'){const id=text(body.id);if(!id)return json({ok:false,error:'id_required'},400);await env.DB.prepare("UPDATE customer_family_members SET deleted_at=datetime('now'),updated_at=datetime('now'),updated_by=? WHERE id=? AND customer_id=?").bind(accessEmail(request)||'local-test',id,customerId).run();return json({ok:true,deleted:true,id})}
  const v=validateMember(body);if(v.error)return json({ok:false,error:v.error},400);const id=text(body.id)||`fm_${crypto.randomUUID()}`;const existing=await safeFirst(env,"SELECT customer_id FROM customer_family_members WHERE id=? LIMIT 1",[id]);if(existing&&text(existing.customer_id)!==customerId)return json({ok:false,error:'family_member_customer_mismatch'},409);
  const m=v.member,actor=accessEmail(request)||'local-test';await env.DB.prepare(`INSERT INTO customer_family_members(id,customer_id,relation,name,furigana,birthdate,gender,school_stage,memo,created_at,updated_at,updated_by,deleted_at) VALUES(?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'),?,NULL) ON CONFLICT(id) DO UPDATE SET relation=excluded.relation,name=excluded.name,furigana=excluded.furigana,birthdate=excluded.birthdate,gender=excluded.gender,school_stage=excluded.school_stage,memo=excluded.memo,updated_at=datetime('now'),updated_by=excluded.updated_by,deleted_at=NULL`).bind(id,customerId,m.relation,m.name,m.furigana,m.birthdate,m.gender,m.school_stage,m.memo,actor).run();return json({ok:true,id,customer_id:customerId});
}
async function profileWrite(request,env){
  if(env?.CRM_CUSTOMER360_WRITE_ENABLED!=='1')return json({ok:false,error:'profile_write_disabled'},403);
  if(!(await tableExists(env,'customer_marketing_profiles')))return json({ok:false,error:'profile_schema_not_applied'},409);
  const body=await request.json().catch(()=>({})),customerId=text(body.customer_id);if(!CUSTOMER_ID_RE.test(customerId))return json({ok:false,error:'invalid_customer_id'},400);if(!(await safeFirst(env,"SELECT customer_id FROM customers WHERE customer_id=? LIMIT 1",[customerId])))return json({ok:false,error:'customer_not_found'},404);
  const opt=body.marketing_opt_out==null?null:(bool(body.marketing_opt_out)?1:0),actor=accessEmail(request)||'local-test';await env.DB.prepare(`INSERT INTO customer_marketing_profiles(customer_id,postal_code,prefecture,city,address_line1,address_line2,marketing_opt_out,preferred_contact_channel,created_at,updated_at,updated_by) VALUES(?,?,?,?,?,?,?,?,datetime('now'),datetime('now'),?) ON CONFLICT(customer_id) DO UPDATE SET postal_code=excluded.postal_code,prefecture=excluded.prefecture,city=excluded.city,address_line1=excluded.address_line1,address_line2=excluded.address_line2,marketing_opt_out=excluded.marketing_opt_out,preferred_contact_channel=excluded.preferred_contact_channel,updated_at=datetime('now'),updated_by=excluded.updated_by`).bind(customerId,text(body.postal_code),text(body.prefecture),text(body.city),text(body.address_line1),text(body.address_line2),opt,text(body.preferred_contact_channel),actor).run();return json({ok:true,customer_id:customerId});
}

export async function handleCustomer360Request(request,env){
  const url=new URL(request.url);if(!url.pathname.startsWith('/api/customer360/'))return null;if(!authorized(request,env))return json({ok:false,error:'authentication_required'},401);
  const asOf=text(url.searchParams.get('as_of'))||jstToday();
  if(request.method==='GET'&&url.pathname==='/api/customer360/status')return json(await customer360ReadOnlyStatus(env));
  if(request.method==='GET'&&url.pathname==='/api/customer360/marketing-home')return json({ok:true,...await marketingHomeData(env,asOf)});
  if(request.method==='GET'&&url.pathname==='/api/customer360/analytics'){const data=await periodAnalyticsData(env,url.searchParams,asOf);return data.error?json({ok:false,error:data.error},Number(data.status||400)):json({ok:true,...data})}
  if(request.method==='GET'&&url.pathname==='/api/customer360/approach-queue'){const data=await approachQueueData(env,url.searchParams,asOf);return data.error?json({ok:false,error:data.error},Number(data.status||400)):json({ok:true,...data})}
  if(request.method==='GET'&&url.pathname==='/api/customer360/customers'){const data=await customerListData(env,url.searchParams,asOf);return data.error?json({ok:false,error:data.error},400):json({ok:true,...data})}
  if(request.method==='POST'&&url.pathname==='/api/customer360/customer-id/allocate'){
    const body=await request.json().catch(()=>null);if(!body)return json({ok:false,error:'invalid_json'},400);
    const out=await assignCanonicalCustomerIdToCustomerRef(env,body);const status=Number(out.statusCode||200);delete out.statusCode;return json(out,status);
  }
  if(request.method==='GET'&&url.pathname==='/api/customer360/family'){const id=text(url.searchParams.get('customer_id'));if(!CUSTOMER_ID_RE.test(id))return json({ok:false,error:'invalid_customer_id'},400);const c=await safeFirst(env,"SELECT * FROM customers WHERE customer_id=? LIMIT 1",[id]);if(!c)return json({ok:false,error:'customer_not_found'},404);return json({ok:true,customer_id:id,family:mergeFamilyMembers(c,await familyRows(env,id))})}
  if(request.method==='POST'&&url.pathname==='/api/customer360/family')return familyWrite(request,env);
  if(request.method==='POST'&&url.pathname==='/api/customer360/profile')return profileWrite(request,env);
  if(request.method==='GET'&&url.pathname.startsWith('/api/customer360/customer/')){const id=decodeURIComponent(url.pathname.slice('/api/customer360/customer/'.length)),data=await customer360Data(env,id,asOf);return data?json({ok:true,customer:data}):json({ok:false,error:'customer_not_found'},404)}
  return json({ok:false,error:'not_found'},404);
}

export function customer360Health(){return{customer360_family_marketing_foundation:true,customer360_search_first:true,customer360_list_privacy_safe:true,customer360_server_filtering:true,customer360_page_size_max:100,family_member_limit:'unlimited',family_identity_source:false,search_identity_source:false,realized_ltv_field:'total_revenue',marketing_line_auto_send:false,marketing_profile_default_opt_in:false,customer360_write_default_enabled:false,customer_id_autofill_owner_authorized:true,customer_id_autofill_customer360_write_flag_required:false,customer360_status_read_only:true,customer360_status_schema_repair:false}}
