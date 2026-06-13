// ======================================================
// CUSTOMER CRM / INQUIRY ACTIONS WRAPPER
// build: customer-crm-api-inquiry-actions-20260613-01
// Adds action links from inquiry pipeline to LINE drafts, follow tasks, and reservation drafts.
// ======================================================

import app from "./production-index-crm-ops-screens.js";

const BUILD = "customer-crm-api-inquiry-actions-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){return v===undefined||v===null?"":String(v).trim();}
function lower(v){return text(v).toLowerCase();}
function num(v){const n=Number(v||0);return Number.isFinite(n)?n:0;}
function nowIso(){return new Date().toISOString();}
function todayJst(){return new Date(Date.now()+9*60*60*1000).toISOString().slice(0,10);}
function addDays(dateText,days){const base=text(dateText)||todayJst();const d=new Date(`${base.slice(0,10)}T00:00:00+09:00`);d.setDate(d.getDate()+Number(days||0));return new Date(d.getTime()+9*60*60*1000).toISOString().slice(0,10);}
function securityHeaders(headers={}){const h=new Headers(headers);h.set("cache-control","no-store, no-cache, must-revalidate, max-age=0");h.set("pragma","no-cache");h.set("expires","0");h.set("x-content-type-options","nosniff");h.set("referrer-policy","no-referrer");h.set("x-frame-options","DENY");return h;}
function json(data,status=200,headers={}){return new Response(JSON.stringify(data,null,2),{status,headers:securityHeaders({"content-type":"application/json; charset=utf-8",...headers})});}
async function readJson(request){try{return await request.json();}catch(_){return {};}}
function getAccessEmail(request){return lower(request.headers.get("cf-access-authenticated-user-email")||request.headers.get("Cf-Access-Authenticated-User-Email")||request.headers.get("x-user-email")||"");}
async function addColumn(db,table,definition){try{await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();}catch(_){}}
async function safeFirst(env,sql,bindings=[]){try{const s=env.DB.prepare(sql);return bindings.length?await s.bind(...bindings).first():await s.first();}catch(_){return null;}}
async function safeAll(env,sql,bindings=[]){try{const s=env.DB.prepare(sql);const r=bindings.length?await s.bind(...bindings).all():await s.all();return r.results||[];}catch(_){return [];}}

async function ensureSchema(env){
  if(!env.DB)throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY,role TEXT,status TEXT,created_by TEXT,created_at TEXT,updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email,role,status,created_by,created_at,updated_at) VALUES(?,'admin','active','system',datetime('now'),datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_inquiry_pipeline(
    id TEXT PRIMARY KEY, customer_id TEXT, customer_name TEXT, source TEXT, status TEXT, genre TEXT,
    preferred_date TEXT, place TEXT, plan_label TEXT, estimated_amount INTEGER DEFAULT 0,
    memo TEXT, next_action TEXT, due_date TEXT, created_by TEXT, updated_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP, deleted_at TEXT, deleted_by TEXT
  )`).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS customer_line_draft_logs(
    id TEXT PRIMARY KEY, customer_id TEXT, customer_name TEXT, action_type TEXT, action_label TEXT,
    message_body TEXT, status TEXT DEFAULT 'pending', priority TEXT, created_by TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP, sent_at TEXT, sent_by TEXT
  )`).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_follow_tasks(
    id TEXT PRIMARY KEY, customer_id TEXT, customer_name TEXT, task_type TEXT, title TEXT,
    due_date TEXT, priority TEXT, status TEXT, memo TEXT, created_by TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP, completed_at TEXT, completed_by TEXT, deleted_at TEXT, deleted_by TEXT
  )`).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_reservation_drafts(
    id TEXT PRIMARY KEY, customer_id TEXT, customer_name TEXT, genre TEXT, shoot_date TEXT, start_time TEXT,
    place TEXT, plan_label TEXT, total_amount INTEGER DEFAULT 0, status TEXT DEFAULT 'draft', memo TEXT,
    created_by TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_inquiry_action_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inquiry_id TEXT,
    action_type TEXT,
    target_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    before_status TEXT,
    after_status TEXT,
    detail_json TEXT,
    actor_email TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();
  for(const col of ["line_log_id TEXT","follow_task_id TEXT","reservation_draft_id TEXT","converted_at TEXT","lost_reason TEXT","action_summary TEXT"]){await addColumn(env.DB,"crm_inquiry_pipeline",col);}
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_inquiry_actions_status ON crm_inquiry_pipeline(status, due_date, updated_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_inquiry_action_logs_inquiry ON crm_inquiry_action_logs(inquiry_id, created_at)`).run();
}
async function requireUser(request,env,roles=READ_ROLES){
  await ensureSchema(env);
  const email=getAccessEmail(request);
  if(!email)return {ok:false,response:json({ok:false,message:"Login required"},401)};
  const user=await env.DB.prepare(`SELECT email,role,status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  if(!user)return {ok:false,response:json({ok:false,message:"User is not allowed"},403)};
  if(roles.length&&!roles.includes(user.role||""))return {ok:false,response:json({ok:false,message:"Permission denied"},403)};
  return {ok:true,email,user};
}
function inquiryIdFromPath(path,tail){const m=path.match(new RegExp(`^/api/inquiry-pipeline/([^/]+)/${tail}$`));return m?decodeURIComponent(m[1]):"";}
async function getInquiry(env,id){return await safeFirst(env,`SELECT * FROM crm_inquiry_pipeline WHERE id=? AND COALESCE(deleted_at,'')='' LIMIT 1`,[id]);}
async function getCustomer(env,id,name){
  if(id){const c=await safeFirst(env,`SELECT * FROM customers WHERE id=? LIMIT 1`,[id]);if(c)return c;}
  if(name){const c=await safeFirst(env,`SELECT * FROM customers WHERE name=? LIMIT 1`,[name]);if(c)return c;}
  return {};
}
function buildInquiryLine(inquiry,customer,kind){
  const name=text(inquiry.customer_name||customer.name||"お客様");
  const genre=text(inquiry.genre||"撮影");
  const date=text(inquiry.preferred_date||"");
  const place=text(inquiry.place||"");
  if(kind==="schedule")return `${name}様\nお問い合わせありがとうございます。${genre}の撮影日程について、${date?`ご希望の${date}前後で`:"ご希望時期に合わせて"}空き状況を確認いたします。${place?`場所は${place}で確認します。`:""}\nご希望のお時間帯もお知らせください。`;
  if(kind==="price")return `${name}様\n${genre}の料金についてご案内します。プラン内容・撮影場所・納品枚数により変わりますので、まずはご希望内容を確認させてください。`;
  if(kind==="reply_wait")return `${name}様\n先日ご案内した${genre}の件ですが、その後いかがでしょうか？日程や料金だけのご相談でも大丈夫ですので、お気軽にご返信ください。`;
  return `${name}様\nお問い合わせありがとうございます。${genre}について、日程・場所・料金など気になる点をこちらで整理してご案内します。`;
}
async function logAction(env,inquiry,actionType,targetId,beforeStatus,afterStatus,detail,actor){
  await env.DB.prepare(`INSERT INTO crm_inquiry_action_logs(inquiry_id,action_type,target_id,customer_id,customer_name,before_status,after_status,detail_json,actor_email,created_at) VALUES(?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`).bind(text(inquiry.id),actionType,targetId,text(inquiry.customer_id),text(inquiry.customer_name),beforeStatus,afterStatus,JSON.stringify(detail||{}),actor).run();
}
async function createLineFromInquiryApi(request,env,inquiryId){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  const body=await readJson(request);
  const inquiry=await getInquiry(env,inquiryId); if(!inquiry)return json({ok:false,message:"inquiry not found"},404);
  const customer=await getCustomer(env,inquiry.customer_id,inquiry.customer_name);
  const kind=text(body.kind||"default");
  const message=text(body.message_body)||buildInquiryLine(inquiry,customer,kind);
  const id=`line-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
  await env.DB.prepare(`INSERT INTO customer_line_draft_logs(id,customer_id,customer_name,action_type,action_label,message_body,status,priority,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`).bind(id,text(inquiry.customer_id||customer.id),text(inquiry.customer_name||customer.name),`inquiry_${kind}`,`問い合わせ：${text(inquiry.status||"対応")}`,message,"pending","high",auth.email).run();
  const before=text(inquiry.status);
  const after=kind==="reply_wait"?"返信待ち":(before==="問い合わせ"?"日程調整中":before);
  await env.DB.prepare(`UPDATE crm_inquiry_pipeline SET line_log_id=?, status=?, next_action=?, action_summary=?, updated_by=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(id,after,"LINE送信待ち",`LINE文面作成: ${kind}`,auth.email,inquiryId).run();
  await logAction(env,inquiry,"line_draft",id,before,after,{kind,message},auth.email);
  return json({ok:true,build:BUILD,line_log_id:id,status:after,message_body:message});
}
async function createFollowFromInquiryApi(request,env,inquiryId){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  const body=await readJson(request);
  const inquiry=await getInquiry(env,inquiryId); if(!inquiry)return json({ok:false,message:"inquiry not found"},404);
  const id=`ft-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
  const due=text(body.due_date)||addDays(todayJst(),1);
  const title=text(body.title)||`問い合わせフォロー：${text(inquiry.genre||"撮影")}`;
  await env.DB.prepare(`INSERT INTO crm_follow_tasks(id,customer_id,customer_name,task_type,title,due_date,priority,status,memo,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`).bind(id,text(inquiry.customer_id),text(inquiry.customer_name),"inquiry_follow",title,due,text(body.priority||"high"),"open",text(body.memo||inquiry.memo||"問い合わせから作成"),auth.email).run();
  const before=text(inquiry.status);
  const after=text(body.next_status)||"返信待ち";
  await env.DB.prepare(`UPDATE crm_inquiry_pipeline SET follow_task_id=?, status=?, next_action=?, due_date=?, action_summary=?, updated_by=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(id,after,title,due,"フォロー予定作成",auth.email,inquiryId).run();
  await logAction(env,inquiry,"follow_task",id,before,after,{title,due},auth.email);
  return json({ok:true,build:BUILD,follow_task_id:id,status:after,due_date:due});
}
async function createReservationDraftFromInquiryApi(request,env,inquiryId){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  const body=await readJson(request);
  const inquiry=await getInquiry(env,inquiryId); if(!inquiry)return json({ok:false,message:"inquiry not found"},404);
  const id=`rd-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
  await env.DB.prepare(`INSERT INTO crm_reservation_drafts(id,customer_id,customer_name,genre,shoot_date,start_time,place,plan_label,total_amount,status,memo,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?, ?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`).bind(id,text(inquiry.customer_id),text(inquiry.customer_name),text(body.genre||inquiry.genre),text(body.shoot_date||inquiry.preferred_date),text(body.start_time),text(body.place||inquiry.place),text(body.plan_label||inquiry.plan_label),num(body.total_amount||inquiry.estimated_amount),"draft",text(body.memo||inquiry.memo||"問い合わせから予約下書き作成"),auth.email).run();
  const before=text(inquiry.status);
  const after="仮予約";
  await env.DB.prepare(`UPDATE crm_inquiry_pipeline SET reservation_draft_id=?, status=?, next_action=?, action_summary=?, updated_by=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(id,after,"予約下書き確認",`予約下書き作成: ${id}`,auth.email,inquiryId).run();
  await logAction(env,inquiry,"reservation_draft",id,before,after,{draft_id:id},auth.email);
  return json({ok:true,build:BUILD,reservation_draft_id:id,status:after});
}
async function inquiryActionLogsApi(request,env,inquiryId){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok)return auth.response;
  const rows=await safeAll(env,`SELECT * FROM crm_inquiry_action_logs WHERE inquiry_id=? ORDER BY datetime(created_at) DESC LIMIT 50`,[inquiryId]);
  return json({ok:true,build:BUILD,items:rows});
}
function injectUi(html){
  if(!html||html.includes("crmInquiryActionScript"))return html;
  const style=`<style id="crmInquiryActionStyle">
.crm-inquiry-action-dock{position:fixed;left:18px;bottom:96px;z-index:1000003;display:flex;flex-direction:column;gap:8px}.crm-inquiry-action-dock button{border:0;background:#7c3aed;color:#fff;border-radius:999px;padding:10px 13px;font-weight:950;box-shadow:0 10px 24px rgba(124,58,237,.24);cursor:pointer}.crm-inq-modal{position:fixed;inset:0;background:rgba(15,23,42,.42);z-index:1000008;display:flex;align-items:center;justify-content:center;padding:14px}.crm-inq-box{background:#fff;border-radius:18px;max-width:760px;width:100%;max-height:88vh;overflow:auto;padding:16px;box-shadow:0 30px 80px rgba(0,0,0,.24);font-family:inherit;color:#111827}.crm-inq-box h2{margin:0 0 8px;font-size:20px}.crm-inq-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}.crm-inq-box input,.crm-inq-box select,.crm-inq-box textarea{width:100%;border:1px solid #ddd6fe;border-radius:10px;padding:9px;font-size:13px;box-sizing:border-box}.crm-inq-box textarea{min-height:90px}.crm-inq-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.crm-inq-actions button{border:0;border-radius:10px;padding:9px 11px;font-weight:950;cursor:pointer;background:#7c3aed;color:#fff}.crm-inq-actions button.secondary{background:#f3f4f6;color:#111827}@media(max-width:760px){.crm-inq-grid{grid-template-columns:1fr}.crm-inquiry-action-dock{left:10px;right:10px;bottom:90px}.crm-inquiry-action-dock button{width:100%}}
</style>`;
  const script=`<script id="crmInquiryActionScript">
(function(){
 if(window.__crmInquiryActionInstalled)return;window.__crmInquiryActionInstalled=true;
 function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
 function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
 function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000010;background:#4c1d95;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
 function modal(id){var html='<div class="crm-inq-modal" id="crmInqModal"><div class="crm-inq-box"><h2>問い合わせから次アクション作成</h2><p>問い合わせIDを指定して、LINE文面・フォロー予定・予約下書きを作成します。</p><div class="crm-inq-grid"><label>問い合わせID<input id="crmInqId" value="'+esc(id||'')+'"></label><label>LINE種類<select id="crmInqLineKind"><option value="default">通常返信</option><option value="schedule">日程調整</option><option value="price">料金案内</option><option value="reply_wait">返信待ち</option></select></label><label>フォロー期日<input id="crmInqDue" type="date"></label><label>フォロータイトル<input id="crmInqTitle" placeholder="問い合わせフォロー"></label><label>撮影日<input id="crmInqShoot" type="date"></label><label>開始時間<input id="crmInqStart" type="time"></label><label>場所<input id="crmInqPlace" placeholder="神社・公園など"></label><label>金額<input id="crmInqAmount" type="number" placeholder="24800"></label></div><label>LINE文面・メモ<textarea id="crmInqBody" placeholder="空欄なら自動生成します"></textarea></label><div class="crm-inq-actions"><button id="crmInqCreateLine">LINE未送信に保存</button><button id="crmInqCreateFollow">フォロー予定作成</button><button id="crmInqCreateDraft">予約下書き作成</button><button id="crmInqClose" class="secondary">閉じる</button></div></div></div>';document.body.insertAdjacentHTML('beforeend',html);}
 function val(id){var e=document.getElementById(id);return e?e.value:'';}
 async function run(kind){var id=val('crmInqId');if(!id){toast('問い合わせIDを入力してください');return;}var url='',body={};if(kind==='line'){url='/api/inquiry-pipeline/'+encodeURIComponent(id)+'/create-line-draft';body={kind:val('crmInqLineKind'),message_body:val('crmInqBody')}}if(kind==='follow'){url='/api/inquiry-pipeline/'+encodeURIComponent(id)+'/create-follow-task';body={due_date:val('crmInqDue'),title:val('crmInqTitle'),memo:val('crmInqBody')}}if(kind==='draft'){url='/api/inquiry-pipeline/'+encodeURIComponent(id)+'/create-reservation-draft';body={shoot_date:val('crmInqShoot'),start_time:val('crmInqStart'),place:val('crmInqPlace'),total_amount:val('crmInqAmount'),memo:val('crmInqBody')}}var d=await api(url,{method:'POST',body:JSON.stringify(body)});if(!d.ok){toast('失敗：'+(d.message||d.status||'unknown'));return;}toast('作成しました');}
 function install(){if(document.getElementById('crmInquiryActionDock'))return;document.body.insertAdjacentHTML('beforeend','<div id="crmInquiryActionDock" class="crm-inquiry-action-dock"><button id="crmInquiryActionOpen">問い合わせ→次アクション</button></div>')}
 document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmInquiryActionOpen')modal('');if(t.id==='crmInqClose'){var m=document.getElementById('crmInqModal');if(m)m.remove();}if(t.id==='crmInqCreateLine')run('line');if(t.id==='crmInqCreateFollow')run('follow');if(t.id==='crmInqCreateDraft')run('draft');});
 document.addEventListener('DOMContentLoaded',install);setTimeout(install,1200);
})();
</script>`;
  return html.replace("</head>",`${style}</head>`).replace("</body>",`${script}</body>`);
}
export default {
 async fetch(request,env,ctx){
  const url=new URL(request.url);const path=url.pathname;
  if((path==="/"||path==="/health"||path==="/api/health")&&request.method==="GET")return json({ok:true,service:"customer-crm-api",build:BUILD,time:nowIso()});
  const lineId=inquiryIdFromPath(path,"create-line-draft"); if(lineId&&request.method==="POST")return createLineFromInquiryApi(request,env,lineId);
  const followId=inquiryIdFromPath(path,"create-follow-task"); if(followId&&request.method==="POST")return createFollowFromInquiryApi(request,env,followId);
  const draftId=inquiryIdFromPath(path,"create-reservation-draft"); if(draftId&&request.method==="POST")return createReservationDraftFromInquiryApi(request,env,draftId);
  const logsId=inquiryIdFromPath(path,"action-logs"); if(logsId&&request.method==="GET")return inquiryActionLogsApi(request,env,logsId);
  const res=await app.fetch(request,env,ctx);const type=res.headers.get("content-type")||"";
  if(type.includes("text/html")){const body=injectUi(await res.text());return new Response(body,{status:res.status,statusText:res.statusText,headers:securityHeaders(res.headers)});}
  return new Response(res.body,{status:res.status,statusText:res.statusText,headers:securityHeaders(res.headers)});
 }
};
