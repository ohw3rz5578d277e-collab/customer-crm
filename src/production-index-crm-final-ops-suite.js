// ======================================================
// CUSTOMER CRM / FINAL OPS SUITE WRAPPER
// build: customer-crm-api-final-ops-suite-20260613-01
// Adds: inquiry shortcut send, unified ops board, customer summary UI, bulk LINE drafts for repeat/dormant candidates, cancel follow-up support.
// ======================================================

import app from "./production-index-crm-inquiry-row-actions.js";

const BUILD = "customer-crm-api-final-ops-suite-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){return v===undefined||v===null?"":String(v).trim();}
function lower(v){return text(v).toLowerCase();}
function num(v){const n=Number(v||0);return Number.isFinite(n)?n:0;}
function now(){return new Date().toISOString();}
function todayJst(){return new Date(Date.now()+9*60*60*1000).toISOString().slice(0,10);}
function addDays(dateText,days){const base=text(dateText)||todayJst();const d=new Date(`${base.slice(0,10)}T00:00:00+09:00`);d.setDate(d.getDate()+Number(days||0));return new Date(d.getTime()+9*60*60*1000).toISOString().slice(0,10);}
function securityHeaders(headers={}){const h=new Headers(headers);h.set("cache-control","no-store, no-cache, must-revalidate, max-age=0");h.set("pragma","no-cache");h.set("expires","0");h.set("x-content-type-options","nosniff");h.set("referrer-policy","no-referrer");return h;}
function json(data,status=200){return new Response(JSON.stringify(data,null,2),{status,headers:securityHeaders({"content-type":"application/json; charset=utf-8"})});}
async function readJson(request){try{return await request.json();}catch(_){return {};}}
function getAccessEmail(request){return lower(request.headers.get("cf-access-authenticated-user-email")||request.headers.get("Cf-Access-Authenticated-User-Email")||request.headers.get("x-user-email")||"");}
async function addColumn(db,table,definition){try{await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();}catch(_){}}
async function safeAll(env,sql,bindings=[]){try{const s=env.DB.prepare(sql);const r=bindings.length?await s.bind(...bindings).all():await s.all();return r.results||[];}catch(_){return [];}}
async function safeFirst(env,sql,bindings=[]){try{const s=env.DB.prepare(sql);return bindings.length?await s.bind(...bindings).first():await s.first();}catch(_){return null;}}
async function ensureSchema(env){
  if(!env.DB)throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY,role TEXT,status TEXT,created_by TEXT,created_at TEXT,updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email,role,status,created_by,created_at,updated_at) VALUES(?,'admin','active','system',datetime('now'),datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_final_ops_logs(id INTEGER PRIMARY KEY AUTOINCREMENT,action_type TEXT,target_type TEXT,target_id TEXT,customer_id TEXT,customer_name TEXT,result TEXT,detail_json TEXT,actor_email TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP)`).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_cancel_followups(id TEXT PRIMARY KEY,customer_id TEXT,customer_name TEXT,reservation_id TEXT,cancel_reason TEXT,follow_status TEXT DEFAULT 'open',follow_due_date TEXT,line_log_id TEXT,created_by TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP,updated_at TEXT DEFAULT CURRENT_TIMESTAMP,deleted_at TEXT)`).run();
  for(const c of ["line_log_id TEXT","follow_task_id TEXT","reservation_draft_id TEXT","converted_at TEXT","action_summary TEXT","last_action_at TEXT"]) await addColumn(env.DB,"crm_inquiry_pipeline",c);
  for(const c of ["next_event_suggestion TEXT","next_line_suggestion TEXT","customer_rank TEXT","customer_rank_reason TEXT","customer_rank_updated_at TEXT"]) await addColumn(env.DB,"customers",c);
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_final_ops_logs ON crm_final_ops_logs(action_type, created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_cancel_followups ON crm_cancel_followups(follow_status, follow_due_date, customer_id)`).run();
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
async function log(env, actor, type, targetType, targetId, detail={}, result="ok"){
  await env.DB.prepare(`INSERT INTO crm_final_ops_logs(action_type,target_type,target_id,customer_id,customer_name,result,detail_json,actor_email,created_at) VALUES(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`).bind(type,targetType,targetId,text(detail.customer_id),text(detail.customer_name),result,JSON.stringify(detail),actor||"system").run();
}
function inquiryId(path, suffix){const m=path.match(new RegExp(`^/api/inquiry-pipeline/([^/]+)/${suffix}$`));return m?decodeURIComponent(m[1]):"";}
function customerId(path,suffix){const m=path.match(new RegExp(`^/api/customers/([^/]+)/${suffix}$`));return m?decodeURIComponent(m[1]):"";}

async function callBaseJson(request, env, ctx, path, method="POST", body={}){
  const email=getAccessEmail(request)||ROOT_ADMIN_EMAIL;
  const url=new URL(request.url); url.pathname=path; url.search="";
  const res=await app.fetch(new Request(url.toString(),{method,headers:{"content-type":"application/json","x-user-email":email,"cf-access-authenticated-user-email":email},body:method==="GET"?undefined:JSON.stringify(body)}),env,ctx);
  let data={}; try{data=await res.json();}catch(_){data={ok:res.ok,status:res.status};}
  return {res,data};
}
async function inquiryCreateDraftAndSendApi(request, env, ctx, id){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  const body=await readJson(request);
  const made=await callBaseJson(request,env,ctx,`/api/inquiry-pipeline/${encodeURIComponent(id)}/create-reservation-draft`,"POST",body);
  if(!made.data.ok)return json({ok:false,step:"create_reservation_draft",result:made.data},made.res.status||400);
  const draftId=text(made.data.reservation_draft_id||made.data.draft_id||made.data.id||made.data.reservation_draft&&made.data.reservation_draft.id);
  if(!draftId)return json({ok:false,message:"reservation draft created but draft id was not returned",result:made.data},500);
  const sent=await callBaseJson(request,env,ctx,`/api/reservation-drafts/${encodeURIComponent(draftId)}/send-to-reservation`,"POST",{});
  await env.DB.prepare(`UPDATE crm_inquiry_pipeline SET reservation_draft_id=?, status='仮予約', last_action_at=CURRENT_TIMESTAMP, action_summary=? WHERE id=?`).bind(draftId,`予約下書き作成・予約管理へ送信`,id).run();
  await log(env,auth.email,"inquiry_draft_send","inquiry",id,{reservation_draft_id:draftId,send_result:sent.data});
  return json({ok:sent.res.ok, draft_id:draftId, created:made.data, sent:sent.data, message: sent.res.ok?"予約下書きを作成し、予約管理へ送信しました":"予約下書きは作成しましたが、予約管理送信に失敗しました"}, sent.res.ok?200:207);
}

function suggestEvent(row){const g=lower(row.genre_history||row.last_genre||"");const last=text(row.last_shoot_date);const days=last?Math.floor((new Date(`${todayJst()}T00:00:00+09:00`)-new Date(`${last.slice(0,10)}T00:00:00+09:00`))/86400000):0;if(g.includes("お宮")||g.includes("宮参り"))return "ハーフバースデー・1歳バースデー";if(g.includes("七五三"))return "入学・卒業 / 家族写真";if(g.includes("バースデ"))return "次回バースデー / 家族写真";if(days>=180)return "季節の家族写真 / 休眠掘り起こし";if(days>=90)return "季節の家族写真";return "家族写真・記念日撮影";}
function lineSuggestion(row){const rank=text(row.customer_rank);const last=text(row.last_shoot_date);const days=last?Math.floor((new Date(`${todayJst()}T00:00:00+09:00`)-new Date(`${last.slice(0,10)}T00:00:00+09:00`))/86400000):999;if(rank==="VIP")return "VIP向け先行案内";if(rank==="リピーター")return "リピーター向け次回提案";if(days>=180)return "休眠掘り起こしLINE";if(days>=90)return "季節の家族写真提案";return "通常フォローLINE";}
async function customerSummaryPlusApi(request,env,id){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok)return auth.response;
  const c=await safeFirst(env,`SELECT * FROM customers WHERE id=? AND COALESCE(deleted_at,'')='' LIMIT 1`,[id]);
  if(!c)return json({ok:false,message:"customer not found"},404);
  const reservations=await safeAll(env,`SELECT * FROM customer_reservations WHERE customer_id=? AND COALESCE(deleted_at,'')='' ORDER BY date(COALESCE(shoot_date,'1900-01-01')) DESC LIMIT 5`,[id]);
  const line=await safeFirst(env,`SELECT COUNT(*) AS n FROM customer_line_draft_logs WHERE customer_id=? AND COALESCE(status,'') NOT IN ('sent','送信済み')`,[id]);
  const follow=await safeFirst(env,`SELECT COUNT(*) AS n FROM crm_follow_tasks WHERE customer_id=? AND COALESCE(status,'') NOT IN ('completed','done','完了')`,[id]);
  const progress=await safeAll(env,`SELECT * FROM crm_reservation_progress WHERE customer_id=? AND COALESCE(deleted_at,'')='' AND progress_status!='完了' ORDER BY date(COALESCE(next_due_date,'2999-12-31')) ASC LIMIT 5`,[id]);
  const enriched={...c,next_event_suggestion:text(c.next_event_suggestion)||suggestEvent(c),next_line_suggestion:text(c.next_line_suggestion)||lineSuggestion(c)};
  return json({ok:true,build:BUILD,customer:enriched,counts:{unsent_line:num(line&&line.n),open_follow:num(follow&&follow.n),open_progress:progress.length},recent_reservations:reservations,progress});
}
async function bulkLineDraftsApi(request,env){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  const body=await readJson(request); const mode=lower(body.mode||"repeat"); const limit=Math.min(Math.max(num(body.limit||50),1),200);
  let rows=[];
  if(mode==="dormant") rows=await safeAll(env,`SELECT * FROM customers WHERE COALESCE(deleted_at,'')='' AND last_shoot_date IS NOT NULL AND date(last_shoot_date)<=date('now','-180 days') LIMIT ?`,[limit]);
  else rows=await safeAll(env,`SELECT * FROM customers WHERE COALESCE(deleted_at,'')='' AND COALESCE(repeat_count,0)>=1 AND last_shoot_date IS NOT NULL AND date(last_shoot_date)<=date('now','-90 days') LIMIT ?`,[limit]);
  const results=[];
  for(const c of rows){
    const title=mode==="dormant"?"休眠掘り起こしLINE":"リピーター提案LINE";
    const bodyText=mode==="dormant"?`${c.name||c.customer_name||"お客様"}様\nご無沙汰しております。季節の家族写真や記念日の撮影もおすすめです。気になる時期があればお気軽にご相談ください。`:`${c.name||c.customer_name||"お客様"}様\n前回の撮影から少し経ちましたので、次回の記念撮影もおすすめです。ご希望の時期があればお気軽にご相談ください。`;
    const id=`line-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
    await env.DB.prepare(`INSERT INTO customer_line_draft_logs(id,customer_id,customer_name,action_type,action_label,message_body,status,priority,created_by,created_at,updated_at) VALUES(?,?,?,?,?,'${bodyText.replace(/'/g,"''")}',?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`).bind(id,text(c.id),text(c.name||c.customer_name),mode,title,"draft",mode==="dormant"?"high":"medium",auth.email).run();
    results.push({id,customer_id:c.id,customer_name:c.name||c.customer_name});
  }
  await log(env,auth.email,"bulk_line_drafts",mode,"",{count:results.length,mode});
  return json({ok:true,mode,count:results.length,items:results});
}
async function cancelFollowupCreateApi(request,env){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  const body=await readJson(request); const customerId=text(body.customer_id); if(!customerId)return json({ok:false,message:"customer_id is required"},400);
  const c=await safeFirst(env,`SELECT * FROM customers WHERE id=? LIMIT 1`,[customerId])||{};
  const id=`cf-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
  const due=text(body.follow_due_date)||addDays(todayJst(),7);
  await env.DB.prepare(`INSERT INTO crm_cancel_followups(id,customer_id,customer_name,reservation_id,cancel_reason,follow_status,follow_due_date,created_by,created_at,updated_at) VALUES(?,?,?,?,?,'open',?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`).bind(id,customerId,text(c.name||body.customer_name),text(body.reservation_id),text(body.cancel_reason||body.reason),due,auth.email).run();
  await log(env,auth.email,"cancel_followup_create","customer",customerId,{customer_id:customerId,customer_name:c.name||body.customer_name,cancel_followup_id:id});
  return json({ok:true,id,follow_due_date:due});
}
async function unifiedOpsApi(request,env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok)return auth.response;
  const url=new URL(request.url); const limit=Math.min(Math.max(num(url.searchParams.get("limit")||100),1),300);
  const logs=await safeAll(env,`SELECT 'final_ops' AS source, action_type, target_type, target_id, customer_id, customer_name, result, detail_json, actor_email, created_at FROM crm_final_ops_logs UNION ALL SELECT 'inquiry' AS source, action_type, 'inquiry' AS target_type, inquiry_id AS target_id, customer_id, customer_name, 'ok' AS result, detail_json, actor_email, created_at FROM crm_inquiry_action_logs ORDER BY datetime(created_at) DESC LIMIT ?`,[limit]);
  return json({ok:true,build:BUILD,items:logs});
}

function injectFinalUi(html){
  if(!html||html.includes("crmFinalOpsScript"))return html;
  const css=`<style id="crmFinalOpsStyle">.crm-final-fab{position:fixed;left:14px;bottom:180px;z-index:1000004;display:flex;flex-direction:column;gap:7px}.crm-final-fab button{border:0;border-radius:999px;background:#111827;color:#fff;padding:9px 12px;font-size:12px;font-weight:900;box-shadow:0 10px 25px rgba(0,0,0,.18);cursor:pointer}.crm-final-modal{position:fixed;inset:0;background:rgba(15,23,42,.52);z-index:1000009;display:none;align-items:center;justify-content:center;padding:18px}.crm-final-box{background:#fff;max-width:980px;width:min(980px,96vw);max-height:88vh;overflow:auto;border-radius:18px;padding:14px;color:#0f172a}.crm-final-head{display:flex;justify-content:space-between;gap:10px;align-items:center}.crm-final-box textarea,.crm-final-box input,.crm-final-box select{width:100%;box-sizing:border-box;border:1px solid #cbd5e1;border-radius:10px;padding:8px;margin:4px 0;font-size:13px}.crm-final-row{border:1px solid #e2e8f0;border-radius:12px;padding:10px;margin:8px 0;font-size:13px}.crm-final-row button,.crm-final-box button{border:0;border-radius:10px;background:#2563eb;color:#fff;padding:7px 9px;font-size:12px;font-weight:900;margin:3px;cursor:pointer}.crm-final-close{background:#64748b!important}.crm-rank-badge{display:inline-block;border-radius:999px;background:#dcfce7;color:#166534;font-weight:900;font-size:11px;padding:3px 7px;margin-left:4px}</style>`;
  const js=`<script id="crmFinalOpsScript">(function(){if(window.__crmFinalOps)return;window.__crmFinalOps=true;function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}function api(u,o){return fetch(u,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},o||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}function toast(m){var d=document.createElement('div');d.textContent=m;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000010;background:#111827;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900';document.body.appendChild(d);setTimeout(function(){d.remove()},2500)}function open(t,b){var m=document.getElementById('crmFinalModal');if(!m){document.body.insertAdjacentHTML('beforeend','<div id="crmFinalModal" class="crm-final-modal"><div class="crm-final-box"><div class="crm-final-head"><h2 id="crmFinalTitle"></h2><button class="crm-final-close" id="crmFinalClose">閉じる</button></div><div id="crmFinalBody"></div></div></div>');m=document.getElementById('crmFinalModal');}document.getElementById('crmFinalTitle').textContent=t;document.getElementById('crmFinalBody').innerHTML=b;m.style.display='flex'}function close(){var m=document.getElementById('crmFinalModal');if(m)m.style.display='none'}async function ops(){var d=await api('/api/final-ops/logs');open('統合操作ログ',(d.items||[]).map(function(x){return '<div class="crm-final-row"><b>'+esc(x.action_type)+'</b> '+esc(x.customer_name||'')+'<br><small>'+esc(x.actor_email||'')+' / '+esc(x.created_at||'')+'</small></div>'}).join('')||'ログなし')}async function bulk(mode){var d=await api('/api/final-ops/bulk-line-drafts',{method:'POST',body:JSON.stringify({mode:mode,limit:50})});toast(d.ok?'未送信LINEを作成しました: '+d.count:'失敗')}function cancelFollow(){open('キャンセル後フォロー作成','<input id="cfCustomer" placeholder="顧客ID"><input id="cfReservation" placeholder="予約ID 任意"><input id="cfReason" placeholder="キャンセル理由"><button id="cfCreate">作成</button>')}async function createCf(){var d=await api('/api/final-ops/cancel-followups',{method:'POST',body:JSON.stringify({customer_id:document.getElementById('cfCustomer').value,reservation_id:document.getElementById('cfReservation').value,cancel_reason:document.getElementById('cfReason').value})});toast(d.ok?'作成しました':'失敗');if(d.ok)close()}function install(){if(document.getElementById('crmFinalFab'))return;document.body.insertAdjacentHTML('beforeend','<div id="crmFinalFab" class="crm-final-fab"><button id="finalOpsLogs">統合ログ</button><button id="finalBulkRepeat">リピーターLINE一括</button><button id="finalBulkDormant">休眠LINE一括</button><button id="finalCancelFollow">キャンセル後フォロー</button></div>')}document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmFinalClose')close();if(t.id==='finalOpsLogs')ops();if(t.id==='finalBulkRepeat')bulk('repeat');if(t.id==='finalBulkDormant')bulk('dormant');if(t.id==='finalCancelFollow')cancelFollow();if(t.id==='cfCreate')createCf();});document.addEventListener('DOMContentLoaded',install);setTimeout(install,1200);})();</script>`;
  return html.replace("</head>",css+"</head>").replace("</body>",js+"</body>");
}

export default {async fetch(request,env,ctx){const url=new URL(request.url),path=url.pathname;if((path==="/"||path==="/health"||path==="/api/health")&&request.method==="GET")return json({ok:true,service:"customer-crm-api",build:BUILD,time:now()});const sendId=inquiryId(path,"create-reservation-draft-and-send");if(sendId&&request.method==="POST")return inquiryCreateDraftAndSendApi(request,env,ctx,sendId);const sumId=customerId(path,"summary-plus-final");if(sumId&&request.method==="GET")return customerSummaryPlusApi(request,env,sumId);if(path==="/api/final-ops/bulk-line-drafts"&&request.method==="POST")return bulkLineDraftsApi(request,env);if(path==="/api/final-ops/cancel-followups"&&request.method==="POST")return cancelFollowupCreateApi(request,env);if(path==="/api/final-ops/logs"&&request.method==="GET")return unifiedOpsApi(request,env);const res=await app.fetch(request,env,ctx);const type=res.headers.get("content-type")||"";if(type.includes("text/html")){const body=injectFinalUi(await res.text());return new Response(body,{status:res.status,statusText:res.statusText,headers:securityHeaders(res.headers)});}return new Response(res.body,{status:res.status,statusText:res.statusText,headers:securityHeaders(res.headers)});}};
