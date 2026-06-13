// ======================================================
// CUSTOMER CRM / FOLLOW TASK -> LINE TEMPLATE WRAPPER
// build: customer-crm-api-follow-template-20260613-01
// Adds follow-task template suggestions, rendering, and draft saving.
// ======================================================

import app from "./production-index-crm-growth-suite-actions.js";

const BUILD = "customer-crm-api-follow-template-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){return v===undefined||v===null?"":String(v).trim();}
function lower(v){return text(v).toLowerCase();}
function securityHeaders(headers={}){const h=new Headers(headers);h.set("cache-control","no-store, no-cache, must-revalidate, max-age=0");h.set("pragma","no-cache");h.set("expires","0");h.set("x-content-type-options","nosniff");h.set("referrer-policy","no-referrer");return h;}
function json(data,status=200){return new Response(JSON.stringify(data,null,2),{status,headers:securityHeaders({"content-type":"application/json; charset=utf-8"})});}
async function readJson(request){try{return await request.json();}catch(_){return {};}}
function getAccessEmail(request){return lower(request.headers.get("cf-access-authenticated-user-email")||request.headers.get("Cf-Access-Authenticated-User-Email")||request.headers.get("x-user-email")||"");}
async function addColumn(db,table,definition){try{await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();}catch(_){}}
async function ensureSchema(env){
  if(!env.DB)throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY,role TEXT,status TEXT,created_by TEXT,created_at TEXT,updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email,role,status,created_by,created_at,updated_at) VALUES(?,'admin','active','system',datetime('now'),datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_line_templates(id TEXT PRIMARY KEY,name TEXT NOT NULL,category TEXT,genre TEXT,body TEXT NOT NULL,variables_json TEXT,status TEXT DEFAULT 'active',usage_count INTEGER DEFAULT 0,created_by TEXT,updated_by TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP,updated_at TEXT DEFAULT CURRENT_TIMESTAMP,deleted_at TEXT,deleted_by TEXT)`).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS customer_line_draft_logs(id TEXT PRIMARY KEY,customer_id TEXT,customer_name TEXT,action_type TEXT,action_label TEXT,draft_body TEXT,status TEXT,priority TEXT,created_by TEXT,sent_by TEXT,sent_at TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP,updated_at TEXT DEFAULT CURRENT_TIMESTAMP)`).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_follow_template_logs(id INTEGER PRIMARY KEY AUTOINCREMENT,follow_task_id TEXT,template_id TEXT,line_log_id TEXT,customer_id TEXT,customer_name TEXT,rendered_body TEXT,actor_email TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP)`).run();
  for(const col of ["template_id TEXT","template_name TEXT","line_log_id TEXT","line_draft_created_at TEXT"]) await addColumn(env.DB,"crm_follow_tasks",col);
  for(const col of ["template_id TEXT","follow_task_id TEXT","template_name TEXT"]) await addColumn(env.DB,"customer_line_draft_logs",col);
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_follow_template_logs_task ON crm_follow_template_logs(follow_task_id, created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_line_draft_logs_follow ON customer_line_draft_logs(follow_task_id, status, created_at)`).run();
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
function taskIdFromPath(path,tail){const m=path.match(new RegExp(`^/api/follow-tasks/([^/]+)/${tail}$`));return m?decodeURIComponent(m[1]):"";}
function renderTemplate(body,data){return text(body).replace(/\{\{\s*([a-zA-Z0-9_\-.]+)\s*\}\}/g,(_,k)=>text((data||{})[k]||""));}
function categoryForTask(task){
  const title=lower(`${task.task_type||""} ${task.title||""}`);
  if(title.includes("口コミ")||title.includes("review"))return "撮影後";
  if(title.includes("先行")||title.includes("納品")||title.includes("preview"))return "納品";
  if(title.includes("次回")||title.includes("repeat")||title.includes("提案"))return "リピート";
  if(title.includes("お礼")||title.includes("thanks"))return "撮影後";
  return "汎用";
}
function keywordForTask(task){
  const title=lower(`${task.task_type||""} ${task.title||""}`);
  if(title.includes("口コミ")||title.includes("review"))return "口コミ";
  if(title.includes("先行")||title.includes("preview"))return "先行";
  if(title.includes("次回")||title.includes("repeat")||title.includes("提案"))return "次回";
  if(title.includes("お礼")||title.includes("thanks"))return "お礼";
  return "";
}
async function getTask(env,taskId){return await env.DB.prepare(`SELECT * FROM crm_follow_tasks WHERE id=? LIMIT 1`).bind(taskId).first();}
async function getCustomer(env,customerId){return customerId?await env.DB.prepare(`SELECT * FROM customers WHERE id=? LIMIT 1`).bind(customerId).first():null;}
async function followTemplateSuggestApi(request,env,taskId){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok)return auth.response;
  const task=await getTask(env,taskId); if(!task)return json({ok:false,message:"follow task not found"},404);
  const customer=await getCustomer(env,task.customer_id)||{};
  const category=categoryForTask(task); const kw=keywordForTask(task);
  let templates=await env.DB.prepare(`SELECT * FROM crm_line_templates WHERE COALESCE(deleted_at,'')='' AND status='active' AND (category=? OR ?='') ORDER BY usage_count DESC, updated_at DESC LIMIT 20`).bind(category,category).all();
  let rows=templates.results||[];
  if(kw) rows=rows.sort((a,b)=>(lower(a.name).includes(kw)?-1:0)-(lower(b.name).includes(kw)?-1:0));
  return json({ok:true,build:BUILD,task,customer,category,keyword:kw,templates:rows});
}
function templateData(task,customer){
  return {
    customer_name:text(customer.name||task.customer_name),
    task_title:text(task.title),
    due_date:text(task.due_date),
    customer_rank:text(customer.customer_rank),
    last_shoot_date:text(customer.last_shoot_date),
    total_revenue:text(customer.total_revenue),
    repeat_count:text(customer.repeat_count)
  };
}
async function followTemplateRenderApi(request,env,taskId){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok)return auth.response;
  const body=await readJson(request);
  const task=await getTask(env,taskId); if(!task)return json({ok:false,message:"follow task not found"},404);
  const tplId=text(body.template_id); if(!tplId)return json({ok:false,message:"template_id is required"},400);
  const tpl=await env.DB.prepare(`SELECT * FROM crm_line_templates WHERE id=? AND COALESCE(deleted_at,'')='' LIMIT 1`).bind(tplId).first();
  if(!tpl)return json({ok:false,message:"template not found"},404);
  const customer=await getCustomer(env,task.customer_id)||{};
  const rendered=renderTemplate(tpl.body,{...templateData(task,customer),...(body.data||{})});
  return json({ok:true,build:BUILD,task,template:tpl,rendered});
}
async function followTemplateSaveDraftApi(request,env,taskId){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  const body=await readJson(request);
  const task=await getTask(env,taskId); if(!task)return json({ok:false,message:"follow task not found"},404);
  const tplId=text(body.template_id); if(!tplId)return json({ok:false,message:"template_id is required"},400);
  const tpl=await env.DB.prepare(`SELECT * FROM crm_line_templates WHERE id=? AND COALESCE(deleted_at,'')='' LIMIT 1`).bind(tplId).first();
  if(!tpl)return json({ok:false,message:"template not found"},404);
  const customer=await getCustomer(env,task.customer_id)||{};
  const rendered=text(body.rendered_body)||renderTemplate(tpl.body,{...templateData(task,customer),...(body.data||{})});
  const logId=`line-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
  await env.DB.prepare(`INSERT INTO customer_line_draft_logs(id,customer_id,customer_name,action_type,action_label,draft_body,status,priority,created_by,template_id,template_name,follow_task_id,created_at,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`).bind(logId,text(task.customer_id),text(customer.name||task.customer_name),text(task.task_type||"follow_template"),text(tpl.name),rendered,"pending",text(task.priority||"medium"),auth.email,tpl.id,tpl.name,task.id).run();
  await env.DB.prepare(`UPDATE crm_follow_tasks SET template_id=?,template_name=?,line_log_id=?,line_draft_created_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(tpl.id,tpl.name,logId,task.id).run();
  await env.DB.prepare(`UPDATE crm_line_templates SET usage_count=COALESCE(usage_count,0)+1,updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(tpl.id).run();
  await env.DB.prepare(`INSERT INTO crm_follow_template_logs(follow_task_id,template_id,line_log_id,customer_id,customer_name,rendered_body,actor_email,created_at) VALUES(?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`).bind(task.id,tpl.id,logId,text(task.customer_id),text(customer.name||task.customer_name),rendered,auth.email).run();
  return json({ok:true,build:BUILD,line_log_id:logId,template_id:tpl.id,rendered_body:rendered});
}
function injectFollowTemplateUi(html){
 if(!html||html.includes("crmFollowTemplateScript"))return html;
 const style=`<style id="crmFollowTemplateStyle">
.crm-follow-template-fab{position:fixed;left:16px;bottom:16px;z-index:1000004;background:#0f766e;color:#fff;border:0;border-radius:999px;padding:10px 13px;font-weight:950;box-shadow:0 10px 26px rgba(15,118,110,.25);cursor:pointer}.crm-follow-template-panel{position:fixed;left:16px;bottom:64px;z-index:1000004;width:min(440px,calc(100vw - 32px));max-height:70vh;overflow:auto;background:#fff;border:1px solid #99f6e4;border-radius:18px;padding:12px;box-shadow:0 18px 50px rgba(0,0,0,.18);display:none;color:#0f172a}.crm-follow-template-panel.open{display:block}.crm-follow-template-panel h3{margin:0 0 6px;font-size:16px}.crm-follow-template-panel input{width:100%;box-sizing:border-box;border:1px solid #99f6e4;border-radius:10px;padding:8px;margin:6px 0}.crm-follow-template-panel button{border:0;background:#0f766e;color:#fff;border-radius:10px;padding:7px 9px;font-size:12px;font-weight:900;cursor:pointer;margin:3px}.crm-follow-template-row{border:1px solid #ccfbf1;border-radius:12px;padding:8px;margin:7px 0;font-size:12px}.crm-follow-template-body{white-space:pre-wrap;background:#f8fafc;border-radius:10px;padding:8px;margin-top:6px;color:#334155}
</style>`;
 const script=`<script id="crmFollowTemplateScript">
(function(){
 if(window.__crmFollowTemplateInstalled)return;window.__crmFollowTemplateInstalled=true;
 function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
 function api(u,o){return fetch(u,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},o||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
 function toast(m){var d=document.createElement('div');d.textContent=m;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000008;background:#0f766e;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900';document.body.appendChild(d);setTimeout(function(){d.remove()},2400)}
 function html(){return '<button id="crmFollowTemplateFab" class="crm-follow-template-fab">フォロー→LINE</button><div id="crmFollowTemplatePanel" class="crm-follow-template-panel"><h3>フォロー予定からLINE文面作成</h3><div style="font-size:12px;color:#64748b">フォロー予定IDを入れると、おすすめテンプレを表示してLINE文面として保存できます。</div><input id="crmFollowTaskId" placeholder="フォロー予定ID"><button id="crmFollowTemplateSearch">テンプレ検索</button><div id="crmFollowTemplateResult"></div></div>'}
 async function search(){var id=(document.getElementById('crmFollowTaskId')||{}).value||'';if(!id){toast('フォロー予定IDを入力してください');return;}var d=await api('/api/follow-tasks/'+encodeURIComponent(id)+'/template-suggestions');var out=document.getElementById('crmFollowTemplateResult');if(!d.ok){out.innerHTML='<div class="crm-follow-template-row">取得失敗：'+esc(d.message||d.status)+'</div>';return;}out.innerHTML=(d.templates||[]).map(function(t){return '<div class="crm-follow-template-row"><b>'+esc(t.name)+'</b><div>'+esc(t.category||'')+'</div><div class="crm-follow-template-body">'+esc(t.body)+'</div><button data-fttpl="'+esc(t.id)+'" data-ftid="'+esc(id)+'">この文面で保存</button></div>'}).join('')||'<div class="crm-follow-template-row">テンプレがありません。先に「テンプレ初期作成」を押してください。</div>';}
 async function save(taskId,tplId){var d=await api('/api/follow-tasks/'+encodeURIComponent(taskId)+'/line-draft-from-template',{method:'POST',body:JSON.stringify({template_id:tplId})});if(!d.ok){toast('保存失敗：'+(d.message||d.status));return;}toast('LINE文面として保存しました');}
 function install(){if(document.getElementById('crmFollowTemplateFab'))return;document.body.insertAdjacentHTML('beforeend',html());}
 document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmFollowTemplateFab')document.getElementById('crmFollowTemplatePanel').classList.toggle('open');if(t.id==='crmFollowTemplateSearch')search();var tpl=t.getAttribute&&t.getAttribute('data-fttpl');if(tpl)save(t.getAttribute('data-ftid'),tpl);});
 document.addEventListener('DOMContentLoaded',install);setTimeout(install,1200);
})();
</script>`;
 return html.replace("</head>",`${style}</head>`).replace("</body>",`${script}</body>`);
}

export default {
 async fetch(request,env,ctx){
  const url=new URL(request.url); const path=url.pathname;
  if((path==="/"||path==="/health"||path==="/api/health")&&request.method==="GET")return json({ok:true,service:"customer-crm-api",build:BUILD,time:new Date().toISOString()});
  const suggest=taskIdFromPath(path,"template-suggestions");
  if(suggest&&request.method==="GET")return followTemplateSuggestApi(request,env,suggest);
  const render=taskIdFromPath(path,"render-template");
  if(render&&request.method==="POST")return followTemplateRenderApi(request,env,render);
  const save=taskIdFromPath(path,"line-draft-from-template");
  if(save&&request.method==="POST")return followTemplateSaveDraftApi(request,env,save);
  const res=await app.fetch(request,env,ctx);
  const type=res.headers.get("content-type")||"";
  if(type.includes("text/html"))return new Response(injectFollowTemplateUi(await res.text()),{status:res.status,statusText:res.statusText,headers:securityHeaders(res.headers)});
  return new Response(res.body,{status:res.status,statusText:res.statusText,headers:securityHeaders(res.headers)});
 }
};
