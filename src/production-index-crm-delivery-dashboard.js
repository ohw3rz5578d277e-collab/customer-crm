// ======================================================
// CUSTOMER CRM / DELIVERY DASHBOARD WRAPPER
// build: customer-crm-api-delivery-dashboard-20260613-01
// Adds delivery progress dashboard, delay alerts, CSV, and top panel.
// ======================================================

import app from "./production-index-crm-follow-template-buttons.js";

const BUILD = "customer-crm-api-delivery-dashboard-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){return v===undefined||v===null?"":String(v).trim();}
function lower(v){return text(v).toLowerCase();}
function num(v){const n=Number(v||0);return Number.isFinite(n)?n:0;}
function todayJst(){return new Date(Date.now()+9*60*60*1000).toISOString().slice(0,10);}
function daysBetween(a,b){if(!a||!b)return null;const da=new Date(`${String(a).slice(0,10)}T00:00:00+09:00`);const db=new Date(`${String(b).slice(0,10)}T00:00:00+09:00`);return Math.floor((db-da)/86400000);}
function securityHeaders(headers={}){const h=new Headers(headers);h.set("cache-control","no-store, no-cache, must-revalidate, max-age=0");h.set("pragma","no-cache");h.set("expires","0");h.set("x-content-type-options","nosniff");h.set("referrer-policy","no-referrer");return h;}
function json(data,status=200,headers={}){return new Response(JSON.stringify(data,null,2),{status,headers:securityHeaders({"content-type":"application/json; charset=utf-8",...headers})});}
function csvEscape(v){const s=String(v==null?"":v);return /[",\n\r]/.test(s)?`"${s.replace(/"/g,'""')}"`:s;}
function csv(rows){return "\ufeff"+rows.map(r=>r.map(csvEscape).join(",")).join("\n");}
function getAccessEmail(request){return lower(request.headers.get("cf-access-authenticated-user-email")||request.headers.get("Cf-Access-Authenticated-User-Email")||request.headers.get("x-user-email")||"");}
async function addColumn(db,table,definition){try{await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();}catch(_){}}
async function safeAll(env,sql,bindings=[]){try{const s=env.DB.prepare(sql);const r=bindings.length?await s.bind(...bindings).all():await s.all();return r.results||[];}catch(_){return [];}}
async function ensureSchema(env){
  if(!env.DB)throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY,role TEXT,status TEXT,created_by TEXT,created_at TEXT,updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email,role,status,created_by,created_at,updated_at) VALUES(?,'admin','active','system',datetime('now'),datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_reservation_progress(reservation_key TEXT PRIMARY KEY,crm_draft_id TEXT,reservation_app_reservation_id TEXT,customer_id TEXT,customer_name TEXT,genre TEXT,shoot_date TEXT,progress_status TEXT,progress_step INTEGER,next_action TEXT,next_due_date TEXT,completed_at TEXT,created_by TEXT,updated_by TEXT,created_at TEXT,updated_at TEXT,deleted_at TEXT,deleted_by TEXT)`).run();
  for(const col of ["delay_level TEXT","delay_reason TEXT","delay_checked_at TEXT","alert_ack_at TEXT","alert_ack_by TEXT"]) await addColumn(env.DB,"crm_reservation_progress",col);
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_delivery_progress_status ON crm_reservation_progress(progress_status, shoot_date, next_due_date, deleted_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_delivery_delay ON crm_reservation_progress(delay_level, delay_checked_at)`).run();
}
async function requireUser(request,env,roles=READ_ROLES){
  await ensureSchema(env);const email=getAccessEmail(request);if(!email)return {ok:false,response:json({ok:false,message:"Login required"},401)};
  const user=await env.DB.prepare(`SELECT email,role,status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  if(!user)return {ok:false,response:json({ok:false,message:"User is not allowed"},403)};
  if(roles.length&&!roles.includes(user.role||""))return {ok:false,response:json({ok:false,message:"Permission denied"},403)};
  return {ok:true,email,user};
}
function delayState(row){
  const today=todayJst();
  const shoot=text(row.shoot_date).slice(0,10);
  const due=text(row.next_due_date).slice(0,10);
  const status=text(row.progress_status)||"予約確定";
  const daysFromShoot=shoot?daysBetween(shoot,today):null;
  const dueOver=due?daysBetween(due,today):null;
  let level="ok", reason="通常", sort=0;
  if(status==="完了") return {level:"done",reason:"完了",sort:99,days_from_shoot:daysFromShoot,days_overdue:0};
  if(daysFromShoot!==null&&daysFromShoot>=60&&!["本納品済み","口コミ依頼済み","完了"].includes(status)){level="danger";reason="撮影から60日以上。本納品遅延の可能性があります。";sort=1;}
  else if(daysFromShoot!==null&&daysFromShoot>=30&&!["本納品準備中","本納品済み","口コミ依頼済み","完了"].includes(status)){level="warning";reason="撮影から30日以上。本納品準備を確認してください。";sort=2;}
  else if(daysFromShoot!==null&&daysFromShoot>=7&&!["先行納品済み","本納品準備中","本納品済み","口コミ依頼済み","完了"].includes(status)){level="warning";reason="撮影から7日以上。先行納品を確認してください。";sort=3;}
  else if(dueOver!==null&&dueOver>0){level="warning";reason=`次アクション期限を${dueOver}日超過しています。`;sort=4;}
  else if(status==="本納品済み"&&daysFromShoot!==null&&daysFromShoot>=67){level="notice";reason="口コミ依頼のタイミングです。";sort=5;}
  return {level,reason,sort,days_from_shoot:daysFromShoot,days_overdue:dueOver&&dueOver>0?dueOver:0};
}
async function deliveryRows(env, filters={}){
  const status=text(filters.status||"");const level=text(filters.level||"");const keyword=lower(filters.keyword||"");
  const rows=await safeAll(env,`SELECT * FROM crm_reservation_progress WHERE COALESCE(deleted_at,'')='' ORDER BY date(COALESCE(next_due_date,'2999-12-31')) ASC, date(COALESCE(shoot_date,'2999-12-31')) ASC LIMIT 1000`);
  let items=rows.map(r=>({...r,delay:delayState(r)}));
  if(status&&status!=="all")items=items.filter(x=>text(x.progress_status)===status);
  if(level&&level!=="all")items=items.filter(x=>x.delay.level===level);
  if(keyword)items=items.filter(x=>lower(`${x.customer_name} ${x.genre} ${x.reservation_app_reservation_id} ${x.crm_draft_id}`).includes(keyword));
  items.sort((a,b)=>(a.delay.sort-b.delay.sort)||String(a.next_due_date||"").localeCompare(String(b.next_due_date||"")));
  return items;
}
async function deliveryDashboardApi(request,env){
  const auth=await requireUser(request,env,READ_ROLES);if(!auth.ok)return auth.response;
  const url=new URL(request.url);const items=await deliveryRows(env,{status:url.searchParams.get("status"),level:url.searchParams.get("level"),keyword:url.searchParams.get("keyword")});
  const counts={total:items.length,open:items.filter(x=>x.progress_status!=="完了").length,danger:items.filter(x=>x.delay.level==="danger").length,warning:items.filter(x=>x.delay.level==="warning").length,notice:items.filter(x=>x.delay.level==="notice").length,done:items.filter(x=>x.delay.level==="done").length};
  return json({ok:true,build:BUILD,counts,items:items.slice(0,200),checked_at:new Date().toISOString()});
}
async function deliveryCsvApi(request,env){
  const auth=await requireUser(request,env,READ_ROLES);if(!auth.ok)return auth.response;
  const url=new URL(request.url);const items=await deliveryRows(env,{status:url.searchParams.get("status"),level:url.searchParams.get("level"),keyword:url.searchParams.get("keyword")});
  const rows=[["level","reason","customer_name","genre","shoot_date","progress_status","next_action","next_due_date","reservation_id","crm_draft_id"],...items.map(x=>[x.delay.level,x.delay.reason,x.customer_name,x.genre,x.shoot_date,x.progress_status,x.next_action,x.next_due_date,x.reservation_app_reservation_id,x.crm_draft_id])];
  return new Response(csv(rows),{headers:securityHeaders({"content-type":"text/csv; charset=utf-8","content-disposition":"attachment; filename=delivery-progress.csv"})});
}
function injectDeliveryUi(html){
  if(!html||html.includes("crmDeliveryDashboardScript"))return html;
  const style=`<style id="crmDeliveryDashboardStyle">
.crm-delivery-panel{margin:10px auto 18px;max-width:1180px;border:1px solid #fecaca;background:linear-gradient(135deg,#fff1f2,#fff);border-radius:18px;padding:12px;box-shadow:0 12px 30px rgba(225,29,72,.08);font-family:inherit;color:#0f172a}.crm-delivery-head{display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap}.crm-delivery-title{font-size:17px;font-weight:950;margin:0}.crm-delivery-sub{font-size:12px;color:#64748b;margin:4px 0 0}.crm-delivery-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.crm-delivery-actions button,.crm-delivery-actions select,.crm-delivery-actions input{border:1px solid #fecaca;border-radius:10px;padding:8px 9px;font-size:12px;background:#fff}.crm-delivery-actions button{border:0;background:#e11d48;color:#fff;font-weight:950;cursor:pointer}.crm-delivery-kpis{display:grid;grid-template-columns:repeat(6,minmax(90px,1fr));gap:8px;margin-top:10px}.crm-delivery-kpi{background:#fff;border:1px solid #ffe4e6;border-radius:13px;padding:9px}.crm-delivery-kpi b{display:block;font-size:20px}.crm-delivery-kpi span{font-size:11px;color:#64748b;font-weight:900}.crm-delivery-list{margin-top:10px;display:grid;grid-template-columns:repeat(2,1fr);gap:8px}.crm-delivery-row{background:#fff;border:1px solid #ffe4e6;border-radius:14px;padding:10px;font-size:12px}.crm-delivery-row.danger{border-color:#fca5a5;background:#fff5f5}.crm-delivery-row.warning{border-color:#fed7aa;background:#fff7ed}.crm-delivery-row.notice{border-color:#bfdbfe;background:#eff6ff}.crm-delivery-row b{font-weight:950}.crm-delivery-row small{display:block;color:#64748b;margin-top:4px;line-height:1.45}.crm-delivery-row button{border:0;background:#e11d48;color:#fff;border-radius:9px;padding:6px 8px;font-size:11px;font-weight:900;margin-top:7px;cursor:pointer}.crm-delivery-empty{background:#fff;border:1px dashed #fca5a5;border-radius:14px;padding:12px;font-size:13px;color:#64748b;margin-top:10px}@media(max-width:860px){.crm-delivery-panel{margin:10px}.crm-delivery-kpis{grid-template-columns:repeat(2,1fr)}.crm-delivery-list{grid-template-columns:1fr}}
</style>`;
  const script=`<script id="crmDeliveryDashboardScript">
(function(){if(window.__crmDeliveryDashboardInstalled)return;window.__crmDeliveryDashboardInstalled=true;
function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
function api(u,o){return fetch(u,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},o||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
function toast(m){var d=document.createElement('div');d.textContent=m;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000018;background:#9f1239;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.2)';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
function panel(){return '<section id="crmDeliveryPanel" class="crm-delivery-panel"><div class="crm-delivery-head"><div><h2 class="crm-delivery-title">納品進捗・遅延アラート</h2><p class="crm-delivery-sub">撮影後の先行納品・本納品・口コミ依頼の抜け漏れを確認できます。</p></div></div><div class="crm-delivery-actions"><select id="crmDeliveryLevel"><option value="all">すべて</option><option value="danger">危険</option><option value="warning">注意</option><option value="notice">通知</option><option value="done">完了</option></select><input id="crmDeliveryKeyword" placeholder="顧客名・予約ID"><button id="crmDeliveryReload">更新</button><button id="crmDeliveryCsv">CSV</button></div><div id="crmDeliveryKpis" class="crm-delivery-kpis"></div><div id="crmDeliveryList" class="crm-delivery-list"><div class="crm-delivery-empty">読み込み中...</div></div></section>'}
function kpis(c){var a=[['open','未完了'],['danger','危険'],['warning','注意'],['notice','通知'],['done','完了'],['total','合計']];return a.map(function(x){return '<div class="crm-delivery-kpi"><b>'+esc(c&&c[x[0]]||0)+'</b><span>'+x[1]+'</span></div>'}).join('')}
function row(x){var l=(x.delay&&x.delay.level)||'ok';return '<div class="crm-delivery-row '+esc(l)+'"><b>'+esc(x.customer_name||'-')+' / '+esc(x.progress_status||'-')+'</b><small>'+esc(x.genre||'-')+' '+esc(x.shoot_date||'-')+'<br>次：'+esc(x.next_action||'-')+' '+esc(x.next_due_date||'')+'<br>'+esc((x.delay&&x.delay.reason)||'')+'<br>予約ID '+esc(x.reservation_app_reservation_id||'-')+'</small><button data-delivery-advance="'+esc(x.reservation_key)+'">次の工程へ</button></div>'}
function params(){var l=document.getElementById('crmDeliveryLevel'),k=document.getElementById('crmDeliveryKeyword');return '?level='+(l?encodeURIComponent(l.value):'all')+'&keyword='+(k?encodeURIComponent(k.value):'')}
async function load(){var kp=document.getElementById('crmDeliveryKpis'),list=document.getElementById('crmDeliveryList');try{var d=await api('/api/delivery-dashboard'+params());if(!d.ok)throw new Error(d.message||'load failed');if(kp)kp.innerHTML=kpis(d.counts||{});if(list)list.innerHTML=(d.items||[]).length?(d.items||[]).map(row).join(''):'<div class="crm-delivery-empty">該当する納品進捗はありません。</div>';}catch(e){if(list)list.innerHTML='<div class="crm-delivery-empty">読み込み失敗：'+esc(e.message||e)+'</div>';}}
async function advance(key){if(!confirm('次の工程へ進めますか？'))return;var d=await api('/api/reservation-progress/'+encodeURIComponent(key),{method:'POST',body:'{}'});if(!d.ok){toast('更新失敗：'+(d.message||d.status||'unknown'));return;}toast('進捗を更新しました');load();var g=document.getElementById('crmGrowthLoad');if(g)g.click();}
function install(){if(document.getElementById('crmDeliveryPanel'))return;var base=document.getElementById('crmGrowthPanel')||document.getElementById('crmFollowTemplatePanel')||document.getElementById('crmTodayFilterPanel');if(base)base.insertAdjacentHTML('afterend',panel());else (document.querySelector('main')||document.body).insertAdjacentHTML('afterbegin',panel());load();}
document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmDeliveryReload')load();if(t.id==='crmDeliveryCsv')location.href='/api/delivery-dashboard.csv'+params();var key=t.getAttribute&&t.getAttribute('data-delivery-advance');if(key)advance(key);});
document.addEventListener('change',function(e){if(e.target&&e.target.id==='crmDeliveryLevel')load();});document.addEventListener('DOMContentLoaded',install);setTimeout(install,1600);setInterval(load,180000);
})();
</script>`;
  return html.replace("</head>",`${style}</head>`).replace("</body>",`${script}</body>`);
}
export default {async fetch(request,env,ctx){const url=new URL(request.url);if((url.pathname==="/"||url.pathname==="/health"||url.pathname==="/api/health")&&request.method==="GET")return json({ok:true,service:"customer-crm-api",build:BUILD,time:new Date().toISOString()});if(url.pathname==="/api/delivery-dashboard"&&request.method==="GET")return deliveryDashboardApi(request,env);if(url.pathname==="/api/delivery-dashboard.csv"&&request.method==="GET")return deliveryCsvApi(request,env);const res=await app.fetch(request,env,ctx);const type=res.headers.get("content-type")||"";if(type.includes("text/html")){const body=injectDeliveryUi(await res.text());return new Response(body,{status:res.status,statusText:res.statusText,headers:securityHeaders(res.headers)});}return new Response(res.body,{status:res.status,statusText:res.statusText,headers:securityHeaders(res.headers)});}};
