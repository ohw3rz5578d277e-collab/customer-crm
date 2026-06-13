// ======================================================
// CUSTOMER CRM / LIST WORKBENCH WRAPPER
// build: customer-crm-api-list-workbench-20260614-01
// Adds sortable/filterable list workbench and bulk actions for customers, inquiries, and marketing candidates.
// ======================================================

import app from "./production-index-crm-customer-smart-panel.js";

const BUILD = "customer-crm-api-list-workbench-20260614-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){ return v === undefined || v === null ? "" : String(v).trim(); }
function lower(v){ return text(v).toLowerCase(); }
function nowIso(){ return new Date().toISOString(); }
function securityHeaders(headers = {}){
  const h = new Headers(headers);
  h.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  h.set("pragma", "no-cache");
  h.set("expires", "0");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  return h;
}
function json(data, status = 200){ return new Response(JSON.stringify(data, null, 2), { status, headers: securityHeaders({ "content-type":"application/json; charset=utf-8" }) }); }
function getEmail(request){ return lower(request.headers.get("cf-access-authenticated-user-email") || request.headers.get("Cf-Access-Authenticated-User-Email") || request.headers.get("x-user-email") || ""); }
async function readJson(request){ try { return await request.json(); } catch(_) { return {}; } }
async function safeAll(env, sql, binds = []){ try { const s = env.DB.prepare(sql); const r = binds.length ? await s.bind(...binds).all() : await s.all(); return r.results || []; } catch(_) { return []; } }
async function safeFirst(env, sql, binds = []){ try { const s = env.DB.prepare(sql); return binds.length ? await s.bind(...binds).first() : await s.first(); } catch(_) { return null; } }
async function safeRun(env, sql, binds = []){ try { const s = env.DB.prepare(sql); return binds.length ? await s.bind(...binds).run() : await s.run(); } catch(e){ return { error:String(e && e.message || e) }; } }
async function addColumn(db, table, def){ try { await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${def}`).run(); } catch(_) {} }

async function ensureSchema(env){
  if(!env.DB) throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY, role TEXT, status TEXT, created_by TEXT, created_at TEXT, updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email, role, status, created_by, created_at, updated_at) VALUES(?, 'admin', 'active', 'system', datetime('now'), datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_list_workbench_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workbench_type TEXT,
    action_type TEXT,
    target_count INTEGER DEFAULT 0,
    target_ids_json TEXT,
    payload_json TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();
  await addColumn(env.DB, "customers", "list_pinned_at TEXT");
  await addColumn(env.DB, "crm_inquiry_pipeline", "list_pinned_at TEXT");
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_list_workbench_logs_created ON crm_list_workbench_logs(created_at)`).run();
}
async function requireUser(request, env, roles = ROLES){
  await ensureSchema(env);
  const email = getEmail(request);
  if(!email) return { ok:false, response:json({ ok:false, message:"Login required" }, 401) };
  const user = await env.DB.prepare(`SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  if(!user) return { ok:false, response:json({ ok:false, message:"User is not allowed" }, 403) };
  if(roles.length && !roles.includes(user.role || "")) return { ok:false, response:json({ ok:false, message:"Permission denied" }, 403) };
  return { ok:true, email, user };
}
function limitOf(url){ const n = Math.max(1, Math.min(200, Number(url.searchParams.get("limit") || 50))); return n; }
function sortCustomers(items, sort){
  const k = sort || "recent";
  return items.sort((a,b)=>{
    if(k === "revenue") return Number(b.total_revenue||0)-Number(a.total_revenue||0);
    if(k === "rank") return String(a.customer_rank||"").localeCompare(String(b.customer_rank||""), "ja");
    if(k === "name") return String(a.name||a.customer_name||"").localeCompare(String(b.name||b.customer_name||""), "ja");
    return String(b.updated_at||b.created_at||"").localeCompare(String(a.updated_at||a.created_at||""));
  });
}
function scoreCustomer(c){
  let s = Number(c.marketing_score || 0);
  if((c.customer_rank||"").includes("VIP")) s += 30;
  if((c.customer_rank||"").includes("リピーター")) s += 20;
  if(Number(c.total_revenue||0) >= 50000) s += 15;
  if(Number(c.repeat_count||0) >= 2) s += 10;
  return s;
}
async function listCustomers(env, url){
  const q = lower(url.searchParams.get("q"));
  const rank = text(url.searchParams.get("rank"));
  const segment = text(url.searchParams.get("segment"));
  let rows = await safeAll(env, `SELECT id, name, customer_name, phone, email, customer_rank, customer_rank_reason, marketing_score, next_offer, next_line_suggestion, total_revenue, repeat_count, last_shoot_date, genre_history, source_type, updated_at, created_at FROM customers ORDER BY updated_at DESC LIMIT 500`);
  rows = rows.filter(r=>{
    const hay = lower([r.id,r.name,r.customer_name,r.phone,r.email,r.customer_rank,r.genre_history,r.source_type].join(" "));
    if(q && !hay.includes(q)) return false;
    if(rank && rank !== "all" && !String(r.customer_rank||"").includes(rank)) return false;
    if(segment === "vip" && !String(r.customer_rank||"").includes("VIP")) return false;
    if(segment === "repeat" && !(Number(r.repeat_count||0) >= 1 || String(r.customer_rank||"").includes("リピーター"))) return false;
    if(segment === "dormant" && !String(r.customer_rank||"").includes("休眠")) return false;
    return true;
  }).map(r=>({ type:"customer", id:String(r.id), title:r.name||r.customer_name||`顧客 ${r.id}`, subtitle:r.customer_rank||"未ランク", score:scoreCustomer(r), amount:Number(r.total_revenue||0), next:r.next_offer||r.next_line_suggestion||"次回提案を確認", raw:r }));
  return sortCustomers(rows, url.searchParams.get("sort")).slice(0, limitOf(url));
}
async function listInquiries(env, url){
  const q = lower(url.searchParams.get("q"));
  const status = text(url.searchParams.get("status"));
  let rows = await safeAll(env, `SELECT id, customer_id, customer_name, status, genre, preferred_date, preferred_location, estimated_amount, memo, source_type, updated_at, created_at FROM crm_inquiry_pipeline ORDER BY updated_at DESC LIMIT 500`);
  rows = rows.filter(r=>{
    const hay = lower([r.id,r.customer_id,r.customer_name,r.status,r.genre,r.preferred_location,r.memo,r.source_type].join(" "));
    if(q && !hay.includes(q)) return false;
    if(status && status !== "all" && r.status !== status) return false;
    return true;
  }).map(r=>({ type:"inquiry", id:String(r.id), title:r.customer_name||`問い合わせ ${r.id}`, subtitle:r.status||"問い合わせ", score:Number(r.estimated_amount||0), amount:Number(r.estimated_amount||0), next:r.status === "予約確定" ? "予約済み" : "LINE・フォロー・予約下書きへ", raw:r }));
  const sort = url.searchParams.get("sort") || "recent";
  rows.sort((a,b)=> sort === "amount" ? b.amount-a.amount : String(b.raw.updated_at||b.raw.created_at||"").localeCompare(String(a.raw.updated_at||a.raw.created_at||"")) );
  return rows.slice(0, limitOf(url));
}
async function listMarketing(env, url){
  const segment = text(url.searchParams.get("segment") || "repeat");
  const fakeUrl = new URL(url.toString());
  fakeUrl.searchParams.set("segment", segment === "all" ? "" : segment);
  const customers = await listCustomers(env, fakeUrl);
  return customers.map(x=>({ ...x, type:"marketing", subtitle:`${x.subtitle} / ${segment}`, next:x.next || "マーケLINE候補" })).slice(0, limitOf(url));
}
async function workbenchApi(request, env){
  const auth = await requireUser(request, env, ROLES); if(!auth.ok) return auth.response;
  const url = new URL(request.url);
  const type = text(url.searchParams.get("type") || "customers");
  let items = [];
  if(type === "inquiries") items = await listInquiries(env, url);
  else if(type === "marketing") items = await listMarketing(env, url);
  else items = await listCustomers(env, url);
  const summary = { count:items.length, total_amount:items.reduce((s,x)=>s+Number(x.amount||0),0), high_score:items.filter(x=>Number(x.score||0)>=50).length };
  return json({ ok:true, build:BUILD, type, summary, items });
}
async function bulkLineApi(request, env){
  const auth = await requireUser(request, env, WRITE_ROLES); if(!auth.ok) return auth.response;
  const body = await readJson(request);
  const ids = Array.isArray(body.ids) ? body.ids.map(String).filter(Boolean).slice(0,100) : [];
  const sourceType = text(body.type || "customers");
  if(!ids.length) return json({ ok:false, message:"対象を選択してください" }, 400);
  let created = 0;
  for(const id of ids){
    let customer = null;
    if(sourceType === "inquiries"){
      const inq = await safeFirst(env, `SELECT customer_id, customer_name, genre, status, memo FROM crm_inquiry_pipeline WHERE id=?`, [id]);
      if(inq && inq.customer_id) customer = await safeFirst(env, `SELECT id, name, customer_name FROM customers WHERE id=?`, [inq.customer_id]);
      if(!customer && inq) customer = { id:inq.customer_id || `inquiry:${id}`, name:inq.customer_name };
    } else {
      customer = await safeFirst(env, `SELECT id, name, customer_name, next_offer, next_line_suggestion FROM customers WHERE id=?`, [id]);
    }
    if(!customer) continue;
    const name = customer.name || customer.customer_name || "お客様";
    const message = text(body.message) || `${name}、いつもありがとうございます。\n前回の撮影から少しお日にちが経ちましたので、次回の記念撮影についてご案内です。\nご希望があれば、日程や場所だけでもお気軽にご相談ください。`;
    await safeRun(env, `INSERT INTO customer_line_draft_logs(customer_id, customer_name, message, status, source_type, created_by, created_at, updated_at) VALUES(?, ?, ?, 'draft', 'list_workbench', ?, datetime('now'), datetime('now'))`, [String(customer.id||id), name, message, auth.email]);
    created++;
  }
  await safeRun(env, `INSERT INTO crm_list_workbench_logs(workbench_type, action_type, target_count, target_ids_json, payload_json, created_by, created_at) VALUES(?, 'bulk_line_draft', ?, ?, ?, ?, datetime('now'))`, [sourceType, created, JSON.stringify(ids), JSON.stringify(body), auth.email]);
  return json({ ok:true, created });
}
async function bulkFollowApi(request, env){
  const auth = await requireUser(request, env, WRITE_ROLES); if(!auth.ok) return auth.response;
  const body = await readJson(request);
  const ids = Array.isArray(body.ids) ? body.ids.map(String).filter(Boolean).slice(0,100) : [];
  if(!ids.length) return json({ ok:false, message:"対象を選択してください" }, 400);
  const due = text(body.due_date) || new Date(Date.now()+86400000).toISOString().slice(0,10);
  let created = 0;
  for(const id of ids){
    const c = await safeFirst(env, `SELECT id, name, customer_name FROM customers WHERE id=?`, [id]);
    if(!c) continue;
    await safeRun(env, `INSERT INTO crm_follow_tasks(customer_id, customer_name, task_type, title, due_date, status, source_type, created_by, created_at, updated_at) VALUES(?, ?, 'list_follow', ?, ?, 'open', 'list_workbench', ?, datetime('now'), datetime('now'))`, [id, c.name||c.customer_name||"", text(body.title)||"一覧から作成したフォロー", due, auth.email]);
    created++;
  }
  await safeRun(env, `INSERT INTO crm_list_workbench_logs(workbench_type, action_type, target_count, target_ids_json, payload_json, created_by, created_at) VALUES('customers', 'bulk_follow_task', ?, ?, ?, ?, datetime('now'))`, [created, JSON.stringify(ids), JSON.stringify(body), auth.email]);
  return json({ ok:true, created });
}
function injectListWorkbench(html){
  const style = `<style id="crmListWorkbenchStyle">
#crmListWorkbenchFab{position:fixed;right:18px;bottom:214px;z-index:99991;border:0;border-radius:999px;background:#0f172a;color:#fff;padding:12px 15px;font-weight:800;box-shadow:0 10px 24px rgba(15,23,42,.25)}
#crmListWorkbenchPanel{display:none;position:fixed;right:18px;bottom:272px;width:min(760px,calc(100vw - 28px));max-height:78vh;overflow:auto;background:#fff;border:1px solid #e2e8f0;border-radius:18px;box-shadow:0 18px 50px rgba(15,23,42,.25);z-index:99991;padding:14px;color:#0f172a}
.crmLwControls{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin:10px 0}.crmLwControls input,.crmLwControls select{font-size:16px;padding:10px;border:1px solid #cbd5e1;border-radius:10px}.crmLwGrid{display:grid;gap:8px}.crmLwRow{border:1px solid #e2e8f0;border-radius:14px;padding:10px;background:#f8fafc}.crmLwRowTop{display:flex;justify-content:space-between;gap:10px}.crmLwTitle{font-weight:900}.crmLwSub{font-size:12px;color:#64748b;margin-top:4px}.crmLwActions{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.crmLwActions button,#crmListWorkbenchPanel button{border:0;border-radius:10px;padding:9px 11px;font-weight:800;background:#028760;color:#fff}.crmLwActions .ghost,#crmListWorkbenchPanel .ghost{background:#e2e8f0;color:#0f172a}.crmLwSummary{display:flex;gap:8px;flex-wrap:wrap}.crmLwChip{background:#ecfdf5;border:1px solid #bbf7d0;color:#166534;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:800}@media(max-width:767px){#crmListWorkbenchFab{right:12px;bottom:268px}#crmListWorkbenchPanel{right:8px;bottom:324px;width:calc(100vw - 16px);max-height:68vh}.crmLwControls{grid-template-columns:1fr}.crmLwRowTop{display:block}}
</style>`;
  const script = `<script id="crmListWorkbenchScript">
(function(){
 if(window.__crmListWorkbenchLoaded)return;window.__crmListWorkbenchLoaded=true;
 var selected={};
 function toast(m){try{var d=document.createElement('div');d.textContent=m;d.style.cssText='position:fixed;left:50%;bottom:90px;transform:translateX(-50%);background:#0f172a;color:#fff;padding:10px 14px;border-radius:999px;z-index:100000;font-weight:800';document.body.appendChild(d);setTimeout(function(){d.remove()},2200)}catch(e){alert(m)}}
 function api(u,o){return fetch(u,Object.assign({headers:{'content-type':'application/json'}},o||{})).then(function(r){return r.json()})}
 function esc(s){return String(s||'').replace(/[&<>\"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[c]})}
 function openPanel(){var p=document.getElementById('crmListWorkbenchPanel');p.style.display=p.style.display==='block'?'none':'block';if(p.style.display==='block')load();}
 function getType(){return document.getElementById('crmLwType').value}
 function load(){var type=getType(),q=document.getElementById('crmLwQ').value,filter=document.getElementById('crmLwFilter').value,sort=document.getElementById('crmLwSort').value;var qs='?type='+encodeURIComponent(type)+'&q='+encodeURIComponent(q)+'&sort='+encodeURIComponent(sort)+'&limit=80';if(type==='inquiries')qs+='&status='+encodeURIComponent(filter);else qs+='&segment='+encodeURIComponent(filter);api('/api/list-workbench'+qs).then(render).catch(function(){toast('一覧を読み込めません')});}
 function render(d){var box=document.getElementById('crmLwBody');if(!d.ok){box.innerHTML='読み込み失敗';return;}selected={};document.getElementById('crmLwSummary').innerHTML='<span class="crmLwChip">件数 '+d.summary.count+'</span><span class="crmLwChip">合計 '+(d.summary.total_amount||0).toLocaleString()+'円</span><span class="crmLwChip">高スコア '+d.summary.high_score+'</span>';box.innerHTML=(d.items||[]).map(function(x){return '<div class="crmLwRow"><div class="crmLwRowTop"><label><input type="checkbox" data-lw-check="'+esc(x.id)+'"> <span class="crmLwTitle">'+esc(x.title)+'</span></label><b>score '+esc(x.score||0)+'</b></div><div class="crmLwSub">'+esc(x.subtitle)+' / '+esc(x.next)+'</div><div class="crmLwActions"><button data-lw-one-line="'+esc(x.id)+'">LINE作成</button><button class="ghost" data-lw-smart="'+esc(x.id)+'">顧客スマート</button></div></div>'}).join('')||'<p>対象がありません。</p>';}
 function ids(){return Array.from(document.querySelectorAll('[data-lw-check]:checked')).map(function(x){return x.getAttribute('data-lw-check')})}
 function bulkLine(single){var arr=single?[single]:ids();if(!arr.length){toast('対象を選択してください');return;}api('/api/list-workbench/bulk-line-drafts',{method:'POST',body:JSON.stringify({type:getType(),ids:arr})}).then(function(d){toast(d.ok?'LINE文面を保存しました: '+d.created:(d.message||'失敗'));load();});}
 function bulkFollow(){var arr=ids();if(!arr.length){toast('対象を選択してください');return;}api('/api/list-workbench/bulk-follow-tasks',{method:'POST',body:JSON.stringify({ids:arr})}).then(function(d){toast(d.ok?'フォローを作成しました: '+d.created:(d.message||'失敗'));load();});}
 function init(){if(document.getElementById('crmListWorkbenchFab'))return;document.body.insertAdjacentHTML('beforeend','<button id="crmListWorkbenchFab">一覧操作</button><div id="crmListWorkbenchPanel"><h3>一覧操作ワークベンチ</h3><div class="crmLwControls"><select id="crmLwType"><option value="customers">顧客</option><option value="inquiries">問い合わせ</option><option value="marketing">マーケ候補</option></select><input id="crmLwQ" placeholder="検索"><select id="crmLwFilter"><option value="all">すべて</option><option value="VIP">VIP</option><option value="repeat">リピート</option><option value="dormant">休眠</option><option value="問い合わせ">問い合わせ</option><option value="返信待ち">返信待ち</option><option value="仮予約">仮予約</option></select><select id="crmLwSort"><option value="recent">新しい順</option><option value="revenue">売上順</option><option value="amount">見込み金額順</option><option value="name">名前順</option></select><button id="crmLwReload">絞り込み</button><button id="crmLwBulkLine">選択LINE作成</button><button id="crmLwBulkFollow" class="ghost">選択フォロー作成</button></div><div id="crmLwSummary" class="crmLwSummary"></div><div id="crmLwBody" class="crmLwGrid" style="margin-top:10px"></div></div>');}
 document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmListWorkbenchFab')openPanel();if(t.id==='crmLwReload')load();if(t.id==='crmLwBulkLine')bulkLine();if(t.id==='crmLwBulkFollow')bulkFollow();var one=t.getAttribute&&t.getAttribute('data-lw-one-line');if(one)bulkLine(one);var sm=t.getAttribute&&t.getAttribute('data-lw-smart');if(sm){var i=document.getElementById('crmSmartCustomerId');if(i){i.value=sm;document.getElementById('crmCustomerSmartFab')&&document.getElementById('crmCustomerSmartFab').click();setTimeout(function(){document.getElementById('crmSmartLoadBtn')&&document.getElementById('crmSmartLoadBtn').click()},200)}else toast('顧客スマートを開いてください')}});
 document.addEventListener('change',function(e){if(e.target&&e.target.id==='crmLwType')load();});document.addEventListener('DOMContentLoaded',init);setTimeout(init,800);
})();
</script>`;
  return html.replace("</head>", `${style}</head>`).replace("</body>", `${script}</body>`);
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    const path = url.pathname;
    if((path === "/" || path === "/health" || path === "/api/health") && request.method === "GET") return json({ ok:true, service:"customer-crm-api", build:BUILD, time:nowIso() });
    if(path === "/api/list-workbench" && request.method === "GET") return workbenchApi(request, env);
    if(path === "/api/list-workbench/bulk-line-drafts" && request.method === "POST") return bulkLineApi(request, env);
    if(path === "/api/list-workbench/bulk-follow-tasks" && request.method === "POST") return bulkFollowApi(request, env);
    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if(type.includes("text/html")){
      const body = injectListWorkbench(await res.text());
      return new Response(body, { status:res.status, statusText:res.statusText, headers:securityHeaders(res.headers) });
    }
    return new Response(res.body, { status:res.status, statusText:res.statusText, headers:securityHeaders(res.headers) });
  }
};
