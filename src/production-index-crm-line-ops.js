// ======================================================
// CUSTOMER CRM / LINE OPS TRACKING WRAPPER
// build: customer-crm-api-line-ops-20260614-01
// ======================================================

import app from "./production-index-crm-list-workbench-safety.js";

const BUILD = "customer-crm-api-line-ops-20260614-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){ return v === undefined || v === null ? "" : String(v).trim(); }
function lower(v){ return text(v).toLowerCase(); }
function nowIso(){ return new Date().toISOString(); }
function json(data, status = 200){ return new Response(JSON.stringify(data, null, 2), { status, headers:{ "content-type":"application/json; charset=utf-8", "cache-control":"no-store" } }); }
function getEmail(request){ return lower(request.headers.get("cf-access-authenticated-user-email") || request.headers.get("Cf-Access-Authenticated-User-Email") || request.headers.get("x-user-email") || ""); }
async function readJson(request){ try { return await request.json(); } catch(_) { return {}; } }
async function addColumn(db, table, def){ try { await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${def}`).run(); } catch(_) {} }
async function all(env, sql, binds=[]){ try { const s=env.DB.prepare(sql); const r=binds.length?await s.bind(...binds).all():await s.all(); return r.results||[]; } catch(_) { return []; } }
async function first(env, sql, binds=[]){ try { const s=env.DB.prepare(sql); return binds.length?await s.bind(...binds).first():await s.first(); } catch(_) { return null; } }
async function run(env, sql, binds=[]){ try { const s=env.DB.prepare(sql); return binds.length?await s.bind(...binds).run():await s.run(); } catch(e){ return { error:String(e && e.message || e) }; } }

async function ensureSchema(env){
  if(!env.DB) throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY, role TEXT, status TEXT, created_by TEXT, created_at TEXT, updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email, role, status, created_by, created_at, updated_at) VALUES(?, 'admin', 'active', 'system', datetime('now'), datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS customer_line_draft_logs(id INTEGER PRIMARY KEY AUTOINCREMENT, customer_id TEXT, customer_name TEXT, action_type TEXT, action_label TEXT, priority TEXT, message_text TEXT, status TEXT DEFAULT 'copied', channel TEXT DEFAULT 'line', created_by TEXT, copied_at TEXT, sent_at TEXT, memo TEXT, raw_json TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)`).run();
  for(const c of ["sent_by TEXT", "reply_status TEXT DEFAULT 'unknown'", "replied_at TEXT", "response_memo TEXT", "response_logged_at TEXT", "led_to_reservation INTEGER DEFAULT 0", "reservation_id TEXT", "reservation_linked_at TEXT"]){ await addColumn(env.DB, "customer_line_draft_logs", c); }
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_line_ops_logs(id INTEGER PRIMARY KEY AUTOINCREMENT, line_log_id INTEGER, customer_id TEXT, action_type TEXT, before_status TEXT, after_status TEXT, payload_json TEXT, created_by TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_customer_line_draft_logs_ops ON customer_line_draft_logs(status, reply_status, led_to_reservation, created_at)`).run();
}

async function requireUser(request, env, roles=ROLES){
  await ensureSchema(env);
  const email = getEmail(request);
  if(!email) return { ok:false, response:json({ ok:false, message:"Login required" }, 401) };
  const user = await env.DB.prepare(`SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  if(!user) return { ok:false, response:json({ ok:false, message:"User is not allowed" }, 403) };
  if(roles.length && !roles.includes(user.role || "")) return { ok:false, response:json({ ok:false, message:"Permission denied" }, 403) };
  return { ok:true, email, user };
}

async function logAction(env, row, actionType, beforeStatus, afterStatus, payload, email){
  await run(env, `INSERT INTO crm_line_ops_logs(line_log_id, customer_id, action_type, before_status, after_status, payload_json, created_by, created_at) VALUES(?,?,?,?,?,?,?,?)`, [row?.id || null, row?.customer_id || "", actionType, beforeStatus || "", afterStatus || "", JSON.stringify(payload || {}), email || "system", nowIso()]);
}

async function dashboard(env){
  const stats = await first(env, `SELECT COUNT(*) AS total, SUM(CASE WHEN status IN ('copied','draft','pending','created') OR status IS NULL THEN 1 ELSE 0 END) AS pending, SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent, SUM(CASE WHEN reply_status='replied' THEN 1 ELSE 0 END) AS replied, SUM(CASE WHEN reply_status='no_reply' THEN 1 ELSE 0 END) AS no_reply, SUM(CASE WHEN led_to_reservation=1 THEN 1 ELSE 0 END) AS reserved FROM customer_line_draft_logs`);
  const latest = await all(env, `SELECT id, customer_id, customer_name, action_label, status, reply_status, led_to_reservation, reservation_id, substr(message_text,1,160) AS message_preview, created_at, sent_at, replied_at FROM customer_line_draft_logs ORDER BY datetime(COALESCE(updated_at, created_at)) DESC LIMIT 50`);
  return { ok:true, stats:stats || {}, latest };
}

async function listDrafts(env, url){
  const status = text(url.searchParams.get("status"));
  const reply = text(url.searchParams.get("reply_status"));
  const q = lower(url.searchParams.get("q"));
  const where = [];
  const binds = [];
  if(status && status !== "all"){ where.push(`COALESCE(status,'')=?`); binds.push(status); }
  if(reply && reply !== "all"){ where.push(`COALESCE(reply_status,'unknown')=?`); binds.push(reply); }
  if(q){ where.push(`(lower(COALESCE(customer_name,'')) LIKE ? OR lower(COALESCE(message_text,'')) LIKE ? OR lower(COALESCE(action_label,'')) LIKE ?)`); binds.push(`%${q}%`, `%${q}%`, `%${q}%`); }
  return all(env, `SELECT id, customer_id, customer_name, action_type, action_label, priority, message_text, status, reply_status, led_to_reservation, reservation_id, created_at, sent_at, replied_at, response_memo FROM customer_line_draft_logs ${where.length ? "WHERE "+where.join(" AND ") : ""} ORDER BY datetime(COALESCE(updated_at, created_at)) DESC LIMIT 120`, binds);
}

function lineId(path, action){ const m = path.match(new RegExp(`^/api/line-ops/(\\d+)/${action}$`)); return m ? m[1] : ""; }

async function markSent(request, env, id, email){
  const body = await readJson(request);
  const row = await first(env, `SELECT * FROM customer_line_draft_logs WHERE id=?`, [id]);
  if(!row) return json({ ok:false, message:"LINEログが見つかりません" }, 404);
  await run(env, `UPDATE customer_line_draft_logs SET status='sent', sent_at=COALESCE(sent_at,?), sent_by=?, memo=COALESCE(?, memo), updated_at=? WHERE id=?`, [nowIso(), email, text(body.memo) || null, nowIso(), id]);
  await logAction(env, row, "mark_sent", row.status, "sent", body, email);
  return json({ ok:true, id:Number(id), status:"sent" });
}

async function response(request, env, id, email){
  const body = await readJson(request);
  const next = ["replied","no_reply","unknown"].includes(text(body.reply_status)) ? text(body.reply_status) : "replied";
  const row = await first(env, `SELECT * FROM customer_line_draft_logs WHERE id=?`, [id]);
  if(!row) return json({ ok:false, message:"LINEログが見つかりません" }, 404);
  await run(env, `UPDATE customer_line_draft_logs SET reply_status=?, replied_at=CASE WHEN ?='replied' THEN COALESCE(replied_at,?) ELSE replied_at END, response_memo=?, response_logged_at=?, updated_at=? WHERE id=?`, [next, next, nowIso(), text(body.memo), nowIso(), nowIso(), id]);
  await logAction(env, row, "response", row.reply_status, next, body, email);
  return json({ ok:true, id:Number(id), reply_status:next });
}

async function linkReservation(request, env, id, email){
  const body = await readJson(request);
  const reservationId = text(body.reservation_id || body.reservation_app_reservation_id || body.draft_id);
  const row = await first(env, `SELECT * FROM customer_line_draft_logs WHERE id=?`, [id]);
  if(!row) return json({ ok:false, message:"LINEログが見つかりません" }, 404);
  if(!reservationId) return json({ ok:false, message:"reservation_id が必要です" }, 400);
  await run(env, `UPDATE customer_line_draft_logs SET led_to_reservation=1, reservation_id=?, reservation_linked_at=?, updated_at=? WHERE id=?`, [reservationId, nowIso(), nowIso(), id]);
  await logAction(env, row, "link_reservation", row.reservation_id || "", reservationId, body, email);
  return json({ ok:true, id:Number(id), reservation_id:reservationId });
}

async function logs(env, url){
  const id = text(url.searchParams.get("line_log_id"));
  return all(env, `SELECT * FROM crm_line_ops_logs ${id ? "WHERE line_log_id=?" : ""} ORDER BY datetime(created_at) DESC LIMIT 100`, id ? [id] : []);
}

function injectLineOpsButton(html){
  const style = `<style id="crm-line-ops-style">.crm-lineops-fab{position:fixed;right:18px;bottom:205px;z-index:2147482100}.crm-lineops-fab button{border:0;border-radius:999px;background:#028760;color:#fff;font-weight:800;padding:12px 16px;box-shadow:0 12px 32px rgba(0,0,0,.18);cursor:pointer}.crm-lineops-panel{position:fixed;right:16px;bottom:16px;width:min(960px,calc(100vw - 32px));max-height:86vh;overflow:auto;background:#fff;border:1px solid #e5e7eb;border-radius:20px;box-shadow:0 20px 50px rgba(0,0,0,.2);z-index:2147482200;display:none;padding:16px}.crm-lineops-panel.open{display:block}.crm-lineops-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:8px}.crm-lineops-card{background:#f9fafb;border:1px solid #e5e7eb;border-radius:14px;padding:10px}.crm-lineops-card b{font-size:22px}.crm-lineops-row{border:1px solid #e5e7eb;border-radius:14px;padding:10px;margin:8px 0}.crm-lineops-actions button{margin:3px;border:1px solid #e5e7eb;border-radius:10px;background:#fff;padding:8px 9px;font-weight:700}@media(max-width:760px){.crm-lineops-fab{bottom:140px;right:10px}.crm-lineops-panel{inset:8px;width:auto}.crm-lineops-grid{grid-template-columns:repeat(2,1fr)}}</style>`;
  const script = `<script id="crm-line-ops-script">(()=>{if(window.__crmLineOps)return;window.__crmLineOps=1;const e=s=>String(s??'').replace(/[&<>]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[m]));async function api(p,o={}){const r=await fetch(p,{credentials:'same-origin',headers:{'content-type':'application/json'},...o});const j=await r.json();if(!r.ok)throw Error(j.message||'APIエラー');return j}function mount(){if(document.getElementById('lineOpsPanel'))return;document.body.insertAdjacentHTML('beforeend','<div class="crm-lineops-fab"><button id="lineOpsOpen">LINE運用</button></div><div class="crm-lineops-panel" id="lineOpsPanel"><button style="float:right" id="lineOpsClose">閉じる</button><h2>LINE送信・反応管理</h2><div id="lineOpsBody">読み込み中...</div></div>');document.getElementById('lineOpsOpen').onclick=()=>{document.getElementById('lineOpsPanel').classList.add('open');load()};document.getElementById('lineOpsClose').onclick=()=>document.getElementById('lineOpsPanel').classList.remove('open')}async function load(){const d=await api('/api/line-ops/dashboard');const s=d.stats||{};document.getElementById('lineOpsBody').innerHTML='<div class="crm-lineops-grid">'+[['未送信',s.pending],['送信済み',s.sent],['返信あり',s.replied],['返信なし',s.no_reply],['予約化',s.reserved],['合計',s.total]].map(x=>'<div class="crm-lineops-card"><b>'+e(x[1]||0)+'</b><br>'+e(x[0])+'</div>').join('')+'</div><p><button id="lineOpsReload">更新</button> <button id="lineOpsLogs">ログ</button></p><div>'+d.latest.map(r=>'<div class="crm-lineops-row"><b>'+e(r.customer_name||r.customer_id)+'</b> #'+e(r.id)+'<br><small>'+e(r.action_label||'')+' / '+e(r.status||'copied')+' / '+e(r.reply_status||'unknown')+(r.led_to_reservation?' / 予約化':'')+'</small><p>'+e(r.message_preview||'')+'</p><div class="crm-lineops-actions"><button data-sent="'+r.id+'">送信済み</button><button data-rep="'+r.id+'">返信あり</button><button data-no="'+r.id+'">返信なし</button><button data-rsv="'+r.id+'">予約化</button></div></div>').join('')+'</div>';bind()}function bind(){document.getElementById('lineOpsReload').onclick=load;document.getElementById('lineOpsLogs').onclick=async()=>{const l=await api('/api/line-ops/logs');document.getElementById('lineOpsBody').innerHTML='<pre>'+e(JSON.stringify(l,null,2))+'</pre>'};document.querySelectorAll('[data-sent]').forEach(b=>b.onclick=()=>post('/api/line-ops/'+b.dataset.sent+'/mark-sent',{}));document.querySelectorAll('[data-rep]').forEach(b=>b.onclick=()=>post('/api/line-ops/'+b.dataset.rep+'/response',{reply_status:'replied'}));document.querySelectorAll('[data-no]').forEach(b=>b.onclick=()=>post('/api/line-ops/'+b.dataset.no+'/response',{reply_status:'no_reply'}));document.querySelectorAll('[data-rsv]').forEach(b=>b.onclick=()=>{const id=prompt('予約ID');if(id)post('/api/line-ops/'+b.dataset.rsv+'/link-reservation',{reservation_id:id})})}async function post(p,b){await api(p,{method:'POST',body:JSON.stringify(b)});load()}document.readyState==='loading'?document.addEventListener('DOMContentLoaded',mount):mount()})();</script>`;
  return html.includes("</body>") ? html.replace("</body>", style + script + "</body>") : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    const path = url.pathname;
    try{
      if(path === "/health"){
        const res = await app.fetch(request, env, ctx);
        const data = await res.json().catch(()=>({}));
        return json({ ...data, lineOpsBuild: BUILD });
      }
      if(path === "/api/line-ops/dashboard" && request.method === "GET"){
        const u = await requireUser(request, env); if(!u.ok) return u.response;
        return json(await dashboard(env));
      }
      if(path === "/api/line-ops/drafts" && request.method === "GET"){
        const u = await requireUser(request, env); if(!u.ok) return u.response;
        return json(await listDrafts(env, url));
      }
      let id = lineId(path, "mark-sent");
      if(id && request.method === "POST"){
        const u = await requireUser(request, env, WRITE_ROLES); if(!u.ok) return u.response;
        return markSent(request, env, id, u.email);
      }
      id = lineId(path, "response");
      if(id && request.method === "POST"){
        const u = await requireUser(request, env, WRITE_ROLES); if(!u.ok) return u.response;
        return response(request, env, id, u.email);
      }
      id = lineId(path, "link-reservation");
      if(id && request.method === "POST"){
        const u = await requireUser(request, env, WRITE_ROLES); if(!u.ok) return u.response;
        return linkReservation(request, env, id, u.email);
      }
      if(path === "/api/line-ops/logs" && request.method === "GET"){
        const u = await requireUser(request, env); if(!u.ok) return u.response;
        return json(await logs(env, url));
      }
      const res = await app.fetch(request, env, ctx);
      const ct = res.headers.get("content-type") || "";
      if(request.method === "GET" && ct.includes("text/html")) return new Response(injectLineOpsButton(await res.text()), { status:res.status, headers:res.headers });
      return res;
    }catch(e){
      return json({ ok:false, build:BUILD, message:String(e && e.message || e) }, 500);
    }
  }
};
