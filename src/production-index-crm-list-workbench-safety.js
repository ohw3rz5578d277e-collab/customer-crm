// ======================================================
// CUSTOMER CRM / LIST WORKBENCH SAFETY WRAPPER
// build: customer-crm-api-list-workbench-safety-20260614-01
// Adds preview, safe execution, detailed results, and log detail for bulk list actions.
// ======================================================

import app from "./production-index-crm-list-workbench.js";

const BUILD = "customer-crm-api-list-workbench-safety-20260614-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){ return v === undefined || v === null ? "" : String(v).trim(); }
function lower(v){ return text(v).toLowerCase(); }
function nowIso(){ return new Date().toISOString(); }
function json(data, status = 200){ return new Response(JSON.stringify(data, null, 2), { status, headers:{ "content-type":"application/json; charset=utf-8", "cache-control":"no-store" } }); }
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
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_list_workbench_runs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type TEXT,
    action_type TEXT,
    status TEXT DEFAULT 'created',
    target_count INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,
    skipped_count INTEGER DEFAULT 0,
    target_ids_json TEXT,
    preview_json TEXT,
    result_json TEXT,
    payload_json TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    executed_at TEXT
  )`).run();
  await addColumn(env.DB, "crm_list_workbench_logs", "run_id INTEGER");
  await addColumn(env.DB, "crm_list_workbench_logs", "success_count INTEGER DEFAULT 0");
  await addColumn(env.DB, "crm_list_workbench_logs", "failed_count INTEGER DEFAULT 0");
  await addColumn(env.DB, "crm_list_workbench_logs", "result_json TEXT");
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_list_workbench_runs_created ON crm_list_workbench_runs(created_at)`).run();
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

async function resolveTargets(env, type, ids){
  const out = [];
  const failures = [];
  for(const rawId of ids.map(String).filter(Boolean).slice(0,200)){
    let row = null;
    if(type === "inquiries"){
      const q = await safeFirst(env, `SELECT id, customer_id, customer_name, genre, status, memo FROM crm_inquiry_pipeline WHERE id=?`, [rawId]);
      if(q) row = { source_id:rawId, customer_id:q.customer_id || `inquiry:${rawId}`, name:q.customer_name || `問い合わせ ${rawId}`, kind:"inquiry", raw:q };
    } else {
      const c = await safeFirst(env, `SELECT id, name, customer_name, customer_rank, next_offer, next_line_suggestion FROM customers WHERE id=?`, [rawId]);
      if(c) row = { source_id:rawId, customer_id:c.id, name:c.name || c.customer_name || `顧客 ${rawId}`, kind:"customer", raw:c };
    }
    if(row) out.push(row); else failures.push({ id:rawId, reason:"対象が見つかりません" });
  }
  return { targets:out, failures };
}

function buildMessage(target, body){
  const name = target.name || "お客様";
  const custom = text(body.message);
  if(custom) return custom.replace(/{{customer_name}}/g, name);
  return `${name}、いつもありがとうございます。\n前回の撮影から少しお日にちが経ちましたので、次回の記念撮影についてご案内です。\nご希望があれば、日程や場所だけでもお気軽にご相談ください。`;
}

async function previewApi(request, env){
  const auth = await requireUser(request, env, ROLES); if(!auth.ok) return auth.response;
  const body = await readJson(request);
  const type = text(body.type || "customers");
  const action = text(body.action || "line_draft");
  const ids = Array.isArray(body.ids) ? body.ids.map(String).filter(Boolean) : [];
  if(!ids.length) return json({ ok:false, message:"対象を選択してください" }, 400);
  const { targets, failures } = await resolveTargets(env, type, ids);
  const due = text(body.due_date) || new Date(Date.now()+86400000).toISOString().slice(0,10);
  const preview = targets.map(t=>({
    id:t.source_id,
    customer_id:t.customer_id,
    name:t.name,
    action,
    message:action === "line_draft" ? buildMessage(t, body) : undefined,
    title:action === "follow_task" ? (text(body.title)||"一覧から作成したフォロー") : undefined,
    due_date:action === "follow_task" ? due : undefined
  }));
  const run = await safeRun(env, `INSERT INTO crm_list_workbench_runs(run_type, action_type, status, target_count, target_ids_json, preview_json, payload_json, created_by, created_at) VALUES(?, ?, 'previewed', ?, ?, ?, ?, ?, datetime('now'))`, [type, action, preview.length, JSON.stringify(ids), JSON.stringify({ preview, failures }), JSON.stringify(body), auth.email]);
  return json({ ok:true, build:BUILD, run_id:run?.meta?.last_row_id || null, summary:{ selected:ids.length, valid:preview.length, invalid:failures.length }, preview, failures });
}

async function executeApi(request, env){
  const auth = await requireUser(request, env, WRITE_ROLES); if(!auth.ok) return auth.response;
  const body = await readJson(request);
  const type = text(body.type || "customers");
  const action = text(body.action || "line_draft");
  const ids = Array.isArray(body.ids) ? body.ids.map(String).filter(Boolean) : [];
  const confirm = body.confirm === true || body.confirm === "true";
  if(!confirm) return json({ ok:false, message:"実行前確認が必要です。confirm:true を指定してください" }, 400);
  const { targets, failures } = await resolveTargets(env, type, ids);
  const results = [];
  let success = 0;
  for(const t of targets){
    if(action === "follow_task"){
      const due = text(body.due_date) || new Date(Date.now()+86400000).toISOString().slice(0,10);
      const title = text(body.title) || "一覧から作成したフォロー";
      const r = await safeRun(env, `INSERT INTO crm_follow_tasks(customer_id, customer_name, task_type, title, due_date, status, source_type, created_by, created_at, updated_at) VALUES(?, ?, 'list_follow', ?, ?, 'open', 'list_workbench_safe', ?, datetime('now'), datetime('now'))`, [String(t.customer_id), t.name, title, due, auth.email]);
      if(r.error) results.push({ id:t.source_id, ok:false, reason:r.error }); else { success++; results.push({ id:t.source_id, ok:true, action, related_id:r?.meta?.last_row_id || null }); }
    } else {
      const msg = buildMessage(t, body);
      const r = await safeRun(env, `INSERT INTO customer_line_draft_logs(customer_id, customer_name, message, status, source_type, created_by, created_at, updated_at) VALUES(?, ?, ?, 'draft', 'list_workbench_safe', ?, datetime('now'), datetime('now'))`, [String(t.customer_id), t.name, msg, auth.email]);
      if(r.error) results.push({ id:t.source_id, ok:false, reason:r.error }); else { success++; results.push({ id:t.source_id, ok:true, action:"line_draft", related_id:r?.meta?.last_row_id || null }); }
    }
  }
  const failed = failures.length + results.filter(x=>!x.ok).length;
  const resultJson = JSON.stringify({ results, failures });
  const run = await safeRun(env, `INSERT INTO crm_list_workbench_runs(run_type, action_type, status, target_count, success_count, failed_count, skipped_count, target_ids_json, result_json, payload_json, created_by, created_at, executed_at) VALUES(?, ?, 'executed', ?, ?, ?, 0, ?, ?, ?, ?, datetime('now'), datetime('now'))`, [type, action, ids.length, success, failed, JSON.stringify(ids), resultJson, JSON.stringify(body), auth.email]);
  await safeRun(env, `INSERT INTO crm_list_workbench_logs(workbench_type, action_type, target_count, target_ids_json, payload_json, created_by, created_at, run_id, success_count, failed_count, result_json) VALUES(?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?)`, [type, action, ids.length, JSON.stringify(ids), JSON.stringify(body), auth.email, run?.meta?.last_row_id || null, success, failed, resultJson]);
  return json({ ok:true, run_id:run?.meta?.last_row_id || null, summary:{ selected:ids.length, success, failed }, results, failures });
}

async function logsApi(request, env){
  const auth = await requireUser(request, env, ROLES); if(!auth.ok) return auth.response;
  const url = new URL(request.url);
  const id = text(url.searchParams.get("id"));
  if(id){
    const row = await safeFirst(env, `SELECT * FROM crm_list_workbench_runs WHERE id=?`, [id]);
    return json({ ok:true, run:row });
  }
  const rows = await safeAll(env, `SELECT id, run_type, action_type, status, target_count, success_count, failed_count, created_by, created_at, executed_at FROM crm_list_workbench_runs ORDER BY id DESC LIMIT 100`);
  return json({ ok:true, runs:rows });
}

function injectSafetyUi(html){
  const style = `<style id="crmListSafetyStyle">
#crmListSafetyFab{position:fixed;right:18px;bottom:326px;z-index:99992;border:0;border-radius:999px;background:#b91c1c;color:#fff;padding:12px 15px;font-weight:900;box-shadow:0 10px 24px rgba(185,28,28,.25)}
#crmListSafetyPanel{display:none;position:fixed;right:18px;bottom:384px;width:min(760px,calc(100vw - 28px));max-height:76vh;overflow:auto;background:#fff;border:1px solid #fecaca;border-radius:18px;box-shadow:0 18px 50px rgba(15,23,42,.25);z-index:99992;padding:14px;color:#0f172a}
.crmSafeGrid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.crmSafeGrid input,.crmSafeGrid select,.crmSafeGrid textarea{border:1px solid #cbd5e1;border-radius:10px;padding:10px;font-size:14px}.crmSafeBtn{border:0;border-radius:10px;padding:10px 12px;font-weight:800;cursor:pointer}.crmSafePrimary{background:#0f172a;color:#fff}.crmSafeDanger{background:#b91c1c;color:#fff}.crmSafeCard{border:1px solid #e2e8f0;border-radius:12px;padding:10px;margin-top:8px;background:#f8fafc;white-space:pre-wrap;font-size:13px}@media(max-width:767px){#crmListSafetyFab{bottom:86px;right:106px}#crmListSafetyPanel{right:10px;bottom:138px;width:calc(100vw - 20px)}.crmSafeGrid{grid-template-columns:1fr}}
</style>`;
  const script = `<script id="crmListSafetyScript">
(function(){
 if(window.__crmListSafetyLoaded) return; window.__crmListSafetyLoaded=true;
 function el(id){return document.getElementById(id)}
 function ids(){return (el('crmSafeIds')?.value||'').split(',').map(x=>x.trim()).filter(Boolean)}
 async function post(path, body){const r=await fetch(path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});return await r.json()}
 function body(){return {type:el('crmSafeType').value,action:el('crmSafeAction').value,ids:ids(),message:el('crmSafeMessage').value,title:el('crmSafeTitle').value,due_date:el('crmSafeDue').value}}
 function show(v){el('crmSafeOut').textContent=typeof v==='string'?v:JSON.stringify(v,null,2)}
 async function preview(){show('確認中...');show(await post('/api/list-workbench/preview',body()))}
 async function execute(){if(!confirm('選択した対象に一括実行します。よろしいですか？'))return;const b=body();b.confirm=true;show('実行中...');show(await post('/api/list-workbench/execute',b))}
 async function logs(){const r=await fetch('/api/list-workbench/runs');show(await r.json())}
 function open(){let p=el('crmListSafetyPanel');if(!p)return;p.style.display=p.style.display==='block'?'none':'block'}
 function init(){if(el('crmListSafetyFab'))return;document.body.insertAdjacentHTML('beforeend','<button id="crmListSafetyFab">一括確認</button><div id="crmListSafetyPanel"><h3 style="margin:0 0 8px">一括操作の確認・実行</h3><p style="font-size:13px;color:#64748b">対象IDをカンマ区切りで入力し、まずプレビューしてから実行します。</p><div class="crmSafeGrid"><select id="crmSafeType"><option value="customers">顧客</option><option value="inquiries">問い合わせ</option><option value="marketing">マーケ候補</option></select><select id="crmSafeAction"><option value="line_draft">LINE未送信作成</option><option value="follow_task">フォロー予定作成</option></select><input id="crmSafeIds" placeholder="対象ID 例: 1,2,3"><input id="crmSafeDue" type="date"><input id="crmSafeTitle" placeholder="フォロータイトル"><textarea id="crmSafeMessage" placeholder="LINE文面。{{customer_name}} 使用可" rows="4"></textarea></div><div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap"><button class="crmSafeBtn crmSafePrimary" id="crmSafePreview">プレビュー</button><button class="crmSafeBtn crmSafeDanger" id="crmSafeExecute">確認して実行</button><button class="crmSafeBtn" id="crmSafeLogs">実行ログ</button></div><pre id="crmSafeOut" class="crmSafeCard">ここに確認結果が表示されます。</pre></div>');el('crmListSafetyFab').onclick=open;el('crmSafePreview').onclick=preview;el('crmSafeExecute').onclick=execute;el('crmSafeLogs').onclick=logs}
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
</script>`;
  return html.includes("</body>") ? html.replace("</body>", `${style}${script}</body>`) : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    if(url.pathname === "/api/list-workbench/preview" && request.method === "POST") return previewApi(request, env);
    if(url.pathname === "/api/list-workbench/execute" && request.method === "POST") return executeApi(request, env);
    if(url.pathname === "/api/list-workbench/runs" && request.method === "GET") return logsApi(request, env);
    const res = await app.fetch(request, env, ctx);
    const ct = res.headers.get("content-type") || "";
    if(request.method === "GET" && ct.includes("text/html")){
      const html = await res.text();
      return new Response(injectSafetyUi(html), { status:res.status, headers:res.headers });
    }
    return res;
  }
};
