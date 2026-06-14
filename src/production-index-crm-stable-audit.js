// ======================================================
// CUSTOMER CRM / STABLE AUDIT WRAPPER
// build: customer-crm-stable-audit-20260614-03
// - Final outer safety layer for admin UI
// - Adds /api/crm-health-check
// - Re-checks critical CRM tables before every request
// - Returns safe empty data for fragile dashboard endpoints
// - Keeps customer list return UX by wrapping customer-list-return
// ======================================================

import app from "./production-index-crm-customer-list-return.js";

const BUILD = "customer-crm-stable-audit-20260614-03";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store, no-cache, must-revalidate, max-age=0"
    }
  });
}

async function safeRun(env, sql){
  if(!env || !env.DB) return {ok:false, error:"DB binding missing"};
  try{ await env.DB.prepare(sql).run(); return {ok:true}; }
  catch(e){ return {ok:false, error:String(e && e.message || e)}; }
}

async function safeAll(env, sql){
  if(!env || !env.DB) return {ok:false, results:[], error:"DB binding missing"};
  try{ return await env.DB.prepare(sql).all(); }
  catch(e){ return {ok:false, results:[], error:String(e && e.message || e)}; }
}

async function addColumn(env, table, definition){
  await safeRun(env, `ALTER TABLE ${table} ADD COLUMN ${definition}`);
}

async function ensureStableSchema(env){
  if(!env || !env.DB) return;

  await safeRun(env, `CREATE TABLE IF NOT EXISTS crm_reservation_drafts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    customer_name TEXT,
    phone TEXT,
    email TEXT,
    genre TEXT,
    shoot_date TEXT,
    start_time TEXT,
    end_time TEXT,
    place TEXT,
    plan_label TEXT,
    total_amount REAL DEFAULT 0,
    status TEXT DEFAULT 'draft',
    memo TEXT,
    draft_json TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);

  await safeRun(env, `CREATE TABLE IF NOT EXISTS customer_line_draft_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    customer_name TEXT,
    action_type TEXT,
    action_label TEXT,
    priority TEXT DEFAULT 'medium',
    message_text TEXT,
    status TEXT DEFAULT 'copied',
    channel TEXT DEFAULT 'line',
    created_by TEXT,
    copied_at TEXT,
    sent_at TEXT,
    sent_by TEXT,
    response_status TEXT DEFAULT 'unknown',
    replied_at TEXT,
    led_to_reservation INTEGER DEFAULT 0,
    reservation_id TEXT,
    reservation_linked_at TEXT,
    memo TEXT,
    raw_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);

  await safeRun(env, `CREATE TABLE IF NOT EXISTS crm_follow_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    customer_name TEXT,
    task_type TEXT,
    title TEXT,
    message_text TEXT,
    due_date TEXT,
    priority TEXT DEFAULT 'medium',
    status TEXT DEFAULT 'open',
    created_by TEXT,
    completed_by TEXT,
    completed_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);

  await safeRun(env, `CREATE TABLE IF NOT EXISTS crm_delivery_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reservation_key TEXT,
    customer_id TEXT,
    customer_name TEXT,
    status TEXT DEFAULT 'open',
    progress_status TEXT,
    shoot_date TEXT,
    due_date TEXT,
    updated_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);

  await safeRun(env, `CREATE TABLE IF NOT EXISTS crm_inquiry_pipeline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    customer_name TEXT,
    status TEXT DEFAULT 'inquiry',
    source_type TEXT,
    source_name TEXT,
    estimated_amount REAL DEFAULT 0,
    memo TEXT,
    line_log_id INTEGER,
    follow_task_id INTEGER,
    reservation_draft_id INTEGER,
    converted_at TEXT,
    action_summary TEXT,
    last_action_at TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);

  await safeRun(env, `CREATE TABLE IF NOT EXISTS crm_line_ops_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    line_log_id INTEGER,
    customer_id TEXT,
    action_type TEXT,
    before_status TEXT,
    after_status TEXT,
    payload_json TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);

  await safeRun(env, `CREATE TABLE IF NOT EXISTS crm_list_workbench_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type TEXT,
    action_type TEXT,
    target_count INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,
    target_ids_json TEXT,
    result_json TEXT,
    payload_json TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);

  await safeRun(env, `CREATE TABLE IF NOT EXISTS crm_list_workbench_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    workbench_type TEXT,
    action_type TEXT,
    target_count INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,
    target_ids_json TEXT,
    payload_json TEXT,
    result_json TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);

  await safeRun(env, `CREATE TABLE IF NOT EXISTS crm_customer_smart_action_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    action_type TEXT,
    action_label TEXT,
    related_id TEXT,
    payload_json TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);

  const reservationCols = [
    "converted_at TEXT", "converted_by TEXT", "reservation_intake_id TEXT", "sent_to_reservation_at TEXT",
    "sent_to_reservation_by TEXT", "sent_to_reservation_response TEXT", "reservation_app_reservation_id TEXT",
    "reservation_app_intake_id TEXT", "reservation_app_created_at TEXT", "reservation_app_created_by TEXT",
    "reservation_app_response TEXT", "reservation_app_updated_at TEXT", "reservation_app_updated_by TEXT",
    "reservation_app_update_response TEXT", "reservation_app_cancelled_at TEXT", "reservation_app_cancelled_by TEXT",
    "reservation_app_cancel_reason TEXT", "reservation_app_cancel_response TEXT", "cancellation_synced_at TEXT", "history_synced_at TEXT"
  ];
  for(const col of reservationCols) await addColumn(env, "crm_reservation_drafts", col);

  const lineCols = [
    "sent_at TEXT", "sent_by TEXT", "response_status TEXT DEFAULT 'unknown'", "replied_at TEXT",
    "led_to_reservation INTEGER DEFAULT 0", "reservation_id TEXT", "reservation_linked_at TEXT", "memo TEXT", "raw_json TEXT", "updated_at TEXT"
  ];
  for(const col of lineCols) await addColumn(env, "customer_line_draft_logs", col);

  const inquiryCols = [
    "line_log_id INTEGER", "follow_task_id INTEGER", "reservation_draft_id INTEGER", "converted_at TEXT",
    "action_summary TEXT", "last_action_at TEXT", "source_type TEXT", "source_name TEXT", "estimated_amount REAL DEFAULT 0"
  ];
  for(const col of inquiryCols) await addColumn(env, "crm_inquiry_pipeline", col);
}

function safePayload(path){
  const base = {ok:true, build:BUILD, degraded:true, message:"一部データを読み込めなかったため、空データで表示しています。"};
  if(path === "/api/today-dashboard") return {...base, counts:{reservation_alerts:0,reservation_danger:0,line_pending:0,line_high:0,follow_due:0,follow_overdue:0,sent_today:0,created_today:0,cancelled_today:0,sales_total:0,customer_count:0,repeat_customers:0,dormant_customers:0}, priority_items:[], reservation_alerts:[], line_pending:[], follow_tasks:[], sales_focus:[]};
  if(path === "/api/today-dashboard/action-queue") return {...base, items:[], counts:{total:0,open:0,completed:0}};
  if(path === "/api/reservation-link-alerts") return {...base, alerts:[], counts:{total:0,danger:0,warn:0}};
  if(path === "/api/reservation-link-monitor") return {...base, rows:[], alerts:[], counts:{total:0}};
  if(path === "/api/delivery-dashboard") return {...base, rows:[], counts:{total:0,open:0,done:0}};
  if(path === "/api/marketing-candidates") return {...base, candidates:[], rows:[], counts:{total:0}};
  if(path === "/api/line-ops/dashboard") return {...base, counts:{total:0,pending:0,sent:0,replied:0,no_reply:0,led_to_reservation:0}};
  if(path === "/api/line-ops/drafts") return {...base, rows:[], items:[], counts:{total:0}};
  if(path === "/api/line-ops/logs") return {...base, rows:[], items:[], counts:{total:0}};
  if(path === "/api/list-workbench/runs") return {...base, rows:[], items:[], counts:{total:0}};
  return null;
}

async function healthCheck(env){
  await ensureStableSchema(env);
  const required = ["customers", "crm_reservation_drafts", "customer_line_draft_logs", "crm_follow_tasks", "crm_delivery_progress", "crm_inquiry_pipeline", "crm_line_ops_logs", "crm_list_workbench_runs", "crm_list_workbench_logs", "crm_customer_smart_action_logs"];
  const tables = [];
  for(const name of required){
    const hit = await safeAll(env, `SELECT name FROM sqlite_master WHERE type='table' AND name='${name.replace(/'/g,"''")}' LIMIT 1`);
    tables.push({table:name, ok:!!(hit.results && hit.results.length)});
  }
  return json({ok:!!env.DB, build:BUILD, checked_at:new Date().toISOString(), bindings:{DB:!!env.DB, LINE_SERVICE:!!env.LINE_SERVICE, RESERVATION_SERVICE:!!env.RESERVATION_SERVICE}, tables});
}

function injectStableAuditUi(html){
  if(!html || html.includes("crm-stable-audit-script")) return html;
  const style = `<style id="crm-stable-audit-style">.crm-stable-audit-btn{position:fixed!important;right:18px!important;bottom:176px!important;z-index:2147482500!important;min-height:44px!important;padding:0 14px!important;border:0!important;border-radius:999px!important;background:#07111f!important;color:#fff!important;font-weight:950!important;box-shadow:0 18px 35px rgba(15,23,42,.25)!important;cursor:pointer!important}.crm-stable-audit-panel{position:fixed!important;right:18px!important;bottom:230px!important;width:min(520px,calc(100vw - 36px))!important;max-height:70vh!important;overflow:auto!important;background:#fff!important;border:1px solid #dbe5ef!important;border-radius:22px!important;box-shadow:0 24px 60px rgba(15,23,42,.22)!important;z-index:2147482600!important;padding:18px!important;display:none!important}.crm-stable-audit-panel.open{display:block!important}.crm-stable-audit-panel pre{white-space:pre-wrap!important;background:#f8fafc!important;border:1px solid #e2e8f0!important;border-radius:14px!important;padding:12px!important;font-size:12px!important;line-height:1.6!important}.crm-stable-audit-close{float:right;border:0!important;background:#f1f5f9!important;border-radius:999px!important;width:34px!important;height:34px!important;font-weight:950!important;cursor:pointer!important}@media(max-width:767px){.crm-stable-audit-btn{right:12px!important;bottom:154px!important}.crm-stable-audit-panel{inset:10px!important;width:auto!important;max-height:calc(100dvh - 20px)!important;border-radius:18px!important}}</style>`;
  const script = `<script id="crm-stable-audit-script">(()=>{if(window.__crmStableAudit)return;window.__crmStableAudit=1;function make(){if(document.getElementById('crmStableAuditBtn'))return;const btn=document.createElement('button');btn.id='crmStableAuditBtn';btn.className='crm-stable-audit-btn';btn.type='button';btn.textContent='状態確認';const panel=document.createElement('div');panel.id='crmStableAuditPanel';panel.className='crm-stable-audit-panel';panel.innerHTML='<button class="crm-stable-audit-close" type="button">×</button><h3>CRM状態確認</h3><p>主要テーブル・連携設定を確認します。</p><pre>未確認</pre>';panel.querySelector('.crm-stable-audit-close').onclick=()=>panel.classList.remove('open');btn.onclick=async()=>{panel.classList.add('open');const pre=panel.querySelector('pre');pre.textContent='確認中...';try{const r=await fetch('/api/crm-health-check',{cache:'no-store'});pre.textContent=JSON.stringify(await r.json(),null,2);}catch(e){pre.textContent='状態確認に失敗しました: '+(e&&e.message||e);}};document.body.appendChild(btn);document.body.appendChild(panel);}if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',make);else make();new MutationObserver(make).observe(document.documentElement,{childList:true,subtree:true});})();</script>`;
  return html.includes("</body>") ? html.replace("</body>", style + script + "</body>") : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    if(request.method === "GET" && url.pathname === "/api/crm-health-check") return healthCheck(env);
    await ensureStableSchema(env);
    try{
      const res = await app.fetch(request, env, ctx);
      const ct = res.headers.get("content-type") || "";
      if(!res.ok && request.method === "GET"){
        const payload = safePayload(url.pathname);
        if(payload) return json(payload, 200);
      }
      if(request.method === "GET" && ct.includes("text/html")){
        const html = await res.text();
        return new Response(injectStableAuditUi(html), {status:res.status, headers:res.headers});
      }
      return res;
    }catch(e){
      const payload = request.method === "GET" ? safePayload(url.pathname) : null;
      if(payload) return json({...payload, caught_error:String(e && e.message || e)}, 200);
      return json({ok:false, build:BUILD, error:String(e && e.message || e)}, 500);
    }
  }
};
