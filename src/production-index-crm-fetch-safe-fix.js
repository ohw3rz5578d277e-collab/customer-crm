// ======================================================
// CUSTOMER CRM / FETCH SAFE + SELF HEAL FIX WRAPPER
// build: customer-crm-api-fetch-safe-fix-20260614-01
// - Prevent dashboard UI from breaking on Failed to fetch
// - Add safe fallback responses for high-traffic dashboard APIs
// - Broaden D1 self-healing for missing CRM tables used by dashboard/UI
// ======================================================

import app from "./production-index-crm-stability-ux-fix.js";

const BUILD = "customer-crm-api-fetch-safe-fix-20260614-01";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store, no-cache, must-revalidate, max-age=0"
    }
  });
}

async function safeExec(env, sql){
  if(!env || !env.DB) return false;
  try{ await env.DB.prepare(sql).run(); return true; }catch(_){ return false; }
}

async function addColumn(env, table, definition){
  await safeExec(env, `ALTER TABLE ${table} ADD COLUMN ${definition}`);
}

async function ensureCoreCrmSchema(env){
  if(!env || !env.DB) return;

  await safeExec(env, `CREATE TABLE IF NOT EXISTS crm_reservation_drafts (
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
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    converted_at TEXT,
    converted_by TEXT,
    reservation_intake_id TEXT,
    sent_to_reservation_at TEXT,
    sent_to_reservation_by TEXT,
    sent_to_reservation_response TEXT,
    reservation_app_reservation_id TEXT,
    reservation_app_intake_id TEXT,
    reservation_app_created_at TEXT,
    reservation_app_created_by TEXT,
    reservation_app_response TEXT,
    reservation_app_updated_at TEXT,
    reservation_app_updated_by TEXT,
    reservation_app_update_response TEXT,
    reservation_app_cancelled_at TEXT,
    reservation_app_cancelled_by TEXT,
    reservation_app_cancel_reason TEXT,
    reservation_app_cancel_response TEXT,
    cancellation_synced_at TEXT,
    history_synced_at TEXT
  )`);

  await safeExec(env, `CREATE TABLE IF NOT EXISTS customer_line_draft_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    customer_name TEXT,
    action_type TEXT,
    action_label TEXT,
    priority TEXT,
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

  await safeExec(env, `CREATE TABLE IF NOT EXISTS crm_follow_tasks (
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

  await safeExec(env, `CREATE TABLE IF NOT EXISTS crm_reservation_link_alert_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    draft_id INTEGER,
    customer_id TEXT,
    stage_key TEXT,
    acknowledged_at TEXT DEFAULT CURRENT_TIMESTAMP,
    acknowledged_by TEXT,
    note TEXT
  )`);

  await safeExec(env, `CREATE TABLE IF NOT EXISTS crm_delivery_progress (
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

  await safeExec(env, `CREATE TABLE IF NOT EXISTS crm_inquiry_pipeline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    customer_name TEXT,
    status TEXT DEFAULT 'inquiry',
    source_type TEXT,
    source_name TEXT,
    estimated_amount REAL DEFAULT 0,
    memo TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`);

  const reservationColumns = [
    "history_synced_at TEXT", "reservation_intake_id TEXT", "sent_to_reservation_at TEXT",
    "sent_to_reservation_by TEXT", "sent_to_reservation_response TEXT", "reservation_app_reservation_id TEXT",
    "reservation_app_intake_id TEXT", "reservation_app_created_at TEXT", "reservation_app_created_by TEXT",
    "reservation_app_response TEXT", "reservation_app_updated_at TEXT", "reservation_app_updated_by TEXT",
    "reservation_app_update_response TEXT", "reservation_app_cancelled_at TEXT", "reservation_app_cancelled_by TEXT",
    "reservation_app_cancel_reason TEXT", "reservation_app_cancel_response TEXT", "cancellation_synced_at TEXT"
  ];
  for(const col of reservationColumns) await addColumn(env, "crm_reservation_drafts", col);

  for(const col of [
    "sent_by TEXT", "response_status TEXT DEFAULT 'unknown'", "replied_at TEXT",
    "led_to_reservation INTEGER DEFAULT 0", "reservation_id TEXT", "reservation_linked_at TEXT"
  ]) await addColumn(env, "customer_line_draft_logs", col);

  await safeExec(env, `CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_status_safe ON crm_reservation_drafts(status, created_at)`);
  await safeExec(env, `CREATE INDEX IF NOT EXISTS idx_customer_line_draft_logs_status_safe ON customer_line_draft_logs(status, created_at)`);
  await safeExec(env, `CREATE INDEX IF NOT EXISTS idx_crm_follow_tasks_status_safe ON crm_follow_tasks(status, due_date)`);
  await safeExec(env, `CREATE INDEX IF NOT EXISTS idx_crm_inquiry_pipeline_status_safe ON crm_inquiry_pipeline(status, created_at)`);
}

function emptyTodayDashboard(){
  const today = new Date(Date.now() + 9 * 60 * 60 * 1000).toISOString().slice(0,10);
  return {
    ok: true,
    build: BUILD,
    degraded: true,
    message: "一部データを読み込めませんでした。画面は空データで表示しています。",
    date_jst: today,
    counts: {
      reservation_alerts: 0,
      reservation_danger: 0,
      line_pending: 0,
      line_high: 0,
      follow_due: 0,
      follow_overdue: 0,
      sent_today: 0,
      created_today: 0,
      cancelled_today: 0,
      sales_total: 0,
      customer_count: 0,
      repeat_customers: 0,
      dormant_customers: 0
    },
    priority_items: [],
    reservation_alerts: [],
    line_pending: [],
    follow_tasks: [],
    sales_focus: [],
    checked_at: new Date().toISOString()
  };
}

function emptyQueue(){
  return { ok:true, build:BUILD, degraded:true, items:[], counts:{ total:0, open:0, completed:0 }, message:"読み込める操作はまだありません。" };
}

function emptyAlerts(){
  return { ok:true, build:BUILD, degraded:true, alerts:[], counts:{ total:0, danger:0, warn:0 }, message:"予約連携アラートはありません。" };
}

async function safeFallbackResponse(path){
  if(path === "/api/today-dashboard") return json(emptyTodayDashboard());
  if(path === "/api/today-dashboard/action-queue") return json(emptyQueue());
  if(path === "/api/reservation-link-alerts") return json(emptyAlerts());
  if(path === "/api/reservation-link-monitor") return json({ ok:true, build:BUILD, degraded:true, rows:[], alerts:[], counts:{ total:0 }, message:"予約連携監視データはまだありません。" });
  if(path === "/api/delivery-dashboard") return json({ ok:true, build:BUILD, degraded:true, rows:[], counts:{ total:0, open:0, done:0 }, message:"納品進捗データはまだありません。" });
  if(path === "/api/marketing-candidates") return json({ ok:true, build:BUILD, degraded:true, candidates:[], rows:[], counts:{ total:0 }, message:"マーケ候補はまだありません。" });
  return null;
}

function injectFetchSafeUi(html){
  if(!html || html.includes("crm-fetch-safe-fix-script")) return html;

  const style = `<style id="crm-fetch-safe-fix-style">
.crm-fetch-safe-note{border:1px solid #bfdbfe!important;background:#eff6ff!important;color:#1e3a8a!important;border-radius:14px!important;padding:10px 12px!important;font-weight:800!important;font-size:13px!important;line-height:1.6!important}
.crm-fetch-safe-error{border:1px solid #fecaca!important;background:#fff1f2!important;color:#991b1b!important;border-radius:14px!important;padding:10px 12px!important;font-weight:900!important;font-size:13px!important;line-height:1.6!important}
.crm-fetch-safe-empty{color:#64748b!important;font-weight:800!important;padding:10px!important;border:1px dashed #cbd5e1!important;border-radius:14px!important;background:#f8fafc!important}
</style>`;

  const script = `<script id="crm-fetch-safe-fix-script">
(()=>{if(window.__crmFetchSafeFix)return;window.__crmFetchSafeFix=1;
const originalFetch=window.fetch.bind(window);
const safePaths=['/api/today-dashboard','/api/today-dashboard/action-queue','/api/reservation-link-alerts','/api/reservation-link-monitor','/api/delivery-dashboard','/api/marketing-candidates'];
function fallback(path){
 const today=new Date(Date.now()+9*60*60*1000).toISOString().slice(0,10);
 if(path==='/api/today-dashboard')return {ok:true,degraded:true,message:'一部データを読み込めませんでした。空データで表示しています。',date_jst:today,counts:{reservation_alerts:0,reservation_danger:0,line_pending:0,line_high:0,follow_due:0,follow_overdue:0,sent_today:0,created_today:0,cancelled_today:0,sales_total:0,customer_count:0,repeat_customers:0,dormant_customers:0},priority_items:[],reservation_alerts:[],line_pending:[],follow_tasks:[],sales_focus:[],checked_at:new Date().toISOString()};
 if(path==='/api/today-dashboard/action-queue')return {ok:true,degraded:true,items:[],counts:{total:0,open:0,completed:0},message:'今日の操作はまだありません。'};
 if(path==='/api/reservation-link-alerts')return {ok:true,degraded:true,alerts:[],counts:{total:0,danger:0,warn:0},message:'予約連携アラートはありません。'};
 if(path==='/api/reservation-link-monitor')return {ok:true,degraded:true,rows:[],alerts:[],counts:{total:0},message:'予約連携監視データはまだありません。'};
 if(path==='/api/delivery-dashboard')return {ok:true,degraded:true,rows:[],counts:{total:0,open:0,done:0},message:'納品進捗データはまだありません。'};
 if(path==='/api/marketing-candidates')return {ok:true,degraded:true,candidates:[],rows:[],counts:{total:0},message:'マーケ候補はまだありません。'};
 return {ok:true,degraded:true,rows:[],message:'データを読み込めませんでした。'};
}
window.fetch=function(input,init){
 const url=typeof input==='string'?input:(input&&input.url)||'';
 let path='';try{path=new URL(url,location.origin).pathname}catch(e){path=url}
 if(!safePaths.includes(path))return originalFetch(input,init);
 return originalFetch(input,init).then(res=>{
   if(res && res.ok)return res;
   return new Response(JSON.stringify(fallback(path)),{status:200,headers:{'content-type':'application/json'}});
 }).catch(()=>new Response(JSON.stringify(fallback(path)),{status:200,headers:{'content-type':'application/json'}}));
};
function cleanupFailedFetchText(){document.querySelectorAll('*').forEach(el=>{if(el.childElementCount)return;const t=(el.textContent||'').trim();if(t==='Failed to fetch'||t.includes('読み込み失敗： Failed to fetch')||t.includes('読み込み失敗: Failed to fetch')){el.className+=' crm-fetch-safe-error';el.textContent='一部データを読み込めませんでした。画面は表示できています。更新、または少し時間をおいて再確認してください。';}})}
setInterval(cleanupFailedFetchText,800);cleanupFailedFetchText();
})();
</script>`;

  return html.includes("</body>") ? html.replace("</body>", style + script + "</body>") : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    try{
      await ensureCoreCrmSchema(env);
      const res = await app.fetch(request, env, ctx);
      const ct = res.headers.get("content-type") || "";

      // Convert failing high-traffic JSON API calls to safe empty responses instead of breaking the UI.
      if(!res.ok && request.method === "GET"){
        const fallback = await safeFallbackResponse(url.pathname);
        if(fallback) return fallback;
      }

      if(request.method === "GET" && ct.includes("text/html")){
        return new Response(injectFetchSafeUi(await res.text()), { status: res.status, headers: res.headers });
      }
      return res;
    }catch(e){
      const fallback = await safeFallbackResponse(url.pathname);
      if(fallback) return fallback;
      return json({ ok:false, build:BUILD, message:String(e && e.message || e) }, 500);
    }
  }
};
