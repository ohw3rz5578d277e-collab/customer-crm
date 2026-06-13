// ======================================================
// CUSTOMER CRM / RESERVATION LINK DAILY SUMMARY WRAPPER
// build: customer-crm-api-reservation-link-daily-summary-20260613-01
// Adds a top dashboard panel for today's reservation-link alerts.
// ======================================================

import app from "./production-index-crm-link-alerts.js";

const BUILD = "customer-crm-api-reservation-link-daily-summary-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function normalizeEmail(v) {
  return text(v).toLowerCase();
}

function securityHeaders(headers = {}) {
  const h = new Headers(headers);
  h.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  h.set("pragma", "no-cache");
  h.set("expires", "0");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  return h;
}

function json(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: securityHeaders({ "content-type": "application/json; charset=utf-8", ...headers })
  });
}

function csv(textBody, filename) {
  return new Response("\ufeff" + textBody, {
    status: 200,
    headers: securityHeaders({
      "content-type": "text/csv; charset=utf-8",
      "content-disposition": `attachment; filename="${filename}"`
    })
  });
}

function csvCell(v) {
  let s = text(v).replace(/\r?\n/g, " ");
  if (/^[=+\-@]/.test(s)) s = "'" + s;
  return `"${s.replace(/"/g, '""')}"`;
}

function getAccessEmail(request) {
  return normalizeEmail(
    request.headers.get("cf-access-authenticated-user-email") ||
    request.headers.get("Cf-Access-Authenticated-User-Email") ||
    request.headers.get("cf-access-user-email") ||
    request.headers.get("x-user-email") ||
    ""
  );
}

async function addColumn(db, table, definition) {
  try { await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run(); } catch (_) {}
}

async function ensureSchema(env) {
  if (!env.DB) throw new Error("D1 DB binding(DB) is missing");

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users (
    email TEXT PRIMARY KEY,
    role TEXT,
    status TEXT,
    created_by TEXT,
    created_at TEXT,
    updated_at TEXT
  )`).run();

  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email, role, status, created_by, created_at, updated_at)
    VALUES(?, 'admin', 'active', 'system', datetime('now'), datetime('now'))`)
    .bind(ROOT_ADMIN_EMAIL)
    .run();

  for (const col of [
    "reservation_intake_id TEXT",
    "sent_to_reservation_at TEXT",
    "reservation_app_reservation_id TEXT",
    "reservation_app_created_at TEXT",
    "history_synced_at TEXT",
    "reservation_app_updated_at TEXT",
    "reservation_app_cancelled_at TEXT",
    "cancellation_synced_at TEXT"
  ]) await addColumn(env.DB, "crm_reservation_drafts", col);

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_reservation_link_alert_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    draft_id INTEGER NOT NULL,
    customer_id TEXT,
    stage_key TEXT NOT NULL,
    acknowledged_at TEXT DEFAULT CURRENT_TIMESTAMP,
    acknowledged_by TEXT,
    note TEXT
  )`).run();

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_daily_drafts_sent ON crm_reservation_drafts(sent_to_reservation_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_daily_drafts_created ON crm_reservation_drafts(reservation_app_created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_daily_drafts_cancelled ON crm_reservation_drafts(reservation_app_cancelled_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_daily_alert_checks ON crm_reservation_link_alert_checks(draft_id, stage_key, acknowledged_at)`).run();
}

async function requireEditor(request, env) {
  await ensureSchema(env);
  const email = getAccessEmail(request);
  if (!email) return { ok: false, response: json({ ok: false, message: "Login required" }, 401) };
  const user = await env.DB.prepare(
    `SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`
  ).bind(email).first();
  if (!user) return { ok: false, response: json({ ok: false, message: "User is not allowed" }, 403) };
  if (!WRITE_ROLES.includes(user.role || "")) return { ok: false, response: json({ ok: false, message: "Permission denied" }, 403) };
  return { ok: true, email, user };
}

function jstDate(value) {
  if (!value) return "";
  const s = String(value);
  const d = new Date(s.includes("T") ? s : s.replace(" ", "T") + "Z");
  if (Number.isNaN(d.getTime())) return s.slice(0, 10);
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

function todayJst() {
  const jst = new Date(Date.now() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

function classifyAlert(row) {
  const cancelled = !!(row.reservation_app_cancelled_at || row.status === "cancelled");
  const sent = !!row.sent_to_reservation_at;
  const created = !!row.reservation_app_created_at || !!row.reservation_app_reservation_id || row.status === "created";
  const historySynced = !!row.history_synced_at;
  const cancelSynced = !!row.cancellation_synced_at;

  if (cancelled && !cancelSynced) {
    return {
      stage_key: "cancel_unsynced",
      stage: "キャンセル未同期",
      severity: "danger",
      reason: "予約管理側でキャンセルされていますが、CRM側のキャンセル同期完了日時が未記録です。",
      alert_time: text(row.reservation_app_cancelled_at || row.updated_at || row.created_at)
    };
  }
  if (!cancelled && created && !historySynced) {
    return {
      stage_key: "created_unreflected",
      stage: "本予約作成済み・CRM履歴未反映",
      severity: "danger",
      reason: "予約管理で本予約は作成済みですが、CRM予約履歴への反映日時が未記録です。",
      alert_time: text(row.reservation_app_created_at || row.updated_at || row.created_at)
    };
  }
  if (!cancelled && sent && !created) {
    return {
      stage_key: "sent_uncreated",
      stage: "送信済み・本予約未作成",
      severity: "warn",
      reason: "予約管理へ送信済みですが、本予約IDがまだCRM側に戻っていません。",
      alert_time: text(row.sent_to_reservation_at || row.updated_at || row.created_at)
    };
  }
  return null;
}

async function getDailySummary(env) {
  await ensureSchema(env);
  const today = todayJst();

  const result = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts
    ORDER BY COALESCE(reservation_app_cancelled_at, reservation_app_updated_at, reservation_app_created_at, sent_to_reservation_at, updated_at, created_at, '') DESC
    LIMIT 1000`).all();
  const rows = result.results || [];

  const draftIds = rows.map((r) => Number(r.id)).filter((id) => Number.isFinite(id) && id > 0);
  const ackMap = new Map();
  if (draftIds.length) {
    const placeholders = draftIds.map(() => "?").join(",");
    const acks = await env.DB.prepare(`SELECT draft_id, stage_key, MAX(acknowledged_at) AS acknowledged_at, acknowledged_by
      FROM crm_reservation_link_alert_checks
      WHERE draft_id IN (${placeholders})
      GROUP BY draft_id, stage_key`).bind(...draftIds).all();
    for (const ack of (acks.results || [])) ackMap.set(`${ack.draft_id}:${ack.stage_key}`, ack);
  }

  const alerts = [];
  const todayItems = [];
  const counts = {
    total_drafts: rows.length,
    open_alerts: 0,
    unacknowledged_alerts: 0,
    danger_alerts: 0,
    warn_alerts: 0,
    sent_today: 0,
    created_today: 0,
    updated_today: 0,
    cancelled_today: 0,
    history_synced_today: 0,
    resync_needed: 0
  };

  for (const row of rows) {
    const alert = classifyAlert(row);
    if (alert) {
      const ack = ackMap.get(`${row.id}:${alert.stage_key}`);
      const acknowledged = !!(ack?.acknowledged_at && (!alert.alert_time || ack.acknowledged_at >= alert.alert_time));
      const item = { ...row, ...alert, acknowledged, acknowledged_at: acknowledged ? ack.acknowledged_at : "", acknowledged_by: acknowledged ? ack.acknowledged_by : "" };
      alerts.push(item);
      counts.open_alerts += 1;
      if (!acknowledged) counts.unacknowledged_alerts += 1;
      if (alert.severity === "danger") counts.danger_alerts += 1;
      if (alert.severity === "warn") counts.warn_alerts += 1;
      if (!acknowledged) counts.resync_needed += 1;
    }

    const flags = [];
    if (jstDate(row.sent_to_reservation_at) === today) { counts.sent_today += 1; flags.push("予約管理へ送信"); }
    if (jstDate(row.reservation_app_created_at) === today) { counts.created_today += 1; flags.push("本予約作成"); }
    if (jstDate(row.reservation_app_updated_at) === today) { counts.updated_today += 1; flags.push("予約変更同期"); }
    if (jstDate(row.reservation_app_cancelled_at) === today) { counts.cancelled_today += 1; flags.push("キャンセル"); }
    if (jstDate(row.history_synced_at) === today) { counts.history_synced_today += 1; flags.push("CRM履歴反映"); }
    if (flags.length) todayItems.push({ ...row, today_flags: flags });
  }

  alerts.sort((a, b) => String(b.alert_time || "").localeCompare(String(a.alert_time || "")));
  todayItems.sort((a, b) => String(b.updated_at || b.created_at || "").localeCompare(String(a.updated_at || a.created_at || "")));

  return {
    ok: true,
    build: BUILD,
    date_jst: today,
    counts,
    alerts: alerts.slice(0, 20),
    today_items: todayItems.slice(0, 30),
    checked_at: new Date().toISOString()
  };
}

async function dailySummaryApi(request, env) {
  const auth = await requireEditor(request, env);
  if (!auth.ok) return auth.response;
  const data = await getDailySummary(env);
  return json(data);
}

async function dailySummaryCsv(request, env) {
  const auth = await requireEditor(request, env);
  if (!auth.ok) return auth.response;
  const data = await getDailySummary(env);
  const header = ["区分", "下書きID", "顧客ID", "顧客名", "ステータス", "アラート", "重要度", "予約ID", "撮影日", "開始時間", "場所", "金額", "送信日時", "本予約作成日時", "CRM履歴反映日時", "キャンセル日時", "確認済み", "理由"];
  const lines = [header.map(csvCell).join(",")];
  for (const row of data.alerts) {
    lines.push([
      "要確認", row.id, row.customer_id, row.customer_name, row.status, row.stage, row.severity,
      row.reservation_app_reservation_id, row.shoot_date, row.start_time, row.place, row.amount,
      row.sent_to_reservation_at, row.reservation_app_created_at, row.history_synced_at, row.reservation_app_cancelled_at,
      row.acknowledged ? "確認済み" : "未確認", row.reason
    ].map(csvCell).join(","));
  }
  for (const row of data.today_items) {
    lines.push([
      "今日の動き", row.id, row.customer_id, row.customer_name, row.status, (row.today_flags || []).join(" / "), "",
      row.reservation_app_reservation_id, row.shoot_date, row.start_time, row.place, row.amount,
      row.sent_to_reservation_at, row.reservation_app_created_at, row.history_synced_at, row.reservation_app_cancelled_at,
      "", ""
    ].map(csvCell).join(","));
  }
  return csv(lines.join("\n"), `crm-reservation-daily-summary-${data.date_jst}.csv`);
}

function injectDailySummaryUi(html) {
  if (!html || html.includes("crmDailySummaryScript")) return html;

  const style = `<style id="crmDailySummaryStyle">
.crm-daily-summary{margin:14px auto 18px;max-width:1180px;border:1px solid #e2e8f0;background:linear-gradient(135deg,#f8fafc,#fff);border-radius:18px;padding:14px;box-shadow:0 10px 30px rgba(15,23,42,.06);font-family:inherit;color:#0f172a}.crm-daily-summary-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap}.crm-daily-summary-title{font-size:18px;font-weight:900;margin:0}.crm-daily-summary-sub{font-size:12px;color:#64748b;margin:4px 0 0}.crm-daily-summary-actions{display:flex;gap:8px;flex-wrap:wrap}.crm-daily-summary-actions button,.crm-daily-summary-actions a{border:1px solid #cbd5e1;background:#fff;border-radius:10px;padding:8px 10px;font-size:12px;font-weight:900;color:#0f172a;text-decoration:none;cursor:pointer}.crm-daily-summary-actions .danger{background:#991b1b;border-color:#991b1b;color:#fff}.crm-daily-summary-kpis{display:grid;grid-template-columns:repeat(6,minmax(100px,1fr));gap:8px;margin-top:12px}.crm-daily-summary-kpi{border:1px solid #e2e8f0;background:#fff;border-radius:14px;padding:10px}.crm-daily-summary-kpi b{display:block;font-size:22px;line-height:1}.crm-daily-summary-kpi span{display:block;font-size:11px;color:#64748b;font-weight:900;margin-top:5px}.crm-daily-summary-kpi.alert{border-color:#fecaca;background:#fff7f7;color:#991b1b}.crm-daily-summary-body{display:grid;grid-template-columns:1.1fr .9fr;gap:10px;margin-top:12px}.crm-daily-summary-box{border:1px solid #e2e8f0;background:#fff;border-radius:14px;padding:10px}.crm-daily-summary-box h3{font-size:14px;margin:0 0 8px;font-weight:900}.crm-daily-summary-row{border-top:1px solid #f1f5f9;padding:8px 0;font-size:13px}.crm-daily-summary-row:first-of-type{border-top:0}.crm-daily-summary-badge{display:inline-block;border-radius:999px;background:#fee2e2;color:#991b1b;font-size:11px;font-weight:900;padding:3px 7px;margin-right:5px}.crm-daily-summary-badge.warn{background:#fef3c7;color:#92400e}.crm-daily-summary-meta{font-size:11px;color:#64748b;margin-top:3px}@media(max-width:820px){.crm-daily-summary{margin:10px}.crm-daily-summary-kpis{grid-template-columns:repeat(2,1fr)}.crm-daily-summary-body{grid-template-columns:1fr}}
</style>`;

  const script = `<script id="crmDailySummaryScript">
(function(){
  if(window.__crmDailySummaryInstalled)return; window.__crmDailySummaryInstalled=true;
  function esc(v){return String(v == null ? '' : v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
  function short(v){return v ? String(v).replace('T',' ').slice(0,16) : '-';}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000002;background:#0f172a;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
  function panelHtml(){return '<section id="crmDailySummary" class="crm-daily-summary"><div class="crm-daily-summary-head"><div><h2 class="crm-daily-summary-title">今日の予約連携 要確認</h2><p class="crm-daily-summary-sub">予約管理との連携状況を毎日ここで確認できます。</p></div><div class="crm-daily-summary-actions"><button id="crmDailySummaryReload">更新</button><button id="crmDailySummaryAlerts" class="danger">アラートを開く</button><button id="crmDailySummaryMonitor">監視画面</button><a id="crmDailySummaryCsv" href="/api/reservation-link-daily-summary.csv">CSV</a></div></div><div id="crmDailySummaryKpis" class="crm-daily-summary-kpis"><div class="crm-daily-summary-kpi"><b>...</b><span>読み込み中</span></div></div><div class="crm-daily-summary-body"><div class="crm-daily-summary-box"><h3>未確認アラート</h3><div id="crmDailySummaryAlertsList"><div class="crm-daily-summary-row">読み込み中...</div></div></div><div class="crm-daily-summary-box"><h3>今日の動き</h3><div id="crmDailySummaryTodayList"><div class="crm-daily-summary-row">読み込み中...</div></div></div></div></section>';}
  function install(){if(document.getElementById('crmDailySummary'))return; var target=document.querySelector('main')||document.querySelector('#app')||document.querySelector('.container')||document.body; if(target===document.body){document.body.insertAdjacentHTML('afterbegin',panelHtml());}else{target.insertAdjacentHTML('afterbegin',panelHtml());} loadSummary();}
  async function loadSummary(){var k=document.getElementById('crmDailySummaryKpis');var a=document.getElementById('crmDailySummaryAlertsList');var t=document.getElementById('crmDailySummaryTodayList');try{var data=await api('/api/reservation-link-daily-summary');if(!data.ok)throw new Error(data.message||'load failed');var c=data.counts||{};if(k)k.innerHTML=[['未確認',c.unacknowledged_alerts||0,'alert'],['危険',c.danger_alerts||0,'alert'],['注意',c.warn_alerts||0,''],['今日送信',c.sent_today||0,''],['今日本予約',c.created_today||0,''],['今日キャンセル',c.cancelled_today||0,'']].map(function(x){return '<div class="crm-daily-summary-kpi '+x[2]+'"><b>'+esc(x[1])+'</b><span>'+esc(x[0])+'</span></div>'}).join('');var alerts=(data.alerts||[]).filter(function(r){return !r.acknowledged}).slice(0,6);if(a)a.innerHTML=alerts.length?alerts.map(function(r){return '<div class="crm-daily-summary-row"><span class="crm-daily-summary-badge '+(r.severity==='warn'?'warn':'')+'">'+esc(r.stage)+'</span><b>'+esc(r.customer_name||'-')+'</b><div class="crm-daily-summary-meta">下書きID '+esc(r.id)+' / 予約ID '+esc(r.reservation_app_reservation_id||'-')+' / '+short(r.alert_time)+'</div><div class="crm-daily-summary-meta">'+esc(r.reason||'')+'</div></div>';}).join(''):'<div class="crm-daily-summary-row">未確認アラートはありません。</div>';var today=(data.today_items||[]).slice(0,7);if(t)t.innerHTML=today.length?today.map(function(r){return '<div class="crm-daily-summary-row"><b>'+esc(r.customer_name||'-')+'</b> <span class="crm-daily-summary-meta">'+esc((r.today_flags||[]).join(' / '))+'</span><div class="crm-daily-summary-meta">撮影 '+esc(r.shoot_date||'-')+' '+esc(r.start_time||'')+' / 予約ID '+esc(r.reservation_app_reservation_id||'-')+'</div></div>';}).join(''):'<div class="crm-daily-summary-row">今日の予約連携の動きはまだありません。</div>'; }catch(e){if(k)k.innerHTML='<div class="crm-daily-summary-kpi alert"><b>!</b><span>読み込み失敗</span></div>'; if(a)a.innerHTML='<div class="crm-daily-summary-row">読み込み失敗：'+esc(e.message||e)+'</div>';}}
  document.addEventListener('click',function(e){var x=e.target;if(!x)return;if(x.id==='crmDailySummaryReload'){loadSummary();toast('更新しました')}if(x.id==='crmDailySummaryAlerts'){var b=document.getElementById('crmLinkAlertOpenBtn'); if(b)b.click(); else toast('アラート画面を開けませんでした')}if(x.id==='crmDailySummaryMonitor'){if(window.crmOpenReservationLinkMonitor){window.crmOpenReservationLinkMonitor();setTimeout(function(){var s=document.getElementById('crmLinkMonitorStatus');if(s){s.value='attention';s.dispatchEvent(new Event('change',{bubbles:true}));}},500)}else toast('監視画面を開けませんでした')}});
  document.addEventListener('DOMContentLoaded',install); setTimeout(install,900); setInterval(loadSummary,120000);
})();
</script>`;

  return html.replace("</head>", `${style}</head>`).replace("</body>", `${script}</body>`);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if ((url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") && request.method === "GET") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, time: new Date().toISOString() });
    }

    if (url.pathname === "/api/reservation-link-daily-summary" && request.method === "GET") {
      return dailySummaryApi(request, env);
    }
    if (url.pathname === "/api/reservation-link-daily-summary.csv" && request.method === "GET") {
      return dailySummaryCsv(request, env);
    }

    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if (type.includes("text/html")) {
      const body = injectDailySummaryUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};