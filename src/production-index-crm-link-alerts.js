// ======================================================
// CUSTOMER CRM / RESERVATION LINK ALERTS WRAPPER
// build: customer-crm-api-reservation-link-alerts-20260613-01
// Adds in-app alerts for reservation link issues.
// ======================================================

import app from "./production-index-crm-link-resync.js";

const BUILD = "customer-crm-api-reservation-link-alerts-20260613-01";
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

async function readJson(request) {
  try { return await request.json(); } catch (_) { return {}; }
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

function safeInt(v, fallback = 100, max = 500) {
  const n = Number.parseInt(String(v || ""), 10);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(n, max);
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

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_reservation_link_alert_checks_draft
    ON crm_reservation_link_alert_checks(draft_id, stage_key, acknowledged_at)`).run();

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_reservation_link_alert_checks_customer
    ON crm_reservation_link_alert_checks(customer_id, acknowledged_at)`).run();
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

function alertPathDraftId(path) {
  const m = path.match(/^\/api\/reservation-link-alerts\/([^/]+)\/ack$/);
  if (!m) return 0;
  const id = parseInt(decodeURIComponent(m[1]), 10);
  return Number.isFinite(id) && id > 0 ? id : 0;
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

async function getAlertRows(env, url) {
  await ensureSchema(env);
  const keyword = text(url.searchParams.get("keyword"));
  const onlyUnacked = ["1", "true", "yes"].includes(text(url.searchParams.get("unacked")).toLowerCase());
  const limit = safeInt(url.searchParams.get("limit"), 100, 500);

  const where = [`(
    (COALESCE(sent_to_reservation_at, '') <> '' AND COALESCE(reservation_app_reservation_id, '') = '' AND COALESCE(reservation_app_cancelled_at, '') = '')
    OR
    (COALESCE(reservation_app_reservation_id, '') <> '' AND COALESCE(history_synced_at, '') = '' AND COALESCE(reservation_app_cancelled_at, '') = '')
    OR
    ((COALESCE(reservation_app_cancelled_at, '') <> '' OR status='cancelled') AND COALESCE(cancellation_synced_at, '') = '')
  )`];
  const binds = [];

  if (keyword) {
    where.push(`(customer_id LIKE ? OR customer_name LIKE ? OR genre LIKE ? OR place LIKE ? OR reservation_app_reservation_id LIKE ? OR reservation_intake_id LIKE ?)`);
    const q = `%${keyword}%`;
    binds.push(q, q, q, q, q, q);
  }

  const result = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts
    WHERE ${where.join(" AND ")}
    ORDER BY COALESCE(reservation_app_cancelled_at, reservation_app_created_at, sent_to_reservation_at, updated_at, created_at, '') DESC
    LIMIT ?`).bind(...binds, limit).all();

  const rawRows = result.results || [];
  const draftIds = rawRows.map((r) => r.id).filter((id) => Number.isFinite(Number(id)));
  let ackMap = new Map();
  if (draftIds.length) {
    const placeholders = draftIds.map(() => "?").join(",");
    const acks = await env.DB.prepare(`SELECT draft_id, stage_key, MAX(acknowledged_at) AS acknowledged_at, acknowledged_by
      FROM crm_reservation_link_alert_checks
      WHERE draft_id IN (${placeholders})
      GROUP BY draft_id, stage_key`).bind(...draftIds).all();
    for (const ack of (acks.results || [])) {
      ackMap.set(`${ack.draft_id}:${ack.stage_key}`, ack);
    }
  }

  const rows = [];
  for (const row of rawRows) {
    const alert = classifyAlert(row);
    if (!alert) continue;
    const ack = ackMap.get(`${row.id}:${alert.stage_key}`);
    const acknowledged = !!(ack?.acknowledged_at && (!alert.alert_time || ack.acknowledged_at >= alert.alert_time));
    const item = {
      ...row,
      ...alert,
      acknowledged,
      acknowledged_at: acknowledged ? ack.acknowledged_at : "",
      acknowledged_by: acknowledged ? ack.acknowledged_by : ""
    };
    if (!onlyUnacked || !item.acknowledged) rows.push(item);
  }

  const summary = rows.reduce((acc, row) => {
    acc.total += 1;
    acc[row.stage_key] = (acc[row.stage_key] || 0) + 1;
    if (!row.acknowledged) acc.unacknowledged += 1;
    if (row.severity === "danger") acc.danger += 1;
    if (row.severity === "warn") acc.warn += 1;
    return acc;
  }, { total: 0, unacknowledged: 0, danger: 0, warn: 0, sent_uncreated: 0, created_unreflected: 0, cancel_unsynced: 0 });

  return { rows, summary, keyword, only_unacked: onlyUnacked, limit };
}

async function alertsApi(request, env, url) {
  const auth = await requireEditor(request, env);
  if (!auth.ok) return auth.response;
  const data = await getAlertRows(env, url);
  return json({ ok: true, build: BUILD, ...data });
}

async function ackOne(request, env, draftId) {
  const auth = await requireEditor(request, env);
  if (!auth.ok) return auth.response;
  const body = await readJson(request);
  const row = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(draftId).first();
  if (!row) return json({ ok: false, message: "reservation draft not found" }, 404);
  const alert = classifyAlert(row);
  if (!alert) return json({ ok: true, skipped: true, message: "現在この下書きに要確認アラートはありません。" });

  await env.DB.prepare(`INSERT INTO crm_reservation_link_alert_checks(
    draft_id, customer_id, stage_key, acknowledged_at, acknowledged_by, note
  ) VALUES(?,?,?,?,?,?)`)
    .bind(draftId, text(row.customer_id), alert.stage_key, new Date().toISOString(), auth.email, text(body.note))
    .run();

  return json({ ok: true, build: BUILD, draft_id: draftId, stage_key: alert.stage_key, acknowledged_by: auth.email });
}

async function ackAll(request, env) {
  const auth = await requireEditor(request, env);
  if (!auth.ok) return auth.response;
  const url = new URL(request.url);
  url.searchParams.set("unacked", "1");
  url.searchParams.set("limit", text(url.searchParams.get("limit") || "500"));
  const data = await getAlertRows(env, url);
  const now = new Date().toISOString();
  let count = 0;
  for (const row of data.rows) {
    if (row.acknowledged) continue;
    await env.DB.prepare(`INSERT INTO crm_reservation_link_alert_checks(
      draft_id, customer_id, stage_key, acknowledged_at, acknowledged_by, note
    ) VALUES(?,?,?,?,?,?)`)
      .bind(row.id, text(row.customer_id), row.stage_key, now, auth.email, "bulk acknowledged")
      .run();
    count += 1;
  }
  return json({ ok: true, build: BUILD, acknowledged: count });
}

function injectAlertUi(html) {
  if (!html || html.includes("crmLinkAlertScript")) return html;

  const style = `<style id="crmLinkAlertStyle">
.crm-link-alert-btn{position:fixed;right:14px;bottom:74px;z-index:999998;border:1px solid #fecaca;background:#fef2f2;color:#991b1b;border-radius:999px;padding:10px 14px;font-weight:900;box-shadow:0 10px 30px rgba(15,23,42,.18);cursor:pointer;display:none}.crm-link-alert-btn.ok{border-color:#bbf7d0;background:#f0fdf4;color:#166534}.crm-link-alert-modal{position:fixed;inset:0;background:rgba(15,23,42,.55);z-index:1000001;display:none;align-items:flex-start;justify-content:center;padding:30px 12px;overflow:auto}.crm-link-alert-panel{width:min(980px,100%);background:#fff;border-radius:18px;padding:18px;box-shadow:0 24px 70px rgba(15,23,42,.28);color:#0f172a}.crm-link-alert-head{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap}.crm-link-alert-title{font-size:20px;font-weight:900;margin:0}.crm-link-alert-sub{font-size:13px;color:#64748b;margin:5px 0 0}.crm-link-alert-actions{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}.crm-link-alert-actions button,.crm-link-alert-row button{border:1px solid #cbd5e1;background:#fff;border-radius:10px;padding:8px 10px;font-weight:800;cursor:pointer}.crm-link-alert-actions .danger{background:#991b1b;border-color:#991b1b;color:#fff}.crm-link-alert-kpis{display:grid;grid-template-columns:repeat(4,minmax(120px,1fr));gap:8px;margin:12px 0}.crm-link-alert-kpi{border:1px solid #e2e8f0;background:#f8fafc;border-radius:14px;padding:10px}.crm-link-alert-kpi b{display:block;font-size:22px}.crm-link-alert-kpi span{font-size:12px;color:#64748b;font-weight:800}.crm-link-alert-list{display:grid;gap:10px}.crm-link-alert-row{border:1px solid #e2e8f0;border-radius:14px;padding:12px;background:#fff}.crm-link-alert-row.danger{border-color:#fecaca;background:#fff7f7}.crm-link-alert-row.warn{border-color:#fde68a;background:#fffbeb}.crm-link-alert-badge{display:inline-flex;border-radius:999px;padding:4px 8px;font-size:12px;font-weight:900;margin-bottom:6px}.crm-link-alert-badge.danger{background:#fee2e2;color:#991b1b}.crm-link-alert-badge.warn{background:#fef3c7;color:#92400e}.crm-link-alert-meta{font-size:12px;color:#64748b;margin-top:4px}.crm-link-alert-row-actions{display:flex;gap:7px;flex-wrap:wrap;margin-top:10px}@media(max-width:760px){.crm-link-alert-kpis{grid-template-columns:repeat(2,1fr)}.crm-link-alert-btn{right:10px;bottom:70px}}
</style>`;

  const script = `<script id="crmLinkAlertScript">
(function(){
  if(window.__crmLinkAlertInstalled)return; window.__crmLinkAlertInstalled=true;
  function esc(v){return String(v == null ? '' : v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
  function short(v){return v ? String(v).replace('T',' ').slice(0,16) : '-';}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000002;background:#0f172a;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function openModal(){var m=document.getElementById('crmLinkAlertModal'); if(m){m.style.display='flex'; loadAlerts(false);}}
  function closeModal(){var m=document.getElementById('crmLinkAlertModal'); if(m)m.style.display='none';}
  async function refreshBadge(){
    try{var data=await api('/api/reservation-link-alerts?unacked=1&limit=100'); var btn=document.getElementById('crmLinkAlertOpenBtn'); if(!btn||!data.ok)return; var n=(data.summary&&data.summary.unacknowledged)||0; btn.style.display='block'; btn.className='crm-link-alert-btn '+(n?'':'ok'); btn.textContent=n?'予約連携 要確認 '+n+'件':'予約連携 OK';}
    catch(e){}
  }
  async function loadAlerts(unacked){
    var list=document.getElementById('crmLinkAlertRows'); var kpi=document.getElementById('crmLinkAlertKpis'); if(list)list.innerHTML='<div class="crm-link-alert-row">読み込み中...</div>';
    try{var data=await api('/api/reservation-link-alerts?'+(unacked?'unacked=1&':'')+'limit=200'); if(!data.ok)throw new Error(data.message||'load failed'); var s=data.summary||{};
      if(kpi)kpi.innerHTML=[['合計',s.total||0],['未確認',s.unacknowledged||0],['危険',s.danger||0],['注意',s.warn||0]].map(function(x){return '<div class="crm-link-alert-kpi"><b>'+esc(x[1])+'</b><span>'+esc(x[0])+'</span></div>'}).join('');
      var rows=data.rows||[]; if(!list)return; if(!rows.length){list.innerHTML='<div class="crm-link-alert-row">現在、要確認の予約連携はありません。</div>';return;}
      list.innerHTML=rows.map(function(r){return '<div class="crm-link-alert-row '+esc(r.severity||'warn')+'" data-draft-id="'+esc(r.id)+'">'+
        '<span class="crm-link-alert-badge '+esc(r.severity||'warn')+'">'+esc(r.stage)+'</span>'+
        '<div><b>'+esc(r.customer_name||'-')+'</b> <span class="crm-link-alert-meta">'+esc(r.customer_id||'')+'</span></div>'+
        '<div class="crm-link-alert-meta">下書きID: '+esc(r.id)+' / 予約ID: '+esc(r.reservation_app_reservation_id||'-')+' / 撮影: '+esc(r.shoot_date||'-')+' '+esc(r.start_time||'')+'</div>'+
        '<div style="margin-top:6px">'+esc(r.reason||'')+'</div>'+
        '<div class="crm-link-alert-meta">発生: '+short(r.alert_time)+' / 確認: '+(r.acknowledged?('確認済み '+short(r.acknowledged_at)):'未確認')+'</div>'+
        '<div class="crm-link-alert-row-actions"><button data-alert-resync="auto" data-draft-id="'+esc(r.id)+'">自動再同期</button><button data-alert-ack data-draft-id="'+esc(r.id)+'">確認済みにする</button><button data-copy="'+esc(r.id)+'">下書きIDコピー</button></div>'+
      '</div>';}).join('');
    }catch(e){if(list)list.innerHTML='<div class="crm-link-alert-row">読み込み失敗：'+esc(e.message||e)+'</div>';}
  }
  async function ack(id){if(!id)return; var res=await api('/api/reservation-link-alerts/'+encodeURIComponent(id)+'/ack',{method:'POST',body:JSON.stringify({})}); if(res.ok){toast('確認済みにしました'); loadAlerts(false); refreshBadge();}else toast('確認済みにできませんでした')}
  async function ackAll(){if(!confirm('表示対象の未確認アラートをまとめて確認済みにしますか？'))return; var res=await api('/api/reservation-link-alerts/ack-all',{method:'POST',body:JSON.stringify({})}); if(res.ok){toast('まとめて確認済みにしました：'+(res.acknowledged||0)+'件'); loadAlerts(false); refreshBadge();}else toast('一括確認に失敗しました')}
  async function resync(id){if(!id)return; if(!confirm('この連携を自動再同期しますか？'))return; var res=await api('/api/reservation-link-monitor/'+encodeURIComponent(id)+'/resync',{method:'POST',body:JSON.stringify({action:'auto'})}); if(res.ok){toast('再同期しました'); loadAlerts(false); refreshBadge();}else toast('再同期に失敗しました')}
  function openMonitorAttention(){ if(window.crmOpenReservationLinkMonitor){ window.crmOpenReservationLinkMonitor(); setTimeout(function(){var s=document.getElementById('crmLinkMonitorStatus'); if(s){s.value='attention'; s.dispatchEvent(new Event('change',{bubbles:true}));}},450); } }
  function install(){if(document.getElementById('crmLinkAlertOpenBtn'))return; var b=document.createElement('button'); b.id='crmLinkAlertOpenBtn'; b.type='button'; b.className='crm-link-alert-btn'; b.textContent='予約連携確認中...'; b.onclick=openModal; document.body.appendChild(b); var modal='<div id="crmLinkAlertModal" class="crm-link-alert-modal"><div class="crm-link-alert-panel"><div class="crm-link-alert-head"><div><h2 class="crm-link-alert-title">予約連携アラート</h2><p class="crm-link-alert-sub">予約管理との連携で止まっている可能性があるものだけを表示します。</p></div><button id="crmLinkAlertClose">閉じる</button></div><div id="crmLinkAlertKpis" class="crm-link-alert-kpis"></div><div class="crm-link-alert-actions"><button id="crmLinkAlertReload">更新</button><button id="crmLinkAlertUnacked">未確認だけ</button><button id="crmLinkAlertAckAll" class="danger">未確認を一括確認済み</button><button id="crmLinkAlertOpenMonitor">監視画面を開く</button></div><div id="crmLinkAlertRows" class="crm-link-alert-list"><div class="crm-link-alert-row">未読み込み</div></div></div></div>'; document.body.insertAdjacentHTML('beforeend',modal); refreshBadge(); setInterval(refreshBadge,60000);}
  document.addEventListener('click',function(e){var t=e.target;if(!t)return; if(t.id==='crmLinkAlertClose')closeModal(); if(t.id==='crmLinkAlertReload')loadAlerts(false); if(t.id==='crmLinkAlertUnacked')loadAlerts(true); if(t.id==='crmLinkAlertAckAll')ackAll(); if(t.id==='crmLinkAlertOpenMonitor')openMonitorAttention(); if(t.dataset&&t.dataset.alertAck)ack(t.dataset.draftId); if(t.dataset&&t.dataset.alertResync)resync(t.dataset.draftId); if(t.dataset&&t.dataset.copy){navigator.clipboard&&navigator.clipboard.writeText(t.dataset.copy);toast('コピーしました');}});
  document.addEventListener('DOMContentLoaded',install); setTimeout(install,800);
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

    if (url.pathname === "/api/reservation-link-alerts" && request.method === "GET") {
      return alertsApi(request, env, url);
    }
    if (url.pathname === "/api/reservation-link-alerts/ack-all" && request.method === "POST") {
      return ackAll(request, env);
    }
    const ackId = alertPathDraftId(url.pathname);
    if (ackId && request.method === "POST") {
      return ackOne(request, env, ackId);
    }

    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if (type.includes("text/html")) {
      const body = injectAlertUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
