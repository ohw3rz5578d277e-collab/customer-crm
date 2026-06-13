// ======================================================
// CUSTOMER CRM / RESERVATION LINK MONITOR WRAPPER
// build: customer-crm-api-reservation-link-monitor-20260613-01
// ======================================================

import app from "./production-index-crm-reservation-cancel-sync.js";

const BUILD = "customer-crm-api-reservation-link-monitor-20260613-01";
const EXPECTED_DRAFT_COLUMNS = [
  "reservation_intake_id TEXT",
  "sent_to_reservation_at TEXT",
  "sent_to_reservation_by TEXT",
  "sent_to_reservation_response TEXT",
  "reservation_app_reservation_id TEXT",
  "reservation_app_intake_id TEXT",
  "reservation_app_created_at TEXT",
  "reservation_app_created_by TEXT",
  "reservation_app_response TEXT",
  "history_synced_at TEXT",
  "history_event_key TEXT",
  "reservation_app_updated_at TEXT",
  "reservation_app_updated_by TEXT",
  "reservation_app_update_response TEXT",
  "reservation_app_cancelled_at TEXT",
  "reservation_app_cancelled_by TEXT",
  "reservation_app_cancel_reason TEXT",
  "reservation_app_cancel_response TEXT",
  "cancellation_synced_at TEXT"
];

function json(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: securityHeaders({ "content-type": "application/json; charset=utf-8", ...headers })
  });
}

function securityHeaders(headers = {}) {
  const h = new Headers(headers);
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  h.set("cache-control", "no-store");
  return h;
}

function text(v) {
  return v === undefined || v === null ? "" : String(v);
}

function safeInt(v, fallback = 200, max = 1000) {
  const n = Number.parseInt(String(v || ""), 10);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(n, max);
}

async function addColumn(db, table, definition) {
  try {
    await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();
  } catch (_) {}
}

async function ensureMonitorSchema(env) {
  if (!env?.DB) return;
  for (const definition of EXPECTED_DRAFT_COLUMNS) {
    await addColumn(env.DB, "crm_reservation_drafts", definition);
  }
  try {
    await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_monitor_status ON crm_reservation_drafts(status, sent_to_reservation_at, reservation_app_created_at, reservation_app_cancelled_at)`).run();
  } catch (_) {}
  try {
    await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_monitor_customer ON crm_reservation_drafts(customer_id, created_at)`).run();
  } catch (_) {}
}

function classifyDraft(row) {
  const cancelled = !!(row.reservation_app_cancelled_at || row.status === "cancelled");
  const sent = !!row.sent_to_reservation_at;
  const created = !!row.reservation_app_created_at || !!row.reservation_app_reservation_id || row.status === "created";
  const historySynced = !!row.history_synced_at;
  const updated = !!row.reservation_app_updated_at;

  let stage = "下書き";
  let stage_key = "draft";
  let attention = false;
  let attention_reason = "";

  if (cancelled) {
    stage = "キャンセル済み";
    stage_key = "cancelled";
  } else if (updated) {
    stage = "変更同期済み";
    stage_key = "updated";
  } else if (historySynced) {
    stage = "CRM予約履歴反映済み";
    stage_key = "synced";
  } else if (created) {
    stage = "本予約作成済み・CRM履歴未反映";
    stage_key = "created_unreflected";
    attention = true;
    attention_reason = "本予約作成済みですが、CRM予約履歴への反映日時が未記録です。";
  } else if (sent) {
    stage = "予約管理へ送信済み・本予約未作成";
    stage_key = "sent_uncreated";
    attention = true;
    attention_reason = "予約管理へ送信済みですが、本予約IDがまだ戻っていません。";
  }

  return { stage, stage_key, attention, attention_reason };
}

function monitorWhere(url) {
  const status = text(url.searchParams.get("status") || "all");
  const customerId = text(url.searchParams.get("customer_id"));
  const keyword = text(url.searchParams.get("keyword")).trim();
  const where = ["1=1"];
  const binds = [];

  if (customerId) {
    where.push("customer_id = ?");
    binds.push(customerId);
  }
  if (keyword) {
    where.push("(customer_id LIKE ? OR customer_name LIKE ? OR genre LIKE ? OR place LIKE ? OR reservation_app_reservation_id LIKE ? OR reservation_intake_id LIKE ?)");
    const q = `%${keyword}%`;
    binds.push(q, q, q, q, q, q);
  }

  if (status === "draft") where.push("COALESCE(sent_to_reservation_at, '') = '' AND COALESCE(reservation_app_cancelled_at, '') = ''");
  if (status === "sent") where.push("COALESCE(sent_to_reservation_at, '') <> '' AND COALESCE(reservation_app_reservation_id, '') = '' AND COALESCE(reservation_app_cancelled_at, '') = ''");
  if (status === "created") where.push("COALESCE(reservation_app_reservation_id, '') <> '' AND COALESCE(reservation_app_cancelled_at, '') = ''");
  if (status === "unreflected") where.push("COALESCE(reservation_app_reservation_id, '') <> '' AND COALESCE(history_synced_at, '') = '' AND COALESCE(reservation_app_cancelled_at, '') = ''");
  if (status === "synced") where.push("COALESCE(history_synced_at, '') <> '' AND COALESCE(reservation_app_cancelled_at, '') = ''");
  if (status === "updated") where.push("COALESCE(reservation_app_updated_at, '') <> '' AND COALESCE(reservation_app_cancelled_at, '') = ''");
  if (status === "cancelled") where.push("(COALESCE(reservation_app_cancelled_at, '') <> '' OR status = 'cancelled')");
  if (status === "attention") {
    where.push(`(
      (COALESCE(sent_to_reservation_at, '') <> '' AND COALESCE(reservation_app_reservation_id, '') = '' AND COALESCE(reservation_app_cancelled_at, '') = '')
      OR
      (COALESCE(reservation_app_reservation_id, '') <> '' AND COALESCE(history_synced_at, '') = '' AND COALESCE(reservation_app_cancelled_at, '') = '')
    )`);
  }

  return { status, where: where.join(" AND "), binds };
}

async function getMonitorRows(env, url) {
  await ensureMonitorSchema(env);
  const limit = safeInt(url.searchParams.get("limit"), 200, 1000);
  const { status, where, binds } = monitorWhere(url);
  const sql = `
    SELECT
      id,
      customer_id,
      customer_name,
      genre,
      shoot_date,
      start_time,
      place,
      plan_label,
      amount,
      status,
      memo,
      created_at,
      updated_at,
      reservation_intake_id,
      sent_to_reservation_at,
      sent_to_reservation_by,
      reservation_app_reservation_id,
      reservation_app_intake_id,
      reservation_app_created_at,
      reservation_app_created_by,
      history_synced_at,
      history_event_key,
      reservation_app_updated_at,
      reservation_app_updated_by,
      reservation_app_cancelled_at,
      reservation_app_cancelled_by,
      reservation_app_cancel_reason,
      cancellation_synced_at
    FROM crm_reservation_drafts
    WHERE ${where}
    ORDER BY COALESCE(reservation_app_cancelled_at, reservation_app_updated_at, history_synced_at, reservation_app_created_at, sent_to_reservation_at, updated_at, created_at, '') DESC
    LIMIT ?
  `;
  const result = await env.DB.prepare(sql).bind(...binds, limit).all();
  const rows = (result.results || []).map((r) => ({ ...r, ...classifyDraft(r) }));
  return { status, limit, rows };
}

async function monitorApi(env, url) {
  const { status, limit, rows } = await getMonitorRows(env, url);
  const summary = rows.reduce((acc, row) => {
    acc.total += 1;
    acc[row.stage_key] = (acc[row.stage_key] || 0) + 1;
    if (row.attention) acc.attention += 1;
    return acc;
  }, {
    total: 0,
    draft: 0,
    sent_uncreated: 0,
    created_unreflected: 0,
    synced: 0,
    updated: 0,
    cancelled: 0,
    attention: 0
  });
  return json({ ok: true, build: BUILD, status, limit, summary, rows });
}

function csvEscape(value) {
  let s = text(value).replace(/\r?\n/g, " ");
  if (/^[=+\-@]/.test(s)) s = `'${s}`;
  return `"${s.replace(/"/g, '""')}"`;
}

async function monitorCsv(env, url) {
  const { rows } = await getMonitorRows(env, url);
  const header = [
    "状態", "要確認", "要確認理由", "下書きID", "顧客ID", "顧客名", "ジャンル", "撮影日", "開始時間", "場所", "プラン", "金額",
    "予約管理候補ID", "予約管理へ送信日時", "予約管理本予約ID", "本予約作成日時", "CRM履歴反映日時", "変更同期日時", "キャンセル日時", "キャンセル理由"
  ];
  const body = rows.map((r) => [
    r.stage,
    r.attention ? "要確認" : "",
    r.attention_reason,
    r.id,
    r.customer_id,
    r.customer_name,
    r.genre,
    r.shoot_date,
    r.start_time,
    r.place,
    r.plan_label,
    r.amount,
    r.reservation_intake_id,
    r.sent_to_reservation_at,
    r.reservation_app_reservation_id,
    r.reservation_app_created_at,
    r.history_synced_at,
    r.reservation_app_updated_at,
    r.reservation_app_cancelled_at,
    r.reservation_app_cancel_reason
  ].map(csvEscape).join(","));
  const csv = "\uFEFF" + [header.map(csvEscape).join(","), ...body].join("\n");
  return new Response(csv, {
    status: 200,
    headers: securityHeaders({
      "content-type": "text/csv; charset=utf-8",
      "content-disposition": `attachment; filename="crm-reservation-link-monitor-${new Date().toISOString().slice(0, 10)}.csv"`
    })
  });
}

function injectMonitorUi(html) {
  if (!html || html.includes("crmLinkMonitorModal")) return html;
  const style = `
<style id="crmLinkMonitorStyle">
  .crm-link-monitor-btn{border:1px solid #cbd5e1;background:#fff;color:#0f172a;border-radius:999px;padding:8px 12px;font-weight:800;cursor:pointer;margin:6px;box-shadow:0 2px 8px rgba(15,23,42,.08)}
  .crm-link-monitor-btn.primary{background:#0f766e;color:#fff;border-color:#0f766e}
  .crm-link-monitor-modal{position:fixed;inset:0;background:rgba(15,23,42,.55);z-index:999999;display:none;align-items:flex-start;justify-content:center;padding:28px 12px;overflow:auto}
  .crm-link-monitor-panel{width:min(1180px,100%);background:#fff;border-radius:18px;box-shadow:0 24px 70px rgba(15,23,42,.28);padding:18px;color:#0f172a}
  .crm-link-monitor-head{display:flex;gap:12px;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;margin-bottom:12px}
  .crm-link-monitor-title{font-size:20px;font-weight:900;margin:0}.crm-link-monitor-sub{font-size:13px;color:#64748b;margin:5px 0 0}
  .crm-link-monitor-filters{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:10px 0 12px}.crm-link-monitor-filters input,.crm-link-monitor-filters select{border:1px solid #cbd5e1;border-radius:10px;padding:9px 10px;background:#fff;min-height:38px}
  .crm-link-monitor-kpis{display:grid;grid-template-columns:repeat(7,minmax(110px,1fr));gap:8px;margin:10px 0 12px}.crm-link-monitor-kpi{border:1px solid #e2e8f0;border-radius:14px;padding:10px;background:#f8fafc}.crm-link-monitor-kpi b{display:block;font-size:20px}.crm-link-monitor-kpi span{font-size:12px;color:#64748b;font-weight:700}
  .crm-link-monitor-table-wrap{overflow:auto;border:1px solid #e2e8f0;border-radius:14px}.crm-link-monitor-table{width:100%;border-collapse:collapse;font-size:13px;min-width:1080px}.crm-link-monitor-table th{background:#f1f5f9;text-align:left;padding:10px;border-bottom:1px solid #e2e8f0;white-space:nowrap}.crm-link-monitor-table td{padding:10px;border-bottom:1px solid #e2e8f0;vertical-align:top}.crm-link-stage{display:inline-flex;border-radius:999px;padding:5px 9px;font-size:12px;font-weight:900;background:#e2e8f0;color:#334155;white-space:nowrap}.crm-link-stage.attention{background:#fef3c7;color:#92400e}.crm-link-stage.cancelled{background:#fee2e2;color:#991b1b}.crm-link-stage.synced,.crm-link-stage.updated{background:#dcfce7;color:#166534}.crm-link-monitor-small{font-size:12px;color:#64748b}.crm-link-monitor-actions{display:flex;gap:6px;flex-wrap:wrap}.crm-link-monitor-actions button{border:1px solid #cbd5e1;background:#fff;border-radius:8px;padding:6px 8px;cursor:pointer;font-weight:800;font-size:12px}
  @media(max-width:760px){.crm-link-monitor-panel{padding:14px;border-radius:14px}.crm-link-monitor-kpis{grid-template-columns:repeat(2,1fr)}.crm-link-monitor-filters input,.crm-link-monitor-filters select{width:100%}}
</style>`;
  const script = `
<script id="crmLinkMonitorScript">
(function(){
  if(window.__crmLinkMonitorInstalled) return; window.__crmLinkMonitorInstalled = true;
  function esc(v){return String(v == null ? '' : v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
  function shortDate(v){return v ? String(v).replace('T',' ').slice(0,16) : '-';}
  function openMonitor(){var m=document.getElementById('crmLinkMonitorModal'); if(m){m.style.display='flex'; loadMonitor();}}
  function closeMonitor(){var m=document.getElementById('crmLinkMonitorModal'); if(m) m.style.display='none';}
  async function loadMonitor(){
    var status=(document.getElementById('crmLinkMonitorStatus')||{}).value || 'all';
    var keyword=(document.getElementById('crmLinkMonitorKeyword')||{}).value || '';
    var limit=(document.getElementById('crmLinkMonitorLimit')||{}).value || '200';
    var url='/api/reservation-link-monitor?status='+encodeURIComponent(status)+'&keyword='+encodeURIComponent(keyword)+'&limit='+encodeURIComponent(limit);
    var tbody=document.getElementById('crmLinkMonitorRows'); var kpi=document.getElementById('crmLinkMonitorKpis');
    if(tbody) tbody.innerHTML='<tr><td colspan="11">読み込み中...</td></tr>';
    try{
      var res=await fetch(url,{cache:'no-store'}); var data=await res.json(); if(!data.ok) throw new Error(data.message || 'load failed');
      var s=data.summary || {};
      if(kpi) kpi.innerHTML=[
        ['合計',s.total||0],['要確認',s.attention||0],['未送信',s.draft||0],['送信済み未作成',s.sent_uncreated||0],['本予約未反映',s.created_unreflected||0],['反映済み',s.synced||0],['キャンセル',s.cancelled||0]
      ].map(function(x){return '<div class="crm-link-monitor-kpi"><b>'+esc(x[1])+'</b><span>'+esc(x[0])+'</span></div>';}).join('');
      var rows=data.rows || [];
      if(!tbody) return;
      if(!rows.length){tbody.innerHTML='<tr><td colspan="11">該当する予約連携データはありません。</td></tr>';return;}
      tbody.innerHTML=rows.map(function(r){
        var cls='crm-link-stage '+(r.attention?'attention ':'')+(r.stage_key==='cancelled'?'cancelled ':'')+((r.stage_key==='synced'||r.stage_key==='updated')?'synced ':'');
        return '<tr>'+
          '<td><span class="'+cls+'">'+esc(r.stage)+'</span>'+(r.attention?'<div class="crm-link-monitor-small">'+esc(r.attention_reason)+'</div>':'')+'</td>'+
          '<td><b>'+esc(r.customer_name||'-')+'</b><div class="crm-link-monitor-small">'+esc(r.customer_id||'')+'</div></td>'+
          '<td>'+esc(r.genre||'-')+'<div class="crm-link-monitor-small">'+esc(r.shoot_date||'-')+' '+esc(r.start_time||'')+'</div></td>'+
          '<td>'+esc(r.place||'-')+'</td>'+
          '<td>'+esc(r.amount||'-')+'</td>'+
          '<td>'+esc(r.reservation_intake_id||'-')+'<div class="crm-link-monitor-small">送信 '+shortDate(r.sent_to_reservation_at)+'</div></td>'+
          '<td><b>'+esc(r.reservation_app_reservation_id||'-')+'</b><div class="crm-link-monitor-small">作成 '+shortDate(r.reservation_app_created_at)+'</div></td>'+
          '<td>'+shortDate(r.history_synced_at)+'</td>'+
          '<td>'+shortDate(r.reservation_app_updated_at)+'</td>'+
          '<td>'+shortDate(r.reservation_app_cancelled_at)+'<div class="crm-link-monitor-small">'+esc(r.reservation_app_cancel_reason||'')+'</div></td>'+
          '<td><div class="crm-link-monitor-actions">'+
             '<button data-copy="'+esc(r.id)+'">下書きIDコピー</button>'+
             (r.customer_id?'<button data-customer="'+esc(r.customer_id)+'">顧客IDコピー</button>':'')+
             (r.reservation_app_reservation_id?'<button data-copy="'+esc(r.reservation_app_reservation_id)+'">予約IDコピー</button>':'')+
          '</div></td>'+
        '</tr>';
      }).join('');
    }catch(e){ if(tbody) tbody.innerHTML='<tr><td colspan="11">読み込み失敗：'+esc(e.message || e)+'</td></tr>'; }
  }
  function downloadCsv(){
    var status=(document.getElementById('crmLinkMonitorStatus')||{}).value || 'all';
    var keyword=(document.getElementById('crmLinkMonitorKeyword')||{}).value || '';
    location.href='/api/reservation-link-monitor.csv?status='+encodeURIComponent(status)+'&keyword='+encodeURIComponent(keyword)+'&limit=1000';
  }
  function installButton(){
    if(document.getElementById('crmLinkMonitorOpenBtn')) return;
    var btn=document.createElement('button'); btn.id='crmLinkMonitorOpenBtn'; btn.type='button'; btn.className='crm-link-monitor-btn primary'; btn.textContent='予約連携監視'; btn.onclick=openMonitor;
    var target=document.querySelector('.toolbar,.actions,.admin-actions,main,#app,body');
    if(target && target.firstChild) target.insertBefore(btn,target.firstChild); else document.body.appendChild(btn);
  }
  document.addEventListener('click',function(e){
    var t=e.target; if(!t) return;
    if(t.id==='crmLinkMonitorClose') closeMonitor();
    if(t.id==='crmLinkMonitorReload') loadMonitor();
    if(t.id==='crmLinkMonitorCsv') downloadCsv();
    if(t.dataset && t.dataset.copy){ navigator.clipboard&&navigator.clipboard.writeText(t.dataset.copy); t.textContent='コピー済み'; setTimeout(function(){t.textContent='コピー';},900); }
    if(t.dataset && t.dataset.customer){ navigator.clipboard&&navigator.clipboard.writeText(t.dataset.customer); t.textContent='コピー済み'; setTimeout(function(){t.textContent='顧客IDコピー';},900); }
  });
  document.addEventListener('change',function(e){ if(e.target && e.target.id==='crmLinkMonitorStatus') loadMonitor(); });
  var modal='<div id="crmLinkMonitorModal" class="crm-link-monitor-modal"><div class="crm-link-monitor-panel"><div class="crm-link-monitor-head"><div><h2 class="crm-link-monitor-title">予約連携ステータス監視</h2><p class="crm-link-monitor-sub">CRM予約下書きから予約管理への送信、本予約作成、CRM履歴反映、変更同期、キャンセル同期を一覧確認できます。</p></div><button id="crmLinkMonitorClose" class="crm-link-monitor-btn">閉じる</button></div><div id="crmLinkMonitorKpis" class="crm-link-monitor-kpis"></div><div class="crm-link-monitor-filters"><select id="crmLinkMonitorStatus"><option value="all">すべて</option><option value="attention">要確認</option><option value="draft">未送信</option><option value="sent">送信済み・本予約未作成</option><option value="unreflected">本予約作成済み・CRM履歴未反映</option><option value="synced">CRM履歴反映済み</option><option value="updated">変更同期済み</option><option value="cancelled">キャンセル済み</option></select><input id="crmLinkMonitorKeyword" placeholder="顧客名・顧客ID・予約IDで検索"><input id="crmLinkMonitorLimit" type="number" value="200" min="1" max="1000" style="width:100px"><button id="crmLinkMonitorReload" class="crm-link-monitor-btn">更新</button><button id="crmLinkMonitorCsv" class="crm-link-monitor-btn">CSV出力</button></div><div class="crm-link-monitor-table-wrap"><table class="crm-link-monitor-table"><thead><tr><th>状態</th><th>顧客</th><th>撮影</th><th>場所</th><th>金額</th><th>予約候補</th><th>本予約</th><th>履歴反映</th><th>変更同期</th><th>キャンセル</th><th>操作</th></tr></thead><tbody id="crmLinkMonitorRows"><tr><td colspan="11">未読み込み</td></tr></tbody></table></div></div></div>';
  document.addEventListener('DOMContentLoaded',function(){ document.body.insertAdjacentHTML('beforeend', modal); installButton(); });
  setTimeout(installButton,800);
  window.crmOpenReservationLinkMonitor=openMonitor;
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
    if (url.pathname === "/api/reservation-link-monitor" && request.method === "GET") {
      return monitorApi(env, url);
    }
    if (url.pathname === "/api/reservation-link-monitor.csv" && request.method === "GET") {
      return monitorCsv(env, url);
    }

    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if (type.includes("text/html")) {
      const body = injectMonitorUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
