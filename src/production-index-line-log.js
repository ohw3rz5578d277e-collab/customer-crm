// ======================================================
// CUSTOMER CRM API / LINE DRAFT LOG WRAPPER
// build: customer-crm-api-line-draft-log-wrapper-20260613-01
// ======================================================

import app from "./production-index.js";

const BUILD = "customer-crm-api-line-draft-log-wrapper-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";

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
  h.set("x-robots-tag", "noindex, nofollow, noarchive");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  return h;
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: securityHeaders({ "content-type": "application/json; charset=utf-8" })
  });
}

async function readJson(request) {
  try {
    return await request.json();
  } catch (_) {
    return {};
  }
}

function getAccessEmail(request) {
  return normalizeEmail(
    request.headers.get("cf-access-authenticated-user-email") ||
    request.headers.get("Cf-Access-Authenticated-User-Email") ||
    request.headers.get("cf-access-user-email") ||
    ""
  );
}

async function addColumn(db, table, definition) {
  try {
    await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();
  } catch (_) {}
}

async function ensureAdminUsers(env) {
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
}

async function getCurrentUser(request, env) {
  const email = getAccessEmail(request);
  if (!email) return null;
  await ensureAdminUsers(env);
  const row = await env.DB.prepare(
    `SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`
  ).bind(email).first();
  return row ? { email: row.email, role: row.role || "viewer" } : null;
}

async function ensureLineDraftLogSchema(env) {
  if (!env.DB) throw new Error("D1 DB binding(DB) is missing");

  await addColumn(env.DB, "customers", "deleted_at TEXT");

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS customer_line_draft_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    action_type TEXT,
    action_label TEXT,
    priority TEXT,
    message_text TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'copied',
    channel TEXT NOT NULL DEFAULT 'line',
    created_by TEXT,
    copied_at TEXT,
    sent_at TEXT,
    memo TEXT,
    raw_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();

  for (const col of [
    "customer_id TEXT",
    "customer_name TEXT",
    "action_type TEXT",
    "action_label TEXT",
    "priority TEXT",
    "message_text TEXT",
    "status TEXT DEFAULT 'copied'",
    "channel TEXT DEFAULT 'line'",
    "created_by TEXT",
    "copied_at TEXT",
    "sent_at TEXT",
    "memo TEXT",
    "raw_json TEXT",
    "created_at TEXT",
    "updated_at TEXT"
  ]) {
    await addColumn(env.DB, "customer_line_draft_logs", col);
  }

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_customer_line_draft_logs_customer ON customer_line_draft_logs(customer_id, created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_customer_line_draft_logs_status ON customer_line_draft_logs(status, created_at)`).run();
}

function parseCustomerIdFromLineLogPath(path) {
  const m = path.match(/^\/api\/customers\/([^/]+)\/line-message-logs$/);
  return m ? decodeURIComponent(m[1]) : "";
}

function parseLogIdFromMarkSentPath(path) {
  const m = path.match(/^\/api\/line-message-logs\/(\d+)\/mark-sent$/);
  return m ? Number(m[1]) : 0;
}

function isLineLogListPath(path) {
  return /^\/api\/customers\/[^/]+\/line-message-logs$/.test(path);
}

function isLineLogMarkSentPath(path) {
  return /^\/api\/line-message-logs\/\d+\/mark-sent$/.test(path);
}

async function getCustomerName(env, customerId) {
  const row = await env.DB.prepare(`
    SELECT customer_id, name, line_display_name
    FROM customers
    WHERE customer_id=? AND (deleted_at IS NULL OR deleted_at='')
    LIMIT 1
  `).bind(customerId).first();
  if (!row) return "";
  return text(row.name || row.line_display_name || row.customer_id);
}

async function listLineMessageLogs(request, env, current) {
  if (!current) return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);
  await ensureLineDraftLogSchema(env);

  const url = new URL(request.url);
  const customerId = parseCustomerIdFromLineLogPath(url.pathname);
  if (!customerId) return json({ ok: false, message: "customer_id required" }, 400);

  const limitRaw = parseInt(url.searchParams.get("limit") || "50", 10);
  const limit = Math.max(1, Math.min(Number.isFinite(limitRaw) ? limitRaw : 50, 200));

  const rows = await env.DB.prepare(`
    SELECT id, customer_id, customer_name, action_type, action_label, priority, message_text, status, channel, created_by, copied_at, sent_at, memo, created_at, updated_at
    FROM customer_line_draft_logs
    WHERE customer_id=?
    ORDER BY COALESCE(sent_at, copied_at, created_at) DESC, id DESC
    LIMIT ?
  `).bind(customerId, limit).all();

  return json({ ok: true, customer_id: customerId, count: (rows.results || []).length, items: rows.results || [] });
}

async function saveLineMessageLog(request, env, current) {
  if (!current) return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);
  await ensureLineDraftLogSchema(env);

  const url = new URL(request.url);
  const customerId = parseCustomerIdFromLineLogPath(url.pathname);
  if (!customerId) return json({ ok: false, message: "customer_id required" }, 400);

  const body = await readJson(request);
  const message = text(body.message_text || body.message || body.text);
  if (!message) return json({ ok: false, message: "message_text required" }, 400);

  const status = ["copied", "saved", "sent"].includes(text(body.status)) ? text(body.status) : "copied";
  const customerName = text(body.customer_name) || await getCustomerName(env, customerId);
  const sentAt = status === "sent" ? "datetime('now')" : "NULL";

  const result = await env.DB.prepare(`
    INSERT INTO customer_line_draft_logs (
      customer_id, customer_name, action_type, action_label, priority, message_text, status, channel, created_by, copied_at, sent_at, memo, raw_json, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'line', ?, datetime('now'), ${sentAt}, ?, ?, datetime('now'), datetime('now'))
  `).bind(
    customerId,
    customerName || null,
    text(body.action_type || body.type) || null,
    text(body.action_label || body.label) || null,
    text(body.priority) || null,
    message,
    status,
    current.email,
    text(body.memo) || null,
    JSON.stringify(body || {})
  ).run();

  return json({ ok: true, id: result.meta && result.meta.last_row_id ? result.meta.last_row_id : null, customer_id: customerId, status, saved_by: current.email });
}

async function markLineMessageLogSent(request, env, current) {
  if (!current) return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);
  await ensureLineDraftLogSchema(env);

  const url = new URL(request.url);
  const id = parseLogIdFromMarkSentPath(url.pathname);
  if (!id) return json({ ok: false, message: "log id required" }, 400);

  await env.DB.prepare(`
    UPDATE customer_line_draft_logs
    SET status='sent', sent_at=COALESCE(sent_at, datetime('now')), updated_at=datetime('now')
    WHERE id=?
  `).bind(id).run();

  return json({ ok: true, id, status: "sent", updated_by: current.email });
}

async function handleLineMessageLogApi(request, env) {
  const current = await getCurrentUser(request, env);
  const url = new URL(request.url);

  if (isLineLogListPath(url.pathname) && request.method === "GET") return await listLineMessageLogs(request, env, current);
  if (isLineLogListPath(url.pathname) && request.method === "POST") return await saveLineMessageLog(request, env, current);
  if (isLineLogMarkSentPath(url.pathname) && request.method === "POST") return await markLineMessageLogSent(request, env, current);

  return json({ ok: false, message: "Method Not Allowed" }, 405);
}

function injectLineLogUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-line-log-style">
.crm-save-line{border:1px solid #475569;background:#fff;color:#0f172a;border-radius:999px;padding:6px 9px;font-size:.74rem;font-weight:900;cursor:pointer;margin-left:6px}.crm-line-log-panel{margin-top:12px;border:1px solid #e5e7eb;background:#fff;border-radius:14px;padding:10px}.crm-line-log-panel h4{margin:0 0 8px;font-size:.9rem}.crm-line-log-item{border-top:1px solid #f1f5f9;padding:8px 0}.crm-line-log-item:first-of-type{border-top:0}.crm-line-log-meta{display:flex;gap:6px;flex-wrap:wrap;color:#64748b;font-size:.72rem;font-weight:800}.crm-line-log-text{margin-top:4px;white-space:pre-wrap;color:#334155;font-size:.78rem;line-height:1.55;max-height:72px;overflow:auto}.crm-mark-sent{border:1px solid #028760;background:#028760;color:#fff;border-radius:999px;padding:5px 8px;font-size:.72rem;font-weight:900;cursor:pointer}.crm-line-log-toast{position:fixed;left:50%;bottom:66px;transform:translateX(-50%);z-index:999999;background:#111827;color:#fff;border-radius:999px;padding:10px 14px;font-size:13px;font-weight:900;box-shadow:0 14px 36px rgba(15,23,42,.25)}@media(max-width:760px){.crm-save-line{margin-left:0;margin-top:6px}.crm-line-draft-head{flex-wrap:wrap}.crm-line-log-text{font-size:.76rem}}
</style>`;

  const script = `<script id="crm-line-log-script">
(function(){
  if(window.__crmLineLogInstalled)return;
  window.__crmLineLogInstalled=true;
  var detailStore={};
  var latestCustomerId='';
  var originalFetch=window.fetch;
  function esc(v){return String(v==null?'':v).replace(/[&<>\"]/g,function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[m]})}
  function toast(msg){var old=document.querySelector('.crm-line-log-toast');if(old)old.remove();var t=document.createElement('div');t.className='crm-line-log-toast';t.textContent=msg||'保存しました';document.body.appendChild(t);setTimeout(function(){t.remove()},1700)}
  function customerIdFromModal(){var modal=document.getElementById('modal');if(!modal)return latestCustomerId;var keys=Object.keys(detailStore);for(var i=0;i<keys.length;i++){if((modal.textContent||'').indexOf(keys[i])>=0)return keys[i]}return latestCustomerId}
  function getCardPayload(btn){var card=btn.closest('.crm-next-action');var cid=customerIdFromModal();var detail=detailStore[cid]||{};var customer=detail.customer||{};var title=card&&card.querySelector('.crm-next-action-title span');var priority=card&&card.querySelector('.crm-next-action-title em');var draft=card&&card.querySelector('.crm-line-draft-text');return {customer_id:cid,customer_name:customer.name||customer.line_display_name||'',action_label:title?title.textContent.trim():'',priority:priority?priority.textContent.trim():'',message_text:draft?draft.textContent.trim():'',status:'copied'}}
  async function saveLog(btn){var p=getCardPayload(btn);if(!p.customer_id||!p.message_text){toast('保存できる文面がありません');return;}btn.disabled=true;var old=btn.textContent;btn.textContent='保存中...';try{var r=await fetch('/api/customers/'+encodeURIComponent(p.customer_id)+'/line-message-logs',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(p)});var j=await r.json();if(!r.ok||j.ok===false)throw new Error(j.message||'保存に失敗しました');toast('LINE文面を履歴に保存しました');await loadLogs(p.customer_id)}catch(e){alert(e.message||String(e))}finally{btn.disabled=false;btn.textContent=old}}
  async function markSent(id,cid){try{var r=await fetch('/api/line-message-logs/'+id+'/mark-sent',{method:'POST'});var j=await r.json();if(!r.ok||j.ok===false)throw new Error(j.message||'更新に失敗しました');toast('送信済みにしました');await loadLogs(cid)}catch(e){alert(e.message||String(e))}}
  function renderLogs(cid,items){var host=document.getElementById('crmNextActionsCard');if(!host)return;var old=document.getElementById('crmLineLogPanel');if(old)old.remove();items=items||[];var html='<div class="crm-line-log-panel" id="crmLineLogPanel"><h4>LINE文面の保存履歴</h4>'+(items.length?items.slice(0,8).map(function(x){var sent=x.status==='sent';return '<div class="crm-line-log-item"><div class="crm-line-log-meta"><span>#'+esc(x.id)+'</span><span>'+esc(x.action_label||'文面')+'</span><span>'+esc(x.status||'copied')+'</span><span>'+esc(x.sent_at||x.copied_at||x.created_at||'')+'</span>'+(sent?'':'<button class="crm-mark-sent" data-log-sent="'+esc(x.id)+'">送信済みにする</button>')+'</div><div class="crm-line-log-text">'+esc(x.message_text||'')+'</div></div>'}).join(''):'<div class="crm-line-log-item"><div class="crm-line-log-meta">まだ保存履歴はありません。</div></div>')+'</div>';host.insertAdjacentHTML('beforeend',html);host.querySelectorAll('[data-log-sent]').forEach(function(b){b.onclick=function(){markSent(b.getAttribute('data-log-sent'),cid)}})}
  async function loadLogs(cid){if(!cid)return;try{var r=await fetch('/api/customers/'+encodeURIComponent(cid)+'/line-message-logs?limit=20');var j=await r.json();if(r.ok&&j.ok!==false)renderLogs(cid,j.items||[])}catch(e){}}
  function installButtons(){document.querySelectorAll('.crm-line-draft').forEach(function(box){if(box.querySelector('.crm-save-line'))return;var head=box.querySelector('.crm-line-draft-head')||box;var b=document.createElement('button');b.className='crm-save-line';b.type='button';b.textContent='保存';b.onclick=function(){saveLog(b)};head.appendChild(b)});var cid=customerIdFromModal();if(cid)loadLogs(cid)}
  window.fetch=function(input,init){return originalFetch(input,init).then(function(res){try{var url=typeof input==='string'?input:(input&&input.url)||'';var u=new URL(url,location.href);if(/^\/api\/customers\/[^/]+$/.test(u.pathname)){res.clone().json().then(function(data){if(data&&data.customer&&data.customer.customer_id){latestCustomerId=data.customer.customer_id;detailStore[latestCustomerId]=data;setTimeout(installButtons,90);setTimeout(installButtons,300)}}).catch(function(){})}}catch(e){}return res})};
  document.addEventListener('click',function(e){setTimeout(installButtons,80);setTimeout(installButtons,250)});
  var mo=new MutationObserver(function(){installButtons()});mo.observe(document.documentElement,{childList:true,subtree:true});
})();
</script>`;

  return html.replace("</head>", style + "</head>").replace("</body>", script + "</body>");
}

async function maybeAddLineLogsToCustomerDetail(res, url, env, request) {
  const contentType = res.headers.get("content-type") || "";
  if (!contentType.includes("application/json") || !/^\/api\/customers\/[^/]+$/.test(url.pathname)) {
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }

  const raw = await res.text();
  try {
    const data = raw ? JSON.parse(raw) : {};
    if (data && data.customer && data.customer.customer_id) {
      const current = await getCurrentUser(request, env);
      if (current) {
        await ensureLineDraftLogSchema(env);
        const logs = await env.DB.prepare(`
          SELECT id, customer_id, customer_name, action_type, action_label, priority, message_text, status, channel, created_by, copied_at, sent_at, memo, created_at, updated_at
          FROM customer_line_draft_logs
          WHERE customer_id=?
          ORDER BY COALESCE(sent_at, copied_at, created_at) DESC, id DESC
          LIMIT 30
        `).bind(data.customer.customer_id).all();
        data.line_message_logs = logs.results || [];
      }
    }
    return json(data, res.status);
  } catch (_) {
    return new Response(raw, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, hasDb: !!env.DB, secure: true, auth: "cloudflare-access-google" });
    }

    if (isLineLogListPath(url.pathname) || isLineLogMarkSentPath(url.pathname)) {
      return await handleLineMessageLogApi(request, env);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (contentType.includes("text/html")) {
      const body = injectLineLogUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }

    return await maybeAddLineLogsToCustomerDetail(res, url, env, request);
  }
};
