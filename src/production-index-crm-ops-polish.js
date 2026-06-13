// ======================================================
// CUSTOMER CRM API / OPS POLISH WRAPPER
// build: customer-crm-api-ops-polish-20260613-01
// Adds practical delete/complete UI for memos, tags, and follow tasks.
// ======================================================

import app from "./production-index-crm-suite.js";

const BUILD = "customer-crm-api-ops-polish-20260613-01";
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

function isEditor(current) {
  return !!current && ["root_admin", "admin", "staff"].includes(current.role || "");
}

async function ensureOpsSchema(env) {
  if (!env.DB) throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_customer_memos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    memo_text TEXT NOT NULL,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_customer_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    color TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, tag)
  )`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_follow_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    task_type TEXT,
    title TEXT NOT NULL,
    message_text TEXT,
    due_date TEXT,
    priority TEXT DEFAULT 'medium',
    status TEXT DEFAULT 'open',
    created_by TEXT,
    completed_by TEXT,
    completed_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();
}

function parseCustomerChildPath(path, section) {
  const prefix = "/api/customers/";
  const marker = `/${section}/`;
  if (!path.startsWith(prefix) || !path.includes(marker)) return null;
  const rest = path.slice(prefix.length);
  const i = rest.indexOf(marker);
  return {
    customerId: decodeURIComponent(rest.slice(0, i)),
    childId: parseInt(rest.slice(i + marker.length), 10)
  };
}

async function updateCustomerMemo(request, env, customerId, memoId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureOpsSchema(env);
  const body = await readJson(request);
  const memo = text(body.memo || body.memo_text || body.text);
  if (!memo) return json({ ok: false, message: "memo required" }, 400);
  const res = await env.DB.prepare(`
    UPDATE crm_customer_memos
    SET memo_text=?, updated_at=datetime('now')
    WHERE customer_id=? AND id=?
  `).bind(memo, customerId, memoId).run();
  return json({ ok: true, updated: res.meta?.changes || 0 });
}

async function deleteCustomerMemo(request, env, customerId, memoId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureOpsSchema(env);
  const res = await env.DB.prepare(`DELETE FROM crm_customer_memos WHERE customer_id=? AND id=?`).bind(customerId, memoId).run();
  return json({ ok: true, deleted: res.meta?.changes || 0 });
}

async function deleteCustomerTagById(request, env, customerId, tagId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureOpsSchema(env);
  const res = await env.DB.prepare(`DELETE FROM crm_customer_tags WHERE customer_id=? AND id=?`).bind(customerId, tagId).run();
  return json({ ok: true, deleted: res.meta?.changes || 0 });
}

async function updateFollowTask(request, env, taskId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureOpsSchema(env);
  const body = await readJson(request);
  const title = text(body.title);
  const dueDate = text(body.due_date || body.dueDate);
  const priority = text(body.priority || "medium");
  const messageText = text(body.message_text || body.message || "");
  if (!title) return json({ ok: false, message: "title required" }, 400);
  const res = await env.DB.prepare(`
    UPDATE crm_follow_tasks
    SET title=?, due_date=?, priority=?, message_text=?, updated_at=datetime('now')
    WHERE id=?
  `).bind(title, dueDate, priority, messageText, taskId).run();
  return json({ ok: true, updated: res.meta?.changes || 0 });
}

async function deleteFollowTask(request, env, taskId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureOpsSchema(env);
  const res = await env.DB.prepare(`DELETE FROM crm_follow_tasks WHERE id=?`).bind(taskId).run();
  return json({ ok: true, deleted: res.meta?.changes || 0 });
}

async function reopenFollowTask(request, env, taskId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureOpsSchema(env);
  const res = await env.DB.prepare(`
    UPDATE crm_follow_tasks
    SET status='open', completed_by=NULL, completed_at=NULL, updated_at=datetime('now')
    WHERE id=?
  `).bind(taskId).run();
  return json({ ok: true, updated: res.meta?.changes || 0 });
}

function injectOpsPolishUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-ops-polish-style">
.crm-ops-mini{font-size:.78rem;color:#64748b;margin-top:3px}.crm-ops-row{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-top:6px}.crm-ops-icon{border:1px solid #cbd5e1;background:#fff;border-radius:999px;padding:5px 8px;font-size:.72rem;font-weight:900;cursor:pointer}.crm-ops-icon:hover{background:#f8fafc}.crm-ops-icon.danger{border-color:#fecaca;color:#be123c;background:#fff1f2}.crm-ops-icon.done{border-color:#bbf7d0;color:#166534;background:#f0fdf4}.crm-ops-edit{width:100%;box-sizing:border-box;border:1px solid #cbd5e1;border-radius:10px;padding:8px 9px;margin-top:6px;font-size:.86rem}.crm-ops-taskline{display:grid;grid-template-columns:1fr auto;gap:8px;align-items:start}.crm-ops-taskline.done{opacity:.55}.crm-suite-log.is-done{opacity:.58;background:#f8fafc;border-radius:12px;padding-left:8px;padding-right:8px}
</style>`;

  const script = `<script id="crm-ops-polish-script">
(function(){
  if(window.__crmOpsPolishInstalled)return;
  window.__crmOpsPolishInstalled=true;

  function qs(sel,root){return (root||document).querySelector(sel)}
  function qsa(sel,root){return Array.prototype.slice.call((root||document).querySelectorAll(sel))}
  function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:999999;background:#0f172a;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2200)}

  function currentCustomerId(){return window.__crmSuiteCustomerId||''}
  function panel(){return qs('#crmSuiteDetailPanel')}
  function pane(name){var p=panel();return p?qs('[data-pane="'+name+'"]',p):null}

  function renderMemos(customerId){
    var p=pane('memo'); if(!p)return;
    api('/api/customers/'+encodeURIComponent(customerId)+'/memos').then(function(d){
      var memos=d.memos||[];
      p.innerHTML='<textarea class="crm-suite-textarea" id="crmOpsMemoInput" placeholder="顧客メモを入力"></textarea><div class="crm-ops-row"><button class="crm-suite-btn primary" id="crmOpsMemoSave">メモ保存</button></div><div id="crmOpsMemoList">'+(memos.length?memos.map(function(m){return '<div class="crm-suite-log" data-memo-id="'+esc(m.id)+'"><div style="white-space:pre-wrap" data-memo-text>'+esc(m.memo_text)+'</div><div class="crm-suite-muted">'+esc(m.created_by||'')+' / '+esc(m.created_at||'')+'</div><div class="crm-ops-row"><button class="crm-ops-icon" data-memo-edit>編集</button><button class="crm-ops-icon danger" data-memo-delete>削除</button></div></div>'}).join(''):'<div class="crm-suite-muted">メモはありません。</div>')+'</div>';
      qs('#crmOpsMemoSave',p).onclick=function(){var v=qs('#crmOpsMemoInput',p).value;if(!v.trim()){toast('メモを入力してください');return}api('/api/customers/'+encodeURIComponent(customerId)+'/memos',{method:'POST',body:JSON.stringify({memo:v})}).then(function(x){toast(x.ok?'メモを保存しました':'保存に失敗しました');renderMemos(customerId)})};
    });
  }

  function renderTags(customerId){
    var p=pane('tags'); if(!p)return;
    api('/api/customers/'+encodeURIComponent(customerId)+'/tags').then(function(d){
      var tags=d.tags||[];
      p.innerHTML='<input class="crm-suite-input" id="crmOpsTagInput" placeholder="タグ 例：七五三候補"><div class="crm-ops-row"><button class="crm-suite-btn primary" id="crmOpsTagAdd">タグ追加</button></div><div id="crmOpsTagList">'+(tags.length?tags.map(function(t){return '<span class="crm-suite-tag" data-tag-id="'+esc(t.id)+'">'+esc(t.tag)+' <button class="crm-ops-icon danger" data-tag-delete style="padding:2px 6px">×</button></span>'}).join(''):'<div class="crm-suite-muted">タグはありません。</div>')+'</div>';
      qs('#crmOpsTagAdd',p).onclick=function(){var v=qs('#crmOpsTagInput',p).value;if(!v.trim()){toast('タグを入力してください');return}api('/api/customers/'+encodeURIComponent(customerId)+'/tags',{method:'POST',body:JSON.stringify({tag:v})}).then(function(x){toast(x.ok?'タグを追加しました':'追加に失敗しました');renderTags(customerId)})};
    });
  }

  function renderTasks(customerId){
    var p=pane('task'); if(!p)return;
    api('/api/customers/'+encodeURIComponent(customerId)+'/follow-tasks?status=all').then(function(d){
      var tasks=d.tasks||[];
      p.innerHTML='<input class="crm-suite-input" id="crmOpsTaskTitle" placeholder="予定タイトル 例：七五三案内"><div class="crm-ops-row"><input class="crm-suite-input" id="crmOpsTaskDue" type="date" style="max-width:180px"><select class="crm-suite-input" id="crmOpsTaskPriority" style="max-width:150px"><option value="high">高</option><option value="medium" selected>中</option><option value="low">低</option></select><button class="crm-suite-btn primary" id="crmOpsTaskAdd">予定追加</button></div><div id="crmOpsTaskList">'+(tasks.length?tasks.map(function(t){var done=(t.status==='done');return '<div class="crm-suite-log '+(done?'is-done':'')+'" data-task-id="'+esc(t.id)+'"><div class="crm-ops-taskline"><div><b>'+esc(t.title)+'</b><div class="crm-suite-muted">'+esc(t.due_date||'期限なし')+' / '+esc(t.priority||'medium')+' / '+esc(t.status||'open')+'</div></div><div class="crm-ops-row">'+(done?'<button class="crm-ops-icon" data-task-reopen>未完了に戻す</button>':'<button class="crm-ops-icon done" data-task-done>完了</button>')+'<button class="crm-ops-icon danger" data-task-delete>削除</button></div></div></div>'}).join(''):'<div class="crm-suite-muted">フォロー予定はありません。</div>')+'</div>';
      qs('#crmOpsTaskAdd',p).onclick=function(){var title=qs('#crmOpsTaskTitle',p).value;if(!title.trim()){toast('予定タイトルを入力してください');return}api('/api/customers/'+encodeURIComponent(customerId)+'/follow-tasks',{method:'POST',body:JSON.stringify({title:title,due_date:qs('#crmOpsTaskDue',p).value,priority:qs('#crmOpsTaskPriority',p).value})}).then(function(x){toast(x.ok?'予定を追加しました':'追加に失敗しました');renderTasks(customerId)})};
    });
  }

  function refreshActivePane(){var id=currentCustomerId(); if(!id)return; renderMemos(id); renderTags(id); renderTasks(id)}
  function enhance(){var id=currentCustomerId(); if(!id||!panel())return; renderMemos(id); renderTags(id); renderTasks(id)}

  document.addEventListener('click',function(e){
    var id=currentCustomerId(); if(!id)return;
    var memo=e.target.closest('[data-memo-id]');
    if(e.target.closest('[data-memo-delete]')&&memo){if(!confirm('このメモを削除しますか？'))return;api('/api/customers/'+encodeURIComponent(id)+'/memos/'+memo.getAttribute('data-memo-id'),{method:'DELETE',body:'{}'}).then(function(x){toast(x.ok?'メモを削除しました':'削除に失敗しました');renderMemos(id)})}
    if(e.target.closest('[data-memo-edit]')&&memo){var old=(qs('[data-memo-text]',memo)||{}).textContent||'';var nv=prompt('メモを編集',old);if(nv==null)return;api('/api/customers/'+encodeURIComponent(id)+'/memos/'+memo.getAttribute('data-memo-id'),{method:'PATCH',body:JSON.stringify({memo:nv})}).then(function(x){toast(x.ok?'メモを更新しました':'更新に失敗しました');renderMemos(id)})}
    var tag=e.target.closest('[data-tag-id]');
    if(e.target.closest('[data-tag-delete]')&&tag){api('/api/customers/'+encodeURIComponent(id)+'/tags/'+tag.getAttribute('data-tag-id'),{method:'DELETE',body:'{}'}).then(function(x){toast(x.ok?'タグを削除しました':'削除に失敗しました');renderTags(id)})}
    var task=e.target.closest('[data-task-id]');
    if(e.target.closest('[data-task-done]')&&task){api('/api/follow-tasks/'+task.getAttribute('data-task-id')+'/complete',{method:'POST',body:'{}'}).then(function(x){toast(x.ok?'予定を完了しました':'完了に失敗しました');renderTasks(id)})}
    if(e.target.closest('[data-task-reopen]')&&task){api('/api/follow-tasks/'+task.getAttribute('data-task-id')+'/reopen',{method:'POST',body:'{}'}).then(function(x){toast(x.ok?'未完了に戻しました':'更新に失敗しました');renderTasks(id)})}
    if(e.target.closest('[data-task-delete]')&&task){if(!confirm('この予定を削除しますか？'))return;api('/api/follow-tasks/'+task.getAttribute('data-task-id'),{method:'DELETE',body:'{}'}).then(function(x){toast(x.ok?'予定を削除しました':'削除に失敗しました');renderTasks(id)})}
  });

  var mo=new MutationObserver(function(){clearTimeout(window.__crmOpsTimer);window.__crmOpsTimer=setTimeout(enhance,500)});
  mo.observe(document.documentElement,{childList:true,subtree:true});
  setTimeout(enhance,1000);setTimeout(enhance,2200);
})();
</script>`;

  return html.replace("</head>", style + "</head>").replace("</body>", script + "</body>");
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/" || path === "/health" || path === "/api/health") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, hasDb: !!env.DB, secure: true, auth: "cloudflare-access-google" });
    }

    try {
      const memoPath = parseCustomerChildPath(path, "memos");
      if (memoPath && Number.isFinite(memoPath.childId)) {
        if (request.method === "PATCH" || request.method === "PUT") return await updateCustomerMemo(request, env, memoPath.customerId, memoPath.childId);
        if (request.method === "DELETE") return await deleteCustomerMemo(request, env, memoPath.customerId, memoPath.childId);
      }

      const tagPath = parseCustomerChildPath(path, "tags");
      if (tagPath && Number.isFinite(tagPath.childId) && request.method === "DELETE") {
        return await deleteCustomerTagById(request, env, tagPath.customerId, tagPath.childId);
      }

      if (path.startsWith("/api/follow-tasks/") && path.endsWith("/reopen") && request.method === "POST") {
        const taskId = parseInt(path.replace("/api/follow-tasks/", "").replace("/reopen", ""), 10);
        return await reopenFollowTask(request, env, taskId);
      }

      if (path.startsWith("/api/follow-tasks/") && request.method === "PATCH") {
        const taskId = parseInt(path.replace("/api/follow-tasks/", ""), 10);
        return await updateFollowTask(request, env, taskId);
      }

      if (path.startsWith("/api/follow-tasks/") && request.method === "DELETE") {
        const taskId = parseInt(path.replace("/api/follow-tasks/", ""), 10);
        return await deleteFollowTask(request, env, taskId);
      }
    } catch (err) {
      return json({ ok: false, message: "crm ops polish error", error: String(err && err.message ? err.message : err) }, 500);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (contentType.includes("text/html")) {
      const body = injectOpsPolishUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }

    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
