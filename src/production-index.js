// ======================================================
// CUSTOMER CRM API / PRODUCTION SAFETY WRAPPER
// build: customer-crm-api-production-wrapper-20260613-01
// ======================================================

import secureApp from "./secure-index.js";

const BUILD = "customer-crm-api-production-wrapper-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const ADMIN_ROLES = ["admin", "root_admin"];

function text(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function normalizeEmail(v) {
  return text(v).toLowerCase();
}

function toNumber(v, fallback = 0) {
  const n = Number(String(v ?? "").replace(/[,円¥\s]/g, ""));
  return Number.isFinite(n) ? n : fallback;
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

function isManager(current) {
  return current && ADMIN_ROLES.includes(current.role);
}

async function ensureSoftDeleteSchema(env) {
  if (!env.DB) throw new Error("D1 DB binding(DB) is missing");
  for (const table of ["customers", "customer_reservations", "customer_items", "customer_timeline", "customer_line_messages", "customer_tags"]) {
    await addColumn(env.DB, table, "deleted_at TEXT");
    await addColumn(env.DB, table, "deleted_by TEXT");
    await addColumn(env.DB, table, "delete_reason TEXT");
  }
}

function customerSelectSql() {
  return `
    SELECT customer_id,name,furigana,line_display_name,phone,email,address,genre_history,first_shoot_date,last_shoot_date,
      repeat_count,repeat_count_1y,repeat_count_90d,repeat_count_365d,repeat_count_730d,total_revenue,avg_order_value,
      acquisition_source,referrer,child1_name,child1_birthdate,child2_name,child2_birthdate,child3_name,child3_birthdate,
      anniversary,nps,photo_public_ok,memo,line_user_id,dormant_days,square_avg_payment,square_last_payment_date,created_at,updated_at,
      deleted_at,deleted_by,delete_reason
    FROM customers
  `;
}

async function listDeletedCustomers(env, url) {
  await ensureSoftDeleteSchema(env);
  const keyword = text(url.searchParams.get("keyword"));
  const limitRaw = parseInt(url.searchParams.get("limit") || "100", 10);
  const limit = Math.max(1, Math.min(Number.isFinite(limitRaw) ? limitRaw : 100, 500));
  const where = ["deleted_at IS NOT NULL", "deleted_at <> ''"];
  const params = [];

  if (keyword) {
    const like = `%${keyword}%`;
    where.push(`(name LIKE ? OR furigana LIKE ? OR line_display_name LIKE ? OR phone LIKE ? OR email LIKE ? OR customer_id LIKE ? OR memo LIKE ?)`);
    params.push(like, like, like, like, like, like, like);
  }

  params.push(limit);
  const rs = await env.DB.prepare(`${customerSelectSql()} WHERE ${where.join(" AND ")} ORDER BY deleted_at DESC, updated_at DESC LIMIT ?`)
    .bind(...params)
    .all();

  return { ok: true, count: (rs.results || []).length, items: rs.results || [] };
}

async function restoreCustomers(request, env, current) {
  if (!isManager(current)) return json({ ok: false, message: "Only admin can restore customers" }, 403);
  await ensureSoftDeleteSchema(env);

  const body = await readJson(request);
  const rawIds = body.customer_ids || body.customer_id || body.ids || [];
  const ids = (Array.isArray(rawIds) ? rawIds : [rawIds]).map(text).filter(Boolean);
  if (!ids.length) return json({ ok: false, message: "customer_ids required" }, 400);

  for (const id of ids) {
    await env.DB.prepare(`UPDATE customers SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL, updated_at=datetime('now') WHERE customer_id=?`).bind(id).run();
    await env.DB.prepare(`UPDATE customer_reservations SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL, updated_at=datetime('now') WHERE customer_id=?`).bind(id).run();
    await env.DB.prepare(`UPDATE customer_items SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL, updated_at=datetime('now') WHERE customer_id=?`).bind(id).run();
    await env.DB.prepare(`UPDATE customer_timeline SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL WHERE customer_id=?`).bind(id).run();
    await env.DB.prepare(`UPDATE customer_line_messages SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL WHERE customer_id=?`).bind(id).run();
    await env.DB.prepare(`UPDATE customer_tags SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL WHERE customer_id=?`).bind(id).run();
  }

  return json({ ok: true, mode: "restore", restored: ids.length, customer_ids: ids, restored_by: current.email });
}

function buildNextActions(customer = {}) {
  const actions = [];
  const dormantDays = toNumber(customer.dormant_days, 0);
  const totalRevenue = toNumber(customer.total_revenue, 0);
  const repeatCount = toNumber(customer.repeat_count, 0);
  const genre = text(customer.genre_history);
  const hasLine = !!text(customer.line_user_id);
  const photoOk = String(customer.photo_public_ok) === "1" || customer.photo_public_ok === true;

  function add(type, label, message, priority = "normal") {
    actions.push({ type, label, message, priority });
  }

  if (dormantDays >= 365) {
    add("dormant_follow", "休眠フォロー", `最終撮影から${dormantDays}日経過しています。近況確認と再撮影の案内候補です。`, "high");
  } else if (dormantDays >= 180) {
    add("revisit_offer", "再来店案内", `最終撮影から${dormantDays}日経過しています。季節撮影や記念日の案内候補です。`, "high");
  }

  if (totalRevenue >= 100000) {
    add("vip_customer", "優良顧客", `累計売上が${Math.round(totalRevenue).toLocaleString("ja-JP")}円です。特別案内や先行予約の候補です。`, "high");
  }

  if (hasLine) {
    add("line_follow", "LINEフォロー可能", "LINE連携済みです。個別メッセージで再来店案内や相談対応ができます。", "normal");
  }

  if (photoOk) {
    add("photo_public", "作例・紹介依頼", "写真公開OKの顧客です。作例掲載や紹介依頼の候補です。", "normal");
  }

  if (repeatCount >= 2) {
    add("repeat_customer", "リピーター向け案内", `${repeatCount}回撮影済みです。兄弟撮影・季節撮影・アルバム提案の候補です。`, "normal");
  }

  if (genre.includes("七五三")) {
    add("shichigosan_next", "入学・兄弟撮影提案", "七五三の履歴があります。入学・卒園・兄弟撮影の提案候補です。", "normal");
  }

  if (genre.includes("お宮参り")) {
    add("omiyamairi_next", "バースデー・七五三提案", "お宮参りの履歴があります。1歳バースデーや七五三への継続案内候補です。", "normal");
  }

  if (!hasLine) {
    add("line_missing", "LINE連携確認", "LINE user IDが未登録です。次回問い合わせ時にLINE連携できると履歴管理がしやすくなります。", "low");
  }

  if (!actions.length) {
    add("basic_follow", "通常フォロー", "大きな優先アクションはありません。次回記念日や季節撮影の案内候補です。", "low");
  }

  return actions;
}

function isCustomerDetailPath(path) {
  return /^\/api\/customers\/[^/]+$/.test(path);
}

async function maybeAddNextActionsToJsonResponse(res, url) {
  const contentType = res.headers.get("content-type") || "";
  if (!contentType.includes("application/json") || !isCustomerDetailPath(url.pathname)) {
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }

  const raw = await res.text();
  try {
    const data = raw ? JSON.parse(raw) : {};
    if (data && data.customer) {
      data.next_actions = buildNextActions(data.customer);
    }
    return json(data, res.status);
  } catch (_) {
    return new Response(raw, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
}

function injectNextActionUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-production-safe-controls">
.header .danger{display:none!important;visibility:hidden!important;pointer-events:none!important}
.crm-next-actions{margin:12px 0;padding:13px;border:1px solid #dbeafe;background:#eff6ff;border-radius:18px;box-shadow:0 6px 18px rgba(37,99,235,.08)}
.crm-next-actions-head{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px}
.crm-next-actions-head b{font-size:1rem}.crm-next-actions-head span{font-size:.75rem;color:#64748b;font-weight:800}
.crm-next-action-list{display:grid;gap:8px}.crm-next-action{border:1px solid #e5e7eb;background:#fff;border-radius:14px;padding:10px}.crm-next-action-title{display:flex;gap:8px;align-items:center;font-weight:950}.crm-next-action-title em{font-style:normal;border-radius:999px;padding:3px 7px;font-size:.68rem;background:#f1f5f9;color:#334155}.crm-next-action-title em.high{background:#fee2e2;color:#991b1b}.crm-next-action-title em.normal{background:#dcfce7;color:#166534}.crm-next-action-title em.low{background:#f8fafc;color:#64748b}.crm-next-action-msg{margin-top:5px;color:#475569;font-size:.84rem;line-height:1.55}
@media(max-width:760px){.crm-next-actions{border-radius:16px;padding:12px}.crm-next-action{padding:10px}.crm-next-action-msg{font-size:.82rem}}
</style>`;

  const script = `<script id="crm-next-actions-script">
(function(){
  if(window.__crmNextActionsInstalled)return;
  window.__crmNextActionsInstalled=true;
  var store={};
  var originalFetch=window.fetch;
  function esc(v){return String(v==null?'':v).replace(/[&<>\"]/g,function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[m]})}
  function money(v){var n=Number(String(v==null?'':v).replace(/[,円¥\\s]/g,''));return Number.isFinite(n)?Math.round(n).toLocaleString('ja-JP')+'円':'0円'}
  function buildActions(c){
    c=c||{};var a=[];var dormant=Number(c.dormant_days||0);var rev=Number(c.total_revenue||0);var repeat=Number(c.repeat_count||0);var genre=String(c.genre_history||'');var hasLine=!!String(c.line_user_id||'').trim();var photo=String(c.photo_public_ok)==='1'||c.photo_public_ok===true;
    function add(type,label,msg,priority){a.push({type:type,label:label,message:msg,priority:priority||'normal'})}
    if(dormant>=365)add('dormant_follow','休眠フォロー','最終撮影から'+dormant+'日経過しています。近況確認と再撮影の案内候補です。','high');
    else if(dormant>=180)add('revisit_offer','再来店案内','最終撮影から'+dormant+'日経過しています。季節撮影や記念日の案内候補です。','high');
    if(rev>=100000)add('vip_customer','優良顧客','累計売上が'+money(rev)+'です。特別案内や先行予約の候補です。','high');
    if(hasLine)add('line_follow','LINEフォロー可能','LINE連携済みです。個別メッセージで再来店案内や相談対応ができます。','normal');
    if(photo)add('photo_public','作例・紹介依頼','写真公開OKの顧客です。作例掲載や紹介依頼の候補です。','normal');
    if(repeat>=2)add('repeat_customer','リピーター向け案内',repeat+'回撮影済みです。兄弟撮影・季節撮影・アルバム提案の候補です。','normal');
    if(genre.indexOf('七五三')>=0)add('shichigosan_next','入学・兄弟撮影提案','七五三の履歴があります。入学・卒園・兄弟撮影の提案候補です。','normal');
    if(genre.indexOf('お宮参り')>=0)add('omiyamairi_next','バースデー・七五三提案','お宮参りの履歴があります。1歳バースデーや七五三への継続案内候補です。','normal');
    if(!hasLine)add('line_missing','LINE連携確認','LINE user IDが未登録です。次回問い合わせ時にLINE連携できると履歴管理がしやすくなります。','low');
    if(!a.length)add('basic_follow','通常フォロー','大きな優先アクションはありません。次回記念日や季節撮影の案内候補です。','low');
    return a;
  }
  function render(actions){
    actions=actions||[];
    return '<div class="crm-next-actions" id="crmNextActionsCard"><div class="crm-next-actions-head"><b>次にやること</b><span>'+actions.length+'件</span></div><div class="crm-next-action-list">'+actions.map(function(x){var p=x.priority||'normal';return '<div class="crm-next-action"><div class="crm-next-action-title"><span>'+esc(x.label)+'</span><em class="'+esc(p)+'">'+esc(p)+'</em></div><div class="crm-next-action-msg">'+esc(x.message)+'</div></div>'}).join('')+'</div></div>';
  }
  function tryInsert(customerId){
    var modal=document.getElementById('modal');if(!modal||!customerId||!store[customerId])return;
    var old=document.getElementById('crmNextActionsCard');if(old)old.remove();
    var actions=store[customerId].next_actions||buildActions(store[customerId].customer||{});
    var actionsBar=modal.querySelector('.actions');
    if(actionsBar){actionsBar.insertAdjacentHTML('afterend',render(actions));return;}
    modal.insertAdjacentHTML('afterbegin',render(actions));
  }
  window.fetch=function(input,init){
    return originalFetch(input,init).then(function(res){
      try{
        var url=typeof input==='string'?input:(input&&input.url)||'';
        var u=new URL(url,location.href);
        if(/^\/api\/customers\/[^/]+$/.test(u.pathname)){
          res.clone().json().then(function(data){
            if(data&&data.customer&&data.customer.customer_id){
              if(!data.next_actions)data.next_actions=buildActions(data.customer);
              store[data.customer.customer_id]=data;
              setTimeout(function(){tryInsert(data.customer.customer_id)},60);
              setTimeout(function(){tryInsert(data.customer.customer_id)},250);
            }
          }).catch(function(){});
        }
      }catch(e){}
      return res;
    });
  };
  var mo=new MutationObserver(function(){
    var modal=document.getElementById('modal');if(!modal)return;
    Object.keys(store).forEach(function(id){if(modal.textContent&&modal.textContent.indexOf(id)>=0)tryInsert(id)});
  });
  mo.observe(document.documentElement,{childList:true,subtree:true});
})();
</script>`;

  return html.replace("</head>", style + "</head>").replace("</body>", script + "</body>");
}

function hasLegacyQuery(url) {
  const key = ["to", "ken"].join("");
  return url.searchParams.has(key);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, hasDb: !!env.DB, secure: true, auth: "cloudflare-access-google" });
    }

    if (hasLegacyQuery(url)) return json({ ok: false, message: "Legacy query auth is disabled." }, 401);
    if (!env.ADMIN_TOKEN) return json({ ok: false, message: "Required admin setting is missing" }, 503);
    if (url.pathname.startsWith("/api/sync/") && !env.SYNC_TOKEN) return json({ ok: false, message: "Required sync setting is missing" }, 503);

    if (url.pathname === "/api/customers/deleted" || url.pathname === "/api/customers/restore") {
      const current = await getCurrentUser(request, env);
      if (!current) return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);
      if (!isManager(current)) return json({ ok: false, message: "Only admin can manage deleted customers" }, 403);
      if (url.pathname === "/api/customers/deleted" && request.method === "GET") return json(await listDeletedCustomers(env, url));
      if (url.pathname === "/api/customers/restore" && request.method === "POST") return await restoreCustomers(request, env, current);
      return json({ ok: false, message: "Method Not Allowed" }, 405);
    }

    const res = await secureApp.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (!contentType.includes("text/html")) {
      return await maybeAddNextActionsToJsonResponse(res, url);
    }

    const body = injectNextActionUi(await res.text());
    return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
