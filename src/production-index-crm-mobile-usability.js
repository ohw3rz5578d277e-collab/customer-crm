// ======================================================
// CUSTOMER CRM / MOBILE USABILITY WRAPPER
// build: customer-crm-api-mobile-usability-20260614-01
// Adds mobile-first bottom navigation, priority alerts, and easier one-tap actions.
// ======================================================

import app from "./production-index-crm-usability-hub.js";

const BUILD = "customer-crm-api-mobile-usability-20260614-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){ return v === undefined || v === null ? "" : String(v).trim(); }
function lower(v){ return text(v).toLowerCase(); }
function nowIso(){ return new Date().toISOString(); }
function securityHeaders(headers = {}){
  const h = new Headers(headers);
  h.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  h.set("pragma", "no-cache");
  h.set("expires", "0");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  return h;
}
function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), { status, headers: securityHeaders({ "content-type":"application/json; charset=utf-8" }) });
}
function getEmail(request){ return lower(request.headers.get("cf-access-authenticated-user-email") || request.headers.get("Cf-Access-Authenticated-User-Email") || request.headers.get("x-user-email") || ""); }
async function readJson(request){ try { return await request.json(); } catch(_) { return {}; } }
async function safeAll(env, sql, binds = []){ try { const s = env.DB.prepare(sql); const r = binds.length ? await s.bind(...binds).all() : await s.all(); return r.results || []; } catch(_) { return []; } }
async function safeFirst(env, sql, binds = []){ try { const s = env.DB.prepare(sql); return binds.length ? await s.bind(...binds).first() : await s.first(); } catch(_) { return null; } }
async function addColumn(db, table, def){ try { await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${def}`).run(); } catch(_) {} }

async function ensureSchema(env){
  if(!env.DB) throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY, role TEXT, status TEXT, created_by TEXT, created_at TEXT, updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email, role, status, created_by, created_at, updated_at) VALUES(?, 'admin', 'active', 'system', datetime('now'), datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_usability_preferences(
    user_email TEXT PRIMARY KEY,
    compact_mode INTEGER DEFAULT 1,
    show_mobile_bar INTEGER DEFAULT 1,
    show_priority_alerts INTEGER DEFAULT 1,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_usability_alert_ack(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_email TEXT,
    alert_key TEXT,
    ack_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_email, alert_key)
  )`).run();
  await addColumn(env.DB, "customers", "usability_pinned INTEGER DEFAULT 0");
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_usability_alert_ack_user ON crm_usability_alert_ack(user_email, alert_key)`).run();
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

async function prioritySummaryApi(request, env){
  const auth = await requireUser(request, env);
  if(!auth.ok) return auth.response;
  const reservationAlerts = await safeFirst(env, `SELECT COUNT(*) AS n FROM crm_reservation_drafts WHERE COALESCE(status,'') IN ('sent_to_reservation','pending','needs_check','') AND COALESCE(deleted_at,'')=''`);
  const linePending = await safeFirst(env, `SELECT COUNT(*) AS n FROM customer_line_draft_logs WHERE COALESCE(status,'pending') NOT IN ('sent','done','deleted')`);
  const inquiryWaiting = await safeFirst(env, `SELECT COUNT(*) AS n FROM crm_inquiry_pipeline WHERE COALESCE(deleted_at,'')='' AND COALESCE(status,'') IN ('問い合わせ','日程調整中','料金案内済み','返信待ち','仮予約')`);
  const deliveryRisk = await safeAll(env, `SELECT delay_level, COUNT(*) AS n FROM crm_reservation_progress WHERE COALESCE(progress_status,'') NOT IN ('完了','キャンセル') GROUP BY delay_level`);
  const marketingCandidates = await safeFirst(env, `SELECT COUNT(*) AS n FROM customers WHERE COALESCE(deleted_at,'')='' AND (COALESCE(marketing_score,0) >= 40 OR COALESCE(customer_rank,'') IN ('VIP','リピーター','休眠'))`);
  const cards = [
    { key:"reservation", label:"予約要確認", count:Number(reservationAlerts?.n||0), level:Number(reservationAlerts?.n||0)>0?"warn":"ok", action:"button:予約連携 要確認" },
    { key:"line", label:"LINE未送信", count:Number(linePending?.n||0), level:Number(linePending?.n||0)>0?"notice":"ok", action:"button:LINE未送信" },
    { key:"inquiry", label:"問い合わせ対応", count:Number(inquiryWaiting?.n||0), level:Number(inquiryWaiting?.n||0)>0?"warn":"ok", action:"button:問い合わせ一覧アクション" },
    { key:"delivery", label:"納品未完了", count:deliveryRisk.reduce((s,r)=>s+Number(r.n||0),0), level:deliveryRisk.some(r=>String(r.delay_level)==='danger')?"danger":"notice", action:"#crmDeliveryPanel" },
    { key:"marketing", label:"マーケ候補", count:Number(marketingCandidates?.n||0), level:Number(marketingCandidates?.n||0)>0?"notice":"ok", action:"button:マーケDB" }
  ];
  return json({ ok:true, build:BUILD, cards, generated_at:nowIso() });
}

async function preferencesApi(request, env){
  const auth = await requireUser(request, env, request.method === "POST" ? WRITE_ROLES : ROLES);
  if(!auth.ok) return auth.response;
  if(request.method === "GET"){
    const row = await safeFirst(env, `SELECT * FROM crm_usability_preferences WHERE user_email=?`, [auth.email]);
    return json({ ok:true, preferences: row || { user_email:auth.email, compact_mode:1, show_mobile_bar:1, show_priority_alerts:1 } });
  }
  const b = await readJson(request);
  const compact = b.compact_mode === false || b.compact_mode === 0 ? 0 : 1;
  const bar = b.show_mobile_bar === false || b.show_mobile_bar === 0 ? 0 : 1;
  const alerts = b.show_priority_alerts === false || b.show_priority_alerts === 0 ? 0 : 1;
  await env.DB.prepare(`INSERT INTO crm_usability_preferences(user_email, compact_mode, show_mobile_bar, show_priority_alerts, updated_at) VALUES(?,?,?,?,CURRENT_TIMESTAMP)
    ON CONFLICT(user_email) DO UPDATE SET compact_mode=excluded.compact_mode, show_mobile_bar=excluded.show_mobile_bar, show_priority_alerts=excluded.show_priority_alerts, updated_at=CURRENT_TIMESTAMP`).bind(auth.email, compact, bar, alerts).run();
  return json({ ok:true, preferences:{ compact_mode:compact, show_mobile_bar:bar, show_priority_alerts:alerts } });
}

function injectMobileUsability(html){
  if(!html || html.includes("crmMobileUsabilityScript")) return html;
  const style = `<style id="crmMobileUsabilityStyle">
:root{--crm-accent:#028760;--crm-ink:#111827;--crm-soft:#f8fafc}body{scroll-behavior:smooth}.crm-mobile-friendly button,.crm-mobile-friendly input,.crm-mobile-friendly select,.crm-mobile-friendly textarea{font-size:16px!important}.crm-mobile-friendly button{min-height:38px}.crmTapTarget{touch-action:manipulation}#crmMobileBar{position:fixed;left:10px;right:10px;bottom:10px;z-index:1000030;display:flex;gap:6px;background:rgba(255,255,255,.96);border:1px solid #e5e7eb;border-radius:18px;padding:7px;box-shadow:0 18px 50px rgba(15,23,42,.18);backdrop-filter:blur(10px)}#crmMobileBar button{flex:1;border:0;border-radius:12px;background:#f1f5f9;color:#0f172a;padding:8px 4px;font-size:11px;font-weight:950;cursor:pointer}#crmMobileBar button.primary{background:var(--crm-accent);color:#fff}#crmPriorityFab{position:fixed;right:16px;bottom:74px;z-index:1000031;background:#dc2626;color:#fff;border:0;border-radius:999px;padding:10px 13px;font-weight:950;box-shadow:0 14px 38px rgba(220,38,38,.28);cursor:pointer}#crmPriorityPanel{position:fixed;right:12px;bottom:126px;z-index:1000032;width:min(380px,calc(100vw - 24px));max-height:70vh;overflow:auto;background:#fff;border:1px solid #e5e7eb;border-radius:18px;padding:12px;box-shadow:0 24px 70px rgba(0,0,0,.24);display:none;color:#0f172a}#crmPriorityPanel h3{margin:0 0 8px;font-size:16px;font-weight:950}.crmPriorityCard{border:1px solid #e5e7eb;border-radius:14px;padding:10px;margin:8px 0;background:#fff;display:flex;justify-content:space-between;gap:10px;align-items:center}.crmPriorityCard b{font-size:13px}.crmPriorityCard strong{font-size:22px}.crmPriorityCard button{border:0;background:#111827;color:#fff;border-radius:10px;padding:7px 9px;font-size:12px;font-weight:900}.crmPriorityCard.danger{border-color:#fecaca;background:#fff1f2}.crmPriorityCard.warn{border-color:#fde68a;background:#fffbeb}.crmPriorityCard.notice{border-color:#bae6fd;background:#f0f9ff}.crmPriorityCard.ok{opacity:.7}.crmUxQuickHint{position:fixed;left:12px;bottom:74px;z-index:1000031;background:#111827;color:#fff;border-radius:999px;padding:8px 11px;font-size:11px;font-weight:900;box-shadow:0 12px 32px rgba(0,0,0,.22)}@media(min-width:861px){#crmMobileBar{display:none}.crmUxQuickHint{display:none}}@media(max-width:860px){body{padding-bottom:82px!important}#crmUxFab{display:none!important}.crmUxGrid,.crmOpsGrid,.crmMarketingGrid{grid-template-columns:1fr!important}.crm-card,.crmCard,.crmUxCard{border-radius:16px!important}.crmPriorityCard button{min-width:72px}}
</style>`;
  const script = `<script id="crmMobileUsabilityScript">
(function(){
 if(window.__crmMobileUsabilityInstalled)return; window.__crmMobileUsabilityInstalled=true;
 function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
 function api(u,o){return fetch(u,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},o||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
 function toast(m){var d=document.createElement('div');d.textContent=m;d.style.cssText='position:fixed;right:16px;bottom:130px;z-index:1000040;background:#111827;color:#fff;padding:10px 12px;border-radius:12px;font-weight:900';document.body.appendChild(d);setTimeout(function(){d.remove()},2200)}
 function clickByText(label){var btn=[].slice.call(document.querySelectorAll('button,a')).find(function(b){return (b.textContent||'').indexOf(label)>=0}); if(btn){btn.click();return true;} return false;}
 function openAction(action){ if(action.indexOf('button:')===0){if(!clickByText(action.slice(7)))toast(action.slice(7)+' が見つかりません');return;} if(action.indexOf('#')===0){var el=document.querySelector(action); if(el){el.scrollIntoView({behavior:'smooth',block:'start'});return;} toast('対象がまだありません');return;} window.open(action,'_blank'); }
 function initDom(){ if(document.getElementById('crmMobileBar'))return; document.body.classList.add('crm-mobile-friendly'); document.body.insertAdjacentHTML('beforeend','<div id="crmMobileBar"><button data-act="today" class="primary">今日</button><button data-act="search">検索</button><button data-act="line">LINE</button><button data-act="inquiry">問合せ</button><button data-act="marketing">マーケ</button></div><button id="crmPriorityFab">重要</button><div id="crmPriorityPanel"><h3>重要アラート</h3><div id="crmPriorityCards">読み込み中...</div></div><div class="crmUxQuickHint">Ctrl/⌘ + K で検索</div>'); }
 function renderPriority(){api('/api/usability-priority-summary').then(function(d){var cards=d.cards||[];document.getElementById('crmPriorityCards').innerHTML=cards.map(function(c){return '<div class="crmPriorityCard '+esc(c.level)+'"><div><b>'+esc(c.label)+'</b><br><small>'+esc(c.level)+'</small></div><strong>'+esc(c.count)+'</strong><button data-priority-action="'+esc(c.action)+'">開く</button></div>';}).join('')||'なし';});}
 document.addEventListener('click',function(e){var t=e.target;if(!t)return;var act=t.getAttribute&&t.getAttribute('data-act'); if(act==='today'){openAction('#crmTodayDashboard')} if(act==='search'){var b=document.getElementById('crmUxFab'); if(b)b.click(); else toast('メニューがまだありません')} if(act==='line'){openAction('button:LINE未送信')} if(act==='inquiry'){openAction('button:問い合わせ一覧アクション')} if(act==='marketing'){openAction('button:マーケDB')} if(t.id==='crmPriorityFab'){var p=document.getElementById('crmPriorityPanel');p.style.display=p.style.display==='block'?'none':'block'; if(p.style.display==='block')renderPriority();} var pa=t.getAttribute&&t.getAttribute('data-priority-action'); if(pa){openAction(pa);document.getElementById('crmPriorityPanel').style.display='none';}});
 document.addEventListener('DOMContentLoaded',initDom); setTimeout(initDom,900);
})();
</script>`;
  return html.replace("</head>", `${style}</head>`).replace("</body>", `${script}</body>`);
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    const path = url.pathname;
    if((path === "/" || path === "/health" || path === "/api/health") && request.method === "GET") return json({ ok:true, service:"customer-crm-api", build:BUILD, time:nowIso() });
    if(path === "/api/usability-priority-summary" && request.method === "GET") return prioritySummaryApi(request, env);
    if(path === "/api/usability-preferences" && (request.method === "GET" || request.method === "POST")) return preferencesApi(request, env);

    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if(type.includes("text/html")){
      const body = injectMobileUsability(await res.text());
      return new Response(body, { status:res.status, statusText:res.statusText, headers:securityHeaders(res.headers) });
    }
    return new Response(res.body, { status:res.status, statusText:res.statusText, headers:securityHeaders(res.headers) });
  }
};
