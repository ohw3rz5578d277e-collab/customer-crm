// ======================================================
// CUSTOMER CRM / USABILITY HUB WRAPPER
// build: customer-crm-api-usability-hub-20260614-01
// Adds a unified quick navigation hub, global search, favorites, and operation guide.
// ======================================================

import app from "./production-index-crm-marketing-suite.js";

const BUILD = "customer-crm-api-usability-hub-20260614-01";
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
  return new Response(JSON.stringify(data, null, 2), { status, headers: securityHeaders({ "content-type": "application/json; charset=utf-8" }) });
}
function getEmail(request){
  return lower(request.headers.get("cf-access-authenticated-user-email") || request.headers.get("Cf-Access-Authenticated-User-Email") || request.headers.get("x-user-email") || "");
}
async function readJson(request){ try { return await request.json(); } catch(_) { return {}; } }
async function safeAll(env, sql, binds = []){ try { const s = env.DB.prepare(sql); const r = binds.length ? await s.bind(...binds).all() : await s.all(); return r.results || []; } catch(_) { return []; } }
async function safeFirst(env, sql, binds = []){ try { const s = env.DB.prepare(sql); return binds.length ? await s.bind(...binds).first() : await s.first(); } catch(_) { return null; } }
async function addColumn(db, table, def){ try { await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${def}`).run(); } catch(_) {} }

async function ensureSchema(env){
  if(!env.DB) throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY, role TEXT, status TEXT, created_by TEXT, created_at TEXT, updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email, role, status, created_by, created_at, updated_at) VALUES(?, 'admin', 'active', 'system', datetime('now'), datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_usability_favorites(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_email TEXT NOT NULL,
    feature_key TEXT NOT NULL,
    feature_label TEXT,
    feature_group TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_email, feature_key)
  )`).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_usability_recent_actions(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_email TEXT,
    action_key TEXT,
    action_label TEXT,
    detail_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();
  await addColumn(env.DB, "customers", "usability_last_opened_at TEXT");
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_usability_favorites_user ON crm_usability_favorites(user_email, feature_group)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_usability_recent_user ON crm_usability_recent_actions(user_email, created_at)`).run();
}
async function requireUser(request, env, roles = ROLES){
  await ensureSchema(env);
  const email = getEmail(request);
  if(!email) return { ok:false, response: json({ ok:false, message:"Login required" }, 401) };
  const user = await env.DB.prepare(`SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  if(!user) return { ok:false, response: json({ ok:false, message:"User is not allowed" }, 403) };
  if(roles.length && !roles.includes(user.role || "")) return { ok:false, response: json({ ok:false, message:"Permission denied" }, 403) };
  return { ok:true, email, user };
}

const FEATURE_GROUPS = [
  { group:"今日", items:[
    ["today", "今日やること", "今日対応・LINE未送信・予約要確認をまとめて見る", "#crmTodayDashboard"],
    ["quick_actions", "クイック操作", "LINE送信済み・フォロー完了・予約再同期", "#crmTodayFilterPanel"],
    ["delivery", "納品進捗", "納品遅延・本納品待ち・口コミ依頼待ち", "#crmDeliveryPanel"]
  ]},
  { group:"予約", items:[
    ["reservation_monitor", "予約連携監視", "CRMと予約管理の連携状態を見る", "button:予約連携監視"],
    ["reservation_alerts", "予約アラート", "止まっている予約連携を確認", "button:予約連携 要確認"],
    ["reservation_resync", "再同期", "予約管理へ再送・CRM履歴反映", "#crmReservationLinkMonitor"]
  ]},
  { group:"顧客", items:[
    ["customer_rank", "顧客ランク", "VIP・リピーター・休眠・要フォロー", "/api/customer-ranks"],
    ["repeat_candidates", "リピート候補", "次に提案できるお客様", "/api/repeat-candidates"],
    ["dormant", "休眠掘り起こし", "最終撮影から時間が空いたお客様", "/api/dormant-customers"]
  ]},
  { group:"LINE", items:[
    ["line_pending", "LINE未送信", "未送信文面を確認", "button:LINE未送信"],
    ["line_templates", "LINEテンプレ", "テンプレ作成・編集・プレビュー", "button:LINEテンプレ管理"],
    ["follow_to_line", "フォロー→LINE", "フォロー予定から文面作成", "button:フォロー→LINE"]
  ]},
  { group:"問い合わせ", items:[
    ["inquiry", "問い合わせ管理", "問い合わせ〜予約化のステータス管理", "button:問い合わせ管理"],
    ["inquiry_actions", "問い合わせ一覧アクション", "問い合わせ行からLINE・フォロー・予約下書き作成", "button:問い合わせ一覧アクション"],
    ["inquiry_next", "問い合わせ→次アクション", "問い合わせIDから次の処理へ進む", "button:問い合わせ→次アクション"]
  ]},
  { group:"マーケ", items:[
    ["marketing_db", "マーケDB", "候補リスト・スコア・一括LINE", "button:マーケDB"],
    ["campaign", "キャンペーン", "季節キャンペーンと配信リスト", "button:キャンペーン"],
    ["funnel", "成約・流入", "成約率・流入元別売上", "button:成約・流入"]
  ]},
  { group:"分析", items:[
    ["sales", "売上分析", "月別売上・平均単価・ジャンル別", "/api/sales-dashboard"],
    ["logs", "統合ログ", "操作履歴をまとめて確認", "button:統合ログ"],
    ["source", "流入元分析", "Meta広告・Instagram・紹介など", "/api/marketing-source-analysis"]
  ]}
];
const FEATURES = FEATURE_GROUPS.flatMap(g => g.items.map(i => ({ key:i[0], label:i[1], description:i[2], action:i[3], group:g.group })));

async function dashboardApi(request, env){
  const auth = await requireUser(request, env);
  if(!auth.ok) return auth.response;
  const favs = await safeAll(env, `SELECT feature_key FROM crm_usability_favorites WHERE user_email=?`, [auth.email]);
  const favSet = new Set(favs.map(f => f.feature_key));
  const recent = await safeAll(env, `SELECT action_key, action_label, detail_json, created_at FROM crm_usability_recent_actions WHERE user_email=? ORDER BY datetime(created_at) DESC LIMIT 10`, [auth.email]);
  return json({ ok:true, build:BUILD, groups: FEATURE_GROUPS.map(g => ({ group:g.group, items:g.items.map(i => ({ key:i[0], label:i[1], description:i[2], action:i[3], favorite:favSet.has(i[0]) })) })), recent });
}
async function favoriteApi(request, env){
  const auth = await requireUser(request, env, WRITE_ROLES);
  if(!auth.ok) return auth.response;
  const body = await readJson(request);
  const key = text(body.feature_key || body.key);
  const feature = FEATURES.find(f => f.key === key);
  if(!feature) return json({ ok:false, message:"feature not found" }, 404);
  if(body.remove){
    await env.DB.prepare(`DELETE FROM crm_usability_favorites WHERE user_email=? AND feature_key=?`).bind(auth.email, key).run();
    return json({ ok:true, removed:true, key });
  }
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_usability_favorites(user_email, feature_key, feature_label, feature_group, created_at) VALUES(?,?,?,?,CURRENT_TIMESTAMP)`).bind(auth.email, key, feature.label, feature.group).run();
  return json({ ok:true, key, favorite:true });
}
async function recordActionApi(request, env){
  const auth = await requireUser(request, env);
  if(!auth.ok) return auth.response;
  const body = await readJson(request);
  await env.DB.prepare(`INSERT INTO crm_usability_recent_actions(user_email, action_key, action_label, detail_json, created_at) VALUES(?,?,?,?,CURRENT_TIMESTAMP)`).bind(auth.email, text(body.action_key), text(body.action_label), JSON.stringify(body.detail || {})).run();
  return json({ ok:true });
}
async function globalSearchApi(request, env){
  const auth = await requireUser(request, env);
  if(!auth.ok) return auth.response;
  const url = new URL(request.url);
  const q = text(url.searchParams.get("q"));
  if(!q) return json({ ok:true, q, items:[] });
  const like = `%${q}%`;
  const customers = await safeAll(env, `SELECT 'customer' AS type, id, COALESCE(name, customer_name, '') AS title, COALESCE(phone,'') AS sub, COALESCE(customer_rank,'') AS badge FROM customers WHERE COALESCE(deleted_at,'')='' AND (name LIKE ? OR phone LIKE ? OR email LIKE ? OR kana LIKE ?) LIMIT 20`, [like, like, like, like]);
  const inquiries = await safeAll(env, `SELECT 'inquiry' AS type, id, customer_id, COALESCE(customer_name,'') AS title, COALESCE(status,'') AS badge, COALESCE(memo, inquiry_text, '') AS sub FROM crm_inquiry_pipeline WHERE COALESCE(deleted_at,'')='' AND (customer_name LIKE ? OR memo LIKE ? OR inquiry_text LIKE ? OR genre LIKE ?) LIMIT 20`, [like, like, like, like]);
  const drafts = await safeAll(env, `SELECT 'reservation_draft' AS type, id, customer_id, COALESCE(customer_name,'') AS title, COALESCE(status,'') AS badge, COALESCE(genre,'') || ' ' || COALESCE(shoot_date,'') AS sub FROM crm_reservation_drafts WHERE COALESCE(deleted_at,'')='' AND (customer_name LIKE ? OR genre LIKE ? OR shoot_date LIKE ? OR reservation_app_reservation_id LIKE ?) LIMIT 20`, [like, like, like, like]);
  const templates = await safeAll(env, `SELECT 'template' AS type, id, name AS title, category AS badge, body AS sub FROM crm_line_templates WHERE COALESCE(deleted_at,'')='' AND (name LIKE ? OR body LIKE ? OR category LIKE ?) LIMIT 20`, [like, like, like]);
  return json({ ok:true, q, items:[...customers, ...inquiries, ...drafts, ...templates].slice(0, 60) });
}
async function guideApi(request, env){
  const auth = await requireUser(request, env);
  if(!auth.ok) return auth.response;
  return json({ ok:true, build:BUILD, guide:[
    { title:"朝に見る", steps:["今日やること", "納品進捗", "予約アラート", "LINE未送信"] },
    { title:"問い合わせ対応", steps:["問い合わせ管理", "問い合わせ一覧アクション", "LINE作成", "フォロー作成", "予約下書き作成"] },
    { title:"リピート施策", steps:["マーケDB", "リピート候補", "LINE一括保存", "送信済み管理"] },
    { title:"納品管理", steps:["納品進捗", "次の工程へ", "口コミ依頼", "完了"] },
    { title:"困った時", steps:["全体検索", "予約連携監視", "再同期", "統合ログ"] }
  ]});
}

function injectUsabilityHub(html){
  if(!html || html.includes("crmUsabilityHubScript")) return html;
  const style = `<style id="crmUsabilityHubStyle">
#crmUxFab{position:fixed;right:16px;bottom:16px;z-index:1000020;background:#111827;color:#fff;border:0;border-radius:999px;padding:12px 16px;font-weight:950;box-shadow:0 18px 45px rgba(0,0,0,.25);cursor:pointer}#crmUxOverlay{position:fixed;inset:0;background:rgba(15,23,42,.48);z-index:1000021;display:none;align-items:center;justify-content:center;padding:14px}#crmUxModal{width:min(1120px,100%);max-height:92vh;overflow:auto;background:#fff;border-radius:22px;padding:16px;box-shadow:0 30px 90px rgba(0,0,0,.25);font-family:inherit;color:#0f172a}.crmUxHead{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;flex-wrap:wrap}.crmUxTitle{font-size:22px;font-weight:950;margin:0}.crmUxSub{font-size:12px;color:#64748b;margin:4px 0 0}.crmUxClose{border:0;background:#e5e7eb;border-radius:12px;padding:8px 10px;font-weight:900;cursor:pointer}.crmUxSearch{display:flex;gap:8px;margin:12px 0}.crmUxSearch input{flex:1;border:1px solid #cbd5e1;border-radius:12px;padding:10px;font-size:14px}.crmUxSearch button,.crmUxCard button,.crmUxMiniBtn{border:0;background:#028760;color:#fff;border-radius:10px;padding:8px 10px;font-size:12px;font-weight:900;cursor:pointer}.crmUxTabs{display:flex;gap:6px;flex-wrap:wrap;margin:8px 0 12px}.crmUxTabs button{border:1px solid #d1d5db;background:#f8fafc;border-radius:999px;padding:7px 10px;font-size:12px;font-weight:900;cursor:pointer}.crmUxTabs button.active{background:#111827;color:#fff}.crmUxGrid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.crmUxCard{border:1px solid #e5e7eb;background:#fff;border-radius:16px;padding:12px}.crmUxCard b{display:block;font-size:15px}.crmUxCard small{display:block;color:#64748b;line-height:1.45;margin:5px 0 9px}.crmUxCard .fav{background:#f59e0b;margin-left:6px}.crmUxSectionTitle{font-size:14px;font-weight:950;margin:16px 0 8px}.crmUxResults{display:grid;gap:8px}.crmUxResult{border:1px solid #e5e7eb;border-radius:14px;padding:10px;background:#f8fafc}.crmUxResult b{font-size:13px}.crmUxResult small{display:block;color:#64748b;margin-top:3px}.crmUxBadge{display:inline-block;background:#e0f2fe;color:#075985;border-radius:999px;padding:2px 7px;font-size:11px;font-weight:900;margin-left:6px}.crmUxGuide{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}.crmUxGuide div{background:#f0fdf4;border:1px solid #bbf7d0;border-radius:14px;padding:10px;font-size:12px}.crmUxGuide b{display:block;margin-bottom:4px}@media(max-width:860px){#crmUxFab{right:12px;bottom:12px}.crmUxGrid{grid-template-columns:1fr}.crmUxGuide{grid-template-columns:1fr}#crmUxModal{padding:12px;border-radius:18px}}
</style>`;
  const script = `<script id="crmUsabilityHubScript">
(function(){
 if(window.__crmUsabilityHubInstalled)return; window.__crmUsabilityHubInstalled=true;
 var state={groups:[],active:'お気に入り',recent:[]};
 function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
 function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
 function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:18px;bottom:76px;z-index:1000025;background:#111827;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.22)';document.body.appendChild(d);setTimeout(function(){d.remove()},2400)}
 function initDom(){if(document.getElementById('crmUxFab'))return;document.body.insertAdjacentHTML('beforeend','<button id="crmUxFab">メニュー</button><div id="crmUxOverlay"><div id="crmUxModal"><div class="crmUxHead"><div><h2 class="crmUxTitle">CRMかんたんメニュー</h2><p class="crmUxSub">増えた機能を、目的別にまとめました。検索もできます。</p></div><button class="crmUxClose" id="crmUxClose">閉じる</button></div><div class="crmUxSearch"><input id="crmUxQ" placeholder="顧客名・電話・問い合わせ・予約ID・テンプレ名で検索"><button id="crmUxSearchBtn">検索</button></div><div id="crmUxTabs" class="crmUxTabs"></div><div id="crmUxContent"></div><div class="crmUxSectionTitle">使い方ガイド</div><div id="crmUxGuide" class="crmUxGuide"></div></div></div>');}
 function action(feature){api('/api/usability-hub/recent',{method:'POST',body:JSON.stringify({action_key:feature.key,action_label:feature.label,detail:{action:feature.action}})}).catch(function(){}); if(feature.action.indexOf('button:')===0){var label=feature.action.slice(7);var btn=[].slice.call(document.querySelectorAll('button')).find(function(b){return (b.textContent||'').indexOf(label)>=0}); if(btn){btn.click();return;} toast('画面内に「'+label+'」ボタンが見つかりません');return;} if(feature.action.indexOf('#')===0){var el=document.querySelector(feature.action); if(el){el.scrollIntoView({behavior:'smooth',block:'start'});document.getElementById('crmUxOverlay').style.display='none';return;} toast('対象パネルがまだ読み込まれていません');return;} window.open(feature.action,'_blank');}
 function fav(feature, remove){api('/api/usability-hub/favorites',{method:'POST',body:JSON.stringify({feature_key:feature.key,remove:!!remove})}).then(function(d){toast(d.ok?'更新しました':'失敗しました');load();});}
 function renderTabs(){var tabs=['お気に入り'].concat(state.groups.map(function(g){return g.group;}));document.getElementById('crmUxTabs').innerHTML=tabs.map(function(t){return '<button class="'+(state.active===t?'active':'')+'" data-tab="'+esc(t)+'">'+esc(t)+'</button>'}).join('');}
 function renderContent(){var groups=state.groups;if(state.active==='お気に入り'){var favs=[];groups.forEach(function(g){(g.items||[]).forEach(function(i){if(i.favorite)favs.push(i);});});document.getElementById('crmUxContent').innerHTML='<div class="crmUxGrid">'+(favs.length?favs.map(card).join(''):'<div class="crmUxResult">まだお気に入りがありません。各機能の★で追加できます。</div>')+'</div>';return;}var g=groups.find(function(x){return x.group===state.active;})||groups[0]||{items:[]};document.getElementById('crmUxContent').innerHTML='<div class="crmUxGrid">'+(g.items||[]).map(card).join('')+'</div>';}
 function card(f){return '<div class="crmUxCard"><b>'+esc(f.label)+'</b><small>'+esc(f.description)+'</small><button data-open="'+esc(f.key)+'">開く</button><button class="fav" data-fav="'+esc(f.key)+'">'+(f.favorite?'★解除':'★追加')+'</button></div>';}
 function renderGuide(g){document.getElementById('crmUxGuide').innerHTML=(g||[]).map(function(x){return '<div><b>'+esc(x.title)+'</b>'+esc((x.steps||[]).join(' → '))+'</div>';}).join('');}
 function findFeature(key){var found=null;state.groups.forEach(function(g){(g.items||[]).forEach(function(i){if(i.key===key)found=i;});});return found;}
 function search(){var q=document.getElementById('crmUxQ').value.trim();if(!q){renderContent();return;}api('/api/usability-hub/search?q='+encodeURIComponent(q)).then(function(d){var rows=(d.items||[]);document.getElementById('crmUxContent').innerHTML='<div class="crmUxResults">'+(rows.length?rows.map(function(r){return '<div class="crmUxResult"><b>'+esc(r.title||'-')+'</b><span class="crmUxBadge">'+esc(r.type||'')+'</span><span class="crmUxBadge">'+esc(r.badge||'')+'</span><small>'+esc(r.sub||'')+'</small></div>';}).join(''):'<div class="crmUxResult">該当なし</div>')+'</div>';});}
 function load(){initDom();api('/api/usability-hub').then(function(d){if(!d.ok){toast('メニュー取得に失敗');return;}state.groups=d.groups||[];state.recent=d.recent||[];renderTabs();renderContent();api('/api/usability-hub/guide').then(function(g){renderGuide(g.guide||[]);});});}
 document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmUxFab'){document.getElementById('crmUxOverlay').style.display='flex';load();}if(t.id==='crmUxClose'||t.id==='crmUxOverlay'){if(t.id==='crmUxOverlay'||t.id==='crmUxClose')document.getElementById('crmUxOverlay').style.display='none';}if(t.id==='crmUxSearchBtn')search();var tab=t.getAttribute&&t.getAttribute('data-tab');if(tab){state.active=tab;renderTabs();renderContent();}var open=t.getAttribute&&t.getAttribute('data-open');if(open){var f=findFeature(open);if(f)action(f);}var fv=t.getAttribute&&t.getAttribute('data-fav');if(fv){var ff=findFeature(fv);if(ff)fav(ff,ff.favorite);}});
 document.addEventListener('keydown',function(e){if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){e.preventDefault();document.getElementById('crmUxOverlay').style.display='flex';setTimeout(function(){document.getElementById('crmUxQ').focus();},50);}});
 document.addEventListener('DOMContentLoaded',function(){initDom();});setTimeout(function(){initDom();},900);
})();
</script>`;
  return html.replace("</head>", `${style}</head>`).replace("</body>", `${script}</body>`);
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    const path = url.pathname;
    if((path === "/" || path === "/health" || path === "/api/health") && request.method === "GET") return json({ ok:true, service:"customer-crm-api", build:BUILD, time:nowIso() });
    if(path === "/api/usability-hub" && request.method === "GET") return dashboardApi(request, env);
    if(path === "/api/usability-hub/favorites" && request.method === "POST") return favoriteApi(request, env);
    if(path === "/api/usability-hub/recent" && request.method === "POST") return recordActionApi(request, env);
    if(path === "/api/usability-hub/search" && request.method === "GET") return globalSearchApi(request, env);
    if(path === "/api/usability-hub/guide" && request.method === "GET") return guideApi(request, env);

    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if(type.includes("text/html")){
      const body = injectUsabilityHub(await res.text());
      return new Response(body, { status:res.status, statusText:res.statusText, headers:securityHeaders(res.headers) });
    }
    return new Response(res.body, { status:res.status, statusText:res.statusText, headers:securityHeaders(res.headers) });
  }
};
