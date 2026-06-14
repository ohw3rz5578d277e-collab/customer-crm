// ======================================================
// CUSTOMER CRM / STABLE CUSTOMER LIST WRAPPER
// build: customer-crm-stable-customer-list-20260614-01
// - Adds guaranteed customer list panel independent of old UI
// - Sorts Japanese names first by Japanese collation
// - Sorts English names after Japanese names by A-Z
// - Adds search and quick open controls for mobile/PC
// ======================================================

import app from "./production-index-crm-number-format-fix.js";

const BUILD = "customer-crm-stable-customer-list-20260614-01";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store, no-cache, must-revalidate, max-age=0"
    }
  });
}

async function safeAll(env, sql, params = []){
  if(!env || !env.DB) return {ok:false, results:[], error:"DB binding missing"};
  try{
    const stmt = env.DB.prepare(sql);
    return params.length ? await stmt.bind(...params).all() : await stmt.all();
  }catch(e){
    return {ok:false, results:[], error:String(e && e.message || e)};
  }
}

function getCustomerName(row){
  return String(row.name || row.customer_name || row.line_display_name || row.display_name || row.full_name || row.id || "").trim();
}

function getSortName(row){
  return String(row.furigana || row.kana || row.name_kana || row.yomi || getCustomerName(row)).trim();
}

function isEnglishName(name){
  const s = String(name || "").trim();
  if(!s) return false;
  return /^[A-Za-z0-9 '\-_.]+$/.test(s);
}

function normalizeCustomer(row){
  const name = getCustomerName(row);
  const sortName = getSortName(row);
  const group = isEnglishName(name || sortName) ? 2 : 1;
  return {
    id: row.id || row.customer_id || row.line_user_id || "",
    customer_id: row.customer_id || row.id || "",
    name,
    sort_name: sortName,
    sort_group: group,
    line_display_name: row.line_display_name || "",
    phone: row.phone || row.tel || "",
    email: row.email || "",
    last_shoot_date: row.last_shoot_date || row.last_reservation_date || "",
    genre_history: row.genre_history || row.last_genre || row.genre || "",
    repeat_count: Number(row.repeat_count || row.reservation_count || 0) || 0,
    total_revenue: Number(row.total_revenue || row.sales_total || 0) || 0,
    customer_rank: row.customer_rank || row.rank || "",
    raw: row
  };
}

function sortCustomers(rows){
  const ja = new Intl.Collator("ja-JP", {usage:"sort", sensitivity:"base", numeric:true});
  const en = new Intl.Collator("en-US", {usage:"sort", sensitivity:"base", numeric:true});
  return rows.map(normalizeCustomer).sort((a, b) => {
    if(a.sort_group !== b.sort_group) return a.sort_group - b.sort_group;
    if(a.sort_group === 2) return en.compare(a.sort_name || a.name, b.sort_name || b.name);
    return ja.compare(a.sort_name || a.name, b.sort_name || b.name);
  });
}

async function customerList(env, url){
  const q = (url.searchParams.get("q") || "").trim();
  const limit = Math.min(Math.max(Number(url.searchParams.get("limit") || 300), 1), 1000);
  const tableHit = await safeAll(env, `SELECT name FROM sqlite_master WHERE type='table' AND name='customers' LIMIT 1`);
  if(!tableHit.results || !tableHit.results.length){
    return json({ok:true, build:BUILD, count:0, customers:[], message:"customers table not found"});
  }

  const res = await safeAll(env, `SELECT * FROM customers LIMIT ${limit}`);
  let customers = sortCustomers(res.results || []);
  if(q){
    const nq = q.toLowerCase();
    customers = customers.filter(c => [c.name, c.sort_name, c.line_display_name, c.phone, c.email, c.genre_history, c.customer_id].some(v => String(v || "").toLowerCase().includes(nq)));
  }
  return json({ok:true, build:BUILD, count:customers.length, customers});
}

function injectStableCustomerList(html){
  if(!html || html.includes("crm-stable-customer-list-script")) return html;

  const style = `<style id="crm-stable-customer-list-style">
.crm-stable-customer-btn{position:fixed!important;right:18px!important;bottom:64px!important;z-index:2147482800!important;min-height:48px!important;padding:0 18px!important;border:0!important;border-radius:999px!important;background:#028760!important;color:#fff!important;font-weight:950!important;box-shadow:0 18px 38px rgba(2,135,96,.30)!important;cursor:pointer!important}.crm-stable-customer-panel{position:fixed!important;right:18px!important;top:18px!important;bottom:18px!important;width:min(760px,calc(100vw - 36px))!important;background:#fff!important;border:1px solid #dbe5ef!important;border-radius:24px!important;box-shadow:0 28px 70px rgba(15,23,42,.24)!important;z-index:2147482900!important;display:none!important;overflow:hidden!important;color:#07111f!important}.crm-stable-customer-panel.open{display:flex!important;flex-direction:column!important}.crm-stable-customer-head{padding:18px!important;border-bottom:1px solid #e2e8f0!important;background:linear-gradient(135deg,#f0fdf4,#fff)!important}.crm-stable-customer-head h2{font-size:24px!important;line-height:1.25!important;margin:0 0 6px!important;font-weight:950!important}.crm-stable-customer-head p{font-size:13px!important;color:#64748b!important;line-height:1.7!important;margin:0!important}.crm-stable-customer-close{position:absolute!important;right:14px!important;top:14px!important;width:42px!important;height:42px!important;border-radius:999px!important;border:1px solid #dbe5ef!important;background:#fff!important;font-size:22px!important;font-weight:950!important;cursor:pointer!important}.crm-stable-customer-search{padding:14px 18px!important;border-bottom:1px solid #e2e8f0!important;display:flex!important;gap:10px!important}.crm-stable-customer-search input{flex:1!important;min-height:46px!important;border:1px solid #cbd5e1!important;border-radius:14px!important;padding:0 14px!important;font-size:16px!important}.crm-stable-customer-search button{min-height:46px!important;border:0!important;border-radius:14px!important;background:#07111f!important;color:#fff!important;font-weight:950!important;padding:0 16px!important}.crm-stable-customer-status{padding:10px 18px!important;color:#64748b!important;font-weight:800!important;font-size:13px!important}.crm-stable-customer-list{padding:0 18px 18px!important;overflow:auto!important;display:grid!important;gap:10px!important}.crm-stable-customer-card{border:1px solid #e2e8f0!important;border-radius:18px!important;padding:14px!important;background:#fff!important;box-shadow:0 8px 20px rgba(15,23,42,.04)!important;cursor:pointer!important}.crm-stable-customer-card:hover{border-color:#028760!important;background:#f8fffb!important}.crm-stable-customer-name{font-size:18px!important;font-weight:950!important;margin:0 0 4px!important}.crm-stable-customer-sub{font-size:13px!important;color:#64748b!important;line-height:1.6!important}.crm-stable-customer-meta{display:grid!important;grid-template-columns:repeat(3,1fr)!important;gap:8px!important;margin-top:10px!important}.crm-stable-customer-pill{border:1px solid #e2e8f0!important;border-radius:12px!important;padding:8px!important;background:#f8fafc!important;font-size:12px!important;color:#475569!important}.crm-stable-customer-pill b{display:block!important;color:#07111f!important;font-size:14px!important}.crm-stable-customer-empty{border:1px dashed #cbd5e1!important;border-radius:18px!important;padding:18px!important;color:#64748b!important;background:#f8fafc!important}@media(max-width:767px){.crm-stable-customer-btn{right:12px!important;bottom:calc(82px + env(safe-area-inset-bottom))!important;min-height:50px!important}.crm-stable-customer-panel{inset:8px!important;width:auto!important;border-radius:20px!important}.crm-stable-customer-head{padding:16px 62px 14px 16px!important}.crm-stable-customer-head h2{font-size:22px!important}.crm-stable-customer-search{padding:12px!important;display:grid!important;grid-template-columns:1fr auto!important}.crm-stable-customer-list{padding:0 12px calc(24px + env(safe-area-inset-bottom))!important}.crm-stable-customer-meta{grid-template-columns:1fr 1fr!important}.crm-stable-customer-card{padding:13px!important}.crm-stable-customer-name{font-size:17px!important}}
</style>`;

  const script = `<script id="crm-stable-customer-list-script">
(()=>{
  if(window.__crmStableCustomerList) return;
  window.__crmStableCustomerList = 1;

  let customers = [];
  let loaded = false;

  function yen(n){ return '¥' + Math.round(Number(n || 0)).toLocaleString('ja-JP'); }
  function text(v){ return String(v == null ? '' : v); }

  function closePanel(){ document.getElementById('crmStableCustomerPanel')?.classList.remove('open'); }
  function openPanel(){ ensurePanel(); document.getElementById('crmStableCustomerPanel')?.classList.add('open'); if(!loaded) loadCustomers(); }

  function tryOpenExistingDetail(c){
    closePanel();
    const id = text(c.customer_id || c.id);
    const name = text(c.name);
    const candidates = Array.from(document.querySelectorAll('button,a,tr,div,li')).filter(el => {
      const t = el.textContent || '';
      return (id && t.includes(id)) || (name && t.includes(name));
    });
    const hit = candidates.find(el => /LINE:|1回|詳細|20\d{2}/.test(el.textContent || '')) || candidates[0];
    if(hit){ hit.scrollIntoView({behavior:'smooth', block:'center'}); setTimeout(()=>{ try{ hit.click(); }catch(e){} }, 250); }
  }

  function render(list){
    const box = document.getElementById('crmStableCustomerList');
    const status = document.getElementById('crmStableCustomerStatus');
    if(!box || !status) return;
    status.textContent = '表示中：' + list.length + '件 / 日本語は五十音順、英語は後ろにA〜Z順';
    if(!list.length){
      box.innerHTML = '<div class="crm-stable-customer-empty">顧客リストに表示できる顧客がありません。CSV取込または予約管理側の同期状態を確認してください。</div>';
      return;
    }
    box.innerHTML = list.map(c => {
      const title = text(c.name || '名前未設定');
      const sub = [c.line_display_name ? 'LINE: ' + c.line_display_name : '', c.genre_history ? 'ジャンル: ' + c.genre_history : '', c.last_shoot_date ? '直近: ' + c.last_shoot_date : ''].filter(Boolean).join(' / ');
      return '<div class="crm-stable-customer-card" data-customer-id="'+text(c.customer_id || c.id).replace(/"/g,'&quot;')+'"><div class="crm-stable-customer-name">'+title+'</div><div class="crm-stable-customer-sub">'+(sub || '詳細情報は未登録です')+'</div><div class="crm-stable-customer-meta"><div class="crm-stable-customer-pill"><b>'+Number(c.repeat_count || 0)+'</b>撮影回数</div><div class="crm-stable-customer-pill"><b>'+yen(c.total_revenue)+'</b>累計売上</div><div class="crm-stable-customer-pill"><b>'+(c.customer_rank || '未設定')+'</b>ランク</div></div></div>';
    }).join('');
    box.querySelectorAll('.crm-stable-customer-card').forEach((el, idx) => el.addEventListener('click', () => tryOpenExistingDetail(list[idx])));
  }

  async function loadCustomers(){
    const status = document.getElementById('crmStableCustomerStatus');
    if(status) status.textContent = '読み込み中...';
    try{
      const r = await fetch('/api/stable-customers?limit=1000', {cache:'no-store'});
      const j = await r.json();
      customers = Array.isArray(j.customers) ? j.customers : [];
      loaded = true;
      render(customers);
    }catch(e){
      customers = [];
      loaded = true;
      if(status) status.textContent = '読み込みに失敗しました';
      const box = document.getElementById('crmStableCustomerList');
      if(box) box.innerHTML = '<div class="crm-stable-customer-empty">顧客リストAPIを読み込めませんでした。状態確認を実行してください。</div>';
    }
  }

  function filter(){
    const q = (document.getElementById('crmStableCustomerSearch')?.value || '').toLowerCase();
    const list = !q ? customers : customers.filter(c => [c.name,c.sort_name,c.line_display_name,c.phone,c.email,c.genre_history,c.customer_id].some(v => text(v).toLowerCase().includes(q)));
    render(list);
  }

  function ensurePanel(){
    if(document.getElementById('crmStableCustomerPanel')) return;
    const panel = document.createElement('div');
    panel.id = 'crmStableCustomerPanel';
    panel.className = 'crm-stable-customer-panel';
    panel.innerHTML = '<button class="crm-stable-customer-close" type="button">×</button><div class="crm-stable-customer-head"><h2>顧客リスト</h2><p>日本語は五十音順、英語名は後ろにA〜Z順で表示します。クリックすると元の詳細表示へ移動します。</p></div><div class="crm-stable-customer-search"><input id="crmStableCustomerSearch" placeholder="名前・LINE名・電話・ジャンルで検索"><button id="crmStableCustomerReload" type="button">更新</button></div><div id="crmStableCustomerStatus" class="crm-stable-customer-status">未読み込み</div><div id="crmStableCustomerList" class="crm-stable-customer-list"></div>';
    panel.querySelector('.crm-stable-customer-close').onclick = closePanel;
    panel.querySelector('#crmStableCustomerReload').onclick = loadCustomers;
    panel.querySelector('#crmStableCustomerSearch').addEventListener('input', filter);
    document.body.appendChild(panel);
  }

  function ensureButton(){
    if(document.getElementById('crmStableCustomerBtn')) return;
    const btn = document.createElement('button');
    btn.id = 'crmStableCustomerBtn';
    btn.className = 'crm-stable-customer-btn';
    btn.type = 'button';
    btn.textContent = '顧客リスト';
    btn.onclick = openPanel;
    document.body.appendChild(btn);
  }

  function addMenuEntry(){
    const existing = document.getElementById('crmStableCustomerMenuBtn');
    if(existing) return;
    const menuCandidates = Array.from(document.querySelectorAll('button,a')).filter(el => /CRMメニュー|メニュー/.test((el.textContent || '').trim()));
    const menuBtn = menuCandidates[menuCandidates.length - 1];
    const host = menuBtn && menuBtn.parentElement ? menuBtn.parentElement : document.body;
    const btn = document.createElement('button');
    btn.id = 'crmStableCustomerMenuBtn';
    btn.type = 'button';
    btn.textContent = '顧客リスト';
    btn.style.cssText = 'min-height:44px;border:0;border-radius:999px;padding:0 16px;font-weight:950;background:#028760;color:#fff;cursor:pointer;margin:4px;';
    btn.onclick = openPanel;
    host.insertBefore(btn, host.firstChild);
  }

  function boot(){ ensurePanel(); ensureButton(); addMenuEntry(); }
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
  new MutationObserver(() => setTimeout(addMenuEntry, 200)).observe(document.documentElement, {childList:true, subtree:true});
  document.addEventListener('keydown', e => { if(e.key === 'Escape') closePanel(); });
})();
</script>`;

  return html.includes("</head>") ? html.replace("</head>", style + "</head>").replace("</body>", script + "</body>") : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    if(request.method === "GET" && url.pathname === "/api/stable-customers") return customerList(env, url);

    const res = await app.fetch(request, env, ctx);
    const ct = res.headers.get("content-type") || "";
    if(request.method === "GET" && ct.includes("text/html")){
      const html = await res.text();
      return new Response(injectStableCustomerList(html), {status:res.status, headers:res.headers});
    }
    return res;
  }
};
