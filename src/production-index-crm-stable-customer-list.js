// ======================================================
// CUSTOMER CRM / STABLE CUSTOMER LIST WRAPPER
// build: customer-crm-stable-customer-list-20260614-02
// - Provides customer list panel and API
// - Does NOT create floating customer list buttons
// - Bottom navigation or other menu can open the panel via window.__crmOpenStableCustomerList()
// - Customer cards show name only for a cleaner UI
// ======================================================

import app from "./production-index-crm-number-format-fix.js";

const BUILD = "customer-crm-stable-customer-list-20260614-02";

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
    let stmt = env.DB.prepare(sql);
    if(params.length) stmt = stmt.bind(...params);
    return await stmt.all();
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
    customers = customers.filter(c => [c.name,c.sort_name,c.line_display_name,c.phone,c.email,c.genre_history,c.customer_id].some(v => String(v || "").toLowerCase().includes(nq)));
  }
  return json({ok:true, build:BUILD, count:customers.length, customers});
}

function injectStableCustomerList(html){
  if(!html || html.includes("crm-stable-customer-list-script")) return html;

  const style = `<style id="crm-stable-customer-list-style">
.crm-stable-customer-btn,#crmStableCustomerBtn,#crmStableCustomerMenuBtn{display:none!important}.crm-stable-customer-panel{position:fixed!important;right:18px!important;top:18px!important;bottom:18px!important;width:min(760px,calc(100vw - 36px))!important;background:#fff!important;border:1px solid #dbe5ef!important;border-radius:24px!important;box-shadow:0 28px 70px rgba(15,23,42,.24)!important;z-index:2147482900!important;display:none!important;overflow:hidden!important;color:#07111f!important}.crm-stable-customer-panel.open{display:flex!important;flex-direction:column!important}.crm-stable-customer-head{padding:18px!important;border-bottom:1px solid #e2e8f0!important;background:linear-gradient(135deg,#f0fdf4,#fff)!important}.crm-stable-customer-head h2{font-size:24px!important;line-height:1.25!important;margin:0!important;font-weight:950!important}.crm-stable-customer-head p,.crm-stable-customer-status,.crm-stable-customer-sub,.crm-stable-customer-meta,.crm-stable-customer-pill{display:none!important}.crm-stable-customer-close{position:absolute!important;right:14px!important;top:14px!important;width:42px!important;height:42px!important;border-radius:999px!important;border:1px solid #dbe5ef!important;background:#fff!important;font-size:22px!important;font-weight:950!important;cursor:pointer!important}.crm-stable-customer-search{padding:14px 18px!important;border-bottom:1px solid #e2e8f0!important;display:flex!important;gap:10px!important}.crm-stable-customer-search input{flex:1!important;min-height:46px!important;border:1px solid #cbd5e1!important;border-radius:14px!important;padding:0 14px!important;font-size:16px!important}.crm-stable-customer-search button{min-height:46px!important;border:0!important;border-radius:14px!important;background:#07111f!important;color:#fff!important;font-weight:950!important;padding:0 16px!important}.crm-stable-customer-list{padding:12px 18px 18px!important;overflow:auto!important;display:grid!important;gap:8px!important}.crm-stable-customer-card{border:1px solid #e2e8f0!important;border-radius:18px!important;padding:15px 18px!important;background:#fff!important;box-shadow:0 8px 20px rgba(15,23,42,.04)!important;cursor:pointer!important;min-height:58px!important;display:flex!important;align-items:center!important;justify-content:space-between!important}.crm-stable-customer-card:hover{border-color:#028760!important;background:#f8fffb!important}.crm-stable-customer-card::after{content:'›';font-size:28px;font-weight:800;color:#94a3b8;line-height:1}.crm-stable-customer-name{font-size:18px!important;font-weight:950!important;margin:0!important;line-height:1.35!important;color:#07111f!important}.crm-stable-customer-empty{border:1px dashed #cbd5e1!important;border-radius:18px!important;padding:18px!important;color:#64748b!important;background:#f8fafc!important}@media(max-width:767px){.crm-stable-customer-panel{inset:8px!important;width:auto!important;border-radius:20px!important}.crm-stable-customer-head{padding:16px 62px 14px 16px!important}.crm-stable-customer-head h2{font-size:24px!important}.crm-stable-customer-search{padding:12px!important;display:grid!important;grid-template-columns:1fr!important}.crm-stable-customer-search button{width:100%!important}.crm-stable-customer-list{padding:12px 12px calc(24px + env(safe-area-inset-bottom))!important}.crm-stable-customer-card{min-height:56px!important;padding:14px 16px!important}.crm-stable-customer-name{font-size:17px!important}}
</style>`;

  const script = `<script id="crm-stable-customer-list-script">
(()=>{
  if(window.__crmStableCustomerList) return;
  window.__crmStableCustomerList = 1;

  let customers = [];
  let loaded = false;

  function text(v){ return String(v == null ? '' : v); }
  function closePanel(){ document.getElementById('crmStableCustomerPanel')?.classList.remove('open'); }
  function openPanel(){ ensurePanel(); document.getElementById('crmStableCustomerPanel')?.classList.add('open'); if(!loaded) loadCustomers(); }
  window.__crmOpenStableCustomerList = openPanel;

  function openDetail(c){
    const id = text(c.customer_id || c.id);
    closePanel();
    if(typeof window.__crmOpenCustomerDetailV2 === 'function'){
      window.__crmOpenCustomerDetailV2(id);
      return;
    }
    window.dispatchEvent(new CustomEvent('crm-open-customer-detail', {detail:{id, customer:c}}));
  }

  function render(list){
    const box = document.getElementById('crmStableCustomerList');
    if(!box) return;
    if(!list.length){
      box.innerHTML = '<div class="crm-stable-customer-empty">顧客リストに表示できる顧客がありません。</div>';
      return;
    }
    box.innerHTML = list.map(c => '<div class="crm-stable-customer-card" data-customer-id="'+text(c.customer_id || c.id).replace(/"/g,'&quot;')+'"><div class="crm-stable-customer-name">'+text(c.name || '名前未設定')+'</div></div>').join('');
    box.querySelectorAll('.crm-stable-customer-card').forEach((el, idx) => el.addEventListener('click', () => openDetail(list[idx])));
  }

  async function loadCustomers(){
    const box = document.getElementById('crmStableCustomerList');
    if(box) box.innerHTML = '<div class="crm-stable-customer-empty">読み込み中...</div>';
    try{
      const r = await fetch('/api/stable-customers?limit=1000', {cache:'no-store'});
      const j = await r.json();
      customers = Array.isArray(j.customers) ? j.customers : [];
      loaded = true;
      render(customers);
    }catch(e){
      customers = [];
      loaded = true;
      if(box) box.innerHTML = '<div class="crm-stable-customer-empty">顧客リストAPIを読み込めませんでした。</div>';
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
    panel.innerHTML = '<button class="crm-stable-customer-close" type="button">×</button><div class="crm-stable-customer-head"><h2>顧客リスト</h2></div><div class="crm-stable-customer-search"><input id="crmStableCustomerSearch" placeholder="名前で検索"><button id="crmStableCustomerReload" type="button">更新</button></div><div id="crmStableCustomerList" class="crm-stable-customer-list"></div>';
    panel.querySelector('.crm-stable-customer-close').onclick = closePanel;
    panel.querySelector('#crmStableCustomerReload').onclick = loadCustomers;
    panel.querySelector('#crmStableCustomerSearch').addEventListener('input', filter);
    document.body.appendChild(panel);
  }

  function removeOldButtons(){
    document.querySelectorAll('#crmStableCustomerBtn,#crmStableCustomerMenuBtn,.crm-stable-customer-btn').forEach(el => el.remove());
  }

  function boot(){ ensurePanel(); removeOldButtons(); }
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
  new MutationObserver(() => setTimeout(removeOldButtons, 120)).observe(document.documentElement, {childList:true, subtree:true});
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
