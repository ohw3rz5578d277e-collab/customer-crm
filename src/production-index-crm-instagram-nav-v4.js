// ======================================================
// CUSTOMER CRM / INSTAGRAM-LIKE MOBILE NAV V4 WRAPPER
// build: customer-crm-instagram-nav-v4-20260614-01
// - Replaces messy mobile bottom controls with 5 icon tabs
// - Home / Search / LINE / Customers / Settings
// - Settings contains logout and user management inherited from previous wrappers
// - Hides duplicate floating buttons and old bottom bars on mobile
// ======================================================

import app from "./production-index-crm-clean-mobile-ux-v3.js";

const BUILD = "customer-crm-instagram-nav-v4-20260614-01";

function injectInstagramNav(html){
  if(!html || html.includes("crm-instagram-nav-v4-script")) return html;

  const style = `<style id="crm-instagram-nav-v4-style">
#crmInstaNav{
  position:fixed!important;
  left:10px!important;
  right:10px!important;
  bottom:calc(10px + env(safe-area-inset-bottom))!important;
  height:74px!important;
  z-index:2147483800!important;
  display:flex!important;
  align-items:center!important;
  justify-content:space-around!important;
  gap:4px!important;
  padding:8px 8px!important;
  border:1px solid rgba(226,232,240,.96)!important;
  border-radius:28px!important;
  background:rgba(255,255,255,.94)!important;
  box-shadow:0 18px 50px rgba(15,23,42,.18)!important;
  backdrop-filter:blur(18px)!important;
  -webkit-backdrop-filter:blur(18px)!important;
}
.crm-insta-tab{
  flex:1!important;
  height:58px!important;
  border:0!important;
  border-radius:22px!important;
  background:transparent!important;
  color:#0f172a!important;
  font-weight:900!important;
  font-size:11px!important;
  display:flex!important;
  flex-direction:column!important;
  align-items:center!important;
  justify-content:center!important;
  gap:2px!important;
  cursor:pointer!important;
  line-height:1.1!important;
  padding:0!important;
}
.crm-insta-tab .crm-insta-icon{
  font-size:22px!important;
  line-height:1!important;
}
.crm-insta-tab.active{
  background:#028760!important;
  color:#fff!important;
  box-shadow:0 10px 26px rgba(2,135,96,.28)!important;
}
.crm-insta-hidden,
.crm-legacy-hidden-by-insta{
  display:none!important;
}
@media(min-width:768px){
  #crmInstaNav{
    width:420px!important;
    left:50%!important;
    right:auto!important;
    transform:translateX(-50%)!important;
    bottom:18px!important;
  }
}
@media(max-width:767px){
  body{padding-bottom:calc(104px + env(safe-area-inset-bottom))!important;}
  .crm-stable-audit-btn,
  .crm-stable-customer-btn,
  #crmStableAuditBtn,
  #crmStableCustomerBtn,
  #crmSettingsMenuBtn,
  #crmLogoutMenuBtn,
  #crmSettingsUnifiedBtn{
    display:none!important;
  }
  .crm-settings-panel,
  .crm-stable-customer-panel,
  .crm-v2-detail-panel{
    z-index:2147483900!important;
  }
  .crm-settings-panel.open,
  .crm-stable-customer-panel.open,
  .crm-v2-detail-panel.open{
    padding-bottom:calc(96px + env(safe-area-inset-bottom))!important;
  }
}
</style>`;

  const script = `<script id="crm-instagram-nav-v4-script">
(()=>{
  if(window.__crmInstagramNavV4) return;
  window.__crmInstagramNavV4 = 1;

  function qsa(s,r=document){return Array.from(r.querySelectorAll(s));}
  function qs(s,r=document){return r.querySelector(s);}

  function clickByText(patterns){
    const list = qsa('button,a').filter(el => {
      if(el.closest('#crmInstaNav')) return false;
      if(el.closest('#crmSettingsPanel') || el.closest('#crmStableCustomerPanel') || el.closest('#crmV2DetailPanel')) return false;
      const t = (el.textContent || '').trim();
      return patterns.some(p => typeof p === 'string' ? t === p || t.includes(p) : p.test(t));
    });
    const target = list[0];
    if(target){ target.click(); return true; }
    return false;
  }

  function openCustomers(){
    const panel = qs('#crmStableCustomerPanel');
    if(panel){
      panel.classList.add('open');
      const reload = qs('#crmStableCustomerReload', panel);
      const status = qs('#crmStableCustomerStatus', panel);
      if(reload && (!status || /未読み込み|読み込み/.test(status.textContent || ''))) reload.click();
      return;
    }
    const hiddenBtn = qs('#crmStableCustomerBtn') || qs('#crmStableCustomerMenuBtn');
    if(hiddenBtn){ hiddenBtn.click(); return; }
    clickByText(['顧客リスト']);
  }

  function openSettings(){
    const panel = qs('#crmSettingsPanel');
    if(panel){ panel.classList.add('open'); return; }
    const btn = qs('#crmSettingsUnifiedBtn') || qs('#crmSettingsMenuBtn');
    if(btn){ btn.click(); return; }
    clickByText(['設定']);
  }

  function openLine(){
    if(!clickByText(['LINE運用','LINE未送信','LINE'])) location.hash = 'line';
  }

  function openSearch(){
    const input = qsa('input').find(el => /検索|名前|LINE|電話|メール|ジャンル/.test(el.placeholder || ''));
    if(input){ input.scrollIntoView({behavior:'smooth', block:'center'}); setTimeout(()=>input.focus(), 300); return; }
    clickByText(['検索']);
  }

  function goHome(){
    window.scrollTo({top:0, behavior:'smooth'});
  }

  function setActive(name){
    qsa('.crm-insta-tab').forEach(b => b.classList.toggle('active', b.dataset.tab === name));
  }

  function makeNav(){
    if(qs('#crmInstaNav')) return;
    const nav = document.createElement('nav');
    nav.id = 'crmInstaNav';
    nav.setAttribute('aria-label','CRM下部メニュー');
    nav.innerHTML = `
      <button type="button" class="crm-insta-tab active" data-tab="home"><span class="crm-insta-icon">🏠</span><span>今日</span></button>
      <button type="button" class="crm-insta-tab" data-tab="search"><span class="crm-insta-icon">🔍</span><span>検索</span></button>
      <button type="button" class="crm-insta-tab" data-tab="line"><span class="crm-insta-icon">💬</span><span>LINE</span></button>
      <button type="button" class="crm-insta-tab" data-tab="customers"><span class="crm-insta-icon">👥</span><span>顧客</span></button>
      <button type="button" class="crm-insta-tab" data-tab="settings"><span class="crm-insta-icon">⚙️</span><span>設定</span></button>`;
    nav.querySelector('[data-tab="home"]').onclick = () => { setActive('home'); goHome(); };
    nav.querySelector('[data-tab="search"]').onclick = () => { setActive('search'); openSearch(); };
    nav.querySelector('[data-tab="line"]').onclick = () => { setActive('line'); openLine(); };
    nav.querySelector('[data-tab="customers"]').onclick = () => { setActive('customers'); openCustomers(); };
    nav.querySelector('[data-tab="settings"]').onclick = () => { setActive('settings'); openSettings(); };
    document.body.appendChild(nav);
  }

  function hideOldBars(){
    const oldTexts = ['今日','検索','LINE','問合せ','マーケ','顧客リストへ','状態確認','顧客リスト','設定','ログアウト'];
    qsa('button,a,div').forEach(el => {
      if(el.closest('#crmInstaNav') || el.closest('#crmSettingsPanel') || el.closest('#crmStableCustomerPanel') || el.closest('#crmV2DetailPanel')) return;
      const t = (el.textContent || '').trim();
      const st = getComputedStyle(el);
      const fixed = st.position === 'fixed' || st.position === 'sticky';
      if(fixed && oldTexts.some(x => t === x || t.includes(x))){
        el.classList.add('crm-legacy-hidden-by-insta');
      }
    });
  }

  function cleanDuplicateSettingsInsideMenu(){
    const seen = new Set();
    qsa('button,a').forEach(el => {
      if(el.closest('#crmInstaNav')) return;
      const t = (el.textContent || '').trim();
      if(t === '設定' || t === 'ログアウト'){
        const key = t + ':' + (el.closest('#crmSettingsPanel') ? 'settings' : 'outside');
        if(seen.has(key) || !el.closest('#crmSettingsPanel')) el.classList.add('crm-legacy-hidden-by-insta');
        seen.add(key);
      }
    });
  }

  function boot(){
    makeNav();
    hideOldBars();
    cleanDuplicateSettingsInsideMenu();
  }
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
  new MutationObserver(() => setTimeout(boot, 120)).observe(document.documentElement, {childList:true, subtree:true});
})();
</script>`;

  return html.includes('</head>') ? html.replace('</head>', style + '</head>').replace('</body>', script + '</body>') : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const res = await app.fetch(request, env, ctx);
    const ct = res.headers.get('content-type') || '';
    if(request.method === 'GET' && ct.includes('text/html')){
      const html = await res.text();
      return new Response(injectInstagramNav(html), {status:res.status, headers:res.headers});
    }
    return res;
  }
};
