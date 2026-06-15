// CUSTOMER CRM / CLEAN MOBILE UX V3
// Stable entrypoint + direct mobile navigation cleanup.
import app from "./production-index-crm-customer-list-detail-v2.js";

const RESERVATION_ADMIN_URL = "https://reservation-app-api.ohw3rz5578d277e.workers.dev/admin";

function injectCleanup(html){
  if(!html || html.includes('crm-direct-mobile-nav-v1')) return html;
  const style = `<style id="crm-direct-mobile-nav-v1-style">
.crm-force-hide-floating{display:none!important;visibility:hidden!important;pointer-events:none!important}.crm-stable-customer-btn,#crmStableCustomerBtn,#crmStableCustomerMenuBtn{display:none!important}.crm-top-menu-btn{position:fixed!important;left:12px!important;top:12px!important;z-index:2147483900!important;width:46px!important;height:46px!important;border:0!important;border-radius:999px!important;background:#07111f!important;color:#fff!important;font-size:22px!important;font-weight:950!important;box-shadow:0 12px 28px rgba(15,23,42,.22)!important}.crm-side-menu{position:fixed!important;left:10px!important;top:66px!important;width:min(320px,calc(100vw - 20px))!important;background:#fff!important;border:1px solid #e2e8f0!important;border-radius:22px!important;box-shadow:0 24px 70px rgba(15,23,42,.22)!important;z-index:2147483900!important;padding:14px!important;display:none!important}.crm-side-menu.open{display:block!important}.crm-side-menu h3{margin:0 0 10px!important;font-size:20px!important;font-weight:950!important}.crm-side-menu button{width:100%!important;min-height:48px!important;border:0!important;border-radius:16px!important;background:#f1f5f9!important;color:#07111f!important;font-weight:950!important;text-align:left!important;padding:0 14px!important;margin:6px 0!important}.crm-side-menu button.danger{background:#fee2e2!important;color:#991b1b!important}.crm-bottom-nav{position:fixed!important;left:0!important;right:0!important;bottom:0!important;height:72px!important;z-index:2147483800!important;display:flex!important;align-items:center!important;justify-content:space-around!important;gap:0!important;padding:6px 6px calc(6px + env(safe-area-inset-bottom))!important;border-top:1px solid rgba(226,232,240,.96)!important;border-radius:0!important;background:#fff!important;box-shadow:none!important}.crm-bottom-nav button{flex:1!important;height:58px!important;border:0!important;border-radius:14px!important;background:transparent!important;color:#0f172a!important;font-weight:900!important;font-size:10px!important;display:flex!important;flex-direction:column!important;align-items:center!important;justify-content:center!important;gap:2px!important;padding:0!important}.crm-bottom-nav button.active{background:#028760!important;color:#fff!important}.crm-bottom-nav .ico{font-size:22px!important;line-height:1!important}@media(max-width:767px){body{padding-bottom:calc(86px + env(safe-area-inset-bottom))!important}.crm-stable-customer-btn,#crmStableCustomerBtn,#crmStableCustomerMenuBtn{display:none!important}}
</style>`;
  const script = `<script id="crm-direct-mobile-nav-v1">
(()=>{
  if(window.__crmDirectMobileNavV1) return;
  window.__crmDirectMobileNavV1 = 1;
  const RES_URL=${JSON.stringify(RESERVATION_ADMIN_URL)};
  const hideWords = ['顧客リストへ','顧客リスト','状態確認','ユーザー管理','ユーザー追加','ログアウト','設定'];
  const qs=(s,r=document)=>r.querySelector(s);
  const qsa=(s,r=document)=>Array.from(r.querySelectorAll(s));
  function clean(){
    document.querySelectorAll('#crmStableCustomerBtn,#crmStableCustomerMenuBtn,.crm-stable-customer-btn').forEach(el=>el.remove());
    document.querySelectorAll('button,a,div').forEach(el=>{
      if(el.closest('.crm-bottom-nav') || el.closest('.crm-side-menu') || el.closest('#crmStableCustomerPanel') || el.closest('#crmV2DetailPanel') || el.closest('#crmSettingsPanel')) return;
      const text=(el.textContent||'').trim();
      if(!hideWords.some(w=>text===w || text.includes(w))) return;
      const st=getComputedStyle(el);
      if(st.position==='fixed' || st.position==='sticky' || st.position==='absolute' || text==='設定' || text==='ログアウト'){
        el.classList.add('crm-force-hide-floating');
        el.style.setProperty('display','none','important');
      }
    });
  }
  function closePanelsOnHome(){qsa('#crmStableCustomerPanel.open,#crmV2DetailPanel.open').forEach(p=>p.classList.remove('open'));}
  function setActive(name){qsa('.crm-bottom-nav button').forEach(b=>b.classList.toggle('active',b.dataset.tab===name));}
  function openCustomers(){if(typeof window.__crmOpenStableCustomerList==='function'){window.__crmOpenStableCustomerList();return;} const p=qs('#crmStableCustomerPanel'); if(p){p.classList.add('open'); qs('#crmStableCustomerReload',p)?.click();}}
  function openSearch(){const i=qsa('input').find(el=>/名前|検索|LINE|電話|メール|ジャンル/.test(el.placeholder||'')); if(i){i.scrollIntoView({behavior:'smooth',block:'center'}); setTimeout(()=>i.focus(),250);}}
  function openLine(){const b=qsa('button,a').find(x=>!x.closest('.crm-bottom-nav')&&/(LINE未送信|LINE運用)/.test(x.textContent||'')); if(b)b.click();}
  function makeBottomNav(){if(qs('.crm-bottom-nav'))return;const nav=document.createElement('nav');nav.className='crm-bottom-nav';nav.innerHTML='<button class="active" data-tab="home"><span class="ico">🏠</span><span>今日</span></button><button data-tab="search"><span class="ico">🔍</span><span>検索</span></button><button data-tab="line"><span class="ico">💬</span><span>LINE</span></button><button data-tab="reservation"><span class="ico">📅</span><span>予約</span></button><button data-tab="customers"><span class="ico">👥</span><span>顧客</span></button>';qs('[data-tab="home"]',nav).onclick=()=>{setActive('home');closePanelsOnHome();scrollTo({top:0,behavior:'smooth'});};qs('[data-tab="search"]',nav).onclick=()=>{setActive('search');closePanelsOnHome();openSearch();};qs('[data-tab="line"]',nav).onclick=()=>{setActive('line');closePanelsOnHome();openLine();};qs('[data-tab="reservation"]',nav).onclick=()=>{setActive('reservation');location.href=RES_URL;};qs('[data-tab="customers"]',nav).onclick=()=>{setActive('customers');openCustomers();};document.body.appendChild(nav);}
  function makeSideMenu(){if(qs('.crm-top-menu-btn'))return;const btn=document.createElement('button');btn.className='crm-top-menu-btn';btn.type='button';btn.textContent='☰';const menu=document.createElement('div');menu.className='crm-side-menu';menu.innerHTML='<h3>メニュー</h3><button data-act="settings">設定</button><button data-act="adduser">ユーザー追加</button><button data-act="diag">診断</button><button class="danger" data-act="logout">ログアウト</button>';btn.onclick=()=>menu.classList.toggle('open');qs('[data-act="settings"]',menu).onclick=()=>qs('#crmSettingsPanel')?.classList.add('open');qs('[data-act="adduser"]',menu).onclick=()=>qs('#crmSettingsPanel')?.classList.add('open');qs('[data-act="diag"]',menu).onclick=()=>{location.href='/api/crm-health-check';};qs('[data-act="logout"]',menu).onclick=()=>{try{localStorage.clear();sessionStorage.clear();}catch(e){} location.reload();};document.body.appendChild(btn);document.body.appendChild(menu);}
  function boot(){makeBottomNav();makeSideMenu();clean();setTimeout(closePanelsOnHome,120);}
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',boot); else boot();
  new MutationObserver(()=>setTimeout(()=>{makeBottomNav();makeSideMenu();clean();},80)).observe(document.documentElement,{childList:true,subtree:true});
})();
</script>`;
  return html.includes('</head>') ? html.replace('</head>',style+'</head>').replace('</body>',script+'</body>') : html+style+script;
}

export default {
  async fetch(request, env, ctx){
    const res = await app.fetch(request, env, ctx);
    const ct = res.headers.get('content-type') || '';
    if(request.method === 'GET' && ct.includes('text/html')){
      const html = await res.text();
      return new Response(injectCleanup(html), {status:res.status, headers:res.headers});
    }
    return res;
  }
};
