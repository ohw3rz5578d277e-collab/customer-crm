// ======================================================
// CUSTOMER CRM / CUSTOMER LIST RETURN UX WRAPPER
// build: customer-crm-customer-list-return-20260614-01
// - Always show a clear way back to customer list
// - Add customer list entry to floating menu area
// - Close right detail drawer before returning
// - Keep all existing APIs and UI intact
// ======================================================

import app from "./production-index-crm-detail-panel-fix.js";

const BUILD = "customer-crm-customer-list-return-20260614-01";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {"content-type":"application/json; charset=utf-8","cache-control":"no-store"}
  });
}

function injectCustomerListReturn(html){
  if(!html || html.includes("crm-customer-list-return-script")) return html;

  const style = `<style id="crm-customer-list-return-style">
:root{--crmReturnGreen:#028760;--crmReturnDark:#07111f;--crmReturnBorder:#dbe5ef;}
.crm-customer-return-fab{
  position:fixed!important;right:18px!important;bottom:118px!important;z-index:2147482400!important;
  display:flex!important;align-items:center!important;gap:8px!important;
  min-height:48px!important;padding:0 18px!important;border:0!important;border-radius:999px!important;
  background:linear-gradient(135deg,#028760,#03a36f)!important;color:#fff!important;
  box-shadow:0 18px 38px rgba(2,135,96,.28)!important;
  font-weight:950!important;font-size:15px!important;letter-spacing:.02em!important;cursor:pointer!important;
}
.crm-customer-return-fab:hover{transform:translateY(-1px)!important;filter:brightness(1.02)!important;}
.crm-customer-return-fab:active{transform:translateY(0)!important;}
.crm-customer-return-top-card{
  margin:12px auto 14px!important;max-width:1180px!important;padding:16px 18px!important;
  border:1px solid rgba(2,135,96,.20)!important;border-radius:22px!important;background:linear-gradient(135deg,#f0fdf4,#ffffff)!important;
  box-shadow:0 14px 38px rgba(15,23,42,.06)!important;display:flex!important;align-items:center!important;justify-content:space-between!important;gap:12px!important;
}
.crm-customer-return-top-card strong{font-size:17px!important;color:#07111f!important;}
.crm-customer-return-top-card span{display:block!important;color:#64748b!important;font-size:13px!important;margin-top:4px!important;}
.crm-customer-return-top-card button{
  border:0!important;border-radius:999px!important;background:#028760!important;color:#fff!important;font-weight:950!important;padding:12px 18px!important;min-height:44px!important;white-space:nowrap!important;
}
.crm-customer-list-focus-hint{
  outline:3px solid rgba(2,135,96,.22)!important;box-shadow:0 0 0 8px rgba(2,135,96,.08)!important;border-radius:18px!important;
}
@media(max-width:767px){
  .crm-customer-return-fab{right:14px!important;bottom:92px!important;min-height:54px!important;padding:0 18px!important;font-size:14px!important;}
  .crm-customer-return-top-card{margin:10px!important;display:block!important;padding:14px!important;}
  .crm-customer-return-top-card button{width:100%!important;margin-top:12px!important;}
}
</style>`;

  const script = `<script id="crm-customer-list-return-script">
(()=>{ if(window.__crmCustomerListReturn) return; window.__crmCustomerListReturn = 1;

const CUSTOMER_WORDS = ['顧客','名前','LINE','リピート','撮影日'];
const DETAIL_WORDS = ['LINE履歴','撮影履歴','購入履歴','タイムライン','累計売上'];

function isVisible(el){
  if(!el || !(el instanceof HTMLElement)) return false;
  const r = el.getBoundingClientRect();
  const cs = getComputedStyle(el);
  return r.width > 0 && r.height > 0 && cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0';
}

function closeDetailPanels(){
  document.querySelectorAll('.crm-detail-managed-panel').forEach(panel=>{
    let host = panel;
    for(let i=0; host && i<8; i++, host=host.parentElement){
      if(!(host instanceof HTMLElement)) continue;
      const r = host.getBoundingClientRect();
      const cs = getComputedStyle(host);
      if(cs.position === 'fixed' && r.width > window.innerWidth * .35 && r.height > window.innerHeight * .5){
        host.style.display = 'none';
        host.setAttribute('aria-hidden','true');
        break;
      }
    }
  });
  document.body.classList.remove('crm-detail-open');
}

function findCustomerList(){
  const nodes = Array.from(document.querySelectorAll('table, .customer-list, [class*=customer], [id*=customer], main, section, div')).filter(isVisible);
  let best = null;
  let bestScore = 0;
  for(const el of nodes){
    const text = (el.innerText || '').slice(0, 4000);
    const score = CUSTOMER_WORDS.reduce((n,w)=>n + (text.includes(w) ? 1 : 0), 0) + ((text.match(/LINE:/g)||[]).length >= 3 ? 3 : 0);
    const r = el.getBoundingClientRect();
    if(score > bestScore && r.width > 320 && r.height > 180){ best = el; bestScore = score; }
  }
  return best;
}

function goCustomerList(){
  closeDetailPanels();
  const list = findCustomerList();
  if(list){
    list.scrollIntoView({behavior:'smooth', block:'start'});
    list.classList.add('crm-customer-list-focus-hint');
    setTimeout(()=>list.classList.remove('crm-customer-list-focus-hint'), 1600);
    return;
  }
  const url = new URL(location.href);
  url.pathname = '/admin';
  url.hash = '';
  location.href = url.toString();
}

function ensureFab(){
  if(document.getElementById('crmCustomerReturnFab')) return;
  const btn = document.createElement('button');
  btn.id = 'crmCustomerReturnFab';
  btn.className = 'crm-customer-return-fab';
  btn.type = 'button';
  btn.innerHTML = '顧客リストへ';
  btn.addEventListener('click', (e)=>{ e.preventDefault(); e.stopPropagation(); goCustomerList(); });
  document.body.appendChild(btn);
}

function ensureTopCard(){
  if(document.getElementById('crmCustomerReturnTopCard')) return;
  const main = document.querySelector('main') || document.querySelector('#app') || document.body;
  const card = document.createElement('div');
  card.id = 'crmCustomerReturnTopCard';
  card.className = 'crm-customer-return-top-card';
  card.innerHTML = '<div><strong>顧客リスト</strong><span>顧客を探す・詳細を確認する場合はこちらから戻れます。</span></div><button type="button">顧客リストを開く</button>';
  card.querySelector('button').addEventListener('click', goCustomerList);
  const first = main.firstElementChild;
  if(first) main.insertBefore(card, first); else main.appendChild(card);
}

function addMenuCustomerShortcut(){
  const menuTexts = ['CRMメニュー','メニュー'];
  Array.from(document.querySelectorAll('button,a')).forEach(btn=>{
    const text = (btn.innerText || btn.textContent || '').trim();
    if(menuTexts.includes(text) && !btn.dataset.crmCustomerShortcutBound){
      btn.dataset.crmCustomerShortcutBound = '1';
      btn.addEventListener('click', ()=>setTimeout(()=>{
        const panels = Array.from(document.querySelectorAll('body *')).filter(isVisible).filter(el=>{
          const t=(el.innerText||'').slice(0,2000);
          return t.includes('今日やること') && t.includes('LINE') && t.includes('マーケ');
        });
        const panel = panels[0];
        if(panel && !panel.querySelector('#crmMenuCustomerListShortcut')){
          const b = document.createElement('button');
          b.id = 'crmMenuCustomerListShortcut';
          b.type = 'button';
          b.textContent = '顧客リスト';
          b.style.cssText = 'width:100%;min-height:48px;border:0;border-radius:14px;background:#028760;color:#fff;font-weight:950;margin:8px 0 12px;cursor:pointer;';
          b.addEventListener('click', goCustomerList);
          panel.prepend(b);
        }
      }, 250));
    }
  });
}

function boot(){ ensureFab(); ensureTopCard(); addMenuCustomerShortcut(); }
if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
new MutationObserver(()=>boot()).observe(document.documentElement, {childList:true, subtree:true});
window.addEventListener('keydown', (e)=>{ if((e.ctrlKey || e.metaKey) && e.shiftKey && e.key.toLowerCase()==='c'){ e.preventDefault(); goCustomerList(); } });

})();
</script>`;

  return html.includes('</body>') ? html.replace('</body>', style + script + '</body>') : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    try{
      const res = await app.fetch(request, env, ctx);
      const ct = res.headers.get('content-type') || '';
      if(request.method === 'GET' && ct.includes('text/html')){
        return new Response(injectCustomerListReturn(await res.text()), {status:res.status, headers:res.headers});
      }
      return res;
    }catch(e){
      return json({ok:false, build:BUILD, message:String(e && e.message || e)}, 500);
    }
  }
};
