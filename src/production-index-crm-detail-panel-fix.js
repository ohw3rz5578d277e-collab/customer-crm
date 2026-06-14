// ======================================================
// CUSTOMER CRM / DETAIL PANEL FIX WRAPPER
// build: customer-crm-detail-panel-fix-20260614-01
// - Add close button to legacy customer detail right drawer
// - Close with Esc / backdrop click
// - Prevent drawer from covering the whole CRM screen
// - Soften LINE history connection error message
// - Keep all existing APIs and UI intact
// ======================================================

import app from "./production-index-crm-fetch-safe-fix.js";

const BUILD = "customer-crm-detail-panel-fix-20260614-01";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store, no-cache, must-revalidate, max-age=0"
    }
  });
}

function safeLineHistoryFallback(path){
  if(/^\/api\/customers\/[^/]+\/(line|line-history|line-messages|messages)/.test(path)){
    return json({
      ok: true,
      build: BUILD,
      degraded: true,
      items: [],
      rows: [],
      count: 0,
      message: "LINE履歴はまだ取得できません。顧客情報と撮影履歴は表示できます。"
    });
  }
  return null;
}

function injectDetailPanelFix(html){
  if(!html || html.includes("crm-detail-panel-fix-script")) return html;

  const style = `<style id="crm-detail-panel-fix-style">
:root{--crmSafeGreen:#028760;--crmSafeDark:#07111f;--crmSafeBorder:#dbe5ef;--crmSafeText:#0f172a;--crmSafeMuted:#64748b;}
.crm-detail-close-btn{
  position:absolute!important;top:16px!important;right:18px!important;z-index:2147483000!important;
  width:42px!important;height:42px!important;border-radius:999px!important;border:1px solid #dbe5ef!important;
  background:#fff!important;color:#0f172a!important;box-shadow:0 10px 30px rgba(15,23,42,.14)!important;
  display:flex!important;align-items:center!important;justify-content:center!important;
  font-size:22px!important;font-weight:950!important;line-height:1!important;cursor:pointer!important;
}
.crm-detail-close-btn:hover{transform:translateY(-1px)!important;background:#f8fafc!important;}
.crm-detail-close-btn:active{transform:translateY(0)!important;}
.crm-detail-managed-panel{
  position:relative!important;
  box-shadow:-12px 0 40px rgba(15,23,42,.18)!important;
  border-left:1px solid #e2e8f0!important;
  background:#fff!important;
  overflow:auto!important;
  -webkit-overflow-scrolling:touch!important;
}
.crm-detail-managed-panel h1:first-child,
.crm-detail-managed-panel h2:first-child,
.crm-detail-managed-panel h3:first-child{padding-right:58px!important;}
.crm-detail-managed-panel [style*="LINE履歴API"],
.crm-detail-managed-panel .crm-line-warning{
  border:1px solid #bfdbfe!important;background:#eff6ff!important;color:#1e3a8a!important;
  border-radius:16px!important;padding:14px!important;font-weight:800!important;line-height:1.7!important;
}
.crm-detail-backdrop-hint{cursor:pointer!important;}
.crm-detail-force-visible-close{padding-top:14px!important;}
@media (min-width: 768px){
  .crm-detail-managed-panel{
    max-width:min(720px,46vw)!important;
    width:min(720px,46vw)!important;
  }
}
@media (max-width: 767px){
  .crm-detail-close-btn{top:10px!important;right:10px!important;width:46px!important;height:46px!important;font-size:24px!important;}
  .crm-detail-managed-panel{
    position:fixed!important;inset:0!important;width:100vw!important;max-width:100vw!important;height:100dvh!important;
    border-radius:0!important;border-left:0!important;padding:18px 14px 110px!important;z-index:2147482000!important;
  }
  body.crm-detail-open{overflow:hidden!important;}
}
/* Keep Cloudflare/Google login card from overlapping CRM action buttons. */
body [class*="access"], body [id*="access"], body [class*="login"], body [id*="login"]{max-width:280px;}
@media (max-width: 767px){
  body [class*="access"], body [id*="access"], body [class*="login"], body [id*="login"]{max-width:210px!important;}
}
</style>`;

  const script = `<script id="crm-detail-panel-fix-script">
(()=>{ if(window.__crmDetailPanelFix) return; window.__crmDetailPanelFix = 1;

const DETAIL_WORDS = ['LINE履歴','撮影履歴','購入履歴','タイムライン','累計売上','平均顧客単価'];
const ERROR_PHRASES = ['LINE履歴APIに接続できませんでした','LINEワーカー単体は成功','CRM側からの fetch','認証で停止'];
let activeHost = null;

function isVisible(el){
  if(!el || !(el instanceof HTMLElement)) return false;
  const r = el.getBoundingClientRect();
  const cs = getComputedStyle(el);
  return r.width > 0 && r.height > 0 && cs.display !== 'none' && cs.visibility !== 'hidden' && cs.opacity !== '0';
}
function hasDetailWords(el){
  const text = (el && el.innerText || '').slice(0, 6000);
  return DETAIL_WORDS.filter(w=>text.includes(w)).length >= 3;
}
function findBestPanel(seed){
  let best = null;
  let el = seed;
  for(let i=0; el && i<12; i++, el=el.parentElement){
    if(!(el instanceof HTMLElement)) continue;
    const r = el.getBoundingClientRect();
    const cs = getComputedStyle(el);
    const bg = cs.backgroundColor || '';
    const rightSide = r.left > window.innerWidth * 0.35 || r.right > window.innerWidth * 0.82;
    const panelLike = r.width >= 320 && r.height >= Math.min(360, window.innerHeight * 0.45) && rightSide;
    const whiteLike = bg.includes('255') || bg === 'white' || bg === '#fff' || bg === '#ffffff';
    if(panelLike && (whiteLike || cs.position === 'fixed' || cs.position === 'absolute')) best = el;
  }
  return best || seed;
}
function findCloseHost(panel){
  let el = panel;
  let candidate = null;
  for(let i=0; el && i<10; i++, el=el.parentElement){
    if(!(el instanceof HTMLElement) || el === document.body || el === document.documentElement) continue;
    const r = el.getBoundingClientRect();
    const cs = getComputedStyle(el);
    const fullOverlay = cs.position === 'fixed' && r.width > window.innerWidth * .84 && r.height > window.innerHeight * .75;
    if(fullOverlay) candidate = el;
  }
  return candidate || panel;
}
function closeDetailPanel(){
  const host = activeHost || document.querySelector('.crm-detail-managed-panel');
  if(host){
    const closeHost = findCloseHost(host);
    closeHost.style.display = 'none';
    closeHost.setAttribute('aria-hidden','true');
  }
  document.body.classList.remove('crm-detail-open');
  activeHost = null;
}
function addCloseButton(panel){
  if(!panel || panel.querySelector(':scope > .crm-detail-close-btn')) return;
  panel.classList.add('crm-detail-managed-panel','crm-detail-force-visible-close');
  panel.style.position = panel.style.position || 'relative';
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'crm-detail-close-btn';
  btn.setAttribute('aria-label','顧客詳細を閉じる');
  btn.textContent = '×';
  btn.addEventListener('click', (e)=>{e.preventDefault();e.stopPropagation();closeDetailPanel();});
  panel.prepend(btn);
}
function softenLineErrors(root){
  const nodes = Array.from((root || document).querySelectorAll('*'));
  nodes.forEach(el=>{
    if(!(el instanceof HTMLElement) || el.childElementCount > 1) return;
    const t = (el.innerText || '').trim();
    if(ERROR_PHRASES.some(p=>t.includes(p))){
      el.classList.add('crm-line-warning');
      el.innerHTML = 'LINE履歴は現在CRM側から取得できません。<br>顧客情報・撮影履歴・購入履歴は通常どおり確認できます。';
    }
  });
}
function scanDetailPanel(){
  const all = Array.from(document.querySelectorAll('body *')).filter(isVisible);
  let seed = null;
  for(const el of all){
    if(hasDetailWords(el)){ seed = el; break; }
  }
  if(!seed) { softenLineErrors(document); return; }
  const panel = findBestPanel(seed);
  if(!panel || panel === document.body || panel === document.documentElement) return;
  addCloseButton(panel);
  softenLineErrors(panel);
  activeHost = panel;
  document.body.classList.add('crm-detail-open');
}
function backdropCloseHandler(e){
  if(!activeHost) return;
  const panel = activeHost;
  if(panel && !panel.contains(e.target)){
    const r = panel.getBoundingClientRect();
    const clickedLeftDim = e.clientX < r.left && r.left > window.innerWidth * .25;
    if(clickedLeftDim) closeDetailPanel();
  }
}
function normalizeLoginCard(){
  Array.from(document.querySelectorAll('body *')).forEach(el=>{
    if(!(el instanceof HTMLElement) || !isVisible(el)) return;
    const text=(el.innerText||'').trim();
    if(text.includes('Googleログイン') && text.includes('ユーザー管理')){
      el.style.right = '18px';
      el.style.bottom = window.innerWidth < 768 ? '92px' : '18px';
      el.style.zIndex = '2147480500';
    }
  });
}

document.addEventListener('click', backdropCloseHandler, true);
document.addEventListener('keydown', (e)=>{ if(e.key === 'Escape') closeDetailPanel(); });
const observer = new MutationObserver(()=>{ scanDetailPanel(); normalizeLoginCard(); });
observer.observe(document.documentElement, {childList:true, subtree:true, characterData:true});
setInterval(()=>{ scanDetailPanel(); normalizeLoginCard(); }, 900);
if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', ()=>{scanDetailPanel();normalizeLoginCard();});
else { scanDetailPanel(); normalizeLoginCard(); }

})();
</script>`;

  return html.includes("</body>") ? html.replace("</body>", style + script + "</body>") : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    const lineFallback = request.method === "GET" ? safeLineHistoryFallback(url.pathname) : null;
    try{
      const res = lineFallback || await app.fetch(request, env, ctx);
      const ct = res.headers.get("content-type") || "";
      if(request.method === "GET" && ct.includes("text/html")){
        return new Response(injectDetailPanelFix(await res.text()), { status: res.status, headers: res.headers });
      }
      return res;
    }catch(e){
      const fallback = request.method === "GET" ? safeLineHistoryFallback(url.pathname) : null;
      if(fallback) return fallback;
      return json({ ok:false, build:BUILD, message:String(e && e.message || e) }, 500);
    }
  }
};
