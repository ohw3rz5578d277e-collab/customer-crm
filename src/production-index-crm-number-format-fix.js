// ======================================================
// CUSTOMER CRM / NUMBER FORMAT + SETTINGS UX WRAPPER
// build: customer-crm-number-format-settings-20260614-02
// - Display-only formatting layer, optimized for speed
// - JPY amounts: no decimals
// - Percent values: max 2 decimals
// - Adds Settings tab and Logout button to CRM menu
// - Keeps DB values and API payloads unchanged
// ======================================================

import app from "./production-index-crm-stable-audit.js";

const BUILD = "customer-crm-number-format-settings-20260614-02";

function injectNumberFormatFix(html){
  if(!html || html.includes("crm-number-format-fix-script")) return html;

  const style = `<style id="crm-number-format-settings-style">
.crm-settings-panel{position:fixed!important;right:18px!important;bottom:230px!important;width:min(520px,calc(100vw - 36px))!important;max-height:72vh!important;overflow:auto!important;background:#fff!important;border:1px solid #dbe5ef!important;border-radius:22px!important;box-shadow:0 24px 60px rgba(15,23,42,.22)!important;z-index:2147482700!important;padding:18px!important;display:none!important;color:#07111f!important}.crm-settings-panel.open{display:block!important}.crm-settings-panel h3{font-size:20px!important;margin:0 0 6px!important;font-weight:950!important}.crm-settings-panel p{color:#64748b!important;line-height:1.7!important;margin:0 0 12px!important}.crm-settings-close{float:right;border:0!important;background:#f1f5f9!important;border-radius:999px!important;width:36px!important;height:36px!important;font-weight:950!important;cursor:pointer!important}.crm-settings-grid{display:grid!important;grid-template-columns:1fr!important;gap:10px!important;margin-top:14px!important}.crm-settings-item{border:1px solid #e2e8f0!important;border-radius:16px!important;padding:12px!important;background:#f8fafc!important}.crm-settings-item b{display:block!important;margin-bottom:4px!important}.crm-settings-actions{display:flex!important;gap:10px!important;flex-wrap:wrap!important;margin-top:14px!important}.crm-settings-actions button,.crm-settings-menu-btn{min-height:44px!important;border:0!important;border-radius:999px!important;padding:0 16px!important;font-weight:950!important;cursor:pointer!important}.crm-settings-menu-btn{background:#f1f5f9!important;color:#07111f!important}.crm-logout-btn{background:#b91c1c!important;color:#fff!important}.crm-settings-save{background:#028760!important;color:#fff!important}@media(max-width:767px){.crm-settings-panel{inset:10px!important;width:auto!important;max-height:calc(100dvh - 20px)!important;border-radius:18px!important;padding-bottom:calc(24px + env(safe-area-inset-bottom))!important}.crm-settings-actions button{width:100%!important}}
</style>`;

  const script = `<script id="crm-number-format-fix-script">
(()=>{
  if(window.__crmNumberFormatFix) return;
  window.__crmNumberFormatFix = 1;

  const yenPattern = /¥\s*(-?\d{1,3}(?:,\d{3})*|-?\d+)(?:\.(\d+))?/g;
  const pctPattern = /(-?\d+(?:\.\d+)?)\s*%/g;
  const formattedAttr = 'data-crm-number-formatted';

  function formatYen(numText){
    const raw = String(numText || '').replace(/,/g, '');
    const n = Number(raw);
    if(!Number.isFinite(n)) return numText;
    return Math.round(n).toLocaleString('ja-JP');
  }

  function formatPercent(numText){
    const n = Number(numText);
    if(!Number.isFinite(n)) return numText;
    const rounded = Math.round(n * 100) / 100;
    return rounded.toLocaleString('ja-JP', { minimumFractionDigits: 0, maximumFractionDigits: 2 });
  }

  function fixText(text){
    if(!text || typeof text !== 'string') return text;
    if(text.indexOf('¥') === -1 && text.indexOf('%') === -1) return text;
    let out = text.replace(yenPattern, (_, n) => '¥' + formatYen(n));
    out = out.replace(pctPattern, (_, n) => formatPercent(n) + '%');
    return out;
  }

  function shouldSkip(node){
    if(!node || !node.parentElement) return true;
    const p = node.parentElement;
    if(['SCRIPT','STYLE','TEXTAREA','INPUT','SELECT','OPTION','CODE','PRE'].includes(p.tagName)) return true;
    if(p.closest('[contenteditable="true"]')) return true;
    if(p.closest('#crmStableAuditPanel')) return true;
    return false;
  }

  function walk(root){
    const target = root && root.nodeType === 1 ? root : document.body;
    if(!target) return;
    const walker = document.createTreeWalker(target, NodeFilter.SHOW_TEXT, {
      acceptNode(node){
        if(shouldSkip(node)) return NodeFilter.FILTER_REJECT;
        const v = node.nodeValue || '';
        return (v.indexOf('¥') !== -1 || v.indexOf('%') !== -1) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
      }
    });
    const nodes = [];
    while(walker.nextNode()) nodes.push(walker.currentNode);
    for(const node of nodes){
      const next = fixText(node.nodeValue);
      if(next !== node.nodeValue) node.nodeValue = next;
    }
  }

  function runOnce(root){
    const target = root && root.nodeType === 1 ? root : document.body;
    if(!target || target.getAttribute?.(formattedAttr) === '1') return;
    walk(target);
    if(target !== document.body) target.setAttribute?.(formattedAttr, '1');
  }

  let timer = null;
  const pending = new Set();
  function schedule(root){
    if(root && root.nodeType === 1) pending.add(root);
    if(timer) return;
    timer = setTimeout(() => {
      timer = null;
      const list = Array.from(pending).slice(0, 12);
      pending.clear();
      if(!list.length) list.push(document.body);
      for(const node of list) runOnce(node);
    }, 120);
  }

  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', () => schedule(document.body));
  }else{
    schedule(document.body);
  }

  const observer = new MutationObserver((mutations) => {
    for(const m of mutations){
      if(m.type !== 'childList') continue;
      for(const node of m.addedNodes){
        if(node && node.nodeType === 1){
          const text = node.textContent || '';
          if(text.indexOf('¥') !== -1 || text.indexOf('%') !== -1) schedule(node);
        }
      }
    }
  });
  observer.observe(document.documentElement, { childList:true, subtree:true });

  function openSettings(){
    ensureSettingsPanel();
    document.getElementById('crmSettingsPanel')?.classList.add('open');
  }
  function closeSettings(){ document.getElementById('crmSettingsPanel')?.classList.remove('open'); }

  function logout(){
    try{ localStorage.clear(); sessionStorage.clear(); }catch(e){}
    const returnTo = encodeURIComponent(location.origin + '/admin');
    location.href = '/cdn-cgi/access/logout?returnTo=' + returnTo;
  }

  function ensureSettingsPanel(){
    if(document.getElementById('crmSettingsPanel')) return;
    const panel = document.createElement('div');
    panel.id = 'crmSettingsPanel';
    panel.className = 'crm-settings-panel';
    panel.innerHTML = '<button class="crm-settings-close" type="button">×</button><h3>設定</h3><p>表示・操作・ログイン状態を確認できます。</p><div class="crm-settings-grid"><div class="crm-settings-item"><b>数値表示</b><span>金額は小数点なし、％は小数点第2位までで表示します。</span></div><div class="crm-settings-item"><b>スマホ表示</b><span>メニュー・顧客リスト・状態確認を下部から操作できます。</span></div><div class="crm-settings-item"><b>状態確認</b><span>CRMメニューまたは状態確認ボタンからDBと連携状態を確認できます。</span></div></div><div class="crm-settings-actions"><button class="crm-settings-save" type="button" data-crm-settings-close>閉じる</button><button class="crm-logout-btn" type="button" data-crm-logout>ログアウト</button></div>';
    panel.querySelector('.crm-settings-close').onclick = closeSettings;
    panel.querySelector('[data-crm-settings-close]').onclick = closeSettings;
    panel.querySelector('[data-crm-logout]').onclick = logout;
    document.body.appendChild(panel);
  }

  function addSettingsToMenu(){
    ensureSettingsPanel();
    const existing = document.getElementById('crmSettingsMenuBtn');
    if(existing) return;

    const candidates = Array.from(document.querySelectorAll('button, a')).filter(el => /CRMメニュー|メニュー/.test((el.textContent || '').trim()));
    const menuBtn = candidates[candidates.length - 1];
    const host = menuBtn && menuBtn.parentElement ? menuBtn.parentElement : document.body;

    const btn = document.createElement('button');
    btn.id = 'crmSettingsMenuBtn';
    btn.type = 'button';
    btn.className = 'crm-settings-menu-btn';
    btn.textContent = '設定';
    btn.onclick = openSettings;

    const logoutBtn = document.createElement('button');
    logoutBtn.id = 'crmLogoutMenuBtn';
    logoutBtn.type = 'button';
    logoutBtn.className = 'crm-logout-btn';
    logoutBtn.textContent = 'ログアウト';
    logoutBtn.onclick = logout;

    host.appendChild(btn);
    host.appendChild(logoutBtn);
  }

  function scheduleMenuPatch(){ setTimeout(addSettingsToMenu, 300); }
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', scheduleMenuPatch); else scheduleMenuPatch();
  new MutationObserver(() => scheduleMenuPatch()).observe(document.documentElement, { childList:true, subtree:true });

  document.addEventListener('keydown', e => { if(e.key === 'Escape') closeSettings(); });
})();
</script>`;

  return html.includes("</head>") ? html.replace("</head>", style + "</head>").replace("</body>", script + "</body>") : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const res = await app.fetch(request, env, ctx);
    const ct = res.headers.get("content-type") || "";
    if(request.method === "GET" && ct.includes("text/html")){
      const html = await res.text();
      return new Response(injectNumberFormatFix(html), {status:res.status, headers:res.headers});
    }
    return res;
  }
};
