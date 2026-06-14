// ======================================================
// CUSTOMER CRM / NUMBER FORMAT FIX WRAPPER
// build: customer-crm-number-format-fix-20260614-01
// - Display-only formatting layer
// - JPY amounts: no decimals
// - Percent values: max 2 decimals
// - Keeps DB values and API payloads unchanged
// ======================================================

import app from "./production-index-crm-stable-audit.js";

const BUILD = "customer-crm-number-format-fix-20260614-01";

function injectNumberFormatFix(html){
  if(!html || html.includes("crm-number-format-fix-script")) return html;

  const script = `<script id="crm-number-format-fix-script">
(()=>{
  if(window.__crmNumberFormatFix) return;
  window.__crmNumberFormatFix = 1;

  const yenPattern = /¥\s*(-?\d{1,3}(?:,\d{3})*|-?\d+)(?:\.(\d+))?/g;
  const pctPattern = /(-?\d+(?:\.\d+)?)\s*%/g;

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
    let out = text.replace(yenPattern, (_, n) => '¥' + formatYen(n));
    out = out.replace(pctPattern, (_, n) => formatPercent(n) + '%');
    return out;
  }

  function shouldSkip(node){
    if(!node || !node.parentElement) return true;
    const p = node.parentElement;
    if(['SCRIPT','STYLE','TEXTAREA','INPUT','SELECT','OPTION','CODE','PRE'].includes(p.tagName)) return true;
    if(p.closest('[contenteditable="true"]')) return true;
    return false;
  }

  function walk(root){
    const walker = document.createTreeWalker(root || document.body, NodeFilter.SHOW_TEXT, {
      acceptNode(node){
        if(shouldSkip(node)) return NodeFilter.FILTER_REJECT;
        const v = node.nodeValue || '';
        return (v.includes('¥') || v.includes('%')) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
      }
    });
    const nodes = [];
    while(walker.nextNode()) nodes.push(walker.currentNode);
    for(const node of nodes){
      const next = fixText(node.nodeValue);
      if(next !== node.nodeValue) node.nodeValue = next;
    }
  }

  function fixInputs(root){
    const fields = (root || document).querySelectorAll ? (root || document).querySelectorAll('input, textarea') : [];
    fields.forEach(el => {
      if(!el || typeof el.value !== 'string') return;
      if(el.matches(':focus')) return;
      const v = el.value;
      if(v.includes('¥') || v.includes('%')){
        const next = fixText(v);
        if(next !== v) el.value = next;
      }
    });
  }

  let timer = null;
  function schedule(root){
    if(timer) clearTimeout(timer);
    timer = setTimeout(() => {
      try{ walk(root || document.body); fixInputs(root || document); }catch(e){}
    }, 60);
  }

  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', () => schedule(document.body));
  }else{
    schedule(document.body);
  }

  const observer = new MutationObserver((mutations) => {
    for(const m of mutations){
      if(m.type === 'childList' || m.type === 'characterData'){
        schedule(document.body);
        break;
      }
    }
  });
  observer.observe(document.documentElement, { childList:true, subtree:true, characterData:true });
})();
</script>`;

  return html.includes("</body>") ? html.replace("</body>", script + "</body>") : html + script;
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
