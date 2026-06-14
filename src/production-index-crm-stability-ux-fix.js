// ======================================================
// CUSTOMER CRM / STABILITY + UX FIX WRAPPER
// build: customer-crm-api-stability-ux-fix-20260614-01
// - crm_reservation_drafts missing table self-healing
// - legacy floating buttons cleanup
// - close buttons for modal panels
// - desktop/mobile modal overlap and clutter fixes
// ======================================================

import app from "./production-index-crm-mobile-first-ux.js";

const BUILD = "customer-crm-api-stability-ux-fix-20260614-01";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }
  });
}

async function safeExec(env, sql){
  if(!env || !env.DB) return false;
  try{
    await env.DB.prepare(sql).run();
    return true;
  }catch(e){
    return false;
  }
}

async function ensureReservationDraftSchema(env){
  if(!env || !env.DB) return;
  await safeExec(env, `CREATE TABLE IF NOT EXISTS crm_reservation_drafts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    customer_name TEXT,
    phone TEXT,
    email TEXT,
    genre TEXT,
    shoot_date TEXT,
    start_time TEXT,
    end_time TEXT,
    place TEXT,
    plan_label TEXT,
    total_amount REAL DEFAULT 0,
    status TEXT DEFAULT 'draft',
    memo TEXT,
    draft_json TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    converted_at TEXT,
    converted_by TEXT,
    reservation_intake_id TEXT,
    sent_to_reservation_at TEXT,
    sent_to_reservation_by TEXT,
    sent_to_reservation_response TEXT,
    reservation_app_reservation_id TEXT,
    reservation_app_intake_id TEXT,
    reservation_app_created_at TEXT,
    reservation_app_created_by TEXT,
    reservation_app_response TEXT,
    reservation_app_updated_at TEXT,
    reservation_app_updated_by TEXT,
    reservation_app_update_response TEXT,
    reservation_app_cancelled_at TEXT,
    reservation_app_cancelled_by TEXT,
    reservation_app_cancel_reason TEXT,
    reservation_app_cancel_response TEXT,
    cancellation_synced_at TEXT
  )`);

  const columns = [
    ["phone", "TEXT"], ["email", "TEXT"], ["end_time", "TEXT"],
    ["reservation_intake_id", "TEXT"], ["sent_to_reservation_at", "TEXT"],
    ["sent_to_reservation_by", "TEXT"], ["sent_to_reservation_response", "TEXT"],
    ["reservation_app_reservation_id", "TEXT"], ["reservation_app_intake_id", "TEXT"],
    ["reservation_app_created_at", "TEXT"], ["reservation_app_created_by", "TEXT"],
    ["reservation_app_response", "TEXT"], ["reservation_app_updated_at", "TEXT"],
    ["reservation_app_updated_by", "TEXT"], ["reservation_app_update_response", "TEXT"],
    ["reservation_app_cancelled_at", "TEXT"], ["reservation_app_cancelled_by", "TEXT"],
    ["reservation_app_cancel_reason", "TEXT"], ["reservation_app_cancel_response", "TEXT"],
    ["cancellation_synced_at", "TEXT"]
  ];
  for(const [name, type] of columns){
    await safeExec(env, `ALTER TABLE crm_reservation_drafts ADD COLUMN ${name} ${type}`);
  }

  await safeExec(env, `CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_customer ON crm_reservation_drafts(customer_id, created_at)`);
  await safeExec(env, `CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_status ON crm_reservation_drafts(status, created_at)`);
  await safeExec(env, `CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_sent ON crm_reservation_drafts(status, sent_to_reservation_at)`);
  await safeExec(env, `CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_app_created ON crm_reservation_drafts(status, reservation_app_created_at)`);
}

function injectStabilityUx(html){
  if(!html || html.includes("crm-stability-ux-fix-script")) return html;

  const style = `<style id="crm-stability-ux-fix-style">
:root{--crm-fix-green:#028760;--crm-fix-red:#b91c1c;--crm-fix-dark:#0f172a;--crm-fix-muted:#64748b;--crm-fix-line:#e5e7eb;--crm-fix-bg:#f8fafc;--crm-fix-safe:env(safe-area-inset-bottom,0px)}
html,body{overflow-x:hidden!important;background:var(--crm-fix-bg)!important}
/* Hide old scattered fixed buttons, but keep the unified entry points */
.crm-legacy-floating-hidden{display:none!important;visibility:hidden!important;pointer-events:none!important}
/* Make the active single entry point obvious */
button[data-crm-main-menu="1"],#crmUnifiedUxButton,#crmUnifiedMenuButton{z-index:2147483600!important}
/* Modal/panel common safety */
#crmUnifiedPanel,#crmLineOpsPanel,#crmBulkSafePanel,#crmListWorkbenchPanel,#crmCustomerSmartPanel,#crmInquiryRowPanel,#crmInquiryActionsPanel,#crmFollowTemplatePanel,#crmTemplatePanel,#crmMarketingPanel,#crmCampaignPanel,#crmSourcePanel,#crmHomeQuickPanel{
  box-sizing:border-box!important;
}
.crm-fix-close{position:absolute!important;top:12px!important;right:12px!important;z-index:2147483647!important;width:38px!important;height:38px!important;min-height:38px!important;border-radius:999px!important;border:1px solid rgba(15,23,42,.12)!important;background:#fff!important;color:#0f172a!important;font-size:18px!important;font-weight:900!important;display:flex!important;align-items:center!important;justify-content:center!important;box-shadow:0 10px 24px rgba(15,23,42,.12)!important;cursor:pointer!important}
.crm-fix-close:hover{background:#fee2e2!important;color:#991b1b!important;border-color:#fecaca!important}
.crm-fix-panel-open{box-shadow:0 24px 80px rgba(15,23,42,.22)!important;border:1px solid rgba(2,135,96,.14)!important}
.crm-fix-scrim{position:fixed;inset:0;background:rgba(15,23,42,.22);z-index:2147483200;display:none;backdrop-filter:blur(2px)}
.crm-fix-scrim[data-open="1"]{display:block!important}
.crm-fix-mini-note{font-size:12px;color:var(--crm-fix-muted);line-height:1.7;margin-top:10px;padding:10px 12px;border-radius:14px;background:#f8fafc;border:1px dashed #cbd5e1}
.crm-fix-error-note{border:1px solid #fecaca!important;background:#fff1f2!important;color:#991b1b!important;border-radius:16px!important;padding:12px!important;font-weight:800!important}
@media(min-width:761px){
  #crmUnifiedPanel,#crmLineOpsPanel,#crmBulkSafePanel,#crmListWorkbenchPanel,#crmCustomerSmartPanel,#crmInquiryRowPanel,#crmInquiryActionsPanel,#crmFollowTemplatePanel,#crmTemplatePanel,#crmMarketingPanel,#crmCampaignPanel,#crmSourcePanel{max-width:min(760px,calc(100vw - 48px))!important;max-height:calc(100vh - 48px)!important;overflow:auto!important;right:24px!important;left:auto!important;top:24px!important;bottom:auto!important}
  #crmBulkSafePanel{max-width:min(780px,calc(100vw - 48px))!important}
}
@media(max-width:760px){
  #crmUnifiedPanel,#crmLineOpsPanel,#crmBulkSafePanel,#crmListWorkbenchPanel,#crmCustomerSmartPanel,#crmInquiryRowPanel,#crmInquiryActionsPanel,#crmFollowTemplatePanel,#crmTemplatePanel,#crmMarketingPanel,#crmCampaignPanel,#crmSourcePanel{inset:8px 8px calc(92px + var(--crm-fix-safe)) 8px!important;width:auto!important;max-width:none!important;max-height:none!important;overflow:auto!important;padding-top:58px!important;border-radius:22px!important}
  .crm-fix-close{top:10px!important;right:10px!important;width:42px!important;height:42px!important;min-height:42px!important}
}
</style>`;

  const script = `<script id="crm-stability-ux-fix-script">
(()=>{if(window.__crmStabilityUxFix)return;window.__crmStabilityUxFix=1;
const PANEL_IDS=['crmUnifiedPanel','crmLineOpsPanel','crmBulkSafePanel','crmListWorkbenchPanel','crmCustomerSmartPanel','crmInquiryRowPanel','crmInquiryActionsPanel','crmFollowTemplatePanel','crmTemplatePanel','crmMarketingPanel','crmCampaignPanel','crmSourcePanel','crmHomeQuickPanel'];
const LEGACY_TEXTS=['統合ログ','リピーターLINE一括','休眠LINE一括','キャンセル後フォロー','問い合わせ対応','フォローLINE','LINE運用','一括確認','一覧操作','顧客スマート','重要','マーケDB','キャンペーン','成約・流入','成約/流入','LINEテンプレ管理','LINEテンプレ','CRM実務強化','納品進捗','予約連携','アラートを開く','監視画面'];
function isVisible(el){const s=getComputedStyle(el);return s.display!=='none'&&s.visibility!=='hidden'&&el.offsetParent!==null}
function closePanel(panel){panel.style.display='none';panel.setAttribute('aria-hidden','true');panel.classList.remove('crm-fix-panel-open');updateScrim()}
function ensureScrim(){let s=document.getElementById('crmFixScrim');if(!s){s=document.createElement('div');s.id='crmFixScrim';s.className='crm-fix-scrim';s.onclick=()=>PANEL_IDS.forEach(id=>{const p=document.getElementById(id);if(p&&isVisible(p))closePanel(p)});document.body.appendChild(s)}return s}
function updateScrim(){const any=PANEL_IDS.some(id=>{const p=document.getElementById(id);return p&&isVisible(p)});ensureScrim().dataset.open=any?'1':'0'}
function addCloseButtons(){PANEL_IDS.forEach(id=>{const p=document.getElementById(id);if(!p)return;if(getComputedStyle(p).position==='static')p.style.position='fixed';p.classList.add('crm-fix-panel-open');if(!p.querySelector(':scope > .crm-fix-close')){const b=document.createElement('button');b.type='button';b.className='crm-fix-close';b.textContent='×';b.setAttribute('aria-label','閉じる');b.onclick=()=>closePanel(p);p.prepend(b)}if(!p.querySelector(':scope > .crm-fix-mini-note')){const note=document.createElement('div');note.className='crm-fix-mini-note';note.textContent='閉じる場合は右上の×、または画面外をタップしてください。';p.appendChild(note)}});updateScrim()}
function cleanupLegacyButtons(){const mainHints=['CRMメニュー','メニュー','＋'];Array.from(document.querySelectorAll('button,a')).forEach(el=>{const text=(el.textContent||'').trim().replace(/\s+/g,' ');if(!text)return;if(mainHints.some(t=>text===t||text.includes(t)))return;if(!LEGACY_TEXTS.some(t=>text===t||text.includes(t)))return;const st=getComputedStyle(el);const r=el.getBoundingClientRect();const fixedish=st.position==='fixed'||st.position==='sticky'||r.left<190||r.right>innerWidth-190;if(fixedish)el.classList.add('crm-legacy-floating-hidden')})}
function rewriteDbError(){document.querySelectorAll('*').forEach(el=>{if(el.childElementCount>0)return;const t=el.textContent||'';if(t.includes('no such table')&&t.includes('crm_reservation_drafts')){el.classList.add('crm-fix-error-note');el.textContent='予約連携テーブルを自動作成しました。画面を更新すると表示されます。'}})}
function patchOpeners(){Array.from(document.querySelectorAll('button,a')).forEach(el=>{if(el.__crmFixOpenPatched)return;el.__crmFixOpenPatched=1;el.addEventListener('click',()=>setTimeout(()=>{addCloseButtons();cleanupLegacyButtons();rewriteDbError()},80),true)})}
function run(){addCloseButtons();cleanupLegacyButtons();rewriteDbError();patchOpeners()}
window.addEventListener('keydown',e=>{if(e.key==='Escape'){PANEL_IDS.forEach(id=>{const p=document.getElementById(id);if(p&&isVisible(p))closePanel(p)})}});
run();new MutationObserver(run).observe(document.body,{childList:true,subtree:true,characterData:true});
})();
</script>`;

  return html.includes("</body>") ? html.replace("</body>", style + script + "</body>") : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    try{
      await ensureReservationDraftSchema(env);
      if(url.pathname === "/health"){
        const res = await app.fetch(request, env, ctx);
        const data = await res.json().catch(()=>({}));
        return json({ ...data, stabilityUxFixBuild: BUILD, reservationDraftSchemaSelfHealing: true });
      }
      const res = await app.fetch(request, env, ctx);
      const ct = res.headers.get("content-type") || "";
      if(request.method === "GET" && ct.includes("text/html")){
        return new Response(injectStabilityUx(await res.text()), { status: res.status, headers: res.headers });
      }
      return res;
    }catch(e){
      return json({ ok:false, build:BUILD, message:String(e && e.message || e) }, 500);
    }
  }
};
