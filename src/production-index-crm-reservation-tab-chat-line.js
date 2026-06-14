// CUSTOMER CRM / RESERVATION TAB + LINE CHAT STYLE WRAPPER
// Adds Reservation tab to bottom nav and renders customer LINE logs as chat bubbles.
import app from "./production-index-crm-clean-mobile-ux-v3.js";

const RESERVATION_ADMIN_URL = "https://reservation-app-api.ohw3rz5578d277e.workers.dev/admin";

function inject(html){
  if(!html || html.includes('crm-reservation-chat-line-script')) return html;
  const style = `<style id="crm-reservation-chat-line-style">
.crm-line-chat-box{display:flex!important;flex-direction:column!important;gap:10px!important;padding:10px!important;border:1px solid #dbe5ef!important;border-radius:18px!important;background:#f1f5f9!important}.crm-line-bubble{max-width:82%!important;padding:10px 12px!important;border-radius:18px!important;font-size:13px!important;line-height:1.55!important;box-shadow:0 3px 10px rgba(15,23,42,.05)!important;word-break:break-word!important}.crm-line-bubble.out{align-self:flex-end!important;background:#06c755!important;color:#fff!important;border-bottom-right-radius:5px!important}.crm-line-bubble.in{align-self:flex-start!important;background:#fff!important;color:#07111f!important;border-bottom-left-radius:5px!important}.crm-line-time{display:block!important;font-size:10px!important;opacity:.72!important;margin-top:4px!important}.crm-line-empty{padding:14px!important;border:1px dashed #cbd5e1!important;border-radius:16px!important;color:#64748b!important;background:#fff!important}@media(max-width:767px){#crmInstaNav{height:76px!important;padding:7px 6px!important;gap:2px!important}#crmInstaNav .crm-insta-tab{height:60px!important;font-size:10px!important;border-radius:18px!important}#crmInstaNav .crm-insta-icon{font-size:20px!important}.crm-line-bubble{max-width:88%!important;font-size:13px!important}}
</style>`;
  const script = `<script id="crm-reservation-chat-line-script">
(()=>{
 if(window.__crmReservationChatLine) return; window.__crmReservationChatLine=1;
 const RES_URL=${JSON.stringify(RESERVATION_ADMIN_URL)};
 const qs=(s,r=document)=>r.querySelector(s); const qsa=(s,r=document)=>Array.from(r.querySelectorAll(s));
 const esc=v=>String(v==null?'':v).replace(/[&<>\"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[c]||c));
 function openReservation(){ window.open(RES_URL,'_blank','noopener'); }
 function setActive(name){ qsa('#crmInstaNav .crm-insta-tab').forEach(b=>b.classList.toggle('active',b.dataset.tab===name)); }
 function patchReservationTab(){
   const nav=qs('#crmInstaNav'); if(!nav) return;
   if(qs('[data-tab="reservation"]',nav)) return;
   const btn=document.createElement('button'); btn.type='button'; btn.className='crm-insta-tab'; btn.dataset.tab='reservation';
   btn.innerHTML='<span class="crm-insta-icon">📅</span><span>予約</span>'; btn.onclick=()=>{setActive('reservation');openReservation();};
   const settings=qs('[data-tab="settings"]',nav); if(settings) nav.insertBefore(btn,settings); else nav.appendChild(btn);
 }
 function textFromLog(r){ return r.message_text||r.message||r.text||r.body||r.memo||r.action_label||r.status||''; }
 function timeFromLog(r){ return r.sent_at||r.replied_at||r.copied_at||r.created_at||r.updated_at||''; }
 function isIncoming(r){ const s=String(r.direction||r.message_type||r.action_type||r.status||'').toLowerCase(); return /in|reply|replied|receive|user/.test(s); }
 function renderChat(logs){
   if(!logs||!logs.length) return '<div class="crm-line-empty">LINE履歴はまだありません。</div>';
   return '<div class="crm-line-chat-box">'+logs.map(r=>{const dir=isIncoming(r)?'in':'out'; const msg=esc(textFromLog(r)||'LINEメモ'); const tm=esc(timeFromLog(r)); return '<div class="crm-line-bubble '+dir+'">'+msg+(tm?'<span class="crm-line-time">'+tm+'</span>':'')+'</div>';}).join('')+'</div>';
 }
 async function fetchDetail(id){ const res=await fetch('/api/stable-customer-detail?id='+encodeURIComponent(id),{cache:'no-store'}); return await res.json(); }
 function enhanceDetailPanel(id){
   setTimeout(async()=>{
     const panel=qs('#crmV2DetailPanel.open'); const body=qs('#crmV2DetailBody'); if(!panel||!body||!id) return;
     try{
       const j=await fetchDetail(id); const logs=j.line_logs||[];
       const sections=qsa('.crm-v2-section',body); const lineSection=sections.find(s=>/LINE履歴/.test(s.textContent||''));
       if(lineSection){ lineSection.innerHTML='<h3>LINE履歴</h3>'+renderChat(logs); }
     }catch(e){}
   },450);
 }
 function patchCustomerOpen(){
   qsa('.crm-stable-customer-card').forEach(card=>{
     const id=card.getAttribute('data-customer-id')||'';
     if(card.dataset.chatLinePatch==='1') return; card.dataset.chatLinePatch='1';
     card.addEventListener('click',()=>enhanceDetailPanel(id),true);
     const btn=qs('.crm-v2-card-detail-btn',card); if(btn) btn.addEventListener('click',()=>enhanceDetailPanel(id),true);
   });
 }
 function patchGlobalDetail(){
   if(window.__crmOpenCustomerDetailV2 && !window.__crmOpenCustomerDetailV2ChatWrapped){
     const old=window.__crmOpenCustomerDetailV2;
     window.__crmOpenCustomerDetailV2=function(id){ const r=old(id); enhanceDetailPanel(id); return r; };
     window.__crmOpenCustomerDetailV2ChatWrapped=1;
   }
 }
 function boot(){ patchReservationTab(); patchCustomerOpen(); patchGlobalDetail(); }
 if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',boot); else boot();
 new MutationObserver(()=>setTimeout(boot,120)).observe(document.documentElement,{childList:true,subtree:true});
})();
</script>`;
  return html.includes('</head>') ? html.replace('</head>',style+'</head>').replace('</body>',script+'</body>') : html+style+script;
}
export default{async fetch(request,env,ctx){const res=await app.fetch(request,env,ctx);const ct=res.headers.get('content-type')||'';if(request.method==='GET'&&ct.includes('text/html')){const html=await res.text();return new Response(inject(html),{status:res.status,headers:res.headers})}return res}};
