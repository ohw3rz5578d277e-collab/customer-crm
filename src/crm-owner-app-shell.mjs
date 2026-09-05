const RESERVATION_ADMIN_URL='https://reservation-app-api.ohw3rz5578d277e.workers.dev/admin';

export function injectOwnerAppShell(html){
  if(!html||html.includes('crm-owner-app-shell-script'))return html;
  const style=String.raw`<style id="crm-owner-app-shell-style">
:root{--crm-shell-bg:#f4f7f8;--crm-shell-card:#fff;--crm-shell-text:#14212b;--crm-shell-muted:#667983;--crm-shell-line:#dfe7ea;--crm-shell-accent:#0b6b55;--crm-shell-accent-soft:#e9f5f1;--crm-shell-danger:#a61b1b}
html,body{background:var(--crm-shell-bg)!important;color:var(--crm-shell-text)!important}
body.crm-owner-shell-v2{margin:0!important}
#crmOwnerAppShell{min-height:100vh;display:grid;grid-template-columns:236px minmax(0,1fr);background:var(--crm-shell-bg);font-family:-apple-system,BlinkMacSystemFont,"Hiragino Sans","Noto Sans JP",sans-serif}
#crmOwnerDesktopSidebar{position:sticky;top:0;height:100vh;box-sizing:border-box;padding:18px 14px;border-right:1px solid var(--crm-shell-line);background:#fff;display:flex;flex-direction:column;gap:18px;z-index:120}
.crm-shell-brand{padding:4px 8px}.crm-shell-brand b{display:block;font-size:18px;letter-spacing:-.02em}.crm-shell-brand span{display:block;margin-top:4px;color:var(--crm-shell-muted);font-size:11px;font-weight:700}
.crm-shell-nav,.crm-shell-tools{display:grid;gap:5px}.crm-shell-tools{margin-top:auto;padding-top:12px;border-top:1px solid var(--crm-shell-line)}
.crm-shell-nav button,.crm-shell-tools button,.crm-shell-tools a{appearance:none;border:0;background:transparent;color:#42545e;text-decoration:none;display:grid;grid-template-columns:30px minmax(0,1fr);gap:9px;align-items:center;width:100%;min-height:46px;padding:8px 10px;border-radius:12px;text-align:left;font:inherit;font-size:13px;font-weight:800;cursor:pointer;box-sizing:border-box}
.crm-shell-nav button:hover,.crm-shell-tools button:hover,.crm-shell-tools a:hover{background:#f3f7f7}.crm-shell-nav button.active{background:var(--crm-shell-accent-soft);color:var(--crm-shell-accent)}
.crm-shell-icon{width:30px;height:30px;border-radius:9px;background:#eef3f4;display:grid;place-items:center;font-size:13px;font-weight:950}.crm-shell-nav button.active .crm-shell-icon{background:#d9eee7}
#crmOwnerWorkspace{min-width:0}
#crmOwnerWorkspaceHeader{position:sticky;top:0;z-index:110;display:flex;justify-content:space-between;gap:16px;align-items:center;min-height:82px;padding:14px 24px;box-sizing:border-box;background:rgba(244,247,248,.96);backdrop-filter:blur(12px);border-bottom:1px solid rgba(223,231,234,.9)}
.crm-shell-heading{min-width:0}.crm-shell-eyebrow{font-size:10px;font-weight:950;letter-spacing:.12em;color:var(--crm-shell-accent)}#crmOwnerWorkspaceTitle{margin:3px 0 2px;font-size:24px;line-height:1.2;letter-spacing:-.025em}#crmOwnerWorkspaceHint{margin:0;color:var(--crm-shell-muted);font-size:12px;line-height:1.45}
.crm-shell-header-actions{display:flex;gap:7px;flex-wrap:wrap;justify-content:flex-end}.crm-shell-action{appearance:none;border:1px solid var(--crm-shell-line);background:#fff;color:#334852;border-radius:11px;min-height:40px;padding:8px 11px;font:inherit;font-size:12px;font-weight:850;cursor:pointer}
#crmOwnerWorkspaceContent{min-width:0;padding:18px 24px 36px;box-sizing:border-box}
#crmOwnerWorkspaceContent>.app,#crmOwnerWorkspaceContent>.crm-final-shell,#crmOwnerWorkspaceContent>main{width:100%!important;max-width:1440px!important;margin:0 auto!important;padding-left:0!important;padding-right:0!important;box-sizing:border-box!important}
.crm-owner-shell-v2 .app>h1:first-child,.crm-owner-shell-v2 #crmOwnerUtilityBar{display:none!important}
.crm-owner-shell-v2 #lineOpsOpen,.crm-owner-shell-v2 .crm-lineops-fab,.crm-owner-shell-v2 #crmMobileBar,.crm-owner-shell-v2 #crmPriorityFab,.crm-owner-shell-v2 .crmUxQuickHint,.crm-owner-shell-v2 .crm-mf-bottom,.crm-owner-shell-v2 .crm-mf-fab,.crm-owner-shell-v2 .crm-mf-scrolltop,.crm-owner-shell-v2 .crm-bottom-nav,.crm-owner-shell-v2 .crm-top-menu-btn,.crm-owner-shell-v2 .crm-side-menu,.crm-owner-shell-v2 #crmStableAuditBtn,.crm-owner-shell-v2 .crm-stable-audit-btn,.crm-owner-shell-v2 #crmSettingsMenuBtn,.crm-owner-shell-v2 #crmLogoutMenuBtn,.crm-owner-shell-v2 #crmUxFab{display:none!important;visibility:hidden!important;pointer-events:none!important}
.crm-owner-shell-v2 #crmMktNav{margin:0 0 14px!important;padding:6px!important;border:1px solid var(--crm-shell-line)!important;border-radius:14px!important;background:#fff!important;box-shadow:none!important}
.crm-owner-shell-v2 #crmMktNav .crm-mkt-btn{min-height:40px!important;border:0!important;background:transparent!important;color:#526872!important;border-radius:10px!important}.crm-owner-shell-v2 #crmMktNav .crm-mkt-btn.primary{background:var(--crm-shell-accent-soft)!important;color:var(--crm-shell-accent)!important}
.crm-owner-shell-v2 #crmTodayDashboard,.crm-owner-shell-v2 #crmReservationStatus,.crm-owner-shell-v2 #crmDeliveryDeadlinePanel,.crm-owner-shell-v2 #crmLegacyOperations,.crm-owner-shell-v2 #crmGrowthPanel,.crm-owner-shell-v2 #crmMktList,.crm-owner-shell-v2 #crmMktHome{box-shadow:none!important;border-color:var(--crm-shell-line)!important}
.crm-owner-shell-v2 .crm-mkt-focus,.crm-owner-shell-v2 .crm-mkt-next,.crm-owner-shell-v2 .crm-period-analytics,.crm-owner-shell-v2 .crm-approach-queue,.crm-owner-shell-v2 .crm-search-head{box-shadow:none!important;border-color:var(--crm-shell-line)!important}
.crm-owner-shell-v2 .crm-mkt-btn,.crm-owner-shell-v2 .crm-chip{touch-action:manipulation}
@media(max-width:900px){
 #crmOwnerAppShell{display:block;min-height:auto}
 #crmOwnerDesktopSidebar{display:none!important}
 #crmOwnerWorkspaceHeader{top:0;min-height:68px;padding:10px 12px;gap:8px}
 #crmOwnerWorkspaceTitle{font-size:19px}.crm-shell-eyebrow{font-size:9px}#crmOwnerWorkspaceHint{font-size:11px;max-width:60vw;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
 .crm-shell-header-actions{gap:5px;flex-wrap:nowrap}.crm-shell-action{min-height:38px;padding:7px 9px;font-size:11px;white-space:nowrap}
 #crmOwnerWorkspaceContent{padding:10px 10px calc(88px + env(safe-area-inset-bottom))}
 .crm-owner-shell-v2 #crmMktNav{position:sticky;top:69px;z-index:90;display:grid!important;grid-template-columns:1fr 1fr!important;gap:5px!important}
 .crm-owner-shell-v2 #crmMktNav .crm-mkt-btn{width:100%!important;padding:8px 7px!important;font-size:12px!important}
 .crm-owner-shell-v2 .crm-list-toolbar{grid-template-columns:1fr!important;gap:8px!important}
 .crm-owner-shell-v2 .crm-list-toolbar>div{display:grid!important;grid-template-columns:minmax(0,1fr) minmax(0,1fr)!important;gap:8px!important;overflow:visible!important}
 .crm-owner-shell-v2 .crm-list-toolbar select,.crm-owner-shell-v2 .crm-list-toolbar .crm-mkt-btn,.crm-owner-shell-v2 #crmSort{width:100%!important;max-width:none!important;min-width:0!important}
 .crm-owner-shell-v2 #crmSort{grid-column:1/-1!important}
 .crm-owner-shell-v2 #crmTodayDashboard,.crm-owner-shell-v2 #crmReservationStatus,.crm-owner-shell-v2 #crmDeliveryDeadlinePanel,.crm-owner-shell-v2 #crmLegacyOperations,.crm-owner-shell-v2 #crmGrowthPanel{margin-left:0!important;margin-right:0!important;border-radius:15px!important}
}
</style>`;
  const script=String.raw`<script id="crm-owner-app-shell-script">
(()=>{
if(window.__crmOwnerAppShell)return;window.__crmOwnerAppShell=1;
const RES_URL=${JSON.stringify(RESERVATION_ADMIN_URL)};
const $=id=>document.getElementById(id);
const META={
 today:{title:'今日やること',hint:'今日の対応・予約状況・納品期限をここで確認します。'},
 customers:{title:'顧客',hint:'顧客を一覧から探して、情報・履歴・家族情報を確認します。'},
 search:{title:'顧客を検索',hint:'名前・電話・Customer ID・家族情報からすぐに探せます。'},
 marketing:{title:'分析・アプローチ',hint:'期間分析と連絡候補を確認します。自動送信は行いません。'},
 line:{title:'LINE',hint:'LINEの送信・反応管理を確認します。'}
};
function esc(v){return String(v==null?'':v).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}
function setShellActive(view){
 const key=META[view]?view:(view==='base'?'today':view);
 document.querySelectorAll('[data-crm-shell-nav]').forEach(el=>el.classList.toggle('active',el.dataset.crmShellNav===key));
 const m=META[key]||META.today,title=$('crmOwnerWorkspaceTitle'),hint=$('crmOwnerWorkspaceHint');
 if(title)title.textContent=m.title;if(hint)hint.textContent=m.hint;
}
function openView(view){
 if(view==='today')return window.__crmOwnerView?.showToday?.();
 if(view==='customers')return window.__crmOwnerView?.showCustomers?.();
 if(view==='search')return window.__crmOwnerView?.showSearch?.();
 if(view==='marketing')return window.__crmOwnerView?.showMarketing?.()??window.__crmCustomer360UI?.showHome?.();
 if(view==='line')return window.__crmOwnerView?.showLine?.();
}
function bindShell(){
 document.querySelectorAll('[data-crm-shell-nav]').forEach(el=>{if(el.tagName==='A')return;el.onclick=()=>openView(el.dataset.crmShellNav)});
 $('crmShellStatus')?.addEventListener('click',()=>window.__crmOwnerMobileInteraction?.showStatus?.());
 $('crmShellSettings')?.addEventListener('click',()=>window.__crmOwnerMobileInteraction?.showSettings?.());
}
function ensureShell(){
 if($('crmOwnerAppShell'))return true;
 const host=document.querySelector('.app')||document.querySelector('.crm-final-shell')||document.querySelector('main');
 if(!host||!host.parentNode)return false;
 const shell=document.createElement('div');shell.id='crmOwnerAppShell';
 shell.innerHTML='<aside id="crmOwnerDesktopSidebar"><div class="crm-shell-brand"><b>顧客管理</b><span>予約 × Customer360</span></div><nav class="crm-shell-nav" aria-label="顧客管理メインメニュー"><button type="button" data-crm-shell-nav="today"><span class="crm-shell-icon">今</span><span>今日やること</span></button><button type="button" data-crm-shell-nav="customers"><span class="crm-shell-icon">顧</span><span>顧客</span></button><button type="button" data-crm-shell-nav="search"><span class="crm-shell-icon">探</span><span>顧客を検索</span></button><button type="button" data-crm-shell-nav="marketing"><span class="crm-shell-icon">析</span><span>分析・アプローチ</span></button><button type="button" data-crm-shell-nav="line"><span class="crm-shell-icon">L</span><span>LINE</span></button></nav><div class="crm-shell-tools"><a data-crm-shell-nav="reservation" href="'+esc(RES_URL)+'" target="_blank" rel="noopener noreferrer"><span class="crm-shell-icon">予</span><span>予約管理アプリ</span></a><button id="crmShellStatus" type="button"><span class="crm-shell-icon">✓</span><span>システム状態</span></button><button id="crmShellSettings" type="button"><span class="crm-shell-icon">設</span><span>設定</span></button></div></aside><section id="crmOwnerWorkspace"><header id="crmOwnerWorkspaceHeader"><div class="crm-shell-heading"><div class="crm-shell-eyebrow">CUSTOMER CRM</div><h1 id="crmOwnerWorkspaceTitle">今日やること</h1><p id="crmOwnerWorkspaceHint">今日の対応・予約状況・納品期限をここで確認します。</p></div><div class="crm-shell-header-actions"><button id="crmShellStatusTop" class="crm-shell-action" type="button">状態確認</button><button id="crmShellSettingsTop" class="crm-shell-action crm-shell-settings-action" type="button">設定</button></div></header><div id="crmOwnerWorkspaceContent"></div></section>';
 host.parentNode.insertBefore(shell,host);$('crmOwnerWorkspaceContent').appendChild(host);
 $('crmShellStatusTop').onclick=()=>window.__crmOwnerMobileInteraction?.showStatus?.();
 $('crmShellSettingsTop').onclick=()=>window.__crmOwnerMobileInteraction?.showSettings?.();
 bindShell();document.body.classList.add('crm-owner-shell-v2');
 setShellActive(document.body.dataset.crmOwnerView||'today');
 return true;
}
let tries=0;function boot(){if(ensureShell())return;if(++tries<40)requestAnimationFrame(boot)}
document.addEventListener('crm:owner-view-change',e=>setShellActive(e.detail?.view||document.body.dataset.crmOwnerView||'today'));
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot,{once:true});else boot();
window.__crmOwnerAppShellApi={openView,setShellActive,ensureShell};
})();
<\/script>`;
  return html.includes('</head>')?html.replace('</head>',style+'</head>').replace('</body>',script+'</body>'):style+html+script;
}
