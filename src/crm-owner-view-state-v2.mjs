export function injectOwnerViewState(html){
  if(!html||html.includes('crm-owner-view-state-v2-script'))return html;
  const style=String.raw`<style id="crm-owner-view-state-v2-style">
body[data-crm-owner-view="today"] #crmMktNav,body[data-crm-owner-view="today"] #crmMktList,body[data-crm-owner-view="today"] #crmMktHome{display:none!important}
body[data-crm-owner-view="customers"] [data-crm-owner-host="1"]>:not(#crmMktNav):not(#crmMktList):not(#crmMktHome),
body[data-crm-owner-view="search"] [data-crm-owner-host="1"]>:not(#crmMktNav):not(#crmMktList):not(#crmMktHome),
body[data-crm-owner-view="marketing"] [data-crm-owner-host="1"]>:not(#crmMktNav):not(#crmMktList):not(#crmMktHome){display:none!important}
body[data-crm-owner-view="customers"] #crmTodayDashboard,body[data-crm-owner-view="search"] #crmTodayDashboard,body[data-crm-owner-view="marketing"] #crmTodayDashboard,
body[data-crm-owner-view="customers"] #crmReservationStatus,body[data-crm-owner-view="search"] #crmReservationStatus,body[data-crm-owner-view="marketing"] #crmReservationStatus,
body[data-crm-owner-view="customers"] #crmDeliveryDeadlinePanel,body[data-crm-owner-view="search"] #crmDeliveryDeadlinePanel,body[data-crm-owner-view="marketing"] #crmDeliveryDeadlinePanel,
body[data-crm-owner-view="customers"] #crmLegacyOperations,body[data-crm-owner-view="search"] #crmLegacyOperations,body[data-crm-owner-view="marketing"] #crmLegacyOperations,
body[data-crm-owner-view="customers"] #crmTodayFilterPanel,body[data-crm-owner-view="search"] #crmTodayFilterPanel,body[data-crm-owner-view="marketing"] #crmTodayFilterPanel,
body[data-crm-owner-view="customers"] #crmGrowthPanel,body[data-crm-owner-view="search"] #crmGrowthPanel,body[data-crm-owner-view="marketing"] #crmGrowthPanel,
body[data-crm-owner-view="customers"] .crm-growth-panel,body[data-crm-owner-view="search"] .crm-growth-panel,body[data-crm-owner-view="marketing"] .crm-growth-panel,
body[data-crm-owner-view="customers"] .crm-today-dash,body[data-crm-owner-view="search"] .crm-today-dash,body[data-crm-owner-view="marketing"] .crm-today-dash,
body[data-crm-owner-view="customers"] .crm-delivery-panel,body[data-crm-owner-view="search"] .crm-delivery-panel,body[data-crm-owner-view="marketing"] .crm-delivery-panel,
body[data-crm-owner-view="customers"] [data-crm-owner-today],body[data-crm-owner-view="search"] [data-crm-owner-today],body[data-crm-owner-view="marketing"] [data-crm-owner-today],
body[data-crm-owner-view="customers"] [data-crm-owner-operational-section],body[data-crm-owner-view="search"] [data-crm-owner-operational-section],body[data-crm-owner-view="marketing"] [data-crm-owner-operational-section]{display:none!important}
body[data-crm-owner-view="customers"] #crmMktNav,body[data-crm-owner-view="search"] #crmMktNav,body[data-crm-owner-view="marketing"] #crmMktNav{display:flex!important}\nbody[data-crm-owner-view="customers"] #crmOwnerCustomerTitle,body[data-crm-owner-view="search"] #crmOwnerCustomerTitle{display:block!important}
body[data-crm-owner-view="customers"] #crmMktList,body[data-crm-owner-view="search"] #crmMktList{display:block!important}
body[data-crm-owner-view="customers"] #crmMktHome,body[data-crm-owner-view="search"] #crmMktHome{display:none!important}
body[data-crm-owner-view="marketing"] #crmMktList{display:none!important}
body[data-crm-owner-view="marketing"] #crmMktHome{display:block!important}
body[data-crm-owner-view="line"] #crmMktNav,body[data-crm-owner-view="line"] #crmMktList,body[data-crm-owner-view="line"] #crmMktHome{display:none!important}
</style>`;
  const script=String.raw`<script id="crm-owner-view-state-v2-script">
(()=>{
if(window.__crmOwnerViewStateControllerV2)return;window.__crmOwnerViewStateControllerV2=1;
const $=id=>document.getElementById(id);
const metrics={mutation_callback_count:0,mutation_record_count:0,relevant_mutation_count:0,full_dom_scan_count:0,nav_bind_count:0,nav_duplicate_bind_count:0,observer_active:false,observer_runaway:false};window.__crmOwnerViewMetrics=metrics;
let bootstrapObserver=null,navBound=false;
function markHost(){const list=$('crmMktList'),host=list?.parentElement;if(host&&host.getAttribute('data-crm-owner-host')!=='1')host.setAttribute('data-crm-owner-host','1');return host}
function setActive(name){document.querySelectorAll('#crmOwnerMobileNav [data-owner-tab]').forEach(el=>el.classList.toggle('active',el.dataset.ownerTab===name));document.querySelectorAll('[data-crm-shell-nav]').forEach(el=>el.classList.toggle('active',el.dataset.crmShellNav===name))}
function closeSheets(){for(const sel of ['#crmFilterDrawer','#crmMktDetail','#crmSettingsPanel','#lineOpsPanel','#crmOwnerStatusSheet'])document.querySelector(sel)?.classList.remove('open');$('crmTodayFilterPanel')?.classList.remove('crm-owner-quick-filter-open');document.body.classList.remove('crm-owner-sheet-open','crm-owner-line-nav-open')}
function setView(name){markHost();document.body.dataset.crmOwnerView=name;document.body.classList.toggle('crm-owner-view-today',name==='today');setActive(name);document.dispatchEvent(new CustomEvent('crm:owner-view-change',{detail:{view:name}}));return name}
function settleCustomerViewport(focusSearch=false){const settle=()=>{const heading=$('crmOwnerCustomerTitle')||$('crmMktList')?.querySelector('h2,.crm-search-head')||$('crmMktList');if(!heading)return false;heading.scrollIntoView({block:'start'});if(focusSearch){const input=$('crmGlobalSearch');if(!input)return false;input.focus({preventScroll:true});input.scrollIntoView({block:'center'})}return true};if(!settle())requestAnimationFrame(settle)}
function showCustomers(){closeSheets();setView('customers');window.__crmCustomer360UI?.showList({scroll:false,ownerStateManaged:true});settleCustomerViewport(false);window.__crmOwnerMobileInteraction?.syncSheetState?.();return true}
function showSearch(){closeSheets();setView('search');window.__crmCustomer360UI?.showList({scroll:false,ownerStateManaged:true});settleCustomerViewport(true);window.__crmOwnerMobileInteraction?.syncSheetState?.();return true}
function showToday(options={}){closeSheets();setView('today');if(options.scroll!==false){const dash=$('crmTodayDashboard');if(dash)dash.scrollIntoView({block:'start'});else scrollTo({top:0})}window.__crmOwnerMobileInteraction?.syncSheetState?.();return true}
function showMarketing(){closeSheets();setView('marketing');window.__crmCustomer360UI?.showHome?.();scrollTo({top:0});window.__crmOwnerMobileInteraction?.syncSheetState?.();return true}
function showLine(){closeSheets();setView('line');const open=$('lineOpsOpen'),panel=$('lineOpsPanel');if(open)open.click();else panel?.classList.add('open');document.body.classList.toggle('crm-owner-line-nav-open',!!panel&&panel.classList.contains('open'));window.__crmOwnerMobileInteraction?.syncSheetState?.();return true}
function getCurrentView(){return document.body.dataset.crmOwnerView||'today'}
const api={showToday,showCustomers,showSearch,showMarketing,showLine,getCurrentView,getMetrics:()=>({...metrics})};
function bindNav(){if(navBound){metrics.nav_duplicate_bind_count++;return true}const today=$('crmOwnerNavToday'),customers=$('crmOwnerNavCustomers'),search=$('crmOwnerNavSearch'),line=$('crmOwnerNavLine');if(!today||!customers||!search||!line)return false;today.onclick=()=>api.showToday();customers.onclick=()=>api.showCustomers();search.onclick=()=>api.showSearch();line.onclick=()=>api.showLine();if(window.__crmOwnerMobileInteraction){window.__crmOwnerMobileInteraction.showToday=()=>api.showToday();window.__crmOwnerMobileInteraction.showCustomers=()=>api.showCustomers();window.__crmOwnerMobileInteraction.showSearch=()=>api.showSearch();window.__crmOwnerMobileInteraction.showLine=()=>api.showLine()}navBound=true;metrics.nav_bind_count=1;return true}
function hasBootstrapTarget(node){if(!node||node.nodeType!==1)return false;if(node.id==='crmMktList'||node.id==='crmOwnerMobileNav'||node.matches?.('[data-owner-tab]'))return true;return !!node.querySelector?.('#crmMktList,#crmOwnerMobileNav,[data-owner-tab]')}
function stopBootstrapObserver(){if(bootstrapObserver){bootstrapObserver.disconnect();bootstrapObserver=null}metrics.observer_active=false}
function maybeFinishBootstrap(){const ready=!!markHost()&&bindNav();if(ready)stopBootstrapObserver();return ready}
function startBootstrapObserver(){if(bootstrapObserver||maybeFinishBootstrap())return;const root=document.body||document.documentElement;if(!root)return;bootstrapObserver=new MutationObserver(records=>{metrics.mutation_callback_count++;metrics.mutation_record_count+=records.length;if(metrics.mutation_callback_count>20){metrics.observer_runaway=true;stopBootstrapObserver();return}let relevant=false;for(const record of records){for(const node of record.addedNodes){if(hasBootstrapTarget(node)){relevant=true;break}}if(relevant)break}if(!relevant)return;metrics.relevant_mutation_count++;maybeFinishBootstrap()});bootstrapObserver.observe(root,{childList:true,subtree:true});metrics.observer_active=true}
function boot(){window.__crmOwnerView=api;startBootstrapObserver();api.showToday({scroll:false})}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot,{once:true});else boot();
})();
<\/script>`;
  return html.includes('</head>')?html.replace('</head>',style+'</head>').replace('</body>',script+'</body>'):style+html+script;
}
