export function injectOwnerViewState(html){
  if(!html||html.includes('crm-owner-view-state-script'))return html;
  const style=String.raw`<style id="crm-owner-view-state-style">
body[data-crm-owner-view="today"] #crmMktNav,body[data-crm-owner-view="today"] #crmMktList,body[data-crm-owner-view="today"] #crmMktHome{display:none!important}
body[data-crm-owner-view="customers"] [data-crm-owner-host="1"]>:not(#crmMktNav):not(#crmMktList):not(#crmMktHome),body[data-crm-owner-view="search"] [data-crm-owner-host="1"]>:not(#crmMktNav):not(#crmMktList):not(#crmMktHome){display:none!important}
body[data-crm-owner-view="customers"] #crmMktHome,body[data-crm-owner-view="search"] #crmMktHome{display:none!important}
body[data-crm-owner-view="customers"] #crmMktNav,body[data-crm-owner-view="search"] #crmMktNav{display:flex!important}
body[data-crm-owner-view="customers"] #crmMktList,body[data-crm-owner-view="search"] #crmMktList{display:block!important}
body[data-crm-owner-view="line"] #crmMktNav,body[data-crm-owner-view="line"] #crmMktList,body[data-crm-owner-view="line"] #crmMktHome{display:none!important}
</style>`;
  const script=String.raw`<script id="crm-owner-view-state-script">
(()=>{
if(window.__crmOwnerViewStateController)return;window.__crmOwnerViewStateController=1;
const $=id=>document.getElementById(id);const mobile=()=>matchMedia('(max-width:900px)').matches;
function markHost(){const list=$('crmMktList'),host=list?.parentElement;if(host)host.setAttribute('data-crm-owner-host','1');return host}
function setActive(name){document.querySelectorAll('#crmOwnerMobileNav [data-owner-tab]').forEach(el=>el.classList.toggle('active',el.dataset.ownerTab===name))}
function closeSheets(){for(const sel of ['#crmFilterDrawer','#crmMktDetail','#crmSettingsPanel','#lineOpsPanel','#crmOwnerStatusSheet'])document.querySelector(sel)?.classList.remove('open');$('crmTodayFilterPanel')?.classList.remove('crm-owner-quick-filter-open');document.body.classList.remove('crm-owner-sheet-open','crm-owner-line-nav-open')}
function setView(name){markHost();document.body.dataset.crmOwnerView=name;document.body.classList.toggle('crm-owner-view-today',name==='today');setActive(name);return name}
function settleCustomerViewport(focusSearch=false){let tries=0;const settle=()=>{markHost();const heading=$('crmOwnerCustomerTitle')||$('crmMktList')?.querySelector('h2,.crm-search-head')||$('crmMktList');if(heading){heading.scrollIntoView({block:'start'});if(focusSearch){const input=$('crmGlobalSearch');if(input){input.focus({preventScroll:true});input.scrollIntoView({block:'center'});return}}if(!focusSearch)return}if(++tries<20)setTimeout(settle,25)};requestAnimationFrame(()=>requestAnimationFrame(settle))}
function showCustomers(){closeSheets();setView('customers');window.__crmCustomer360UI?.showList({scroll:false,ownerStateManaged:true});settleCustomerViewport(false);window.__crmOwnerMobileInteraction?.syncSheetState?.();return true}
function showSearch(){closeSheets();setView('search');window.__crmCustomer360UI?.showList({scroll:false,ownerStateManaged:true});settleCustomerViewport(true);window.__crmOwnerMobileInteraction?.syncSheetState?.();return true}
function showToday(options={}){closeSheets();setView('today');if(options.scroll!==false){requestAnimationFrame(()=>{const dash=$('crmTodayDashboard');if(dash)dash.scrollIntoView({block:'start'});else scrollTo({top:0})})}window.__crmOwnerMobileInteraction?.syncSheetState?.();return true}
function showLine(){closeSheets();setView('line');const open=$('lineOpsOpen'),panel=$('lineOpsPanel');if(open)open.click();else panel?.classList.add('open');document.body.classList.toggle('crm-owner-line-nav-open',!!panel&&panel.classList.contains('open'));window.__crmOwnerMobileInteraction?.syncSheetState?.();return true}
function getCurrentView(){return document.body.dataset.crmOwnerView||'base'}
const api={showToday,showCustomers,showSearch,showLine,getCurrentView};
function bindNav(){const today=$('crmOwnerNavToday'),customers=$('crmOwnerNavCustomers'),search=$('crmOwnerNavSearch'),line=$('crmOwnerNavLine');if(today)today.onclick=()=>api.showToday();if(customers)customers.onclick=()=>api.showCustomers();if(search)search.onclick=()=>api.showSearch();if(line)line.onclick=()=>api.showLine();if(window.__crmOwnerMobileInteraction){window.__crmOwnerMobileInteraction.showToday=()=>api.showToday();window.__crmOwnerMobileInteraction.showCustomers=()=>api.showCustomers();window.__crmOwnerMobileInteraction.showSearch=()=>api.showSearch();window.__crmOwnerMobileInteraction.showLine=()=>api.showLine()}}
function boot(){window.__crmOwnerView=api;markHost();bindNav();if(mobile())api.showToday({scroll:false});new MutationObserver(()=>{markHost();bindNav()}).observe(document.documentElement,{childList:true,subtree:true});window.addEventListener('resize',()=>{markHost();bindNav()},{passive:true})}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot,{once:true});else boot();
})();
<\/script>`;
  return html.includes('</head>')?html.replace('</head>',style+'</head>').replace('</body>',script+'</body>'):style+html+script;
}
