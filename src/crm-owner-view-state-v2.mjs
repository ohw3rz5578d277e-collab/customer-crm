export function injectOwnerViewState(html){
  if(!html||html.includes('crm-owner-view-state-v2-script'))return html;
  const style=String.raw`<style id="crm-owner-view-state-v2-style">
#crmOwnerLineChat{display:none}
body[data-crm-owner-view="customers"] #crmMktNav,body[data-crm-owner-view="search"] #crmMktNav,body[data-crm-owner-view="marketing"] #crmMktNav,body[data-crm-owner-view="today"] #crmMktNav{display:flex!important}
body[data-crm-owner-view="customers"] #crmMktList,body[data-crm-owner-view="search"] #crmMktList,body[data-crm-owner-view="today"] #crmMktList{display:block!important}
body[data-crm-owner-view="customers"] #crmMktHome,body[data-crm-owner-view="search"] #crmMktHome,body[data-crm-owner-view="today"] #crmMktHome{display:none!important}
body[data-crm-owner-view="marketing"] #crmMktList{display:none!important}
body[data-crm-owner-view="marketing"] #crmMktHome{display:block!important}
body[data-crm-owner-view="line"] #crmMktNav,body[data-crm-owner-view="line"] #crmMktList,body[data-crm-owner-view="line"] #crmMktHome{display:none!important}
body[data-crm-owner-view="line"] #crmOwnerLineChat{display:block!important}
body[data-crm-owner-view="customers"] #crmTodayDashboard,body[data-crm-owner-view="search"] #crmTodayDashboard,body[data-crm-owner-view="marketing"] #crmTodayDashboard,body[data-crm-owner-view="line"] #crmTodayDashboard,body[data-crm-owner-view="today"] #crmTodayDashboard{display:none!important}
</style>`;
  const script=String.raw`<script id="crm-owner-view-state-v2-script">
(()=>{
if(window.__crmOwnerViewStateControllerV2)return;window.__crmOwnerViewStateControllerV2=1;
const $=id=>document.getElementById(id);
const metrics={mutation_callback_count:0,mutation_record_count:0,relevant_mutation_count:0,full_dom_scan_count:0,nav_bind_count:0,nav_duplicate_bind_count:0,observer_active:false,observer_runaway:false};window.__crmOwnerViewMetrics=metrics;
let bootstrapObserver=null,navBound=false;
function markHost(){const list=$('crmMktList'),host=list?.parentElement;if(host&&host.getAttribute('data-crm-owner-host')!=='1')host.setAttribute('data-crm-owner-host','1');return host}
function setActive(name){const key=name==='today'?'customers':name;document.querySelectorAll('#crmOwnerMobileNav [data-owner-tab]').forEach(el=>el.classList.toggle('active',el.dataset.ownerTab===key));document.querySelectorAll('[data-crm-shell-nav]').forEach(el=>el.classList.toggle('active',el.dataset.crmShellNav===key))}
function closeSheets(){for(const sel of ['#crmFilterDrawer','#crmMktDetail','#crmSettingsPanel','#crmOwnerStatusSheet','#crmOwnerCanonicalSettings'])document.querySelector(sel)?.classList.remove('open');document.body.classList.remove('crm-owner-sheet-open','crm-owner-line-nav-open')}
function setView(name){markHost();document.body.dataset.crmOwnerView=name;document.body.classList.remove('crm-owner-view-today');setActive(name);document.dispatchEvent(new CustomEvent('crm:owner-view-change',{detail:{view:name}}));return name}
function settleCustomerViewport(focusSearch=false){let tries=0;const settle=()=>{const list=$('crmMktList');if(!list){if(++tries<20)setTimeout(settle,25);return false}const top=$('crmOwnerWorkspaceHeader')||$('crmOwnerAppShell')||list;top.scrollIntoView({block:'start'});if(focusSearch){const input=$('crmGlobalSearch');if(!input){if(++tries<20)setTimeout(settle,25);return false}input.focus({preventScroll:true});input.scrollIntoView({block:'center'})}return true};if(!settle())requestAnimationFrame(()=>requestAnimationFrame(settle))}
function showCustomers(){closeSheets();setView('customers');window.__crmCustomer360UI?.showList({scroll:false,ownerStateManaged:true});settleCustomerViewport(false);return true}
function showSearch(){closeSheets();setView('search');window.__crmCustomer360UI?.showList({scroll:false,ownerStateManaged:true});settleCustomerViewport(true);return true}
function showToday(){return showCustomers()}
function showMarketing(){closeSheets();setView('marketing');window.__crmCustomer360UI?.showHome?.();scrollTo({top:0});return true}
function showLine(){closeSheets();setView('line');window.__crmOwnerLineChat?.open?.();scrollTo({top:0});return true}
function getCurrentView(){const v=document.body.dataset.crmOwnerView||'customers';return v==='today'?'customers':v}
const api={showToday,showCustomers,showSearch,showMarketing,showLine,getCurrentView,getMetrics:()=>({...metrics})};
function bindNav(){
 const customers=$('crmOwnerNavCustomers'),search=$('crmOwnerNavSearch'),line=$('crmOwnerNavLine'),marketing=$('crmOwnerNavMarketing'),today=$('crmOwnerNavToday');
 if(!customers||!search||!line)return false;
 if(!customers.dataset.ownerBound){customers.onclick=()=>api.showCustomers();customers.dataset.ownerBound='1'}
 if(!search.dataset.ownerBound){search.onclick=()=>api.showSearch();search.dataset.ownerBound='1'}
 if(!line.dataset.ownerBound){line.onclick=()=>api.showLine();line.dataset.ownerBound='1'}
 if(marketing&&!marketing.dataset.ownerBound){marketing.onclick=()=>api.showMarketing();marketing.dataset.ownerBound='1'}
 if(today&&!today.dataset.ownerBound){today.onclick=()=>api.showCustomers();today.dataset.ownerBound='1'}
 if(!navBound){navBound=true;metrics.nav_bind_count=1}
 return true;
}
function hasBootstrapTarget(node){if(!node||node.nodeType!==1)return false;if(node.id==='crmMktList'||node.id==='crmOwnerMobileNav'||node.matches?.('[data-owner-tab]'))return true;return !!node.querySelector?.('#crmMktList,#crmOwnerMobileNav,[data-owner-tab]')}
function stopBootstrapObserver(){if(bootstrapObserver){bootstrapObserver.disconnect();bootstrapObserver=null}metrics.observer_active=false}
function maybeFinishBootstrap(){const ready=!!markHost()&&bindNav();if(ready)stopBootstrapObserver();return ready}
function startBootstrapObserver(){if(bootstrapObserver||maybeFinishBootstrap())return;const root=document.body||document.documentElement;if(!root)return;bootstrapObserver=new MutationObserver(records=>{metrics.mutation_callback_count++;metrics.mutation_record_count+=records.length;if(metrics.mutation_callback_count>30){metrics.observer_runaway=true;stopBootstrapObserver();return}let relevant=false;for(const record of records){for(const node of record.addedNodes){if(hasBootstrapTarget(node)){relevant=true;break}}if(relevant)break}if(!relevant)return;metrics.relevant_mutation_count++;maybeFinishBootstrap()});bootstrapObserver.observe(root,{childList:true,subtree:true});metrics.observer_active=true}
function boot(){window.__crmOwnerView=api;startBootstrapObserver();setTimeout(()=>api.showCustomers(),0)}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot,{once:true});else boot();
})();
<\/script>`;
  return html.includes('</head>')?html.replace('</head>',style+'</head>').replace('</body>',script+'</body>'):style+html+script;
}
