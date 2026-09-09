const BUILD='crm-customer360-exact-edit-handoff-20260909-01';
const MARKER='crm-customer360-exact-edit-handoff-20260909-01';

const SCRIPT=`<script id="${MARKER}">(function(){
if(window.__crmCustomer360ExactEditHandoff20260909)return;
window.__crmCustomer360ExactEditHandoff20260909=1;
function text(v){return v==null?'':String(v).trim()}
function delay(ms){return new Promise(function(resolve){setTimeout(resolve,ms)})}
function cleanUrl(){var u=new URL(location.href);u.searchParams.delete('customer_id');u.searchParams.delete('edit_customer');history.replaceState(null,'',u.pathname+(u.search?'?'+u.searchParams.toString():'')+u.hash)}
async function waitFor(fn,tries){for(var i=0;i<(tries||50);i++){var v=fn();if(v)return v;await delay(60)}return null}
async function run(){var u=new URL(location.href),id=text(u.searchParams.get('customer_id')),edit=u.searchParams.get('edit_customer')==='1';if(!edit||!/^[0-9]{8}$/.test(id))return;
  await waitFor(function(){return window.__crmOwnerView||window.__crmCustomer360UI},60);
  try{window.__crmOwnerView&&window.__crmOwnerView.showCustomers&&window.__crmOwnerView.showCustomers()}catch(_){}
  var search=await waitFor(function(){return document.getElementById('crmGlobalSearch')},50);if(search){search.value=id;search.dispatchEvent(new Event('input',{bubbles:true}));search.dispatchEvent(new Event('change',{bubbles:true}))}
  var opener=await waitFor(function(){return document.querySelector('[data-open="'+id+'"]')||document.querySelector('[data-direct-customer="'+id+'"]')},80);
  if(opener){opener.click()}else{var refresh=window.__crmCustomer360UI&&window.__crmCustomer360UI.refreshList;if(refresh){try{await refresh({force:true})}catch(_){}opener=await waitFor(function(){return document.querySelector('[data-open="'+id+'"]')||document.querySelector('[data-direct-customer="'+id+'"]')},40);if(opener)opener.click()}}
  var editButton=await waitFor(function(){return document.getElementById('crmPeEdit')},80);if(editButton){cleanUrl();editButton.click()}
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',function(){void run()},{once:true});else void run();
})();</script>`;

export function injectCustomer360ExactEditHandoff(html){let out=String(html||'');if(!out||out.includes(MARKER))return out;return out.includes('</body>')?out.replace('</body>',SCRIPT+'</body>'):out+SCRIPT}
export function customer360ExactEditHandoffHealth(){return{customer360_exact_edit_handoff:true,customer360_exact_edit_handoff_build:BUILD,customer360_exact_edit_handoff_customer_id_only:true,customer360_exact_edit_handoff_owner_ui_only:true,customer360_exact_edit_handoff_direct_write:false}}
