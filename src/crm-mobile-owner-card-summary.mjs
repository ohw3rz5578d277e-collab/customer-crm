export function injectMobileOwnerCardSummary(html){
  if(!html||html.includes('crm-mobile-owner-card-summary-script'))return html;
  const script=String.raw`<script id="crm-mobile-owner-card-summary-script">
(()=>{
if(window.__crmMobileOwnerCardSummary)return;window.__crmMobileOwnerCardSummary=1;
const mq=matchMedia('(max-width:900px)'),cache=new Map();let busy=false,timer=null;
function requestKey(){const out=new URLSearchParams(),u=new URL(location.href);for(const [k,v] of u.searchParams){if(k.startsWith('crm_'))out.set(k.slice(4),v)}const q=document.getElementById('crmGlobalSearch')?.value||'';if(q)out.set('q',q);const sort=document.getElementById('crmSort')?.value||'recommended';out.set('sort',sort);out.set('page',u.searchParams.get('crm_page')||'1');out.set('page_size','100');return out.toString()}
function apply(items){const byId=new Map((items||[]).map(x=>[String(x.customer_id||''),x]));document.querySelectorAll('#crmMktList [data-open]').forEach(btn=>{const id=btn.getAttribute('data-open')||'',row=btn.closest('tr'),first=row?.querySelector('td:first-child'),item=byId.get(id);if(!first||!item||first.querySelector('.crm-mobile-shoot-count'))return;const el=document.createElement('div');el.className='crm-mkt-sub crm-mobile-shoot-count';el.textContent='撮影 '+Number(item.shoot_count||0)+'回';first.appendChild(el)})}
async function decorate(){timer=null;if(!mq.matches||busy)return;const rows=[...document.querySelectorAll('#crmMktList [data-open]')];if(!rows.length||rows.every(b=>b.closest('tr')?.querySelector('.crm-mobile-shoot-count')))return;const key=requestKey();if(cache.has(key)){apply(cache.get(key));return}busy=true;try{const r=await fetch('/api/customer360/customers?'+key,{cache:'no-store',credentials:'same-origin'}),j=await r.json();if(r.ok&&j.ok){cache.set(key,j.items||[]);apply(j.items||[])}}catch(e){console.error('crm mobile card summary',e)}finally{busy=false}}
function schedule(){clearTimeout(timer);timer=setTimeout(decorate,80)}
let observer=null,callbacks=0;function stop(){if(observer){observer.disconnect();observer=null}}function ready(){return !!document.querySelector('#crmMktList [data-open]')}function relevant(node){if(!node||node.nodeType!==1)return false;if(node.id==='crmMktList'||node.matches?.('[data-open]'))return true;return !!node.querySelector?.('#crmMktList,[data-open]')}function observe(){if(ready())return;const root=document.body||document.documentElement;if(!root)return;observer=new MutationObserver(records=>{callbacks++;if(callbacks>30){stop();return}if(records.some(r=>[...r.addedNodes].some(relevant)))schedule();if(ready()){schedule();stop()}});observer.observe(root,{childList:true,subtree:true})}
document.addEventListener('crm:customer-list-rendered',schedule);if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',()=>{schedule();observe()},{once:true});else{schedule();observe()}mq.addEventListener?.('change',schedule);
})();
<\/script>`;
  return html.includes('</body>')?html.replace('</body>',script+'</body>'):html+script;
}
