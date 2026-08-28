export function injectCustomer360SearchFocus(html){
  if(!html||html.includes('crm-customer360-search-focus'))return html;
  const script=`<script id="crm-customer360-search-focus">(()=>{
    let keepSearchFocus=false;
    const search=()=>document.getElementById('crmGlobalSearch');
    const focusSearch=()=>{const el=search();if(!el)return;el.focus({preventScroll:true});const end=el.value.length;try{el.setSelectionRange(end,end)}catch(_){}};
    document.addEventListener('focusin',e=>{if(e.target?.id==='crmGlobalSearch')keepSearchFocus=true},true);
    document.addEventListener('pointerdown',e=>{const t=e.target;if(t?.id==='crmGlobalSearch'||t?.id==='crmSearchClear')return;keepSearchFocus=false},true);
    document.addEventListener('keydown',e=>{if(e.key!=='/'||/INPUT|TEXTAREA|SELECT/.test(document.activeElement?.tagName||''))return;keepSearchFocus=true;e.preventDefault();queueMicrotask(focusSearch);setTimeout(focusSearch,80)},true);
    const observer=new MutationObserver(()=>{if(keepSearchFocus)queueMicrotask(focusSearch)});
    const start=()=>observer.observe(document.body,{subtree:true,childList:true});
    if(document.body)start();else document.addEventListener('DOMContentLoaded',start,{once:true});
  })();<\/script>`;
  return html.includes('</body>')?html.replace('</body>',script+'</body>'):html+script;
}
