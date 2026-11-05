export function injectCustomer360SearchFocus(html){
  if(!html||html.includes('crm-customer360-search-focus'))return html;
  const script=`<script id="crm-customer360-search-focus">(()=>{
    let keepSearchFocus=false,searchSelection=null;
    const search=()=>document.getElementById('crmGlobalSearch');
    const saveSearchSelection=()=>{const el=search();if(!el||document.activeElement!==el)return;try{searchSelection={start:el.selectionStart,end:el.selectionEnd,direction:el.selectionDirection||'none'}}catch(_){}};
    const focusSearch=()=>{if(!keepSearchFocus)return;const el=search();if(!el||document.activeElement===el)return;const saved=searchSelection;el.focus({preventScroll:true});const end=el.value.length;try{el.setSelectionRange(saved?Math.min(saved.start,end):end,saved?Math.min(saved.end,end):end,saved?.direction||'none')}catch(_){}};
    document.addEventListener('focusin',e=>{keepSearchFocus=e.target?.id==='crmGlobalSearch';if(!keepSearchFocus)searchSelection=null},true);
    document.addEventListener('selectionchange',saveSearchSelection,true);
    document.addEventListener('input',e=>{if(e.target?.id==='crmGlobalSearch')saveSearchSelection()},true);
    document.addEventListener('pointerdown',e=>{const t=e.target;if(t?.id==='crmGlobalSearch'||t?.id==='crmSearchClear')return;keepSearchFocus=false},true);
    document.addEventListener('keydown',e=>{if(e.key!=='/'||/INPUT|TEXTAREA|SELECT/.test(document.activeElement?.tagName||''))return;keepSearchFocus=true;searchSelection=null;e.preventDefault();queueMicrotask(focusSearch);setTimeout(focusSearch,80)},true);
    const observer=new MutationObserver(()=>{if(keepSearchFocus)queueMicrotask(focusSearch)});
    const start=()=>observer.observe(document.body,{subtree:true,childList:true});
    if(document.body)start();else document.addEventListener('DOMContentLoaded',start,{once:true});
  })();<\/script>`;
  return html.includes('</body>')?html.replace('</body>',script+'</body>'):html+script;
}
