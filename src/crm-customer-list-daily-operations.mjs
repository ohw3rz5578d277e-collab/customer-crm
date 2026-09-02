const DAILY_STYLE_ID='crm-customer-list-daily-operations-style';
const DAILY_SCRIPT_ID='crm-customer-id-autofill-script';

const NEW_ROW=`function dailyRelationship(v){const n=Number(v.shoot_count||0);return n>=2?'リピーター':n===1?'初回利用':'利用前'}
function dailyCanonicalId(v){return /^\\d{8}$/.test(String(v.customer_id||'').trim())}
function dailyIdUi(v){const id=String(v.customer_id||'').trim(),ref=String(v.customer_ref||'').trim();if(dailyCanonicalId(v))return'<div class="crm-mkt-sub crm-customer-id-value">Customer ID '+esc(id)+'</div>';if(id)return'<div class="crm-mkt-sub crm-customer-id-review">顧客ID 要確認</div>';return'<div class="crm-mkt-sub crm-customer-id-missing">顧客ID 未発行</div>'+(ref?'<button type="button" class="crm-id-autofill-btn" data-customer-id-allocate="'+esc(ref)+'">顧客IDを自動生成</button>':'')}
function dailyDetailUi(v){if(dailyCanonicalId(v))return'<button class="crm-mkt-btn" data-open="'+esc(v.customer_id)+'">詳細を見る</button>';return'<div class="crm-id-detail-blocked">顧客IDを先に生成してください</div>'}
function marketingRow(v){return'<tr data-customer-ref="'+esc(v.customer_ref||'')+'"><td data-label="顧客"><div class="crm-mkt-name">'+esc(v.name||'名前未設定')+'</div>'+dailyIdUi(v)+'</td><td data-label="次イベント">'+opp(v.next_opportunity)+'</td><td data-label="実績LTV"><b>'+yen(v.realized_ltv)+'</b></td><td data-label="家族">'+esc(v.family_summary||('子'+Number(v.child_count||0)+'人'))+'</td><td data-label="最終撮影" class="crm-col-low">'+esc(v.last_shoot_date||'—')+'</td><td data-label="地域" class="crm-col-mobile-hide">'+esc(v.area_summary||'—')+'</td><td data-label="LINE"><span class="crm-status '+(v.line_linked?'good':'')+'">'+(v.line_linked?'連携済':'未連携')+'</span></td><td data-label="おすすめ">'+esc(v.recommendation?.next_offer||'—')+'</td><td data-label="操作">'+dailyDetailUi(v)+'</td></tr>'}
function row(v){const rel=dailyRelationship(v);return'<tr class="crm-daily-row" data-customer-ref="'+esc(v.customer_ref||'')+'"><td data-label="顧客"><div class="crm-mkt-name">'+esc(v.name||'名前未設定')+'</div>'+dailyIdUi(v)+'</td><td data-label="関係"><span class="crm-status '+(Number(v.shoot_count||0)>=2?'good':'')+'">'+esc(rel)+'</span></td><td data-label="撮影回数"><b class="crm-daily-count">'+esc(Number(v.shoot_count||0))+'回</b></td><td data-label="累計売上"><b class="crm-daily-ltv">'+yen(v.realized_ltv)+'</b></td><td data-label="最終撮影">'+esc(v.last_shoot_date||'—')+'</td><td data-label="LINE"><span class="crm-status '+(v.line_linked?'good':'')+'">'+(v.line_linked?'連携済':'未連携')+'</span></td><td data-label="次の候補">'+opp(v.next_opportunity)+'</td><td data-label="詳細">'+dailyDetailUi(v)+'</td></tr>'}`;

const OLD_HEADER='<thead><tr><th>顧客</th><th>次イベント</th><th>実績LTV</th><th>家族</th><th>最終撮影</th><th>地域</th><th>LINE</th><th>おすすめ</th><th>操作</th></tr></thead>';
const NEW_HEADER='<thead><tr><th>顧客</th><th>関係</th><th>撮影回数</th><th>累計売上</th><th>最終撮影</th><th>LINE</th><th>次の候補</th><th>詳細</th></tr></thead>';
const OLD_SEARCH='名前・電話・Customer ID・家族名から検索';
const NEW_SEARCH='名前・顧客ID・LINE表示名・電話で検索';

const DAILY_STYLE=`<style id="${DAILY_STYLE_ID}">
.crm-daily-row .crm-daily-count,.crm-daily-row .crm-daily-ltv{font-weight:900;color:#10212d}.crm-daily-row td[data-label="詳細"] .crm-mkt-btn{white-space:nowrap}.crm-daily-row td[data-label="関係"] .crm-status{white-space:nowrap}.crm-customer-id-missing{font-weight:800;color:#9a5b09}.crm-customer-id-review{font-weight:800;color:#a21e1e}.crm-id-autofill-btn{min-height:44px;margin-top:7px;border:1px solid #8ba69a;border-radius:11px;background:#f4fbf7;color:#07583f;padding:8px 11px;font-weight:900;cursor:pointer}.crm-id-autofill-btn:disabled{opacity:.6;cursor:wait}.crm-id-autofill-error{margin-top:6px;color:#a21e1e;font-size:12px;font-weight:700}.crm-id-detail-blocked{font-size:12px;color:#7a5a20;padding:8px 0}
@media(max-width:900px){#crmMktList .crm-daily-row{padding:12px!important}#crmMktList .crm-daily-row td{display:grid!important;grid-template-columns:78px minmax(0,1fr)!important}#crmMktList .crm-daily-row td:first-child{display:block!important;padding:3px 3px 9px!important}#crmMktList .crm-daily-row td:first-child:before{display:none!important}#crmMktList .crm-daily-row .crm-mkt-name{font-size:17px}#crmMktList .crm-daily-row td[data-label="詳細"]{display:block!important;padding-top:9px!important}#crmMktList .crm-daily-row td[data-label="詳細"]:before{display:none!important}#crmMktList .crm-daily-row td[data-label="詳細"] .crm-mkt-btn{width:100%;min-height:44px}#crmMktList .crm-daily-row td[data-label="次の候補"]{padding-top:8px!important}.crm-id-autofill-btn{width:100%;max-width:260px}}
</style>`;

const DAILY_SCRIPT=String.raw`<script id="${DAILY_SCRIPT_ID}">
(()=>{
if(window.__crmCustomerIdAutofillUi)return;window.__crmCustomerIdAutofillUi=1;const inflight=new Set();
function message(row,text){let el=row?.querySelector('.crm-id-autofill-error');if(!text){el?.remove();return}if(!el){el=document.createElement('div');el.className='crm-id-autofill-error';row?.querySelector('td:first-child')?.appendChild(el)}if(el)el.textContent=text}
async function refresh(){try{await window.__crmCustomer360RefreshList?.()}catch(_){}try{await window.__crmCustomer360UI?.refreshList?.({force:true})}catch(_){}}
document.addEventListener('click',async e=>{const b=e.target.closest?.('[data-customer-id-allocate]');if(!b)return;e.preventDefault();e.stopPropagation();const ref=b.dataset.customerIdAllocate||'',row=b.closest('tr');if(!ref||inflight.has(ref))return;inflight.add(ref);message(row,'');const peers=[...document.querySelectorAll('[data-customer-id-allocate]')].filter(x=>x.dataset.customerIdAllocate===ref);for(const x of peers){x.disabled=true;x.dataset.previousLabel=x.textContent;x.textContent='生成中…'}try{const r=await fetch('/api/customer360/customer-id/allocate',{method:'POST',credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'},body:JSON.stringify({customer_ref:ref})});let j={};try{j=await r.json()}catch(_){}if(!r.ok||!j.ok){const review=r.status===409||j.review_required;throw new Error(review?'__REVIEW__':'__FAILED__')}for(const target of document.querySelectorAll('tr[data-customer-ref]'))if(target.dataset.customerRef===ref){const id=target.querySelector('.crm-customer-id-missing,.crm-customer-id-review,.crm-customer-id-value');if(id){id.className='crm-mkt-sub crm-customer-id-value';id.textContent='Customer ID '+j.customer_id}target.querySelectorAll('[data-customer-id-allocate]').forEach(x=>x.remove())}await refresh()}catch(err){message(row,err?.message==='__REVIEW__'?'顧客情報の確認が必要です。既存の顧客IDを確認してください。':'ID発行に失敗しました。もう一度お試しください。');for(const x of peers){if(x.isConnected){x.disabled=false;x.textContent=x.dataset.previousLabel||'顧客IDを自動生成'}}}finally{inflight.delete(ref)}},true);
})();
<\/script>`;

export function injectCustomerListDailyOperations(html){
  let out=String(html||'');
  if(!out||out.includes(DAILY_STYLE_ID))return out;
  if(!out.includes('crm-customer360-marketing-script'))return out;

  const rowStart=out.indexOf('function row(v){');
  const rowEnd=out.indexOf('\nconst QUICK=',rowStart);
  if(rowStart<0||rowEnd<0)return out;
  out=out.slice(0,rowStart)+NEW_ROW+out.slice(rowEnd);

  if(!out.includes(OLD_SEARCH)||!out.includes(OLD_HEADER)||!out.includes("top.map(row).join('')"))return String(html||'');
  out=out.replace(OLD_SEARCH,NEW_SEARCH);
  out=out.replace(OLD_HEADER,NEW_HEADER);
  out=out.replace("top.map(row).join('')","top.map(marketingRow).join('')");
  out=out.replace('function debounceList(){','window.__crmCustomer360RefreshList=()=>requestList();\nfunction debounceList(){');

  if(out.includes('</head>'))out=out.replace('</head>',DAILY_STYLE+'</head>');else out=DAILY_STYLE+out;
  return out.includes('</body>')?out.replace('</body>',DAILY_SCRIPT+'</body>'):out+DAILY_SCRIPT;
}

export const customerListDailyOperationsContract=Object.freeze({
  read_only:false,
  identity_mutation:'missing_customer_id_only',
  production_write:'owner_click_only_after_deploy',
  line_send:false,
  list_priority:['customer','relationship','shoot_count','realized_ltv','last_shoot_date','line','next_candidate','detail']
});
