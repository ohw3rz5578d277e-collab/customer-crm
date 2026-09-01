const DAILY_STYLE_ID='crm-customer-list-daily-operations-style';

const NEW_ROW=`function dailyRelationship(v){const n=Number(v.shoot_count||0);return n>=2?'リピーター':n===1?'初回利用':'利用前'}
function marketingRow(v){return'<tr><td data-label="顧客"><div class="crm-mkt-name">'+esc(v.name||'名前未設定')+'</div><div class="crm-mkt-sub">ID '+esc(v.customer_id)+'</div></td><td data-label="次イベント">'+opp(v.next_opportunity)+'</td><td data-label="実績LTV"><b>'+yen(v.realized_ltv)+'</b></td><td data-label="家族">'+esc(v.family_summary||('子'+Number(v.child_count||0)+'人'))+'</td><td data-label="最終撮影" class="crm-col-low">'+esc(v.last_shoot_date||'—')+'</td><td data-label="地域" class="crm-col-mobile-hide">'+esc(v.area_summary||'—')+'</td><td data-label="LINE"><span class="crm-status '+(v.line_linked?'good':'')+'">'+(v.line_linked?'連携済':'未連携')+'</span></td><td data-label="おすすめ">'+esc(v.recommendation?.next_offer||'—')+'</td><td data-label="操作"><button class="crm-mkt-btn" data-open="'+esc(v.customer_id)+'">顧客を見る</button></td></tr>'}
function row(v){const rel=dailyRelationship(v);return'<tr class="crm-daily-row"><td data-label="顧客"><div class="crm-mkt-name">'+esc(v.name||'名前未設定')+'</div><div class="crm-mkt-sub">ID '+esc(v.customer_id)+'</div></td><td data-label="関係"><span class="crm-status '+(Number(v.shoot_count||0)>=2?'good':'')+'">'+esc(rel)+'</span></td><td data-label="撮影回数"><b class="crm-daily-count">'+esc(Number(v.shoot_count||0))+'回</b></td><td data-label="累計売上"><b class="crm-daily-ltv">'+yen(v.realized_ltv)+'</b></td><td data-label="最終撮影">'+esc(v.last_shoot_date||'—')+'</td><td data-label="LINE"><span class="crm-status '+(v.line_linked?'good':'')+'">'+(v.line_linked?'連携済':'未連携')+'</span></td><td data-label="次の候補">'+opp(v.next_opportunity)+'</td><td data-label="詳細"><button class="crm-mkt-btn" data-open="'+esc(v.customer_id)+'">詳細を見る</button></td></tr>'}`;

const OLD_HEADER='<thead><tr><th>顧客</th><th>次イベント</th><th>実績LTV</th><th>家族</th><th>最終撮影</th><th>地域</th><th>LINE</th><th>おすすめ</th><th>操作</th></tr></thead>';
const NEW_HEADER='<thead><tr><th>顧客</th><th>関係</th><th>撮影回数</th><th>累計売上</th><th>最終撮影</th><th>LINE</th><th>次の候補</th><th>詳細</th></tr></thead>';
const OLD_SEARCH='名前・電話・Customer ID・家族名から検索';
const NEW_SEARCH='名前・顧客ID・LINE表示名・電話で検索';

const DAILY_STYLE=`<style id="${DAILY_STYLE_ID}">
.crm-daily-row .crm-daily-count,.crm-daily-row .crm-daily-ltv{font-weight:900;color:#10212d}.crm-daily-row td[data-label="詳細"] .crm-mkt-btn{white-space:nowrap}.crm-daily-row td[data-label="関係"] .crm-status{white-space:nowrap}
@media(max-width:767px){.crm-daily-row{padding:12px!important}.crm-daily-row td:first-child{display:block!important;padding:3px 3px 9px!important}.crm-daily-row td:first-child:before{display:none!important}.crm-daily-row .crm-mkt-name{font-size:17px}.crm-daily-row td{grid-template-columns:78px minmax(0,1fr)!important}.crm-daily-row td[data-label="詳細"]{display:block!important;padding-top:9px!important}.crm-daily-row td[data-label="詳細"]:before{display:none!important}.crm-daily-row td[data-label="詳細"] .crm-mkt-btn{width:100%;min-height:44px}.crm-daily-row td[data-label="次の候補"]{padding-top:8px}}
</style>`;

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

  return out.includes('</head>')?out.replace('</head>',DAILY_STYLE+'</head>'):DAILY_STYLE+out;
}

export const customerListDailyOperationsContract=Object.freeze({
  read_only:true,
  identity_mutation:false,
  production_write:false,
  line_send:false,
  list_priority:['customer','relationship','shoot_count','realized_ltv','last_shoot_date','line','next_candidate','detail']
});
