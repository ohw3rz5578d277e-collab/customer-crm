import { buildCustomerMarketingModel, filterSortCustomers, loadMarketingSource } from './crm-marketing-read-model.mjs';
import { marketingRulesHealth } from './crm-marketing-rules.mjs';

const BUILD = 'crm-marketing-cockpit-20260823-responsive-02';
function text(v){return v==null?'':String(v).trim();}
function json(data,status=200){return new Response(JSON.stringify(data,null,2),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-crm-marketing-cockpit-build':BUILD}});}
function authorized(request){return !!text(request.headers.get('cf-access-authenticated-user-email')||request.headers.get('Cf-Access-Authenticated-User-Email')||request.headers.get('x-user-email')||request.headers.get('x-admin-token'));}
async function model(env){const source=await loadMarketingSource(env);return buildCustomerMarketingModel(source,{today:new Date().toISOString().slice(0,10)});}

export async function handleMarketingCrmRoute(request, env){
  const url=new URL(request.url);
  if(!url.pathname.startsWith('/api/marketing/')) return null;
  if(request.method!=='GET') return json({ok:false,error:'method_not_allowed'},405);
  if(!authorized(request)) return json({ok:false,error:'login_required'},401);
  const m=await model(env);
  if(url.pathname==='/api/marketing/overview') return json(m);
  if(url.pathname==='/api/marketing/customers'){
    const rows=filterSortCustomers(m.customers,{search:url.searchParams.get('search'),segment:url.searchParams.get('segment'),line:url.searchParams.get('line'),sort:url.searchParams.get('sort')});
    return json({ok:true,build:BUILD,total:rows.length,customers:rows.slice(0,Number(url.searchParams.get('limit')||100)),metric_definitions:m.metric_definitions,data_gaps:m.data_gaps});
  }
  if(url.pathname==='/api/marketing/segments') return json({ok:true,build:BUILD,segments:m.segments,opportunities:m.opportunities,metric_definitions:m.metric_definitions});
  if(url.pathname==='/api/marketing/analytics') return json({ok:true,build:BUILD,overview:m.overview,analytics:m.analytics,metric_definitions:m.metric_definitions,data_gaps:m.data_gaps});
  if(url.pathname==='/api/marketing/customer'){
    const id=text(url.searchParams.get('customer_id'));
    const customer=m.customers.find(c=>c.customer_id===id);
    if(!customer) return json({ok:false,error:'customer_not_found'},404);
    return json({ok:true,build:BUILD,customer,metric_definitions:m.metric_definitions});
  }
  return json({ok:false,error:'not_found'},404);
}

export function marketingCockpitHealth(){return {...marketingRulesHealth(),marketing_cockpit_enabled:true,marketing_cockpit_build:BUILD,marketing_customer_id_owner:'customer-crm',marketing_customer_id_generation:false,marketing_line_send_enabled:false,marketing_mass_send_enabled:false,marketing_schema_change:false};}

export async function patchMarketingCrmHtml(response){
  const ct=response.headers.get('content-type')||'';
  if(response.status!==200 || !ct.includes('text/html')) return response;
  let html=await response.text();
  if(html.includes('crm-marketing-cockpit-20260823')) return response;
  const addon = `
<style id="crm-marketing-cockpit-20260823-style">
:root{--crmMText:#0f172a;--crmMMuted:#64748b;--crmMBorder:#e2e8f0;--crmMSurface:#fff;--crmMBg:#f8fafc;--crmMPrimary:#0f172a;--crmMSoft:#f1f5f9}
.crmMarketingShell,.crmMarketingShell *{box-sizing:border-box}
.crmMarketingShell{width:100%;min-width:0;max-width:none;font-family:-apple-system,BlinkMacSystemFont,'Noto Sans JP',sans-serif;color:var(--crmMText);background:var(--crmMBg);margin:0;padding:16px;border-bottom:1px solid var(--crmMBorder);isolation:isolate}
.crmMLayout{width:100%;min-width:0;margin:0 auto;display:grid;gap:14px}
.crmMSide{align-self:start;background:var(--crmMSurface);border:1px solid var(--crmMBorder);border-radius:20px;padding:12px;box-shadow:0 10px 30px rgba(15,23,42,.04)}
.crmMSideTitle{font-size:13px;font-weight:950;letter-spacing:.02em;margin:0 0 10px}
.crmMNav{display:grid;gap:4px}
.crmMNav button,.crmMBottom button,.crmMButton{min-height:44px}
.crmMNav button{border:0;background:transparent;text-align:left;border-radius:13px;padding:11px 12px;font-weight:850;color:#334155;cursor:pointer}
.crmMNav button:focus-visible,.crmMBottom button:focus-visible,.crmMButton:focus-visible,.crmMTable tr:focus-visible{outline:3px solid #93c5fd;outline-offset:2px}
.crmMNav button.active,.crmMBottom button.active{background:var(--crmMPrimary);color:#fff}
.crmMMain{display:grid;gap:14px;min-width:0}
.crmMHeader{display:grid;gap:12px;align-items:center}
.crmMTitle{margin:0;font-size:clamp(22px,2.4vw,32px);line-height:1.15}
.crmMSearch{display:flex;background:var(--crmMSurface);border:1px solid var(--crmMBorder);border-radius:16px;overflow:hidden;min-width:0}
.crmMSearch input{flex:1;min-width:0;border:0;padding:14px 16px;font-size:16px;outline:none;background:#fff}
.crmMSearch button,.crmMButton{border:0;background:var(--crmMPrimary);color:#fff;padding:10px 14px;border-radius:12px;font-weight:900;cursor:pointer}
.crmMKpis,.crmMOpps,.crmMGrid,.crmMCustomerCards{display:grid;gap:12px;min-width:0}
.crmMSectionTitle{margin:8px 0 0;font-size:18px}
.crmMCard{background:var(--crmMSurface);border:1px solid var(--crmMBorder);border-radius:18px;padding:14px;box-shadow:0 8px 24px rgba(15,23,42,.04);min-width:0}
button.crmMCard{width:100%;text-align:left;cursor:pointer}
.crmMCard h3{margin:0 0 8px;font-size:16px}
.crmMValue{font-size:24px;font-weight:950;line-height:1.15}
.crmMMuted{color:var(--crmMMuted);font-size:12px;line-height:1.5}
.crmMBadge{display:inline-flex;border-radius:999px;padding:3px 8px;font-size:11px;font-weight:900;background:#e0f2fe;color:#075985;margin:0 3px 4px 0}
.crmMBadge.attn{background:#ffedd5;color:#9a3412}.crmMBadge.good{background:#dcfce7;color:#166534}
.crmMTableWrap{min-width:0;overflow-x:auto;border-radius:16px;border:1px solid var(--crmMBorder);background:#fff}
.crmMTable{width:100%;border-collapse:collapse;background:#fff}
.crmMTable th,.crmMTable td{padding:11px 12px;border-bottom:1px solid var(--crmMBorder);font-size:13px;text-align:left;vertical-align:middle;white-space:nowrap}
.crmMTable th{background:var(--crmMSoft);color:#475569;font-weight:900}.crmMTable tr{cursor:pointer}.crmMTable tr:hover{background:#f8fafc}
.crmMCustomerCards{display:none}
.crmMMobileCustomerCard{display:grid;gap:8px}
.crmMMobileCustomerCard .crmMButton{width:100%;margin-top:4px}
.crmM360Grid{display:grid;gap:12px}
.crmM360Summary{grid-area:summary}.crmM360Action{grid-area:action}.crmM360Value{grid-area:value}.crmM360Booking{grid-area:booking}.crmM360History{grid-area:history}.crmM360Timeline{grid-area:timeline}.crmM360Info{grid-area:info}.crmM360Identity{grid-area:identity}
.crmMTimeline{display:grid;gap:8px}.crmMTimeline div{border-left:3px solid #cbd5e1;padding:6px 10px;background:#f8fafc;border-radius:8px}
.crmMEmpty{padding:18px;border:1px dashed #cbd5e1;border-radius:16px;background:#fff;color:#64748b;text-align:center}
.crmMBottom{display:none}
.crmMCampaignPlan .crmMButton{background:#94a3b8;cursor:not-allowed}
.crmMLegacyAccess{margin-top:12px;padding-top:10px;border-top:1px solid var(--crmMBorder)}
@media (min-width:1200px){
  .crmMarketingShell{padding:24px clamp(32px,4vw,48px)}
  .crmMLayout{grid-template-columns:224px minmax(0,1fr);max-width:1600px}
  .crmMSide{display:block;position:sticky;top:18px}
  .crmMHeader{grid-template-columns:minmax(0,1fr) minmax(360px,480px)}
  .crmMKpis{grid-template-columns:repeat(4,minmax(0,1fr))}
  .crmMOpps,.crmMGrid{grid-template-columns:repeat(4,minmax(0,1fr))}
  .crmM360Grid{grid-template-columns:1.25fr .85fr;grid-template-areas:"summary action" "value booking" "history history" "timeline info" "identity identity"}
  .crmMBottom{display:none!important}
}
@media (min-width:1600px){
  .crmMLayout{max-width:1600px}
  .crmMKpis{grid-template-columns:repeat(4,minmax(0,1fr))}
  .crmMOpps,.crmMGrid{grid-template-columns:repeat(4,minmax(0,1fr))}
}
@media (min-width:768px) and (max-width:1199px){
  .crmMarketingShell{padding:18px 20px}
  .crmMLayout{grid-template-columns:156px minmax(0,1fr);max-width:1120px}
  .crmMSide{display:block;position:sticky;top:12px}
  .crmMSideTitle{font-size:12px}
  .crmMNav button{padding:10px 9px;font-size:13px}
  .crmMHeader{grid-template-columns:1fr}
  .crmMKpis{grid-template-columns:repeat(2,minmax(0,1fr))}
  .crmMOpps,.crmMGrid,.crmMCustomerCards{grid-template-columns:repeat(2,minmax(0,1fr))}
  .crmMOptional{display:none}
  .crmM360Grid{grid-template-columns:1fr 1fr;grid-template-areas:"summary action" "value booking" "history history" "timeline info" "identity identity"}
  .crmMBottom{display:none!important}
}
@media (max-width:767px){
  .crmMarketingShell{padding:12px 12px calc(84px + env(safe-area-inset-bottom));overflow-x:hidden}
  .crmMLayout{display:block;max-width:none}
  .crmMSide{display:none!important}
  .crmMMain{gap:12px}
  .crmMHeader{display:block}
  .crmMSearch{margin-top:10px}
  .crmMSearch button{padding-left:12px;padding-right:12px}
  .crmMKpis{grid-template-columns:repeat(2,minmax(0,1fr))}
  .crmMOpps,.crmMGrid{grid-template-columns:1fr}
  .crmMDesktopTable{display:none!important}
  .crmMCustomerCards{display:grid;grid-template-columns:1fr}
  .crmM360Grid{grid-template-columns:1fr;grid-template-areas:"summary" "action" "value" "booking" "history" "timeline" "info" "identity"}
  .crmMTitle{font-size:24px}
  .crmMValue{font-size:22px}
  .crmMCard{padding:13px}
  .crmMBottom{position:fixed;left:0;right:0;bottom:0;z-index:9999;display:grid;grid-template-columns:repeat(5,1fr);background:#fff;border-top:1px solid var(--crmMBorder);padding:6px 6px calc(6px + env(safe-area-inset-bottom));box-shadow:0 -8px 24px rgba(15,23,42,.08)}
  .crmMBottom button{border:0;background:#fff;border-radius:12px;font-size:11px;font-weight:900;color:#334155}
}
@media (max-width:420px){
  .crmMarketingShell{padding-left:10px;padding-right:10px}
  .crmMKpis{grid-template-columns:1fr}
  .crmMValue{font-size:21px}
  .crmMCard{padding:12px}
  .crmMTitle{font-size:23px}
}
</style>
<script id="crm-marketing-cockpit-20260823-script">
(()=>{if(window.__crmMarketingCockpit20260823)return;window.__crmMarketingCockpit20260823=true;const S={view:'home',data:null,segment:'',search:''};const nav=[['home','HOME'],['customers','顧客'],['segments','セグメント'],['campaigns','キャンペーン'],['analytics','分析']];const yen=n=>'¥'+Math.round(Number(n||0)).toLocaleString('ja-JP');const pct=n=>(Number(n||0)*100).toFixed(1)+'%';const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));async function api(p){const r=await fetch(p,{credentials:'same-origin',cache:'no-store'});const j=await r.json().catch(()=>({ok:false,error:'invalid_json'}));if(!r.ok)throw new Error(j.error||r.status);return j}function shell(){let root=document.getElementById('crmMarketingCockpit');if(root)return root;root=document.createElement('section');root.id='crmMarketingCockpit';root.className='crmMarketingShell';root.setAttribute('data-crm-root-mount','body-level');root.setAttribute('aria-label','Marketing CRM workspace');document.documentElement.classList.add('crmMarketingWorkspaceMounted');document.body.classList.add('crmMarketingWorkspaceMounted');document.body.insertBefore(root,document.body.firstChild);return root}function navHtml(){return '<div class="crmMNav" aria-label="Marketing CRM navigation">'+nav.map(([k,l])=>'<button type="button" data-mnav="'+k+'" class="'+(S.view===k?'active':'')+'">'+l+'</button>').join('')+'</div><div class="crmMLegacyAccess crmMMuted">既存管理機能はこの下の管理画面から継続利用できます。</div>'}function base(inner){return '<div class="crmMLayout" data-layout="responsive-workspace"><aside class="crmMSide" data-device-nav="desktop-tablet"><div class="crmMSideTitle">Repeat Revenue Cockpit</div>'+navHtml()+'</aside><div class="crmMMain"><div class="crmMHeader"><div><h1 class="crmMTitle">Marketing CRM</h1><div class="crmMMuted">誰に・何を・いつ・なぜ提案するかをCustomer ID単位で判断します。</div></div><div class="crmMSearch"><input id="crmMSearch" placeholder="名前・顧客ID・LINEで検索" value="'+esc(S.search)+'"><button type="button" id="crmMSearchBtn">検索</button></div></div>'+inner+'</div></div><nav class="crmMBottom" data-device-nav="mobile">'+nav.map(([k,l])=>'<button type="button" data-mnav="'+k+'" class="'+(S.view===k?'active':'')+'">'+l+'</button>').join('')+'</nav>'}function kpi(l,v,s){return '<div class="crmMCard"><div class="crmMMuted">'+l+'</div><div class="crmMValue">'+v+'</div><div class="crmMMuted">'+(s||'')+'</div></div>'}function customerMobileCard(c){return '<article class="crmMCard crmMMobileCustomerCard"><div><h3>'+esc(c.name)+'</h3><div class="crmMMuted">Customer ID '+esc(c.customer_id)+'</div></div><div><span class="crmMBadge">'+esc((c.segments||[])[0]||'未分類')+'</span><span class="crmMBadge">撮影 '+c.shoot_count+'回</span><span class="crmMBadge '+(c.line_connected?'good':'attn')+'">LINE '+(c.line_connected?'接続済':'未接続')+'</span></div><div class="crmMValue">'+esc(c.ltv_label)+'</div><div class="crmMMuted">最終撮影 '+esc(c.last_shoot_date||'未取得')+'</div><div>次の提案: <b>'+esc(c.opportunity?.category||'未取得')+'</b></div><button type="button" class="crmMButton" data-detail="'+esc(c.customer_id)+'">Customer 360</button></article>'}function card(c){return '<div class="crmMCard"><h3>'+esc(c.name)+'</h3><div class="crmMMuted">'+esc(c.customer_id)+'</div><p><span class="crmMBadge '+(c.line_connected?'good':'attn')+'">LINE '+(c.line_connected?'接続済':'未接続')+'</span> <span class="crmMBadge">撮影 '+c.shoot_count+'回</span></p><div class="crmMValue">'+esc(c.ltv_label)+'</div><div class="crmMMuted">最終撮影 '+esc(c.last_shoot_date)+' / 次の提案: '+esc(c.opportunity?.category)+'</div><button type="button" class="crmMButton" data-detail="'+esc(c.customer_id)+'">Customer 360</button></div>'}function customerTable(rows,preview){return '<div class="crmMTableWrap crmMDesktopTable"><table class="crmMTable"><thead><tr><th>お客様</th><th>Customer ID</th><th>顧客状態</th><th>最終撮影</th><th>撮影回数</th><th>LTV</th><th>次の提案</th><th>LINE状態</th><th class="crmMOptional">最終接触</th><th class="crmMOptional">次回予約</th></tr></thead><tbody>'+rows.map(c=>'<tr tabindex="0" data-detail="'+esc(c.customer_id)+'"><td><b>'+esc(c.name)+'</b></td><td>'+esc(c.customer_id)+'</td><td>'+esc((c.segments||[]).join(' / ')||'未分類')+'</td><td>'+esc(c.last_shoot_date||'未取得')+'</td><td>'+c.shoot_count+'回</td><td>'+esc(c.ltv_label)+'</td><td>'+esc(c.opportunity?.category||'未取得')+'</td><td>'+(c.line_connected?'接続済':'未接続')+'</td><td class="crmMOptional">'+esc(c.last_contact_date||'未取得')+'</td><td class="crmMOptional">'+esc(c.next_booking_date||'未取得')+'</td></tr>').join('')+'</tbody></table></div>'+(preview?'':'<div class="crmMCustomerCards crmMMobileCards">'+rows.map(customerMobileCard).join('')+'</div>')}function home(d){const o=d.overview||{};const preview=(d.customers||[]).slice(0,8);return base('<div class="crmMKpis">'+kpi('今月売上',yen(o.month_sales),'新規 '+yen(o.new_sales)+' / リピート '+yen(o.repeat_sales))+kpi('今月予約件数',o.month_reservations||0,'新規 '+(o.new_reservations||0)+' / リピート '+(o.repeat_reservations||0))+kpi('リピート率',pct(o.repeat_rate),'平均撮影 '+Number(o.average_shoot_count||0).toFixed(1)+'回')+kpi('平均LTV',yen(o.average_ltv),'平均単価 '+yen(o.average_order_value))+'</div><h2 class="crmMSectionTitle">マーケティング機会</h2><div class="crmMOpps">'+(d.opportunities||[]).map(x=>'<button type="button" class="crmMCard" data-seg="'+x.segment+'"><h3>'+esc(x.label)+'</h3><div class="crmMValue">'+x.count+'</div><div class="crmMMuted">クリックして対象顧客を見る</div></button>').join('')+'</div><h2 class="crmMSectionTitle">Recommended Customer</h2><div class="crmMGrid">'+(d.customers||[]).slice(0,4).map(card).join('')+'</div><h2 class="crmMSectionTitle">Customer list preview</h2>'+customerTable(preview,true))}function customers(rows){return base(rows.length?customerTable(rows,false):'<div class="crmMEmpty">対象顧客はいません</div>')}function segments(d){return base('<h2 class="crmMSectionTitle">セグメント</h2><div class="crmMGrid">'+Object.values(d.segments||{}).map(s=>'<div class="crmMCard"><h3>'+esc(s.label)+'</h3><div class="crmMValue">'+s.count+'</div><p class="crmMMuted">'+esc(s.description)+'</p><button type="button" class="crmMButton" data-seg="'+s.key+'">顧客を見る</button></div>').join('')+'</div>')}function campaigns(d){return base('<div class="crmMCard crmMCampaignPlan"><h2>キャンペーン計画</h2><p>このTASKではLINE一斉送信・自動送信・予約配信を実装しません。対象選定とpreviewのみです。</p><button type="button" class="crmMButton" disabled>配信実行なし</button></div><div class="crmMGrid">'+(d.opportunities||[]).map(o=>'<div class="crmMCard"><h3>'+esc(o.label)+'</h3><div class="crmMValue">'+o.count+'</div><div class="crmMMuted">対象segment: '+esc(o.segment)+' / 反応・開封・売上attribution: 未計測</div></div>').join('')+'</div>')}function analytics(d){const o=d.overview||{},a=d.analytics||{};return base('<div class="crmMKpis">'+kpi('総顧客数',o.customer_count||0,'新規・リピーター分析')+kpi('リピーター数',o.repeat_count||0,'リピート率 '+pct(o.repeat_rate))+kpi('平均撮影回数',Number(o.average_shoot_count||0).toFixed(1),'Customer ID単位')+kpi('休眠顧客数',(d.segments?.DORMANT?.count)||0,'365日以上')+'</div><div class="crmMGrid"><div class="crmMCard"><h3>ジャンル分布</h3>'+((a.genre_distribution||[]).map(x=>'<div>'+esc(x.label)+'：'+x.count+'</div>').join('')||'<div class="crmMMuted">UNAVAILABLE / DATA_GAP</div>')+'</div><div class="crmMCard"><h3>未計測</h3><div>開封・クリック・反応・売上attributionは現在sourceなし。</div></div></div>')}function detail(c){const history=(c.reservations||c.history||[]).slice(0,6);return base('<div class="crmM360Grid"><section class="crmMCard crmM360Summary"><h3>Customer Summary</h3><h2>'+esc(c.name)+'</h2><div class="crmMMuted">Customer ID '+esc(c.customer_id)+'</div><p><span class="crmMBadge '+(c.line_connected?'good':'attn')+'">LINE '+(c.line_connected?'接続済':'未接続')+'</span><span class="crmMBadge">'+esc((c.segments||[]).join(' / ')||'未分類')+'</span></p></section><section class="crmMCard crmM360Action"><h3>Next Action</h3><div class="crmMValue">'+esc(c.opportunity?.category||'未取得')+'</div><p>推奨時期: '+esc(c.opportunity?.timing||'未取得')+'</p><ul>'+((c.opportunity?.reason||[]).map(r=>'<li>'+esc(r)+'</li>').join(''))+'</ul></section><section class="crmMCard crmM360Value"><h3>Customer Value</h3><p>撮影回数: '+c.shoot_count+'回</p><p>予約回数: '+c.reservation_count+'件</p><p>LTV: '+esc(c.ltv_label)+'</p><p>平均単価: '+esc(c.average_order_value_label)+'</p></section><section class="crmMCard crmM360Booking"><h3>Next Booking</h3><p>次回予約: '+esc(c.next_booking_date||'未取得')+'</p><p>最終撮影: '+esc(c.last_shoot_date||'未取得')+'</p></section><section class="crmMCard crmM360History"><h3>Reservation / Shoot History</h3>'+((history.length?history.map(h=>'<div>'+esc(h.shoot_date||h.date||'未取得')+' '+esc(h.genre||h.label||'')+' '+esc(h.amount_label||'')+'</div>').join(''):'<div class="crmMMuted">履歴詳細は未取得です。</div>'))+'</section><section class="crmMCard crmM360Timeline"><h3>Contact Timeline</h3><div class="crmMTimeline">'+((c.timeline||[]).slice(0,5).map(t=>'<div>'+esc(t.date||'未取得')+' '+esc(t.label||t.memo||'')+'</div>').join('')||'<div>最終接触: '+esc(c.last_contact_date||'未取得')+'</div>')+'</div></section><section class="crmMCard crmM360Info"><h3>Customer Info</h3><p>初回撮影: '+esc(c.first_shoot_date||'未取得')+'</p><p>最終撮影: '+esc(c.last_shoot_date||'未取得')+'</p></section><section class="crmMCard crmM360Identity"><h3>System / identity detail</h3><p>Customer ID: '+esc(c.customer_id)+'</p><p>LINE user ID: '+esc(c.line_user_id||'未取得')+'</p></section></div>')}async function render(){const root=shell();try{const data=S.data||await api('/api/marketing/overview');S.data=data;let html='';if(S.view==='home')html=home(data);if(S.view==='customers'){const r=await api('/api/marketing/customers?search='+encodeURIComponent(S.search)+'&segment='+encodeURIComponent(S.segment));html=customers(r.customers||[])}if(S.view==='segments')html=segments(await api('/api/marketing/segments'));if(S.view==='campaigns')html=campaigns(data);if(S.view==='analytics')html=analytics(data);root.innerHTML=html;bind(root)}catch(e){root.innerHTML='<div class="crmMEmpty">Marketing CRMを読み込めません: '+esc(e.message)+'</div>'}}async function showDetail(id){const r=await api('/api/marketing/customer?customer_id='+encodeURIComponent(id));const root=shell();root.innerHTML=detail(r.customer);bind(root)}function bind(root){root.querySelectorAll('[data-mnav]').forEach(b=>b.onclick=()=>{S.view=b.dataset.mnav;S.segment='';render()});root.querySelectorAll('[data-seg]').forEach(b=>b.onclick=()=>{S.view='customers';S.segment=b.dataset.seg;render()});root.querySelectorAll('[data-detail]').forEach(b=>{b.onclick=()=>showDetail(b.dataset.detail);b.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();showDetail(b.dataset.detail)}}});const s=root.querySelector('#crmMSearch');const btn=root.querySelector('#crmMSearchBtn');if(s){s.oninput=()=>S.search=s.value;btn.onclick=()=>{S.view='customers';S.segment='';render()};s.onkeydown=e=>{if(e.key==='Enter'){S.view='customers';S.segment='';render()}}}}if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',render,{once:true});else render();})();
</script>`;
  html = html.includes('</body>') ? html.replace('</body>', addon + '</body>') : html + addon;
  const headers=new Headers(response.headers); headers.delete('content-length'); headers.set('cache-control','no-store, no-cache, must-revalidate, max-age=0'); headers.set('x-crm-marketing-cockpit-build',BUILD);
  return new Response(html,{status:response.status,statusText:response.statusText,headers});
}
