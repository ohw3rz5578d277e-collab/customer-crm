export function injectOwnerLineChat(html){
  if(!html||html.includes('crm-owner-line-chat-script'))return html;
  const style=String.raw`<style id="crm-owner-line-chat-style">
#crmOwnerLineChat{display:none;max-width:1580px;margin:0 auto}
.crm-line-chat-shell{height:min(760px,calc(100vh - 150px));min-height:540px;display:grid;grid-template-columns:330px minmax(0,1fr);background:#fff;border:1px solid #dfe8e4;border-radius:22px;overflow:hidden;box-shadow:0 12px 34px rgba(20,50,40,.07)}
.crm-line-chat-list-pane{min-width:0;border-right:1px solid #e4ebe8;background:#fbfdfc;display:flex;flex-direction:column}.crm-line-chat-list-head{padding:16px;border-bottom:1px solid #e4ebe8}.crm-line-chat-list-head h2{margin:3px 0 4px;font-size:22px}.crm-line-chat-list-head p{margin:0;color:#6b7b75;font-size:12px}
.crm-line-chat-search{margin-top:12px;width:100%;min-height:44px;border:1px solid #d8e4df;border-radius:12px;padding:0 12px;font-size:16px;background:#fff;box-sizing:border-box;outline:none}.crm-line-chat-search:focus{border-color:#087a5b;box-shadow:0 0 0 3px rgba(8,122,91,.10)}
.crm-line-chat-customers{overflow:auto;padding:8px;display:grid;gap:4px}.crm-line-chat-customer{appearance:none;border:0;background:transparent;width:100%;padding:11px;border-radius:13px;text-align:left;cursor:pointer;display:grid;grid-template-columns:42px minmax(0,1fr);gap:10px;align-items:center}.crm-line-chat-customer:hover,.crm-line-chat-customer.active{background:#eaf6f1}.crm-line-chat-avatar{width:42px;height:42px;border-radius:50%;display:grid;place-items:center;background:#dcefe7;color:#086348;font-weight:900}.crm-line-chat-name{font-weight:900;color:#19332a;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.crm-line-chat-meta{font-size:11px;color:#718078;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.crm-line-chat-main{min-width:0;display:flex;flex-direction:column;background:#f7faf9}.crm-line-chat-main-head{height:68px;box-sizing:border-box;padding:11px 16px;background:#fff;border-bottom:1px solid #e4ebe8;display:flex;align-items:center;gap:10px}.crm-line-chat-main-head h3{margin:0;font-size:17px}.crm-line-chat-main-head p{margin:2px 0 0;color:#718078;font-size:11px}.crm-line-chat-back{display:none;appearance:none;border:1px solid #dfe8e4;background:#fff;border-radius:11px;min-width:42px;height:42px;font-weight:900}
.crm-line-chat-messages{flex:1;overflow:auto;padding:18px;display:flex;flex-direction:column;gap:9px}.crm-line-chat-empty{margin:auto;color:#718078;text-align:center;line-height:1.7}.crm-line-chat-row{display:flex}.crm-line-chat-row.outbound{justify-content:flex-end}.crm-line-chat-row.inbound{justify-content:flex-start}.crm-line-chat-bubble{max-width:min(72%,620px);padding:10px 12px;border-radius:16px;background:#fff;border:1px solid #dfe8e4;box-shadow:0 4px 14px rgba(20,50,40,.04);white-space:pre-wrap;word-break:break-word;line-height:1.55;font-size:14px}.crm-line-chat-row.outbound .crm-line-chat-bubble{background:#dff4e9;border-color:#c7e8da}.crm-line-chat-time{font-size:10px;color:#809087;margin-top:5px}
.crm-line-chat-foot{padding:10px 14px;background:#fff;border-top:1px solid #e4ebe8;color:#6b7b75;font-size:11px;display:flex;justify-content:space-between;gap:10px;align-items:center}.crm-line-chat-refresh{appearance:none;border:1px solid #d8e4df;background:#fff;border-radius:10px;min-height:36px;padding:0 12px;font-weight:800;color:#315449}
@media(max-width:767px){
 .crm-line-chat-shell{height:calc(100vh - 164px);min-height:500px;display:block;border-radius:16px}
 .crm-line-chat-list-pane,.crm-line-chat-main{height:100%}.crm-line-chat-main{display:none}.crm-line-chat-shell[data-chat-open="1"] .crm-line-chat-list-pane{display:none}.crm-line-chat-shell[data-chat-open="1"] .crm-line-chat-main{display:flex}
 .crm-line-chat-back{display:inline-grid;place-items:center}.crm-line-chat-bubble{max-width:86%}.crm-line-chat-messages{padding:14px 10px}
}
</style>`;
  const script=String.raw`<script id="crm-owner-line-chat-script">
(()=>{
if(window.__crmOwnerLineChat)return;
const $=id=>document.getElementById(id),state={customers:[],filtered:[],selected:null,loading:false};
function esc(v){return String(v==null?'':v).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}
function messageText(m){return m.message_text||m.text||m.message||m.body||m.content||''}
function outbound(m){const d=String(m.direction||m.type||'').toLowerCase();return ['out','outbound','sent','send'].some(x=>d.includes(x))}
function time(m){return m.sent_at||m.created_at||m.timestamp||m.occurred_at||''}
function shell(){return $('crmOwnerLineChat')?.querySelector('.crm-line-chat-shell')}
function ensure(){
 if($('crmOwnerLineChat'))return true;
 const content=$('crmOwnerWorkspaceContent');if(!content)return false;
 const section=document.createElement('section');section.id='crmOwnerLineChat';
 section.innerHTML='<div class="crm-line-chat-shell"><aside class="crm-line-chat-list-pane"><div class="crm-line-chat-list-head"><div class="crm-shell-eyebrow">LINE CHAT</div><h2>LINE</h2><p>顧客ごとのLINE履歴をチャット形式で確認します。</p><input id="crmLineChatSearch" class="crm-line-chat-search" type="search" placeholder="顧客名・Customer IDで検索"></div><div id="crmLineChatCustomers" class="crm-line-chat-customers"><div class="crm-line-chat-empty">読み込み前</div></div></aside><main class="crm-line-chat-main"><div class="crm-line-chat-main-head"><button id="crmLineChatBack" class="crm-line-chat-back" type="button">‹</button><div><h3 id="crmLineChatTitle">顧客を選択</h3><p id="crmLineChatSubtitle">LINE履歴</p></div></div><div id="crmLineChatMessages" class="crm-line-chat-messages"><div class="crm-line-chat-empty">左の顧客一覧から選択してください。</div></div><div class="crm-line-chat-foot"><span>履歴表示専用。CRMから自動送信はしません。</span><button id="crmLineChatRefresh" class="crm-line-chat-refresh" type="button">更新</button></div></main></div>';
 content.appendChild(section);
 $('crmLineChatSearch').addEventListener('input',filter);
 $('crmLineChatBack').onclick=()=>{shell()?.setAttribute('data-chat-open','0')};
 $('crmLineChatRefresh').onclick=()=>state.selected?openConversation(state.selected.customer_id,true):loadCustomers(true);
 return true;
}
function renderCustomers(){
 const host=$('crmLineChatCustomers');if(!host)return;
 if(!state.filtered.length){host.innerHTML='<div class="crm-line-chat-empty">LINE連携済みの顧客が見つかりません。</div>';return}
 host.innerHTML=state.filtered.map(c=>'<button type="button" class="crm-line-chat-customer '+(state.selected?.customer_id===c.customer_id?'active':'')+'" data-line-customer="'+esc(c.customer_id)+'"><span class="crm-line-chat-avatar">'+esc((c.name||c.line_display_name||'?').slice(0,1))+'</span><span><div class="crm-line-chat-name">'+esc(c.name||c.line_display_name||'名称未設定')+'</div><div class="crm-line-chat-meta">'+esc(c.line_display_name||'LINE連携済み')+' · '+esc(c.customer_id||'')+'</div></span></button>').join('');
 host.querySelectorAll('[data-line-customer]').forEach(b=>b.onclick=()=>openConversation(b.dataset.lineCustomer));
}
function filter(){const q=String($('crmLineChatSearch')?.value||'').trim().toLowerCase();state.filtered=!q?state.customers:state.customers.filter(c=>[c.name,c.line_display_name,c.customer_id].some(v=>String(v||'').toLowerCase().includes(q)));renderCustomers()}
async function fetchCustomers(){
 let r=await fetch('/api/customer360/customers?page=1&page_size=100&line=linked',{credentials:'same-origin',cache:'no-store'}),j=await r.json().catch(()=>({}));
 if(!r.ok||j.ok===false){r=await fetch('/api/customers?segment=line&limit=100',{credentials:'same-origin',cache:'no-store'});j=await r.json().catch(()=>({}));if(!r.ok||j.ok===false)throw Error(j.error||j.message||'顧客一覧を取得できません')}
 return j.items||j.customers||[];
}
async function loadCustomers(force=false){
 if(state.loading&&!force)return;state.loading=true;ensure();const host=$('crmLineChatCustomers');if(host)host.innerHTML='<div class="crm-line-chat-empty">読み込み中…</div>';
 try{state.customers=(await fetchCustomers()).filter(c=>c.line_linked!==false);state.filtered=state.customers;renderCustomers()}catch(e){if(host)host.innerHTML='<div class="crm-line-chat-empty">読み込み失敗<br>'+esc(e.message||e)+'</div>'}finally{state.loading=false}
}
function renderMessages(data,c){
 const host=$('crmLineChatMessages');if(!host)return;const arr=data.messages||data.items||[];
 $('crmLineChatTitle').textContent=c?.name||c?.line_display_name||'LINE';
 $('crmLineChatSubtitle').textContent=(data.connected?'LINE接続済み':'保存済み履歴')+' · '+arr.length+'件';
 if(!arr.length){host.innerHTML='<div class="crm-line-chat-empty">'+esc(data.message||'LINE履歴はまだありません。')+'</div>';return}
 host.innerHTML=arr.map(m=>'<div class="crm-line-chat-row '+(outbound(m)?'outbound':'inbound')+'"><div class="crm-line-chat-bubble">'+esc(messageText(m)||'（テキストなし）')+'<div class="crm-line-chat-time">'+esc(time(m))+'</div></div></div>').join('');host.scrollTop=host.scrollHeight;
}
async function openConversation(id,refresh=false){
 ensure();const c=state.customers.find(x=>String(x.customer_id)===String(id))||{customer_id:id,name:id};state.selected=c;renderCustomers();shell()?.setAttribute('data-chat-open','1');$('crmLineChatTitle').textContent=c.name||c.line_display_name||'LINE';$('crmLineChatMessages').innerHTML='<div class="crm-line-chat-empty">履歴を読み込み中…</div>';
 try{const r=await fetch('/api/customers/'+encodeURIComponent(id)+'/line-history'+(refresh?'?refresh=1':''),{credentials:'same-origin',cache:'no-store'}),j=await r.json().catch(()=>({}));if(!r.ok||j.ok===false)throw Error(j.error||j.message||'LINE履歴を取得できません');renderMessages(j,c)}catch(e){$('crmLineChatMessages').innerHTML='<div class="crm-line-chat-empty">読み込み失敗<br>'+esc(e.message||e)+'</div>'}
}
async function open(){let tries=0;while(!ensure()&&tries++<40)await new Promise(r=>setTimeout(r,25));if(!state.customers.length)await loadCustomers();return true}
window.__crmOwnerLineChat={open,loadCustomers,openConversation,getState:()=>({...state})};
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',()=>{let n=0;const t=setInterval(()=>{if(ensure()||++n>40)clearInterval(t)},25)},{once:true});else ensure();
})();
<\/script>`;
  return html.includes('</head>')?html.replace('</head>',style+'</head>').replace('</body>',script+'</body>'):style+html+script;
}
