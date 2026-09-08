const STYLE_ID='crm-proposal-c-analysis-approach-style';
const SCRIPT_ID='crm-proposal-c-analysis-approach-script';

const STYLE=String.raw`<style id="${STYLE_ID}">
:root{--crm-c-blue:#1769ff;--crm-c-blue-soft:#edf4ff;--crm-c-mint:#1ab58a;--crm-c-mint-soft:#eafaf5;--crm-c-orange:#f5a524;--crm-c-orange-soft:#fff6e5;--crm-c-violet:#7657ff;--crm-c-violet-soft:#f1edff;--crm-c-ink:#12213a;--crm-c-muted:#6c7a8a;--crm-c-line:#dfe7f0}
body.crm-proposal-c{--crm-shell-accent:var(--crm-c-blue);--crm-shell-accent-soft:var(--crm-c-blue-soft)}
body.crm-proposal-c #crmOwnerDesktopSidebar{background:linear-gradient(180deg,#10243c 0%,#0e1f35 100%)!important;border-right:0!important}
body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-brand b,body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-brand span{color:#fff!important}
body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-brand span{opacity:.64}
body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-nav button,body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-tools button,body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-tools a{color:#d7e3ef!important}
body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-nav button:hover,body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-tools button:hover,body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-tools a:hover{background:rgba(255,255,255,.08)!important}
body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-nav button.active{background:linear-gradient(135deg,#1769ff,#4186ff)!important;color:#fff!important;box-shadow:0 10px 28px rgba(23,105,255,.28)}
body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-icon{background:rgba(255,255,255,.08)!important;color:#f6faff!important}
body.crm-proposal-c #crmOwnerDesktopSidebar .crm-shell-nav button.active .crm-shell-icon{background:rgba(255,255,255,.16)!important}
body.crm-proposal-c #crmOwnerWorkspaceHeader{background:rgba(247,250,255,.96)!important}
body.crm-proposal-c #crmOwnerWorkspaceContent{background:linear-gradient(180deg,#f7faff 0,#f5f8fc 360px,#f4f7f8 100%)}
body[data-crm-owner-view="marketing"].crm-proposal-c #crmMktHome{display:grid!important;grid-template-columns:repeat(12,minmax(0,1fr));gap:14px;align-items:start}
body[data-crm-owner-view="marketing"].crm-proposal-c #crmMktHome>.crm-mkt-hero{grid-column:1/-1;margin:0}
body[data-crm-owner-view="marketing"].crm-proposal-c #crmMktHome>.crm-mkt-kpis{grid-column:1/-1;margin:0}
body[data-crm-owner-view="marketing"].crm-proposal-c #crmMktHome>.crm-period-analytics{grid-column:span 5;margin:0;align-self:stretch}
body[data-crm-owner-view="marketing"].crm-proposal-c #crmMktHome>.crm-approach-queue{grid-column:span 7;margin:0;align-self:stretch}
body[data-crm-owner-view="marketing"].crm-proposal-c #crmMktHome>.crm-perf{grid-column:1/-1}
body.crm-proposal-c .crm-mkt-hero{grid-template-columns:minmax(0,1.65fr) minmax(280px,.75fr);gap:14px}
body.crm-proposal-c .crm-mkt-focus{position:relative;overflow:hidden;background:linear-gradient(135deg,#fff 0%,#f5f9ff 55%,#ebf4ff 100%);border-color:#cfe0fb;padding:22px}
body.crm-proposal-c .crm-mkt-focus:after{content:"";position:absolute;right:-64px;top:-82px;width:220px;height:220px;border-radius:50%;background:radial-gradient(circle,rgba(23,105,255,.12),rgba(23,105,255,0) 70%);pointer-events:none}
body.crm-proposal-c .crm-mkt-focus .crm-mkt-eyebrow{color:var(--crm-c-blue);letter-spacing:.11em}
body.crm-proposal-c .crm-mkt-focus h1{color:var(--crm-c-ink);font-size:clamp(24px,3.2vw,38px);margin-top:4px}
body.crm-proposal-c .crm-mkt-focus p{max-width:720px;color:#506274;font-size:14px;line-height:1.75}
body.crm-proposal-c .crm-mkt-next{background:linear-gradient(145deg,#eff7ff,#fff);border-color:#cfe0fb}
body.crm-proposal-c .crm-mkt-next .crm-mkt-eyebrow{color:var(--crm-c-blue)}
body.crm-proposal-c .crm-mkt-kpis{grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
body.crm-proposal-c .crm-mkt-kpi{position:relative;overflow:hidden;background:#fff;border-color:var(--crm-c-line);padding:14px 14px 13px;min-height:90px;box-shadow:0 7px 22px rgba(18,33,58,.045)}
body.crm-proposal-c .crm-mkt-kpi:before{content:"";position:absolute;left:0;top:0;bottom:0;width:4px;background:var(--crm-c-blue)}
body.crm-proposal-c .crm-mkt-kpi:nth-child(4n+2):before{background:var(--crm-c-mint)}
body.crm-proposal-c .crm-mkt-kpi:nth-child(4n+3):before{background:var(--crm-c-orange)}
body.crm-proposal-c .crm-mkt-kpi:nth-child(4n):before{background:var(--crm-c-violet)}
body.crm-proposal-c .crm-mkt-kpi span{font-size:11px;font-weight:850;color:#6c7b8c}
body.crm-proposal-c .crm-mkt-kpi b{font-size:24px;color:var(--crm-c-ink)}
body.crm-proposal-c .crm-period-analytics,body.crm-proposal-c .crm-approach-queue{border-color:#cfdef0;background:#fff;border-radius:20px;box-shadow:0 10px 30px rgba(18,33,58,.055);padding:16px}
body.crm-proposal-c .crm-period-analytics .crm-mkt-eyebrow,body.crm-proposal-c .crm-approach-queue .crm-mkt-eyebrow{color:var(--crm-c-blue)}
body.crm-proposal-c .crm-period-analytics h2,body.crm-proposal-c .crm-approach-queue h2{color:var(--crm-c-ink)}
body.crm-proposal-c .crm-period-range{grid-template-columns:1fr 1fr;gap:8px}
body.crm-proposal-c .crm-period-range .crm-mkt-btn{grid-column:1/-1;background:var(--crm-c-blue);border-color:var(--crm-c-blue);color:#fff}
body.crm-proposal-c .crm-period-grid{grid-template-columns:1fr}
body.crm-proposal-c .crm-period-presets .crm-chip.active,body.crm-proposal-c .crm-chip.active{background:var(--crm-c-blue-soft);border-color:#8ab4ff;color:#0d57da}
body.crm-proposal-c .crm-approach-summary{grid-template-columns:repeat(4,minmax(0,1fr));gap:8px}
body.crm-proposal-c .crm-approach-summary .crm-mkt-kpi{min-height:74px;padding:11px}
body.crm-proposal-c .crm-approach-summary .crm-mkt-kpi b{font-size:20px}
body.crm-proposal-c .crm-approach-list{gap:8px}
body.crm-proposal-c .crm-approach-row{grid-template-columns:54px minmax(0,1fr) minmax(155px,.42fr);background:#fbfdff;border-color:#dfe8f3;box-shadow:0 3px 12px rgba(18,33,58,.035)}
body.crm-proposal-c .crm-approach-score{background:linear-gradient(135deg,#1769ff,#7048ff);box-shadow:0 7px 18px rgba(23,105,255,.2)}
body.crm-proposal-c .crm-approach-row:nth-child(1) .crm-approach-score:before{content:"1";font-size:9px;position:absolute;transform:translate(-13px,-16px);background:#ffb51f;color:#fff;width:18px;height:18px;display:grid;place-items:center;border-radius:50%}
body.crm-proposal-c .crm-approach-row:nth-child(2) .crm-approach-score:before{content:"2";font-size:9px;position:absolute;transform:translate(-13px,-16px);background:#91a0b2;color:#fff;width:18px;height:18px;display:grid;place-items:center;border-radius:50%}
body.crm-proposal-c .crm-approach-row:nth-child(3) .crm-approach-score:before{content:"3";font-size:9px;position:absolute;transform:translate(-13px,-16px);background:#c98950;color:#fff;width:18px;height:18px;display:grid;place-items:center;border-radius:50%}
body.crm-proposal-c .crm-approach-score{position:relative}
body.crm-proposal-c .crm-approach-offer{color:#173a70}
body.crm-proposal-c .crm-approach-draft summary{display:inline-flex;align-items:center;gap:6px;border:1px solid #a8c5f7;background:#f3f7ff;color:#1559c9;border-radius:10px;padding:7px 10px;margin-top:3px;list-style:none;cursor:pointer}
body.crm-proposal-c .crm-approach-draft summary::-webkit-details-marker{display:none}
body.crm-proposal-c .crm-approach-draft summary:after{content:"文案を確認";font-size:11px;font-weight:900}
body.crm-proposal-c .crm-approach-draft summary{font-size:0}
body.crm-proposal-c .crm-approach-draft>p,body.crm-proposal-c .crm-approach-draft>.crm-mkt-sub{display:none}
body.crm-proposal-c .crm-approach-contact .crm-mkt-btn{border-color:#b8cef1;color:#1559c9}
#crmProposalCComposer{position:fixed;z-index:2147486100;inset:18px 18px 18px auto;width:min(520px,calc(100vw - 36px));background:#f8fbff;border:1px solid #c9daf0;border-radius:24px;box-shadow:0 30px 90px rgba(10,31,62,.28);padding:18px;box-sizing:border-box;overflow:auto;display:none}
#crmProposalCComposer.open{display:block}
.crm-c-compose-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}
.crm-c-compose-head h2{margin:2px 0 5px;color:var(--crm-c-ink);font-size:22px}.crm-c-compose-head p{margin:0;color:var(--crm-c-muted);font-size:12px}
.crm-c-compose-close{width:42px;height:42px;border-radius:12px;border:1px solid #d5e0ec;background:#fff;color:#24364c;font-size:22px;cursor:pointer}
.crm-c-compose-person{margin-top:14px;padding:13px;border:1px solid #dce6f2;border-radius:16px;background:#fff}
.crm-c-compose-person strong{display:block;font-size:18px;color:var(--crm-c-ink)}.crm-c-compose-person .meta{margin-top:4px;color:var(--crm-c-muted);font-size:12px}
.crm-c-compose-status{display:flex;gap:7px;flex-wrap:wrap;margin-top:9px}.crm-c-compose-chip{display:inline-flex;align-items:center;border-radius:999px;padding:5px 9px;font-size:11px;font-weight:900;background:#eef3f8;color:#42576d}.crm-c-compose-chip.ready{background:var(--crm-c-mint-soft);color:#08745a}.crm-c-compose-chip.blocked{background:#fff0f0;color:#a82b2b}
.crm-c-compose-label{display:block;margin:14px 0 6px;font-size:12px;font-weight:900;color:#40536a}
#crmProposalCText{width:100%;min-height:210px;resize:vertical;border:1px solid #cddaea;border-radius:15px;background:#fff;padding:13px;box-sizing:border-box;font:inherit;font-size:14px;line-height:1.75;color:#1c2d43;outline:none}
#crmProposalCText:focus{border-color:#77a8ff;box-shadow:0 0 0 3px rgba(23,105,255,.1)}
.crm-c-compose-actions{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:12px}.crm-c-compose-actions button{min-height:44px;border-radius:12px;border:1px solid #bcd0ea;background:#fff;color:#184b93;font-weight:900;cursor:pointer}.crm-c-compose-actions button.primary{background:var(--crm-c-blue);border-color:var(--crm-c-blue);color:#fff}.crm-c-compose-actions button:disabled{opacity:.45;cursor:not-allowed}
.crm-c-compose-safe{margin-top:12px;padding:11px 12px;border:1px solid #f0d39a;background:#fff8e9;border-radius:13px;color:#77500e;font-size:12px;line-height:1.55;font-weight:750}
.crm-c-compose-result{margin-top:8px;min-height:18px;font-size:12px;font-weight:800;color:#315477}
@media(max-width:1180px){
 body[data-crm-owner-view="marketing"].crm-proposal-c #crmMktHome>.crm-period-analytics,body[data-crm-owner-view="marketing"].crm-proposal-c #crmMktHome>.crm-approach-queue{grid-column:1/-1}
 body.crm-proposal-c .crm-period-grid{grid-template-columns:1fr 1fr}
}
@media(max-width:900px){
 body.crm-proposal-c #crmOwnerWorkspaceContent{background:#f6f9fd}
 body[data-crm-owner-view="marketing"].crm-proposal-c #crmMktHome{display:block!important}
 body[data-crm-owner-view="marketing"].crm-proposal-c #crmMktHome>*{margin-bottom:10px!important}
 body.crm-proposal-c .crm-mkt-hero{grid-template-columns:1fr}
 body.crm-proposal-c .crm-mkt-focus{padding:16px}
 body.crm-proposal-c .crm-mkt-focus h1{font-size:25px}
 body.crm-proposal-c .crm-mkt-kpis{grid-template-columns:repeat(2,minmax(0,1fr))}
 body.crm-proposal-c .crm-mkt-kpi{min-height:78px;padding:12px}
 body.crm-proposal-c .crm-mkt-kpi b{font-size:21px}
 body.crm-proposal-c .crm-period-grid{grid-template-columns:1fr}
 body.crm-proposal-c .crm-approach-row{grid-template-columns:44px minmax(0,1fr)}
 body.crm-proposal-c .crm-approach-contact{grid-column:2}
 #crmProposalCComposer{inset:auto 0 0 0;width:100%;max-height:88dvh;border-radius:24px 24px 0 0;padding:16px}
 .crm-c-compose-actions{grid-template-columns:1fr}
}
@media(max-width:430px){
 body.crm-proposal-c .crm-mkt-kpis{grid-template-columns:1fr 1fr}
 body.crm-proposal-c .crm-mkt-kpi{min-height:74px}
 body.crm-proposal-c .crm-approach-summary{grid-template-columns:1fr 1fr}
 body.crm-proposal-c .crm-period-range{grid-template-columns:1fr}
 body.crm-proposal-c .crm-period-range .crm-mkt-btn{grid-column:auto}
}
</style>`;

const SCRIPT=String.raw`<script id="${SCRIPT_ID}">
(()=>{
if(window.__crmProposalCAnalysisApproach)return;window.__crmProposalCAnalysisApproach=1;
const $=id=>document.getElementById(id);
const loaded={analytics:false,approach:false};
function composer(){
 let el=$('crmProposalCComposer');if(el)return el;
 el=document.createElement('aside');el.id='crmProposalCComposer';el.setAttribute('aria-hidden','true');
 el.innerHTML='<div class="crm-c-compose-head"><div><div class="crm-mkt-eyebrow">OWNER REVIEW</div><h2>メッセージ文案</h2><p>内容を確認・編集してからLINE画面へ進みます。</p></div><button type="button" class="crm-c-compose-close" aria-label="閉じる">×</button></div><div class="crm-c-compose-person"><strong id="crmProposalCName">—</strong><div class="meta" id="crmProposalCMeta">—</div><div class="crm-c-compose-status"><span id="crmProposalCPermission" class="crm-c-compose-chip">確認中</span><span id="crmProposalCChannel" class="crm-c-compose-chip">チャネル —</span></div></div><label class="crm-c-compose-label" for="crmProposalCText">提案メッセージ</label><textarea id="crmProposalCText"></textarea><div class="crm-c-compose-actions"><button id="crmProposalCCopy" type="button">文案をコピー</button><button id="crmProposalCLine" class="primary" type="button">文案をコピーしてLINEへ</button></div><div class="crm-c-compose-safe">メッセージは自動送信されません。送信前にOwnerが内容と連絡許可を確認してください。</div><div id="crmProposalCResult" class="crm-c-compose-result" aria-live="polite"></div>';
 document.body.appendChild(el);
 el.querySelector('.crm-c-compose-close').onclick=closeComposer;
 $('crmProposalCCopy').onclick=async()=>copyDraft(false);
 $('crmProposalCLine').onclick=async()=>copyDraft(true);
 return el;
}
function closeComposer(){const el=$('crmProposalCComposer');if(!el)return;el.classList.remove('open');el.setAttribute('aria-hidden','true');document.body.classList.remove('crm-owner-sheet-open')}
async function writeClipboard(text){if(!text)return false;try{await navigator.clipboard.writeText(text);return true}catch(_){return false}}
async function copyDraft(goLine){
 const text=$('crmProposalCText')?.value||'',result=$('crmProposalCResult'),btn=$('crmProposalCLine');
 const copied=await writeClipboard(text);
 if(result)result.textContent=copied?'文案をコピーしました。':'コピーできませんでした。文案を選択してコピーしてください。';
 if(goLine&&btn&&!btn.disabled){if(copied){window.__crmOwnerView?.showLine?.();closeComposer()}else if(result)result.textContent='コピー後にLINE画面へ進んでください。'}
}
function openComposer(row){
 const sheet=composer(),name=row?.querySelector('.crm-mkt-name')?.textContent?.trim()||'顧客',sub=[...row?.querySelectorAll('.crm-mkt-sub')||[]].map(x=>x.textContent.trim()),id=(sub.join(' ').match(/Customer ID\s+([0-9]{8})/)||[])[1]||'—';
 const draft=row?.querySelector('.crm-approach-draft p')?.textContent?.trim()||'',status=row?.querySelector('.crm-approach-contact .crm-status'),channelText=row?.querySelector('.crm-approach-contact .crm-mkt-sub')?.textContent?.trim()||'候補チャネル —',ready=!!status?.classList.contains('good'),channel=channelText.replace(/^候補チャネル\s*/,'').trim();
 $('crmProposalCName').textContent=name;$('crmProposalCMeta').textContent='Customer ID '+id;
 const permission=$('crmProposalCPermission');permission.textContent=status?.textContent?.trim()||'連絡可否の確認が必要';permission.className='crm-c-compose-chip '+(ready?'ready':'blocked');
 $('crmProposalCChannel').textContent='候補チャネル '+(channel||'—');$('crmProposalCText').value=draft;
 const line=$('crmProposalCLine');line.disabled=!(ready&&String(channel).toUpperCase()==='LINE');line.title=line.disabled?'LINEでの手動連絡条件が整っていません。':'この操作では送信されません。';
 $('crmProposalCResult').textContent=line.disabled?'連絡許可・LINE連携を確認してから送信手続きへ進んでください。':'';
 sheet.classList.add('open');sheet.setAttribute('aria-hidden','false');document.body.classList.add('crm-owner-sheet-open');setTimeout(()=>$('crmProposalCText')?.focus({preventScroll:true}),0);
}
function loadReadOnlyMarketingData(){
 if(document.body.dataset.crmOwnerView!=='marketing')return;
 requestAnimationFrame(()=>{
   const analytics=$('crmAnalyticsApply');if(!loaded.analytics&&analytics){loaded.analytics=true;analytics.click()}
   const approach=$('crmApproachLoad');if(!loaded.approach&&approach){loaded.approach=true;approach.click()}
 });
}
function refreshCopy(){
 if(document.body.dataset.crmOwnerView!=='marketing')return;
 const hero=document.querySelector('#crmMktHome .crm-mkt-focus');if(hero){const eye=hero.querySelector('.crm-mkt-eyebrow'),h=hero.querySelector('h1'),p=hero.querySelector('p');if(eye)eye.textContent='CUSTOMER GROWTH';if(h)h.textContent='分析・アプローチ';if(p)p.textContent='データから「今、連絡すべきお客様」を見つけ、Owner確認のうえで最適な一言につなげます。'}
 const q=document.querySelector('#crmMktHome .crm-approach-queue h2');if(q)q.textContent='アプローチ候補';
 const qs=document.querySelector('#crmMktHome .crm-approach-queue .crm-mkt-sub');if(qs)qs.textContent='優先度・タイミング・連絡許可を確認し、送信前の判断までをここで行います。';
}
document.addEventListener('crm:owner-view-change',e=>{if(e.detail?.view==='marketing'){refreshCopy();loadReadOnlyMarketingData()}else closeComposer()});
document.addEventListener('crm:marketing-home-rendered',()=>{if(document.body.dataset.crmOwnerView==='marketing'){refreshCopy();loadReadOnlyMarketingData()}});
document.addEventListener('click',e=>{
 const summary=e.target.closest?.('.crm-approach-draft summary');if(summary){e.preventDefault();e.stopPropagation();openComposer(summary.closest('.crm-approach-row'));return}
 if(e.target.closest?.('#crmProposalCComposer'))return;
});
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeComposer()});
function boot(){document.body.classList.add('crm-proposal-c');composer();if(document.body.dataset.crmOwnerView==='marketing'){refreshCopy();loadReadOnlyMarketingData()}}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot,{once:true});else boot();
})();
<\/script>`;

export function injectProposalCAnalysisApproach(html){
  const source=String(html||'');
  if(!source||source.includes(STYLE_ID)||source.includes(SCRIPT_ID))return source;
  const withStyle=source.includes('</head>')?source.replace('</head>',STYLE+'</head>'):STYLE+source;
  return withStyle.includes('</body>')?withStyle.replace('</body>',SCRIPT+'</body>'):withStyle+SCRIPT;
}

export const proposalCAnalysisApproachContract=Object.freeze({
  mode:'analysis-approach-strengthened',
  mobile_first:true,
  read_only_auto_load:['analytics','approach_queue'],
  automatic_contact:false,
  automatic_line_send:false,
  owner_review_required:true,
  contact_permission_fail_closed:true,
  no_mutation_observer:true
});
