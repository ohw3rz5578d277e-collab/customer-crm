import { createHash } from 'node:crypto';
import { buildLineHistoryOwnerReviewQueue } from './crm-line-history-owner-review-queue.mjs';

function text(v){return v==null?'':String(v).trim()}
function esc(v){
  return String(v??'').replace(/[&<>"']/g,ch=>({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[ch]);
}
function lineHash(v){
  return createHash('sha256').update(text(v)).digest('hex').slice(0,16);
}
function normalizeMaster(r){
  return {
    customer_id:text(r.customer_id),
    line_user_id:text(r.line_user_id),
    name:text(r.name),
    line_name:text(r.line_name??r.line_display_name)
  };
}
function normalizeCustomer(r){
  return {
    customer_id:text(r.customer_id),
    name:text(r.name),
    line_display_name:text(r.line_display_name),
    line_user_id:text(r.line_user_id),
    phone:text(r.phone),
    email:text(r.email),
    deleted_at:text(r.deleted_at)
  };
}
function singleQueueItem(row){
  const q=buildLineHistoryOwnerReviewQueue({
    candidate_message_rows:Number(row.message_rows||0),
    classifications:[row]
  });
  return q.items[0]||null;
}
function dedupeById(rows){
  const map=new Map();
  for(const row of rows){
    const key=row.customer_id||[row.line_user_id,row.name,row.line_name].join('|');
    if(!key||map.has(key))continue;
    map.set(key,row);
  }
  return [...map.values()];
}
function sourceRowsFor(classification,masters){
  const ids=new Set([
    ...(classification.customer_id_hints||[]),
    ...(classification.legacy_customer_id_hints||[])
  ].map(text).filter(Boolean));
  const hash=text(classification.line_id_hash);
  return dedupeById(masters.filter(m=>
    (m.customer_id&&ids.has(m.customer_id))||
    (hash&&m.line_user_id&&lineHash(m.line_user_id)===hash)
  ));
}
function categoryLabel(v){
  return ({
    BLOCKED_CONFLICT:'競合あり',
    REVIEW_REQUIRED:'確認が必要',
    UNRESOLVED:'追加証拠が必要'
  })[v]||v;
}
function sourceCard(row){
  return `<div class="identity">
    <div class="identityTitle">Customer Master</div>
    <div class="name">${esc(row.name||row.line_name||'名称なし')}</div>
    <div class="kv"><span>Customer ID</span><b>${esc(row.customer_id||'なし')}</b></div>
    <div class="kv"><span>LINE表示名</span><b>${esc(row.line_name||'なし')}</b></div>
    <div class="kv"><span>LINE ID</span><b>${row.line_user_id?'あり':'なし'}</b></div>
  </div>`;
}
function targetCard(row,targetId){
  if(!row){
    return `<div class="identity missing">
      <div class="identityTitle">CRM Target</div>
      <div class="name">現在のCRM候補なし</div>
      <div class="kv"><span>Target ID</span><b>${esc(targetId||'なし')}</b></div>
    </div>`;
  }
  return `<div class="identity">
    <div class="identityTitle">CRM Target</div>
    <div class="name">${esc(row.name||row.line_display_name||'名称なし')}</div>
    <div class="kv"><span>Customer ID</span><b>${esc(row.customer_id)}</b></div>
    <div class="kv"><span>LINE表示名</span><b>${esc(row.line_display_name||'なし')}</b></div>
    <div class="kv"><span>LINE連携</span><b>${row.line_user_id?'あり':'なし'}</b></div>
    <div class="kv"><span>電話</span><b>${row.phone?'登録あり':'なし'}</b></div>
    <div class="kv"><span>メール</span><b>${row.email?'登録あり':'なし'}</b></div>
  </div>`;
}

export function renderLineHistoryOwnerPrivateReviewHtml({
  triage={},
  customerMaster=[],
  customers=[]
}={}){
  const masters=(customerMaster||[]).map(normalizeMaster);
  const current=(customers||[]).map(normalizeCustomer).filter(x=>x.customer_id&&!x.deleted_at);
  const currentById=new Map(current.map(x=>[x.customer_id,x]));
  const classifications=(triage.classifications||[]).filter(x=>
    ['REVIEW_REQUIRED','BLOCKED_CONFLICT','UNRESOLVED'].includes(text(x.category))
  );

  const records=[];
  for(const row of classifications){
    const q=singleQueueItem(row);
    if(!q)continue;
    const source=sourceRowsFor(row,masters);
    const targetId=text(row.target_customer_id);
    const target=targetId?currentById.get(targetId)||null:null;
    records.push({queue:q,row,source,target,targetId});
  }

  records.sort((a,b)=>{
    const rank={BLOCKED_CONFLICT:0,REVIEW_REQUIRED:1,UNRESOLVED:2};
    return (rank[a.queue.category]??9)-(rank[b.queue.category]??9)||
      b.queue.message_rows-a.queue.message_rows||
      a.queue.queue_id.localeCompare(b.queue.queue_id);
  });

  const cards=records.map((rec,index)=>{
    const q=rec.queue;
    const source=rec.source.length
      ?rec.source.map(sourceCard).join('')
      :'<div class="identity missing"><div class="identityTitle">Customer Master</div><div class="name">一致するMaster候補なし</div></div>';
    const target=targetCard(rec.target,rec.targetId);
    const canSame=!!rec.target;
    const evidence=(q.evidence_types||[]).map(x=>'<span class="chip">'+esc(x)+'</span>').join('');
    const conflicts=(q.conflict_types||[]).map(x=>'<span class="chip danger">'+esc(x)+'</span>').join('');
    return `<article class="case" data-queue="${esc(q.queue_id)}" data-category="${esc(q.category)}">
      <div class="caseHead">
        <div><div class="eyebrow">#${index+1} · ${esc(q.queue_id)}</div><h2>${esc(categoryLabel(q.category))}</h2></div>
        <div class="msgCount"><b>${Number(q.message_rows||0)}</b><span>messages</span></div>
      </div>
      <div class="reason">${esc(q.reason||'理由なし')}</div>
      <div class="identityGrid">${source}${target}</div>
      <div class="summaryGrid">
        <div><small>Current hint</small><b>${Number(q.current_hint_count||0)}</b></div>
        <div><small>Legacy hint</small><b>${Number(q.legacy_hint_count||0)}</b></div>
        <div><small>LINE</small><b>${q.line_user_id_present?'あり':'なし'}</b></div>
      </div>
      ${evidence?'<div class="section"><small>Evidence</small><div class="chips">'+evidence+'</div></div>':''}
      ${conflicts?'<div class="section"><small>Conflict</small><div class="chips">'+conflicts+'</div></div>':''}
      <div class="decisionBar">
        <button data-decision="SAME_PERSON" ${canSame?'':'disabled'}>同一人物</button>
        <button data-decision="DIFFERENT_PERSON" ${canSame?'':'disabled'}>別人</button>
        <button data-decision="DEFERRED">保留</button>
        <button data-decision="NEEDS_MORE_EVIDENCE">追加証拠</button>
      </div>
      <div class="decisionState" data-state>未判断</div>
    </article>`;
  }).join('');

  const safeQueue=buildLineHistoryOwnerReviewQueue(triage);
  const bootstrap=JSON.stringify({
    planner:'line_history_owner_private_review_v1',
    queue_ids:records.map(x=>x.queue.queue_id)
  }).replace(/</g,'\\u003c');

  return `<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="robots" content="noindex,nofollow,noarchive">
<title>LINE履歴 Private Owner Review</title>
<style>
:root{--bg:#f5f6f7;--card:#fff;--text:#17202a;--muted:#667085;--line:#e4e7ec;--danger:#b42318;--ok:#067647}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Noto Sans JP",sans-serif}
.app{max-width:1180px;margin:auto;padding:20px}
header{display:flex;justify-content:space-between;gap:16px;align-items:flex-end}
h1{font-size:26px;margin:0 0 5px}.sub{font-size:12px;color:var(--muted);line-height:1.6}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:9px;margin:15px 0}
.stat,.case{background:#fff;border:1px solid var(--line);border-radius:17px}
.stat{padding:13px}.stat b{display:block;font-size:22px}.stat span{font-size:11px;color:var(--muted)}
.toolbar{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
button,input{font:inherit}button{border:1px solid var(--line);background:#fff;border-radius:999px;padding:9px 12px;font-weight:700;cursor:pointer}
button.active{background:var(--text);color:#fff}button:disabled{opacity:.35;cursor:not-allowed}
.search{flex:1;min-width:230px;border:1px solid var(--line);border-radius:999px;padding:10px 14px;background:#fff}
.list{display:grid;gap:12px}.case{padding:15px}.caseHead{display:flex;justify-content:space-between;gap:10px}
.eyebrow{font-size:10px;color:var(--muted)}h2{font-size:18px;margin:4px 0}.msgCount{display:flex;align-items:baseline;gap:4px}.msgCount b{font-size:20px}.msgCount span{font-size:10px;color:var(--muted)}
.reason{font-size:13px;font-weight:700;padding:9px 0;border-top:1px solid #f2f4f7;border-bottom:1px solid #f2f4f7}
.identityGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;margin-top:10px}.identity{background:#f9fafb;border-radius:13px;padding:11px}.identity.missing{border:1px dashed var(--line)}
.identityTitle{font-size:10px;color:var(--muted);font-weight:700}.name{font-size:16px;font-weight:800;margin:4px 0 8px}.kv{display:flex;justify-content:space-between;gap:8px;font-size:11px;padding:4px 0}.kv span{color:var(--muted)}.kv b{text-align:right;overflow-wrap:anywhere}
.summaryGrid{display:grid;grid-template-columns:repeat(3,1fr);gap:7px;margin-top:9px}.summaryGrid>div{background:#fafafa;border-radius:10px;padding:8px}.summaryGrid small,.section small{display:block;color:var(--muted);font-size:10px}.summaryGrid b{font-size:13px}
.section{margin-top:9px}.chips{display:flex;gap:5px;flex-wrap:wrap}.chip{font-size:10px;background:#f2f4f7;border-radius:999px;padding:4px 7px}.chip.danger{background:#fef3f2;color:var(--danger)}
.decisionBar{display:flex;gap:6px;flex-wrap:wrap;margin-top:12px}.decisionBar button.selected{outline:2px solid var(--text);outline-offset:1px}.decisionState{font-size:11px;color:var(--muted);margin-top:7px}
.progressNotice{margin:8px 0 12px;padding:10px 12px;background:#fff;border:1px solid var(--line);border-radius:12px;font-size:12px;font-weight:800;line-height:1.5}
.hidden{display:none!important}.privateNotice{margin-top:15px;padding:10px 12px;background:#fff6ed;border-radius:12px;font-size:11px;line-height:1.6}
@media(max-width:760px){.app{padding:12px}header{align-items:flex-start;flex-direction:column}h1{font-size:22px}.stats{grid-template-columns:1fr 1fr}.identityGrid{grid-template-columns:1fr}.search{flex-basis:100%}}
</style>
</head>
<body>
<div class="app">
<header>
<div><h1>LINE履歴 Private Owner Review</h1><div class="sub">Mac内だけで使う照合画面です。判断はブラウザ内に保持され、Exportするまで外部へ送信されません。</div></div>
<div class="sub">Private local file</div>
</header>
<section class="stats">
<div class="stat"><b>${safeQueue.review_queue_groups}</b><span>確認グループ</span></div>
<div class="stat"><b>${safeQueue.review_queue_message_rows}</b><span>messages</span></div>
<div class="stat"><b>${safeQueue.blocked_conflict_groups}</b><span>競合あり</span></div>
<div class="stat"><b id="decisionCount">0</b><span>判断済み</span></div>
</section>
<div class="toolbar">
<button class="active" data-filter="ALL">すべて</button>
<button data-filter="BLOCKED_CONFLICT">競合</button>
<button data-filter="REVIEW_REQUIRED">要確認</button>
<button data-filter="UNRESOLVED">未解決</button>
<input id="search" class="search" placeholder="名前 / Customer ID / Queue IDで検索">
<button id="bulkDeferred">未判断をすべて保留</button>
<button id="bulkMoreEvidence">未判断をすべて追加証拠</button>
<button id="export" disabled>判断JSONを書き出す</button>
<button id="reset">判断をリセット</button>
</div>
<div id="decisionProgress" class="progressNotice">判断保存済み: 0 / ${records.length} — すべて判断するとExportできます</div>
<div class="list">${cards||'<div class="case">確認対象はありません。</div>'}</div>
<div class="privateNotice">このHTMLには顧客名・Customer ID等の個人情報が含まれる場合があります。ローカル確認専用です。外部アップロード・Pages公開・メール添付は行わない運用にしてください。</div>
</div>
<script type="application/json" id="bootstrap">${bootstrap}</script>
<script>
(()=>{
  const cards=[...document.querySelectorAll('.case[data-queue]')];
  const filters=[...document.querySelectorAll('[data-filter]')];
  const search=document.getElementById('search');
  const exportButton=document.getElementById('export');
  const progress=document.getElementById('decisionProgress');
  const allowedDecisions=new Set(['SAME_PERSON','DIFFERENT_PERSON','DEFERRED','NEEDS_MORE_EVIDENCE']);
  const bootstrap=JSON.parse(document.getElementById('bootstrap').textContent||'{}');
  const storageKey='crm-line-history-owner-review-v2:'+(bootstrap.queue_ids||[]).join('|');
  const decisions={};
  let filter='ALL';

  function persist(){
    try{
      localStorage.setItem(storageKey,JSON.stringify(decisions));
    }catch{}
  }

  function restore(){
    let saved={};
    try{
      saved=JSON.parse(localStorage.getItem(storageKey)||'{}')||{};
    }catch{}
    cards.forEach(card=>{
      const queueId=card.dataset.queue;
      const decision=saved[queueId];
      if(!allowedDecisions.has(decision))return;
      const button=[...card.querySelectorAll('[data-decision]')]
        .find(x=>x.dataset.decision===decision&&!x.disabled);
      if(!button)return;
      decisions[queueId]=decision;
      card.querySelectorAll('[data-decision]')
        .forEach(x=>x.classList.toggle('selected',x===button));
      const state=card.querySelector('[data-state]');
      if(state)state.textContent='判断: '+button.textContent;
    });
  }

  function updateCount(){
    const count=Object.keys(decisions).length;
    document.getElementById('decisionCount').textContent=count;
    const complete=count===cards.length;
    exportButton.disabled=!complete;
    progress.textContent='判断保存済み: '+count+' / '+cards.length+
      (complete?' — Exportできます':' — すべて判断するとExportできます');
  }

  function setDecision(card,decision){
    const queueId=card.dataset.queue;
    const button=[...card.querySelectorAll('[data-decision]')]
      .find(x=>x.dataset.decision===decision&&!x.disabled);
    if(!button)return false;
    decisions[queueId]=decision;
    card.querySelectorAll('[data-decision]')
      .forEach(x=>x.classList.toggle('selected',x===button));
    const state=card.querySelector('[data-state]');
    if(state)state.textContent='判断: '+button.textContent;
    persist();
    updateCount();
    return true;
  }

  function apply(){
    const q=(search.value||'').trim().toLowerCase();
    cards.forEach(card=>{
      const category=card.dataset.category||'';
      const body=(card.textContent||'').toLowerCase();
      card.classList.toggle('hidden',!((filter==='ALL'||category===filter)&&(!q||body.includes(q))));
    });
  }

  function bulkUndecided(decision,label){
    const remaining=cards.filter(card=>!decisions[card.dataset.queue]);
    if(!remaining.length)return;
    if(!confirm('未判断 '+remaining.length+' 件を「'+label+'」に設定します。\\n既に判断済みの項目は変更しません。'))return;
    remaining.forEach(card=>{
      const queueId=card.dataset.queue;
      const button=[...card.querySelectorAll('[data-decision]')]
        .find(x=>x.dataset.decision===decision&&!x.disabled);
      if(!button)return;
      decisions[queueId]=decision;
      card.querySelectorAll('[data-decision]')
        .forEach(x=>x.classList.toggle('selected',x===button));
      const state=card.querySelector('[data-state]');
      if(state)state.textContent='判断: '+button.textContent;
    });
    persist();
    updateCount();
  }

  filters.forEach(button=>button.addEventListener('click',()=>{
    filter=button.dataset.filter||'ALL';
    filters.forEach(x=>x.classList.toggle('active',x===button));
    apply();
  }));
  search.addEventListener('input',apply);

  cards.forEach(card=>{
    card.querySelectorAll('[data-decision]').forEach(button=>button.addEventListener('click',()=>{
      setDecision(card,button.dataset.decision);
    }));
  });

  document.getElementById('bulkDeferred').addEventListener('click',()=>{
    bulkUndecided('DEFERRED','保留');
  });
  document.getElementById('bulkMoreEvidence').addEventListener('click',()=>{
    bulkUndecided('NEEDS_MORE_EVIDENCE','追加証拠');
  });

  document.getElementById('reset').addEventListener('click',()=>{
    if(Object.keys(decisions).length&&!confirm('保存済みの判断をすべてリセットしますか？'))return;
    Object.keys(decisions).forEach(k=>delete decisions[k]);
    try{localStorage.removeItem(storageKey);}catch{}
    cards.forEach(card=>{
      card.querySelectorAll('[data-decision]').forEach(x=>x.classList.remove('selected'));
      const state=card.querySelector('[data-state]');
      if(state)state.textContent='未判断';
    });
    updateCount();
  });

  exportButton.addEventListener('click',()=>{
    if(Object.keys(decisions).length!==cards.length){
      alert('すべての確認グループを判断してからExportしてください。');
      return;
    }
    const payload={
      planner:'line_history_owner_review_decisions_v1',
      generated_at:new Date().toISOString(),
      decisions:cards.map(card=>({
        queue_id:card.dataset.queue,
        decision:decisions[card.dataset.queue]
      }))
    };
    const blob=new Blob([JSON.stringify(payload,null,2)+'\\n'],{type:'application/json'});
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a');
    a.href=url;
    a.download='line-history-owner-review-decisions.json';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  });

  window.addEventListener('beforeunload',event=>{
    const count=Object.keys(decisions).length;
    if(count>0&&count<cards.length){
      event.preventDefault();
      event.returnValue='';
    }
  });

  restore();
  persist();
  updateCount();
})();
</script>
</body>
</html>`;
}
