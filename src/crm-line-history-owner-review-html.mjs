function esc(v){
  return String(v??'').replace(/[&<>"']/g,ch=>({
    '&':'&amp;',
    '<':'&lt;',
    '>':'&gt;',
    '"':'&quot;',
    "'":'&#39;'
  })[ch]);
}

function labelCategory(v){
  return ({
    BLOCKED_CONFLICT:'競合あり',
    REVIEW_REQUIRED:'確認が必要',
    UNRESOLVED:'追加証拠が必要'
  })[v]||v;
}

function labelAction(v){
  return ({
    RESOLVE_CONFLICT:'競合を確認',
    OWNER_REVIEW:'Owner確認',
    NEEDS_MORE_EVIDENCE:'追加証拠待ち'
  })[v]||v;
}

function renderList(items){
  if(!items.length){
    return '<div class="empty">該当する確認項目はありません。</div>';
  }
  return items.map((item,index)=>{
    const ev=(item.evidence_types||[]).map(x=>'<span class="chip">'+esc(x)+'</span>').join('');
    const cf=(item.conflict_types||[]).map(x=>'<span class="chip danger">'+esc(x)+'</span>').join('');
    return [
      '<article class="card" data-category="'+esc(item.category)+'" data-queue="'+esc(item.queue_id)+'">',
      '<div class="cardTop">',
      '<div>',
      '<div class="eyebrow">#'+(index+1)+' · '+esc(labelAction(item.review_action))+'</div>',
      '<h2>'+esc(labelCategory(item.category))+'</h2>',
      '</div>',
      '<div class="count"><b>'+Number(item.message_rows||0)+'</b><span>messages</span></div>',
      '</div>',
      '<div class="meta">',
      '<span>Queue '+esc(item.queue_id)+'</span>',
      '<span>Identity '+esc(item.source_identity_hash)+'</span>',
      '<span>LINE '+(item.line_user_id_present?'あり':'なし')+'</span>',
      '</div>',
      '<div class="reason">'+esc(item.reason||'理由なし')+'</div>',
      '<div class="grid">',
      '<div><small>Current hint</small><b>'+Number(item.current_hint_count||0)+'</b></div>',
      '<div><small>Legacy hint</small><b>'+Number(item.legacy_hint_count||0)+'</b></div>',
      '<div><small>Target候補</small><b>'+(item.target_customer_id_present?'あり':'なし')+'</b></div>',
      '</div>',
      ev?'<div class="section"><small>Evidence</small><div class="chips">'+ev+'</div></div>':'',
      cf?'<div class="section"><small>Conflict</small><div class="chips">'+cf+'</div></div>':'',
      '</article>'
    ].join('');
  }).join('');
}

export function renderLineHistoryOwnerReviewHtml(queue={}){
  const items=Array.isArray(queue.items)?queue.items:[];
  const data=JSON.stringify({
    planner:queue.planner||'',
    review_queue_groups:Number(queue.review_queue_groups||0),
    review_queue_message_rows:Number(queue.review_queue_message_rows||0),
    blocked_conflict_groups:Number(queue.blocked_conflict_groups||0),
    review_required_groups:Number(queue.review_required_groups||0),
    unresolved_groups:Number(queue.unresolved_groups||0),
    items
  }).replace(/</g,'\\u003c');

  const list=renderList(items);

  return `<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="robots" content="noindex,nofollow,noarchive">
<title>LINE履歴 Owner Review Queue</title>
<style>
:root{--bg:#f6f7f8;--card:#fff;--text:#18212a;--muted:#6b7280;--line:#e5e7eb;--accent:#0f766e;--warn:#9a6700;--danger:#b42318}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Noto Sans JP","Hiragino Kaku Gothic ProN",sans-serif}
.app{max-width:1120px;margin:0 auto;padding:22px}
header{display:flex;justify-content:space-between;gap:18px;align-items:flex-end;margin-bottom:18px}
h1{font-size:26px;line-height:1.2;margin:0 0 6px}
.sub{color:var(--muted);font-size:13px;line-height:1.6}
.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:16px 0}
.stat{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:14px}
.stat b{display:block;font-size:23px;margin-bottom:3px}
.stat span{color:var(--muted);font-size:12px}
.toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:14px 0}
button,input{font:inherit}
button{border:1px solid var(--line);background:#fff;border-radius:999px;padding:9px 13px;font-weight:700;cursor:pointer}
button.active{background:var(--text);color:#fff;border-color:var(--text)}
.search{flex:1;min-width:220px;border:1px solid var(--line);border-radius:999px;padding:10px 14px;background:#fff}
.list{display:grid;gap:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:18px;padding:15px}
.cardTop{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
.eyebrow{font-size:11px;color:var(--muted);font-weight:700;letter-spacing:.02em}
h2{font-size:18px;margin:4px 0 0}
.count{display:flex;align-items:baseline;gap:4px;background:#f9fafb;border-radius:12px;padding:8px 10px}
.count b{font-size:19px}.count span{font-size:10px;color:var(--muted)}
.meta{display:flex;flex-wrap:wrap;gap:6px;margin:12px 0 10px}
.meta span,.chip{font-size:11px;border-radius:999px;padding:5px 8px;background:#f3f4f6;color:#4b5563}
.reason{font-weight:700;font-size:14px;padding:10px 0;border-top:1px solid #f0f1f3;border-bottom:1px solid #f0f1f3}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px}
.grid>div{background:#fafafa;border-radius:12px;padding:10px}
.grid small,.section small{display:block;color:var(--muted);font-size:10px;margin-bottom:4px}
.grid b{font-size:14px}
.section{margin-top:10px}.chips{display:flex;gap:5px;flex-wrap:wrap}.chip.danger{background:#fef3f2;color:var(--danger)}
.empty{background:#fff;border:1px solid var(--line);border-radius:18px;padding:28px;text-align:center;color:var(--muted)}
.privacy{font-size:11px;color:var(--muted);background:#fff;border:1px dashed var(--line);border-radius:14px;padding:10px 12px;margin-top:18px}
.hidden{display:none!important}
@media(max-width:720px){
  .app{padding:14px}
  header{align-items:flex-start;flex-direction:column}
  h1{font-size:22px}
  .stats{grid-template-columns:1fr 1fr}
  .grid{grid-template-columns:1fr 1fr 1fr}
  .search{width:100%;flex-basis:100%}
}
@media print{
  .toolbar{display:none}
  body{background:#fff}
  .app{max-width:none;padding:0}
  .card{break-inside:avoid}
}
</style>
</head>
<body>
<div class="app">
<header>
  <div>
    <h1>LINE履歴 Owner Review Queue</h1>
    <div class="sub">完全一致で自動解決できなかった項目だけを表示します。これはローカル確認用で、顧客データの変更・LINE送信・Production書き込みは行いません。</div>
  </div>
  <div class="sub">Planner: ${esc(queue.planner||'')}</div>
</header>
<section class="stats">
  <div class="stat"><b>${Number(queue.review_queue_groups||0)}</b><span>確認グループ</span></div>
  <div class="stat"><b>${Number(queue.review_queue_message_rows||0)}</b><span>対象messages</span></div>
  <div class="stat"><b>${Number(queue.blocked_conflict_groups||0)}</b><span>競合あり</span></div>
  <div class="stat"><b>${Number(queue.review_required_groups||0)+Number(queue.unresolved_groups||0)}</b><span>確認 / 追加証拠</span></div>
</section>
<div class="toolbar">
  <button class="active" data-filter="ALL">すべて</button>
  <button data-filter="BLOCKED_CONFLICT">競合</button>
  <button data-filter="REVIEW_REQUIRED">要確認</button>
  <button data-filter="UNRESOLVED">未解決</button>
  <input class="search" id="search" placeholder="Queue ID / Identity hash で検索">
  <button id="print">印刷</button>
</div>
<div class="list" id="list">${list}</div>
<div class="privacy">Privacy-safe view: raw Customer ID / raw LINE User ID / 顧客名 / CSV名 / メッセージ本文はこのHTMLに含めません。</div>
</div>
<script type="application/json" id="queue-data">${data}</script>
<script>
(()=>{
  const buttons=[...document.querySelectorAll('[data-filter]')];
  const cards=[...document.querySelectorAll('.card')];
  const search=document.getElementById('search');
  let filter='ALL';

  function apply(){
    const q=(search.value||'').trim().toLowerCase();
    for(const card of cards){
      const category=card.dataset.category||'';
      const queue=card.dataset.queue||'';
      const text=(card.textContent||'').toLowerCase();
      const categoryOk=filter==='ALL'||category===filter;
      const queryOk=!q||queue.toLowerCase().includes(q)||text.includes(q);
      card.classList.toggle('hidden',!(categoryOk&&queryOk));
    }
  }

  for(const button of buttons){
    button.addEventListener('click',()=>{
      filter=button.dataset.filter||'ALL';
      buttons.forEach(x=>x.classList.toggle('active',x===button));
      apply();
    });
  }

  search.addEventListener('input',apply);
  document.getElementById('print').addEventListener('click',()=>window.print());
})();
</script>
</body>
</html>`;
}
