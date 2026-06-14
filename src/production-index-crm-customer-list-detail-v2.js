// ======================================================
// CUSTOMER CRM / CUSTOMER LIST DETAIL V2 WRAPPER
// build: customer-crm-customer-list-detail-v2-20260614-03
// - Makes stable customer list cards open a reliable detail panel
// - Adds dedicated customer detail API independent from old DOM click
// - LINE history is shown as a light timeline list, not chat bubbles
// ======================================================

import app from "./production-index-crm-stable-customer-list.js";

const BUILD = "customer-crm-customer-list-detail-v2-20260614-03";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store, no-cache, must-revalidate, max-age=0"
    }
  });
}

async function safeAll(env, sql, params = []){
  if(!env || !env.DB) return {ok:false, results:[], error:"DB binding missing"};
  try{
    let stmt = env.DB.prepare(sql);
    if(params.length) stmt = stmt.bind(...params);
    return await stmt.all();
  }catch(e){
    return {ok:false, results:[], error:String(e && e.message || e)};
  }
}

async function tableExists(env, name){
  const r = await safeAll(env, `SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1`, [name]);
  return !!(r.results && r.results.length);
}

function firstValue(row, names, fallback = ""){
  for(const name of names){
    if(row && row[name] !== undefined && row[name] !== null && String(row[name]).trim() !== "") return row[name];
  }
  return fallback;
}

function normalizeCustomer(row){
  return {
    id: firstValue(row, ["id", "customer_id", "line_user_id"]),
    customer_id: firstValue(row, ["customer_id", "id"]),
    name: firstValue(row, ["name", "customer_name", "line_display_name", "display_name", "full_name"], "名前未設定"),
    kana: firstValue(row, ["furigana", "kana", "name_kana", "yomi"]),
    line_display_name: firstValue(row, ["line_display_name", "line_name"]),
    phone: firstValue(row, ["phone", "tel", "telephone"]),
    email: firstValue(row, ["email", "mail"]),
    last_shoot_date: firstValue(row, ["last_shoot_date", "last_reservation_date", "shoot_date"]),
    genre_history: firstValue(row, ["genre_history", "last_genre", "genre"]),
    repeat_count: Number(firstValue(row, ["repeat_count", "reservation_count"], 0)) || 0,
    total_revenue: Number(firstValue(row, ["total_revenue", "sales_total"], 0)) || 0,
    customer_rank: firstValue(row, ["customer_rank", "rank"], "未設定"),
    memo: firstValue(row, ["memo", "note", "notes"]),
    raw: row || {}
  };
}

async function customerDetail(env, url){
  const id = (url.searchParams.get("id") || url.searchParams.get("customer_id") || "").trim();
  if(!id) return json({ok:false, build:BUILD, error:"customer id is required"}, 400);
  if(!await tableExists(env, "customers")) return json({ok:true, build:BUILD, customer:null, reservations:[], line_logs:[], message:"customers table not found"});

  const cRes = await safeAll(env, `SELECT * FROM customers WHERE CAST(id AS TEXT)=? OR CAST(customer_id AS TEXT)=? OR CAST(line_user_id AS TEXT)=? LIMIT 1`, [id, id, id]);
  const customer = cRes.results && cRes.results[0] ? normalizeCustomer(cRes.results[0]) : null;
  if(!customer) return json({ok:true, build:BUILD, customer:null, reservations:[], line_logs:[], message:"customer not found"});

  const reservations = [];
  const lineLogs = [];
  const followTasks = [];

  if(await tableExists(env, "customer_reservations")){
    const r = await safeAll(env, `SELECT * FROM customer_reservations WHERE CAST(customer_id AS TEXT)=? OR CAST(customer_name AS TEXT)=? ORDER BY COALESCE(shoot_date, created_at, '') DESC LIMIT 20`, [String(customer.customer_id || customer.id), customer.name]);
    if(r.results && r.results.length) reservations.push(...r.results);
  }
  if(await tableExists(env, "crm_reservation_drafts")){
    const r = await safeAll(env, `SELECT * FROM crm_reservation_drafts WHERE CAST(customer_id AS TEXT)=? OR customer_name=? ORDER BY COALESCE(shoot_date, created_at, '') DESC LIMIT 20`, [String(customer.customer_id || customer.id), customer.name]);
    if(r.results && r.results.length) reservations.push(...r.results.map(x => ({...x, source:"draft"})));
  }
  if(await tableExists(env, "customer_line_draft_logs")){
    const r = await safeAll(env, `SELECT * FROM customer_line_draft_logs WHERE CAST(customer_id AS TEXT)=? OR customer_name=? ORDER BY COALESCE(created_at, copied_at, '') DESC LIMIT 20`, [String(customer.customer_id || customer.id), customer.name]);
    if(r.results && r.results.length) lineLogs.push(...r.results);
  }
  if(await tableExists(env, "crm_follow_tasks")){
    const r = await safeAll(env, `SELECT * FROM crm_follow_tasks WHERE CAST(customer_id AS TEXT)=? OR customer_name=? ORDER BY COALESCE(due_date, created_at, '') DESC LIMIT 20`, [String(customer.customer_id || customer.id), customer.name]);
    if(r.results && r.results.length) followTasks.push(...r.results);
  }

  return json({ok:true, build:BUILD, customer, reservations, line_logs:lineLogs, follow_tasks:followTasks});
}

function injectDetailV2(html){
  if(!html || html.includes("crm-customer-detail-v2-script")) return html;

  const style = `<style id="crm-customer-detail-v2-style">
.crm-v2-detail-panel{position:fixed!important;right:18px!important;top:18px!important;bottom:18px!important;width:min(760px,calc(100vw - 36px))!important;background:#fff!important;border:1px solid #dbe5ef!important;border-radius:24px!important;box-shadow:0 28px 70px rgba(15,23,42,.24)!important;z-index:2147483100!important;display:none!important;overflow:hidden!important;color:#07111f!important}.crm-v2-detail-panel.open{display:flex!important;flex-direction:column!important}.crm-v2-detail-head{padding:18px 64px 16px 18px!important;border-bottom:1px solid #e2e8f0!important;background:linear-gradient(135deg,#f8fafc,#fff)!important}.crm-v2-detail-head h2{font-size:24px!important;line-height:1.25!important;margin:0 0 6px!important;font-weight:950!important}.crm-v2-detail-head p{font-size:13px!important;color:#64748b!important;line-height:1.7!important;margin:0!important}.crm-v2-detail-close{position:absolute!important;right:14px!important;top:14px!important;width:42px!important;height:42px!important;border-radius:999px!important;border:1px solid #dbe5ef!important;background:#fff!important;font-size:22px!important;font-weight:950!important;cursor:pointer!important}.crm-v2-detail-body{overflow:auto!important;padding:18px!important;display:grid!important;gap:14px!important}.crm-v2-detail-grid{display:grid!important;grid-template-columns:repeat(2,1fr)!important;gap:10px!important}.crm-v2-detail-card{border:1px solid #e2e8f0!important;border-radius:18px!important;background:#fff!important;padding:14px!important}.crm-v2-detail-card b{display:block!important;font-size:20px!important;color:#07111f!important}.crm-v2-detail-card span{font-size:13px!important;color:#64748b!important}.crm-v2-section h3{font-size:18px!important;margin:0 0 8px!important;font-weight:950!important}.crm-v2-row{border:1px solid #e2e8f0!important;border-radius:16px!important;padding:12px!important;background:#f8fafc!important;margin-bottom:8px!important;font-size:13px!important;line-height:1.6!important}.crm-v2-empty{border:1px dashed #cbd5e1!important;border-radius:16px!important;padding:14px!important;color:#64748b!important;background:#f8fafc!important}.crm-line-lite-list{display:grid!important;gap:8px!important}.crm-line-lite-item{border:1px solid #e2e8f0!important;border-radius:14px!important;background:#fff!important;padding:10px 12px!important;display:grid!important;gap:4px!important}.crm-line-lite-top{display:flex!important;align-items:center!important;gap:8px!important;font-size:12px!important;color:#64748b!important}.crm-line-lite-badge{display:inline-flex!important;align-items:center!important;justify-content:center!important;min-width:44px!important;height:22px!important;border-radius:999px!important;font-size:11px!important;font-weight:950!important}.crm-line-lite-badge.out{background:#dcfce7!important;color:#166534!important}.crm-line-lite-badge.in{background:#e0f2fe!important;color:#075985!important}.crm-line-lite-text{font-size:14px!important;line-height:1.6!important;color:#07111f!important;white-space:normal!important;word-break:break-word!important}@media(max-width:767px){.crm-v2-detail-panel{inset:8px!important;width:auto!important;border-radius:20px!important}.crm-v2-detail-head{padding:16px 62px 14px 16px!important}.crm-v2-detail-head h2{font-size:22px!important}.crm-v2-detail-body{padding:12px 12px calc(24px + env(safe-area-inset-bottom))!important}.crm-v2-detail-grid{grid-template-columns:1fr 1fr!important}}
</style>`;

  const script = `<script id="crm-customer-detail-v2-script">
(()=>{
  if(window.__crmCustomerDetailV2) return;
  window.__crmCustomerDetailV2 = 1;
  function yen(n){ return '¥' + Math.round(Number(n || 0)).toLocaleString('ja-JP'); }
  function text(v){ return String(v == null ? '' : v); }
  function closeDetail(){ document.getElementById('crmV2DetailPanel')?.classList.remove('open'); }
  function esc(s){ return text(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
  function ensureDetailPanel(){
    if(document.getElementById('crmV2DetailPanel')) return;
    const panel = document.createElement('div');
    panel.id = 'crmV2DetailPanel';
    panel.className = 'crm-v2-detail-panel';
    panel.innerHTML = '<button class="crm-v2-detail-close" type="button">×</button><div class="crm-v2-detail-head"><h2>顧客詳細</h2><p>顧客情報・撮影履歴・LINE・フォローを確認できます。</p></div><div id="crmV2DetailBody" class="crm-v2-detail-body"><div class="crm-v2-empty">顧客を選択してください。</div></div>';
    panel.querySelector('.crm-v2-detail-close').onclick = closeDetail;
    document.body.appendChild(panel);
  }
  function row(title, value){ return '<div class="crm-v2-detail-card"><b>'+esc(value || '未設定')+'</b><span>'+esc(title)+'</span></div>'; }
  function lineText(r){ return r.message_text || r.message || r.text || r.body || r.memo || r.action_label || r.status || ''; }
  function lineTime(r){ return r.sent_at || r.replied_at || r.copied_at || r.created_at || r.updated_at || ''; }
  function isIncoming(r){ const s=String(r.direction||r.message_type||r.action_type||r.status||'').toLowerCase(); return /in|reply|replied|receive|user/.test(s); }
  function lineRows(logs){
    if(!logs || !logs.length) return '<div class="crm-v2-empty">LINE履歴はまだありません。</div>';
    return '<div class="crm-line-lite-list">' + logs.map(r => {
      const incoming = isIncoming(r);
      const badge = incoming ? '返信' : '送信';
      const cls = incoming ? 'in' : 'out';
      return '<div class="crm-line-lite-item"><div class="crm-line-lite-top"><span class="crm-line-lite-badge '+cls+'">'+badge+'</span><span>'+esc(lineTime(r) || '日時未設定')+'</span></div><div class="crm-line-lite-text">'+esc(lineText(r) || '内容なし')+'</div></div>';
    }).join('') + '</div>';
  }
  function listRows(rows, emptyText){
    if(!rows || !rows.length) return '<div class="crm-v2-empty">'+esc(emptyText)+'</div>';
    return rows.map(r => '<div class="crm-v2-row">'+esc([r.shoot_date || r.due_date || r.created_at || '', r.genre || r.task_type || r.action_label || r.status || '', r.total_amount ? yen(r.total_amount) : '', r.message_text || r.memo || ''].filter(Boolean).join(' / '))+'</div>').join('');
  }
  async function openDetail(id){
    ensureDetailPanel();
    const panel = document.getElementById('crmV2DetailPanel');
    const body = document.getElementById('crmV2DetailBody');
    panel.classList.add('open');
    body.innerHTML = '<div class="crm-v2-empty">読み込み中...</div>';
    try{
      const r = await fetch('/api/stable-customer-detail?id=' + encodeURIComponent(id), {cache:'no-store'});
      const j = await r.json();
      if(!j.customer){ body.innerHTML = '<div class="crm-v2-empty">顧客詳細が見つかりませんでした。</div>'; return; }
      const c = j.customer;
      panel.querySelector('h2').textContent = c.name || '顧客詳細';
      panel.querySelector('.crm-v2-detail-head p').textContent = ['LINE: '+(c.line_display_name || '未設定'), c.genre_history ? 'ジャンル: '+c.genre_history : '', c.last_shoot_date ? '直近: '+c.last_shoot_date : ''].filter(Boolean).join(' / ');
      body.innerHTML = '<div class="crm-v2-detail-grid">'+row('撮影回数', c.repeat_count || 0)+row('累計売上', yen(c.total_revenue))+row('ランク', c.customer_rank || '未設定')+row('電話', c.phone || '未設定')+'</div><div class="crm-v2-section"><h3>撮影・予約履歴</h3>'+listRows(j.reservations, '撮影・予約履歴はまだありません。')+'</div><div class="crm-v2-section"><h3>LINE履歴</h3>'+lineRows(j.line_logs)+'</div><div class="crm-v2-section"><h3>フォロー予定</h3>'+listRows(j.follow_tasks, 'フォロー予定はまだありません。')+'</div>';
    }catch(e){ body.innerHTML = '<div class="crm-v2-empty">詳細の読み込みに失敗しました。状態確認を実行してください。</div>'; }
  }
  window.__crmOpenCustomerDetailV2 = openDetail;
  function patchCards(){
    document.querySelectorAll('.crm-stable-customer-card').forEach(card => {
      if(card.dataset.v2Patched === '1') return;
      card.dataset.v2Patched = '1';
      const id = card.getAttribute('data-customer-id') || '';
      card.addEventListener('click', e => { e.preventDefault(); e.stopImmediatePropagation(); openDetail(id); }, true);
    });
  }
  function boot(){ ensureDetailPanel(); patchCards(); }
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
  new MutationObserver(() => setTimeout(boot, 120)).observe(document.documentElement, {childList:true, subtree:true});
  document.addEventListener('keydown', e => { if(e.key === 'Escape') closeDetail(); });
})();
</script>`;

  return html.includes('</head>') ? html.replace('</head>', style + '</head>').replace('</body>', script + '</body>') : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    if(request.method === 'GET' && url.pathname === '/api/stable-customer-detail') return customerDetail(env, url);
    const res = await app.fetch(request, env, ctx);
    const ct = res.headers.get('content-type') || '';
    if(request.method === 'GET' && ct.includes('text/html')){
      const html = await res.text();
      return new Response(injectDetailV2(html), {status:res.status, headers:res.headers});
    }
    return res;
  }
};
