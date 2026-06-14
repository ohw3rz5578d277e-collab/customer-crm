// ======================================================
// CUSTOMER CRM / HOME DASHBOARD UX WRAPPER
// build: customer-crm-api-home-dashboard-20260614-01
// - トップ画面を1枚のダッシュボードに整理
// - 今日やること / LINE / 問い合わせ / 納品 / マーケ / 売上
// ======================================================

import app from "./production-index-crm-unified-ux.js";

const BUILD = "customer-crm-api-home-dashboard-20260614-01";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }
  });
}

function injectHomeDashboard(html){
  if(!html || html.includes("crm-home-dashboard-script")) return html;
  const style = `<style id="crm-home-dashboard-style">
:root{--home-green:#028760;--home-dark:#111827;--home-muted:#6b7280;--home-line:#e5e7eb;--home-bg:#f7f8fa;--home-red:#dc2626;--home-orange:#f59e0b;--home-blue:#2563eb;--home-shadow:0 16px 42px rgba(15,23,42,.10)}
.crm-home-wrap{max-width:1180px;margin:18px auto 18px;padding:0 14px;box-sizing:border-box}.crm-home-hero{background:linear-gradient(135deg,#ffffff,#ecfdf5);border:1px solid #bbf7d0;border-radius:24px;padding:20px;box-shadow:var(--home-shadow);display:flex;justify-content:space-between;gap:16px;align-items:flex-start}.crm-home-title{font-size:24px;font-weight:950;margin:0 0 6px;color:var(--home-dark)}.crm-home-sub{font-size:13px;color:var(--home-muted);line-height:1.7}.crm-home-actions{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}.crm-home-btn{border:0;border-radius:999px;background:var(--home-green);color:#fff;font-weight:900;padding:11px 14px;cursor:pointer}.crm-home-btn.secondary{background:#fff;color:var(--home-dark);border:1px solid var(--home-line)}.crm-home-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-top:12px}.crm-home-card{background:#fff;border:1px solid var(--home-line);border-radius:18px;padding:14px;box-shadow:0 8px 20px rgba(15,23,42,.05);min-height:94px}.crm-home-card strong{display:block;font-size:12px;color:var(--home-muted);margin-bottom:8px}.crm-home-num{font-size:28px;font-weight:950;color:var(--home-dark);line-height:1}.crm-home-note{font-size:12px;color:var(--home-muted);margin-top:8px;line-height:1.45}.crm-home-card.alert .crm-home-num{color:var(--home-red)}.crm-home-card.warn .crm-home-num{color:var(--home-orange)}.crm-home-card.good .crm-home-num{color:var(--home-green)}.crm-home-row{display:grid;grid-template-columns:1.3fr .7fr;gap:12px;margin-top:12px}.crm-home-panel{background:#fff;border:1px solid var(--home-line);border-radius:20px;padding:16px;box-shadow:0 8px 22px rgba(15,23,42,.05)}.crm-home-panel h3{margin:0 0 10px;font-size:16px}.crm-home-list{display:grid;gap:8px}.crm-home-task{display:flex;justify-content:space-between;gap:12px;align-items:center;border:1px solid var(--home-line);border-radius:14px;padding:11px;background:#fafafa}.crm-home-task b{font-size:13px}.crm-home-task span{font-size:12px;color:var(--home-muted)}.crm-home-open{border:1px solid var(--home-line);background:#fff;border-radius:999px;padding:8px 10px;font-weight:900;cursor:pointer;white-space:nowrap}.crm-home-loading{font-size:13px;color:var(--home-muted)}@media(max-width:900px){.crm-home-hero{display:block}.crm-home-actions{justify-content:flex-start;margin-top:12px}.crm-home-grid{grid-template-columns:repeat(2,1fr)}.crm-home-row{grid-template-columns:1fr}.crm-home-title{font-size:21px}}@media(max-width:520px){.crm-home-wrap{padding:0 10px;margin-top:10px}.crm-home-grid{grid-template-columns:1fr}.crm-home-card{min-height:auto}.crm-home-task{align-items:flex-start;flex-direction:column}.crm-home-open{width:100%}}
</style>`;
  const script = `<script id="crm-home-dashboard-script">
(()=>{if(window.__crmHomeDash)return;window.__crmHomeDash=1;const $=(s,r=document)=>r.querySelector(s);const $$=(s,r=document)=>Array.from(r.querySelectorAll(s));const yen=n=>new Intl.NumberFormat('ja-JP',{style:'currency',currency:'JPY',maximumFractionDigits:0}).format(Number(n||0));const esc=v=>String(v??'').replace(/[&<>]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[m]));async function get(p){try{const r=await fetch(p,{cache:'no-store'});return await r.json()}catch(e){return null}}function clickLabel(re){const b=$$('button,a,[role=button]').find(x=>re.test((x.textContent||'').trim()));if(b){b.click();return true}return false}function openMenu(){clickLabel(/CRMメニュー|メニュー/)}function openBy(label){openMenu();setTimeout(()=>clickLabel(new RegExp(label)),120)}
function mount(){if($('#crmHomeDash'))return;const target=document.querySelector('main')||document.body;target.insertAdjacentHTML('afterbegin','<section id="crmHomeDash" class="crm-home-wrap"><div class="crm-home-hero"><div><h1 class="crm-home-title">今日のCRMダッシュボード</h1><div class="crm-home-sub">重要な未対応だけを先に確認できます。迷ったら、左から順に対応してください。</div></div><div class="crm-home-actions"><button class="crm-home-btn" data-open="今日">今日やること</button><button class="crm-home-btn secondary" data-open="LINE運用">LINE運用</button><button class="crm-home-btn secondary" data-open="問い合わせ">問い合わせ</button><button class="crm-home-btn secondary" data-open="マーケ">マーケ</button></div></div><div id="crmHomeCards" class="crm-home-grid"><div class="crm-home-loading">読み込み中...</div></div><div class="crm-home-row"><div class="crm-home-panel"><h3>優先アクション</h3><div id="crmHomeTasks" class="crm-home-list"><div class="crm-home-loading">確認中...</div></div></div><div class="crm-home-panel"><h3>今日の見方</h3><div class="crm-home-sub">① 問い合わせ対応<br>② LINE未送信<br>③ 予約連携アラート<br>④ 納品未完了<br>⑤ マーケ候補へLINE作成</div></div></div></section>');$$('#crmHomeDash [data-open]').forEach(b=>b.onclick=()=>openBy(b.dataset.open));load()}
function card(title,num,note,cls=''){return '<div class="crm-home-card '+cls+'"><strong>'+esc(title)+'</strong><div class="crm-home-num">'+esc(num)+'</div><div class="crm-home-note">'+esc(note)+'</div></div>'}function task(title,note,open){return '<div class="crm-home-task"><div><b>'+esc(title)+'</b><br><span>'+esc(note)+'</span></div><button class="crm-home-open" data-open="'+esc(open)+'">開く</button></div>'}
async function load(){const [today,line,alerts,delivery,forecast,marketing]=await Promise.all([get('/api/today-dashboard'),get('/api/line-ops/dashboard'),get('/api/priority-alerts'),get('/api/delivery-dashboard'),get('/api/marketing-revenue-forecast'),get('/api/marketing-candidates?segment=all&limit=20')]);const todo=today?.summary||today||{};const l=line?.summary||line||{};const a=alerts?.summary||alerts||{};const d=delivery?.summary||delivery||{};const f=forecast||{};const m=Array.isArray(marketing?.items)?marketing.items:[];$('#crmHomeCards').innerHTML=[card('問い合わせ',todo.inquiry_waiting||a.inquiry||0,'返信・日程調整・仮予約',Number(todo.inquiry_waiting||a.inquiry||0)>0?'alert':''),card('LINE未送信',l.unsent||todo.line_unsent||0,'送る前の文面',Number(l.unsent||todo.line_unsent||0)>0?'warn':''),card('予約要確認',todo.reservation_alerts||a.reservation||0,'連携停止・再同期',Number(todo.reservation_alerts||a.reservation||0)>0?'alert':''),card('納品未完了',d.incomplete||a.delivery||0,'納品進捗を確認',Number(d.incomplete||a.delivery||0)>0?'warn':''),card('マーケ候補',m.length||a.marketing||0,'リピート・休眠・VIP',Number(m.length||a.marketing||0)>0?'good':''),card('見込み売上',yen((f.confirmed||0)+(f.pipeline||0)),'確定＋見込み')].join('');const tasks=[];if(Number(todo.inquiry_waiting||a.inquiry||0)>0)tasks.push(task('問い合わせを先に対応','返信待ち・仮予約を確認','問い合わせ'));if(Number(l.unsent||todo.line_unsent||0)>0)tasks.push(task('未送信LINEを送る','作成済み文面を送信済みにする','LINE運用'));if(Number(todo.reservation_alerts||a.reservation||0)>0)tasks.push(task('予約連携を確認','送信漏れ・再同期を確認','予約連携'));if(Number(d.incomplete||a.delivery||0)>0)tasks.push(task('納品進捗を確認','遅延や未完了を確認','納品進捗'));if(m.length)tasks.push(task('マーケ候補へ提案','リピート/休眠候補を確認','マーケDB'));$('#crmHomeTasks').innerHTML=(tasks.length?tasks.join(''):'<div class="crm-home-task"><div><b>今すぐの高優先タスクはありません</b><br><span>必要に応じてCRMメニューから確認してください。</span></div><button class="crm-home-open" data-open="CRMメニュー">メニュー</button></div>');$$('#crmHomeTasks [data-open]').forEach(b=>b.onclick=()=>openBy(b.dataset.open))}
document.readyState==='loading'?document.addEventListener('DOMContentLoaded',mount):mount();})();
</script>`;
  return html.includes("</body>") ? html.replace("</body>", style + script + "</body>") : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    try{
      if(url.pathname === "/health"){
        const res = await app.fetch(request, env, ctx);
        const data = await res.json().catch(()=>({}));
        return json({ ...data, homeDashboardBuild: BUILD });
      }
      const res = await app.fetch(request, env, ctx);
      const ct = res.headers.get("content-type") || "";
      if(request.method === "GET" && ct.includes("text/html")){
        return new Response(injectHomeDashboard(await res.text()), { status: res.status, headers: res.headers });
      }
      return res;
    }catch(e){
      return json({ ok:false, build:BUILD, message:String(e && e.message || e) }, 500);
    }
  }
};