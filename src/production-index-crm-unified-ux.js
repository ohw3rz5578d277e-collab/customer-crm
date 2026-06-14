// ======================================================
// CUSTOMER CRM / UNIFIED UX SHELL WRAPPER
// build: customer-crm-api-unified-ux-20260614-01
// - 右下ボタン乱立を統合
// - サイドメニュー / コマンド検索 / スマホ最適化
// ======================================================

import app from "./production-index-crm-line-ops.js";

const BUILD = "customer-crm-api-unified-ux-20260614-01";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }
  });
}

function injectUnifiedUx(html){
  if(!html || html.includes("crm-unified-ux-script")) return html;
  const style = `<style id="crm-unified-ux-style">
:root{--crm-green:#028760;--crm-dark:#111827;--crm-muted:#6b7280;--crm-line:#e5e7eb;--crm-bg:#f7f8fa;--crm-card:#fff;--crm-red:#dc2626;--crm-shadow:0 18px 45px rgba(15,23,42,.18)}
html{scroll-behavior:smooth}body{background:var(--crm-bg)!important;color:var(--crm-dark)!important}.crm-unified-hide{display:none!important}.crm-ux-fab{position:fixed;right:18px;bottom:18px;z-index:2147483000}.crm-ux-fab button{border:0;border-radius:999px;background:var(--crm-green);color:#fff;font-weight:900;padding:14px 18px;box-shadow:var(--crm-shadow);cursor:pointer;letter-spacing:.02em}.crm-ux-backdrop{position:fixed;inset:0;background:rgba(15,23,42,.35);z-index:2147483001;display:none}.crm-ux-backdrop.open{display:block}.crm-ux-drawer{position:fixed;top:0;right:0;width:min(460px,94vw);height:100vh;background:#fff;z-index:2147483002;box-shadow:-20px 0 50px rgba(0,0,0,.24);transform:translateX(105%);transition:.2s ease;display:flex;flex-direction:column}.crm-ux-drawer.open{transform:translateX(0)}.crm-ux-head{padding:18px;border-bottom:1px solid var(--crm-line);background:linear-gradient(180deg,#fff,#f9fafb)}.crm-ux-head-row{display:flex;align-items:center;justify-content:space-between;gap:10px}.crm-ux-title{font-size:20px;font-weight:900}.crm-ux-close{border:1px solid var(--crm-line);background:#fff;border-radius:12px;padding:9px 12px;font-weight:800;cursor:pointer}.crm-ux-search{width:100%;box-sizing:border-box;border:1px solid var(--crm-line);border-radius:14px;padding:13px 14px;margin-top:12px;font-size:16px}.crm-ux-body{overflow:auto;padding:14px 18px 26px}.crm-ux-section{margin:14px 0}.crm-ux-section h3{font-size:13px;color:var(--crm-muted);margin:0 0 8px;font-weight:900;letter-spacing:.08em}.crm-ux-grid{display:grid;grid-template-columns:1fr 1fr;gap:9px}.crm-ux-item{border:1px solid var(--crm-line);background:#fff;border-radius:16px;padding:12px;text-align:left;cursor:pointer;box-shadow:0 6px 18px rgba(15,23,42,.04)}.crm-ux-item:hover{border-color:var(--crm-green);box-shadow:0 10px 24px rgba(2,135,96,.12)}.crm-ux-item b{display:block;font-size:14px;margin-bottom:4px}.crm-ux-item span{display:block;font-size:12px;color:var(--crm-muted);line-height:1.45}.crm-ux-chip{display:inline-flex;align-items:center;gap:6px;border-radius:999px;background:#ecfdf5;color:#047857;font-size:11px;font-weight:900;padding:4px 8px;margin-top:8px}.crm-ux-guide{border:1px solid #bbf7d0;background:#f0fdf4;border-radius:16px;padding:12px;font-size:13px;line-height:1.7}.crm-ux-toast{position:fixed;left:50%;bottom:22px;transform:translateX(-50%);background:#111827;color:#fff;border-radius:999px;padding:10px 14px;z-index:2147483100;font-weight:800;display:none}.crm-ux-toast.open{display:block}.crm-ux-shortcut{font-size:12px;color:var(--crm-muted);margin-top:8px}.crm-ux-polish button,.crm-ux-polish input,.crm-ux-polish select,.crm-ux-polish textarea{font-size:16px}.crm-ux-polish button{min-height:38px}.crm-ux-polish [class*="panel"],.crm-ux-polish [class*="card"],.crm-ux-polish table{border-radius:14px}@media(max-width:760px){body{padding-bottom:72px}.crm-ux-fab{right:12px;bottom:84px}.crm-ux-fab button{padding:13px 16px}.crm-ux-drawer{width:100vw}.crm-ux-grid{grid-template-columns:1fr}.crm-ux-head{padding:14px}.crm-ux-body{padding:12px 14px 86px}.crm-ux-item{padding:13px}.crm-ux-toast{bottom:82px;width:max-content;max-width:88vw;text-align:center}}
</style>`;

  const script = `<script id="crm-unified-ux-script">
(()=>{if(window.__crmUnifiedUx)return;window.__crmUnifiedUx=1;const $=(s,r=document)=>r.querySelector(s);const $$=(s,r=document)=>Array.from(r.querySelectorAll(s));const esc=v=>String(v??'').replace(/[&<>]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[m]));
const items=[
 ['今日','今日やること','今日の確認・クイック操作','button,[role=button]','今日|やること|クイック|重要'],
 ['予約','予約連携監視','予約連携の停止や再同期を確認','button,[role=button]','予約連携|予約アラート|再同期'],
 ['顧客','顧客スマート','顧客の次アクションを確認','button,[role=button]','顧客スマート'],
 ['顧客','一覧操作','顧客・問い合わせ・マーケ候補を一括操作','button,[role=button]','一覧操作'],
 ['LINE','LINE運用','送信済み・返信・予約化を管理','button,[role=button]','LINE運用'],
 ['LINE','LINEテンプレ管理','テンプレ作成・編集','button,[role=button]','LINEテンプレ'],
 ['LINE','フォロー→LINE','フォロー予定からLINE文面作成','button,[role=button]','フォロー→LINE'],
 ['問い合わせ','問い合わせ管理','問い合わせパイプライン管理','button,[role=button]','問い合わせ管理'],
 ['問い合わせ','問い合わせ一覧アクション','問い合わせ行からLINE/予約/フォロー作成','button,[role=button]','問い合わせ一覧'],
 ['マーケ','マーケDB','リピート・休眠・VIP候補を確認','button,[role=button]','マーケDB'],
 ['マーケ','キャンペーン','キャンペーン配信リスト','button,[role=button]','キャンペーン'],
 ['分析','成約・流入','成約率・流入元別売上','button,[role=button]','成約|流入'],
 ['分析','統合ログ','操作履歴を確認','button,[role=button]','統合ログ'],
 ['納品','納品進捗','納品遅延・進捗確認','button,[role=button]','納品進捗']
];
function toast(t){let el=$('#crmUxToast');if(!el){document.body.insertAdjacentHTML('beforeend','<div id="crmUxToast" class="crm-ux-toast"></div>');el=$('#crmUxToast')}el.textContent=t;el.classList.add('open');setTimeout(()=>el.classList.remove('open'),1800)}
function clickBy(pattern){const re=new RegExp(pattern);const target=$$('button,a,[role=button]').find(b=>re.test((b.textContent||'').trim()));if(target){target.classList.remove('crm-unified-hide');target.click();setTimeout(()=>hideLegacyButtons(),200);return true}return false}
function openFeature(name,pat){if(clickBy(pat)){toast(name+'を開きました');close();return}toast('まだ画面上に入口がありません: '+name)}
function render(filter=''){const q=filter.trim().toLowerCase();const groups={};items.filter(x=>!q||x.join(' ').toLowerCase().includes(q)).forEach(x=>{(groups[x[0]] ||= []).push(x)});$('#crmUxBody').innerHTML=Object.keys(groups).map(g=>'<section class="crm-ux-section"><h3>'+esc(g)+'</h3><div class="crm-ux-grid">'+groups[g].map(x=>'<button class="crm-ux-item" data-name="'+esc(x[1])+'" data-pat="'+esc(x[4])+'"><b>'+esc(x[1])+'</b><span>'+esc(x[2])+'</span><em class="crm-ux-chip">開く</em></button>').join('')+'</div></section>').join('')+'<div class="crm-ux-guide"><b>おすすめの使い方</b><br>朝：今日やること → 重要 → LINE運用<br>問い合わせ対応：問い合わせ管理 → 問い合わせ一覧アクション → 顧客スマート<br>営業：マーケDB → 一覧操作 → 一括確認 → LINE運用</div>' ; $$('.crm-ux-item').forEach(b=>b.onclick=()=>openFeature(b.dataset.name,b.dataset.pat))}
function open(){mount();$('#crmUxBackdrop').classList.add('open');$('#crmUxDrawer').classList.add('open');$('#crmUxSearch').focus();render($('#crmUxSearch').value)}function close(){$('#crmUxBackdrop')?.classList.remove('open');$('#crmUxDrawer')?.classList.remove('open')}
function hideLegacyButtons(){const keep=['crmUxOpen'];$$('button').forEach(b=>{const t=(b.textContent||'').trim();if(keep.includes(b.id))return;if(/^(LINE運用|一括確認|一覧操作|顧客スマート|重要|メニュー|マーケDB|キャンペーン|成約・流入|問い合わせ一覧アクション|問い合わせ→次アクション|フォロー→LINE|LINEテンプレ管理)$/.test(t)){const box=b.closest('div');(box||b).classList.add('crm-unified-hide')}})}
function mount(){if($('#crmUxDrawer'))return;document.body.classList.add('crm-ux-polish');document.body.insertAdjacentHTML('beforeend','<div class="crm-ux-fab"><button id="crmUxOpen">CRMメニュー</button></div><div id="crmUxBackdrop" class="crm-ux-backdrop"></div><aside id="crmUxDrawer" class="crm-ux-drawer"><div class="crm-ux-head"><div class="crm-ux-head-row"><div><div class="crm-ux-title">CRMメニュー</div><div class="crm-ux-shortcut">Ctrl / ⌘ + K で開く</div></div><button id="crmUxClose" class="crm-ux-close">閉じる</button></div><input id="crmUxSearch" class="crm-ux-search" placeholder="機能名で検索：LINE、予約、問い合わせ、マーケ…"></div><div id="crmUxBody" class="crm-ux-body"></div></aside>');$('#crmUxOpen').onclick=open;$('#crmUxClose').onclick=close;$('#crmUxBackdrop').onclick=close;$('#crmUxSearch').oninput=e=>render(e.target.value);hideLegacyButtons();setInterval(hideLegacyButtons,1500)}
document.addEventListener('keydown',e=>{if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){e.preventDefault();open()}if(e.key==='Escape')close()});document.readyState==='loading'?document.addEventListener('DOMContentLoaded',mount):mount();})();
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
        return json({ ...data, unifiedUxBuild: BUILD });
      }
      const res = await app.fetch(request, env, ctx);
      const ct = res.headers.get("content-type") || "";
      if(request.method === "GET" && ct.includes("text/html")){
        return new Response(injectUnifiedUx(await res.text()), { status: res.status, headers: res.headers });
      }
      return res;
    }catch(e){
      return json({ ok:false, build:BUILD, message:String(e && e.message || e) }, 500);
    }
  }
};
