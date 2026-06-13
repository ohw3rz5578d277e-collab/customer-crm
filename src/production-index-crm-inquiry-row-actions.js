// CUSTOMER CRM / INQUIRY ROW ACTIONS WRAPPER
// build: customer-crm-api-inquiry-row-actions-20260613-01

import app from "./production-index-crm-inquiry-actions.js";

const BUILD = "customer-crm-api-inquiry-row-actions-20260613-01";

function securityHeaders(headers = {}) {
  const h = new Headers(headers);
  h.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  h.set("pragma", "no-cache");
  h.set("expires", "0");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  return h;
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), { status, headers: securityHeaders({ "content-type": "application/json; charset=utf-8" }) });
}
function injectRowActions(html) {
  if (!html || html.includes("crmInquiryRowActionScript")) return html;
  const style = `<style id="crmInquiryRowActionStyle">
.crm-inquiry-row-dock{position:fixed;left:18px;bottom:148px;z-index:1000004}.crm-inquiry-row-dock button{border:0;background:#6d28d9;color:#fff;border-radius:999px;padding:10px 13px;font-weight:950;box-shadow:0 10px 24px rgba(109,40,217,.22);cursor:pointer}.crm-inquiry-row-modal{position:fixed;inset:0;background:rgba(15,23,42,.45);z-index:1000011;display:flex;align-items:center;justify-content:center;padding:14px}.crm-inquiry-row-box{background:#fff;border-radius:18px;max-width:980px;width:100%;max-height:88vh;overflow:auto;padding:16px;box-shadow:0 30px 80px rgba(0,0,0,.26);font-family:inherit;color:#111827}.crm-inquiry-row-head{display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap}.crm-inquiry-row-head h2{margin:0 0 4px;font-size:20px}.crm-inquiry-row-controls{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0}.crm-inquiry-row-controls input,.crm-inquiry-row-controls select{border:1px solid #ddd6fe;border-radius:10px;padding:8px;font-size:13px}.crm-inquiry-row-controls button,.crm-inquiry-row-item button{border:0;border-radius:10px;padding:8px 10px;font-size:12px;font-weight:950;cursor:pointer;background:#7c3aed;color:#fff}.crm-inquiry-row-item button.secondary,.crm-inquiry-row-controls button.secondary{background:#f3f4f6;color:#111827}.crm-inquiry-row-list{display:grid;gap:9px}.crm-inquiry-row-item{border:1px solid #ede9fe;border-radius:14px;padding:10px;background:#fff}.crm-inquiry-row-item b{font-size:14px}.crm-inquiry-row-meta{font-size:12px;color:#64748b;line-height:1.45;margin:4px 0}.crm-inquiry-row-badge{display:inline-block;border-radius:999px;background:#ede9fe;color:#5b21b6;font-size:11px;font-weight:950;padding:3px 8px;margin-right:5px}.crm-inquiry-row-actions{display:flex;gap:7px;flex-wrap:wrap;margin-top:8px}.crm-inquiry-row-empty{border:1px dashed #c4b5fd;border-radius:14px;padding:14px;color:#6b21a8;background:#faf5ff}@media(max-width:760px){.crm-inquiry-row-dock{left:10px;right:10px;bottom:142px}.crm-inquiry-row-dock button{width:100%}.crm-inquiry-row-controls input,.crm-inquiry-row-controls select,.crm-inquiry-row-controls button{width:100%}}
</style>`;
  const script = `<script id="crmInquiryRowActionScript">
(function(){
 if(window.__crmInquiryRowActionsInstalled)return;window.__crmInquiryRowActionsInstalled=true;
 function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
 function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
 function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000012;background:#4c1d95;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900';document.body.appendChild(d);setTimeout(function(){d.remove()},2500)}
 function modal(){return '<div id="crmInquiryRowModal" class="crm-inquiry-row-modal"><div class="crm-inquiry-row-box"><div class="crm-inquiry-row-head"><div><h2>問い合わせ一覧アクション</h2><p>各問い合わせから直接、LINE・フォロー・予約下書きを作成します。</p></div><button class="secondary" id="crmInquiryRowClose">閉じる</button></div><div class="crm-inquiry-row-controls"><input id="crmInquiryRowKeyword" placeholder="検索"><select id="crmInquiryRowStatus"><option value="">全ステータス</option><option>問い合わせ</option><option>日程調整中</option><option>料金案内済み</option><option>返信待ち</option><option>仮予約</option><option>予約確定</option><option>失注</option></select><button id="crmInquiryRowLoad">更新</button></div><div id="crmInquiryRowList" class="crm-inquiry-row-list"><div class="crm-inquiry-row-empty">読み込み中...</div></div></div></div>'}
 function q(){var k=document.getElementById('crmInquiryRowKeyword');var s=document.getElementById('crmInquiryRowStatus');var p=['limit=100'];if(k&&k.value)p.push('keyword='+encodeURIComponent(k.value));if(s&&s.value)p.push('status='+encodeURIComponent(s.value));return '?'+p.join('&')}
 async function load(){var list=document.getElementById('crmInquiryRowList');try{var d=await api('/api/inquiry-pipeline'+q());if(!d.ok)throw new Error(d.message||'load failed');var rows=d.items||d.inquiries||d.results||[];list.innerHTML=rows.length?rows.map(row).join(''):'<div class="crm-inquiry-row-empty">該当なし</div>';}catch(e){if(list)list.innerHTML='<div class="crm-inquiry-row-empty">読み込み失敗：'+esc(e.message||e)+'</div>';}}
 function row(x){var id=x.id||x.inquiry_id||'';var done=[];if(x.line_log_id)done.push('LINE済');if(x.follow_task_id)done.push('フォロー済');if(x.reservation_draft_id)done.push('予約下書き済');return '<div class="crm-inquiry-row-item"><span class="crm-inquiry-row-badge">'+esc(x.status||'-')+'</span><span class="crm-inquiry-row-badge">ID '+esc(id)+'</span><br><b>'+esc(x.customer_name||'-')+'</b><div class="crm-inquiry-row-meta">ジャンル '+esc(x.genre||'-')+' / 希望日 '+esc(x.preferred_date||'-')+' / 場所 '+esc(x.place||'-')+'<br>'+esc(x.memo||x.action_summary||'')+'<br>'+esc(done.join(' / '))+'</div><div class="crm-inquiry-row-actions"><button data-act="line" data-id="'+esc(id)+'">LINE作成</button><button data-act="follow" data-id="'+esc(id)+'">フォロー作成</button><button data-act="draft" data-id="'+esc(id)+'">予約下書き作成</button><button class="secondary" data-act="logs" data-id="'+esc(id)+'">ログ</button></div></div>'}
 async function run(id,kind){var url='/api/inquiry-pipeline/'+encodeURIComponent(id)+'/'+(kind==='line'?'create-line-draft':kind==='follow'?'create-follow-task':'create-reservation-draft');var body={};if(kind==='line')body={kind:'default'};if(kind==='follow')body={};if(kind==='draft'&&!confirm('予約下書きを作成しますか？'))return;var d=await api(url,{method:'POST',body:JSON.stringify(body)});if(!d.ok){toast('失敗：'+(d.message||d.status||'unknown'));return;}toast('作成しました');load();}
 async function logs(id){var d=await api('/api/inquiry-pipeline/'+encodeURIComponent(id)+'/action-logs');if(!d.ok){toast('ログ取得失敗');return;}alert((d.items||[]).map(function(x){return '['+(x.created_at||'-')+'] '+(x.action_type||'-')+' -> '+(x.target_id||'-');}).join('\n')||'ログなし');}
 function open(){if(!document.getElementById('crmInquiryRowModal'))document.body.insertAdjacentHTML('beforeend',modal());load();}
 function install(){if(document.getElementById('crmInquiryRowDock'))return;document.body.insertAdjacentHTML('beforeend','<div id="crmInquiryRowDock" class="crm-inquiry-row-dock"><button id="crmInquiryRowOpen">問い合わせ一覧アクション</button></div>')}
 document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmInquiryRowOpen')open();if(t.id==='crmInquiryRowClose'){var m=document.getElementById('crmInquiryRowModal');if(m)m.remove();}if(t.id==='crmInquiryRowLoad')load();var a=t.getAttribute&&t.getAttribute('data-act');var id=t.getAttribute&&t.getAttribute('data-id');if(a==='line'||a==='follow'||a==='draft')run(id,a);if(a==='logs')logs(id);});
 document.addEventListener('DOMContentLoaded',install);setTimeout(install,1500);
})();
</script>`;
  return html.replace("</head>", `${style}</head>`).replace("</body>", `${script}</body>`);
}
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if ((url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") && request.method === "GET") return json({ ok: true, service: "customer-crm-api", build: BUILD, time: new Date().toISOString() });
    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if (type.includes("text/html")) return new Response(injectRowActions(await res.text()), { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
