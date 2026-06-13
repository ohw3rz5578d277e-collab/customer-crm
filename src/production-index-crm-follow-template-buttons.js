// ======================================================
// CUSTOMER CRM / FOLLOW TEMPLATE DIRECT BUTTONS WRAPPER
// build: customer-crm-api-follow-template-buttons-20260613-01
// Adds direct "LINE draft" buttons to follow task cards / today action queue.
// ======================================================

import app from "./production-index-crm-follow-template.js";

const BUILD = "customer-crm-api-follow-template-buttons-20260613-01";

function securityHeaders(headers = {}) {
  const h = new Headers(headers);
  h.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  h.set("pragma", "no-cache");
  h.set("expires", "0");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  return h;
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: securityHeaders({ "content-type": "application/json; charset=utf-8" })
  });
}

function injectDirectButtons(html) {
  if (!html || html.includes("crmFollowTemplateButtonScript")) return html;
  const style = `<style id="crmFollowTemplateButtonStyle">
.crm-follow-line-btn{border:0!important;background:#0f766e!important;color:#fff!important;border-radius:10px!important;padding:7px 9px!important;font-size:12px!important;font-weight:950!important;cursor:pointer!important;margin-left:6px!important}.crm-follow-template-modal{position:fixed;inset:0;z-index:1000010;background:rgba(15,23,42,.38);display:none;align-items:center;justify-content:center;padding:16px}.crm-follow-template-modal.open{display:flex}.crm-follow-template-box{width:min(560px,100%);max-height:78vh;overflow:auto;background:#fff;border-radius:18px;padding:14px;border:1px solid #99f6e4;box-shadow:0 20px 70px rgba(0,0,0,.28);color:#0f172a}.crm-follow-template-box h3{margin:0 0 6px;font-size:17px}.crm-follow-template-choice{border:1px solid #ccfbf1;border-radius:13px;padding:10px;margin:8px 0;font-size:13px;background:#f8fffd}.crm-follow-template-choice b{font-weight:950}.crm-follow-template-choice pre{white-space:pre-wrap;background:#f8fafc;border-radius:10px;padding:8px;color:#334155;font-family:inherit}.crm-follow-template-choice button,.crm-follow-template-close{border:0;background:#0f766e;color:#fff;border-radius:10px;padding:8px 10px;font-size:12px;font-weight:950;cursor:pointer;margin-top:6px}.crm-follow-template-close{background:#475569}
</style>`;
  const script = `<script id="crmFollowTemplateButtonScript">
(function(){
 if(window.__crmFollowTemplateButtonsInstalled)return;window.__crmFollowTemplateButtonsInstalled=true;
 function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
 function api(u,o){return fetch(u,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},o||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
 function toast(m){var d=document.createElement('div');d.textContent=m;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000012;background:#0f766e;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.2)';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
 function ensureModal(){var m=document.getElementById('crmFollowTemplateDirectModal');if(m)return m;document.body.insertAdjacentHTML('beforeend','<div id="crmFollowTemplateDirectModal" class="crm-follow-template-modal"><div class="crm-follow-template-box"><button id="crmFollowTemplateDirectClose" class="crm-follow-template-close">閉じる</button><h3>LINE文面作成</h3><div id="crmFollowTemplateDirectBody">読み込み中...</div></div></div>');return document.getElementById('crmFollowTemplateDirectModal');}
 function extractTaskId(row){
   var b=row.querySelector('[data-crm-today-filter-action*="/api/today-dashboard/actions/follow/"]');
   if(b){var u=b.getAttribute('data-crm-today-filter-action')||'';var m=u.match(/\/follow\/([^/]+)\/complete/);if(m)return decodeURIComponent(m[1]);}
   var t=row.textContent||'';var id=t.match(/ID\s+([A-Za-z0-9_.:-]+)/);if(id)return id[1];
   return '';
 }
 function addButtons(){
   document.querySelectorAll('.crm-today-filter-row').forEach(function(row){
     if(row.getAttribute('data-follow-line-ready'))return;
     var txt=row.textContent||'';
     if(!/follow|フォロー/.test(txt))return;
     var id=extractTaskId(row); if(!id)return;
     var btn=document.createElement('button'); btn.type='button'; btn.className='crm-follow-line-btn'; btn.textContent='LINE文面作成'; btn.setAttribute('data-follow-line-task-id',id);
     row.appendChild(btn); row.setAttribute('data-follow-line-ready','1');
   });
 }
 async function openTemplates(taskId){
   var modal=ensureModal();var body=document.getElementById('crmFollowTemplateDirectBody');modal.classList.add('open');body.innerHTML='読み込み中...';
   var d=await api('/api/follow-tasks/'+encodeURIComponent(taskId)+'/template-suggestions');
   if(!d.ok){body.innerHTML='<div class="crm-follow-template-choice">取得失敗：'+esc(d.message||d.status)+'</div>';return;}
   var templates=d.templates||[];
   if(!templates.length){body.innerHTML='<div class="crm-follow-template-choice">テンプレがありません。CRM実務強化パネルの「テンプレ初期作成」を押してください。</div>';return;}
   body.innerHTML=templates.slice(0,8).map(function(t){return '<div class="crm-follow-template-choice"><b>'+esc(t.name)+'</b><div>'+esc(t.category||'')+'</div><pre>'+esc(t.body||'')+'</pre><button data-direct-template-id="'+esc(t.id)+'" data-direct-task-id="'+esc(taskId)+'">この文面で未送信LINEに保存</button></div>'}).join('');
 }
 async function saveTemplate(taskId,tplId){
   var d=await api('/api/follow-tasks/'+encodeURIComponent(taskId)+'/line-draft-from-template',{method:'POST',body:JSON.stringify({template_id:tplId})});
   if(!d.ok){toast('保存失敗：'+(d.message||d.status||'unknown'));return;}
   toast('未送信LINEに保存しました');
   var modal=document.getElementById('crmFollowTemplateDirectModal'); if(modal)modal.classList.remove('open');
   var r=document.getElementById('crmTodayFilterApply'); if(r)r.click();
   var q=document.getElementById('crmTodayActionReload'); if(q)q.click();
 }
 document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmFollowTemplateDirectClose'){var m=document.getElementById('crmFollowTemplateDirectModal');if(m)m.classList.remove('open');return;}var task=t.getAttribute&&t.getAttribute('data-follow-line-task-id');if(task){openTemplates(task);return;}var tpl=t.getAttribute&&t.getAttribute('data-direct-template-id');var tid=t.getAttribute&&t.getAttribute('data-direct-task-id');if(tpl&&tid){saveTemplate(tid,tpl);return;}});
 document.addEventListener('DOMContentLoaded',addButtons);setTimeout(addButtons,1500);setInterval(addButtons,2500);
})();
</script>`;
  return html.replace("</head>", `${style}</head>`).replace("</body>", `${script}</body>`);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if ((url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") && request.method === "GET") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, time: new Date().toISOString() });
    }
    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if (type.includes("text/html")) {
      const body = injectDirectButtons(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
