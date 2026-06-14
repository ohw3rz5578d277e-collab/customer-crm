// ======================================================
// CUSTOMER CRM / CLEAN MOBILE UX V3 WRAPPER
// build: customer-crm-clean-mobile-ux-v3-20260614-01
// - Hides overlapping floating buttons on mobile
// - Consolidates Settings/User management into one panel
// - Makes customer detail panel reliable from stable customer list
// - Keeps existing APIs and data unchanged
// ======================================================

import app from "./production-index-crm-customer-list-detail-v2.js";

const BUILD = "customer-crm-clean-mobile-ux-v3-20260614-01";

function injectCleanUx(html){
  if(!html || html.includes("crm-clean-mobile-ux-v3-script")) return html;

  const style = `<style id="crm-clean-mobile-ux-v3-style">
/* === Global cleanup === */
.crm-stable-audit-btn,
.crm-stable-customer-btn{
  display:none!important;
}
#crmSettingsMenuBtn,
#crmLogoutMenuBtn{
  position:static!important;
  box-shadow:none!important;
}
.crm-settings-panel{
  z-index:2147483600!important;
}
.crm-settings-panel .crm-user-tools{
  margin-top:16px!important;
  padding-top:16px!important;
  border-top:1px solid #e2e8f0!important;
}
.crm-user-tools h3{
  font-size:18px!important;
  font-weight:950!important;
  margin:0 0 8px!important;
  color:#07111f!important;
}
.crm-user-add-form{
  display:grid!important;
  grid-template-columns:1fr 110px 90px!important;
  gap:8px!important;
  margin:10px 0 12px!important;
}
.crm-user-add-form input,
.crm-user-add-form select{
  min-height:44px!important;
  border:1px solid #cbd5e1!important;
  border-radius:12px!important;
  padding:0 12px!important;
  font-size:16px!important;
  background:#fff!important;
}
.crm-user-add-form button,
.crm-user-refresh-btn{
  min-height:44px!important;
  border:0!important;
  border-radius:12px!important;
  background:#028760!important;
  color:#fff!important;
  font-weight:950!important;
  cursor:pointer!important;
}
.crm-user-list{
  display:grid!important;
  gap:8px!important;
}
.crm-user-row{
  border:1px solid #e2e8f0!important;
  border-radius:14px!important;
  padding:10px!important;
  background:#f8fafc!important;
  display:flex!important;
  justify-content:space-between!important;
  gap:8px!important;
  align-items:center!important;
}
.crm-user-row b{
  display:block!important;
  color:#07111f!important;
  font-size:14px!important;
}
.crm-user-row span{
  color:#64748b!important;
  font-size:12px!important;
}
.crm-user-row button{
  min-height:36px!important;
  border:0!important;
  border-radius:999px!important;
  background:#fee2e2!important;
  color:#991b1b!important;
  font-weight:900!important;
  padding:0 12px!important;
}
.crm-user-msg{
  margin:8px 0!important;
  color:#64748b!important;
  font-weight:800!important;
  font-size:13px!important;
}
.crm-user-msg.err{
  color:#b91c1c!important;
}
/* Make customer list detail buttons stable */
.crm-stable-customer-card{
  position:relative!important;
}
.crm-v2-card-detail-btn{
  pointer-events:auto!important;
}
/* Hide old fixed duplicate buttons by text with JS; fallback by classes */
.crm-legacy-hidden-by-v3{
  display:none!important;
}
@media(max-width:767px){
  html,body{
    max-width:100%!important;
    overflow-x:hidden!important;
  }
  body{
    padding-bottom:calc(118px + env(safe-area-inset-bottom))!important;
  }
  .crm-settings-panel,
  .crm-stable-customer-panel,
  .crm-v2-detail-panel{
    inset:8px!important;
    width:auto!important;
    max-width:none!important;
    border-radius:20px!important;
  }
  .crm-user-add-form{
    grid-template-columns:1fr!important;
  }
  .crm-user-row{
    align-items:flex-start!important;
    flex-direction:column!important;
  }
  .crm-user-row button{
    width:100%!important;
  }
  .crm-login-card-moved,
  div:has(> button):has(button){
    max-width:100%!important;
  }
  .crm-stable-customer-panel .crm-stable-customer-search{
    grid-template-columns:1fr!important;
  }
  .crm-stable-customer-panel .crm-stable-customer-search button{
    width:100%!important;
  }
  .crm-v2-detail-grid,
  .crm-stable-customer-meta{
    grid-template-columns:1fr 1fr!important;
  }
}
</style>`;

  const script = `<script id="crm-clean-mobile-ux-v3-script">
(()=>{
  if(window.__crmCleanMobileUxV3) return;
  window.__crmCleanMobileUxV3 = 1;

  function qs(s,r=document){return r.querySelector(s)}
  function qsa(s,r=document){return Array.from(r.querySelectorAll(s))}
  function txt(v){return String(v==null?'':v)}
  function esc(v){return txt(v).replace(/[&<>\"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[c]||c))}

  async function api(path,opt){
    opt=opt||{}; opt.headers=opt.headers||{};
    if(opt.body && !opt.headers['content-type']) opt.headers['content-type']='application/json';
    const r=await fetch(path,opt);
    const j=await r.json().catch(()=>({ok:false,message:'JSON parse error'}));
    if(!r.ok || j.ok===false) throw new Error(j.message||j.error||('HTTP '+r.status));
    return j;
  }

  function hideDuplicateFloatingButtons(){
    const hideTexts = ['状態確認','顧客リストへ','顧客リスト','ユーザー管理','ユーザー追加','一括確認','LINE運用'];
    qsa('button,a').forEach(el=>{
      if(el.closest('#crmSettingsPanel') || el.closest('#crmStableCustomerPanel') || el.closest('#crmV2DetailPanel')) return;
      const t=(el.textContent||'').trim();
      const st=getComputedStyle(el);
      const fixed = st.position === 'fixed' || el.className.toString().includes('stable-audit') || el.className.toString().includes('stable-customer');
      if(fixed && hideTexts.includes(t)) el.classList.add('crm-legacy-hidden-by-v3');
      if(t === 'ユーザー管理' || t === 'ユーザー追加'){
        const p = el.closest('div');
        if(p && /Googleログイン|role: admin|@/.test(p.textContent||'')) p.classList.add('crm-legacy-hidden-by-v3');
      }
    });
  }

  function openSettings(){
    const panel = qs('#crmSettingsPanel');
    if(panel) panel.classList.add('open');
  }

  function ensureSettingsAccess(){
    let btn = qs('#crmSettingsUnifiedBtn');
    if(btn) return;
    const host = qs('#crmStableCustomerMenuBtn')?.parentElement || qsa('button,a').find(el=>/CRMメニュー|メニュー/.test((el.textContent||'').trim()))?.parentElement || document.body;
    btn = document.createElement('button');
    btn.id = 'crmSettingsUnifiedBtn';
    btn.type = 'button';
    btn.textContent = '設定';
    btn.className = 'crm-settings-menu-btn';
    btn.onclick = openSettings;
    host.appendChild(btn);
  }

  async function loadUsers(){
    const list = qs('#crmUnifiedUserList');
    const msg = qs('#crmUnifiedUserMsg');
    if(!list) return;
    msg.textContent = '読み込み中...';
    msg.className = 'crm-user-msg';
    try{
      const j = await api('/api/admin-users');
      const items = j.items || [];
      if(!items.length){
        list.innerHTML = '<div class="crm-v2-empty">管理ユーザーはまだありません。</div>';
      }else{
        list.innerHTML = items.map(u=>{
          const email=esc(u.email||'');
          const role=esc(u.role||'viewer');
          const status=esc(u.status||'');
          const removable = status === 'active' && email && !email.includes('ohw3rz5578d277e@gmail.com');
          return '<div class="crm-user-row"><div><b>'+email+'</b><span>'+role+' / '+status+'</span></div>'+(removable?'<button type="button" data-remove-user="'+email+'">無効化</button>':'')+'</div>';
        }).join('');
        qsa('[data-remove-user]', list).forEach(b=>b.onclick=()=>removeUser(b.getAttribute('data-remove-user')));
      }
      msg.textContent = '';
    }catch(e){
      list.innerHTML = '<div class="crm-v2-empty">ユーザー一覧を読み込めませんでした。</div>';
      msg.textContent = e.message || '読み込みに失敗しました';
      msg.className = 'crm-user-msg err';
    }
  }

  async function addUser(){
    const email = qs('#crmUnifiedUserEmail')?.value || '';
    const role = qs('#crmUnifiedUserRole')?.value || 'viewer';
    const msg = qs('#crmUnifiedUserMsg');
    if(!email.trim()){ msg.textContent='メールアドレスを入力してください'; msg.className='crm-user-msg err'; return; }
    msg.textContent = '追加中...'; msg.className = 'crm-user-msg';
    try{
      await api('/api/admin-users/add',{method:'POST',body:JSON.stringify({email,role})});
      qs('#crmUnifiedUserEmail').value='';
      await loadUsers();
      msg.textContent = '追加しました: '+email;
    }catch(e){ msg.textContent=e.message||'追加に失敗しました'; msg.className='crm-user-msg err'; }
  }

  async function removeUser(email){
    if(!confirm(email+' を無効化しますか？')) return;
    const msg = qs('#crmUnifiedUserMsg');
    msg.textContent = '無効化中...'; msg.className = 'crm-user-msg';
    try{
      await api('/api/admin-users/remove',{method:'POST',body:JSON.stringify({email})});
      await loadUsers();
      msg.textContent = '無効化しました: '+email;
    }catch(e){ msg.textContent=e.message||'無効化に失敗しました'; msg.className='crm-user-msg err'; }
  }

  function patchSettingsUserTools(){
    const panel = qs('#crmSettingsPanel');
    if(!panel || qs('#crmUnifiedUserTools', panel)) return;
    const old = qsa('.crm-user-tools', panel);
    old.forEach(x=>x.remove());
    const box = document.createElement('div');
    box.id = 'crmUnifiedUserTools';
    box.className = 'crm-user-tools';
    box.innerHTML = '<h3>ユーザー設定</h3><p style="margin:0 0 10px;color:#64748b;line-height:1.7;">ユーザー管理・ユーザー追加をここにまとめました。</p><div class="crm-user-add-form"><input id="crmUnifiedUserEmail" type="email" placeholder="追加するGoogleメール"><select id="crmUnifiedUserRole"><option value="viewer">viewer</option><option value="admin">admin</option></select><button id="crmUnifiedUserAdd" type="button">追加</button></div><div><button class="crm-user-refresh-btn" id="crmUnifiedUserRefresh" type="button">ユーザー一覧を更新</button></div><div id="crmUnifiedUserMsg" class="crm-user-msg"></div><div id="crmUnifiedUserList" class="crm-user-list"></div>';
    panel.appendChild(box);
    qs('#crmUnifiedUserAdd', box).onclick = addUser;
    qs('#crmUnifiedUserRefresh', box).onclick = loadUsers;
    loadUsers();
  }

  function patchCustomerCards(){
    qsa('.crm-stable-customer-card').forEach(card=>{
      const id = card.getAttribute('data-customer-id') || '';
      if(!id) return;
      const btn = card.querySelector('.crm-v2-card-detail-btn');
      if(btn && btn.dataset.boundV3 !== '1'){
        btn.dataset.boundV3 = '1';
        btn.onclick = e => {
          e.preventDefault(); e.stopPropagation();
          window.dispatchEvent(new CustomEvent('crm-open-customer-detail-v2',{detail:{id}}));
          if(typeof window.__crmOpenCustomerDetailV2 === 'function') window.__crmOpenCustomerDetailV2(id);
          else btn.closest('.crm-stable-customer-card')?.click();
        };
      }
    });
  }

  function exposeDetailOpen(){
    if(window.__crmOpenCustomerDetailV2) return;
    const oldFetch = window.fetch;
    // no-op placeholder, real detail opener is in previous wrapper; cards still have bound click.
  }

  function boot(){
    hideDuplicateFloatingButtons();
    ensureSettingsAccess();
    patchSettingsUserTools();
    patchCustomerCards();
    exposeDetailOpen();
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',boot); else boot();
  new MutationObserver(()=>setTimeout(boot,150)).observe(document.documentElement,{childList:true,subtree:true});
})();
</script>`;

  return html.includes('</head>') ? html.replace('</head>', style + '</head>').replace('</body>', script + '</body>') : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const res = await app.fetch(request, env, ctx);
    const ct = res.headers.get('content-type') || '';
    if(request.method === 'GET' && ct.includes('text/html')){
      const html = await res.text();
      return new Response(injectCleanUx(html), {status:res.status, headers:res.headers});
    }
    return res;
  }
};
