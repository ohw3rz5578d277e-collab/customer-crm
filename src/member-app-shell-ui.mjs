const BUILD='member-app-shell-ui-20260925-01';

export const MEMBER_APP_TABS=Object.freeze([
  {id:'home',label:'HOME',caption:'家族の今'},
  {id:'memories',label:'MEMORIES',caption:'写真と記憶'},
  {id:'create',label:'CREATE',caption:'つくる'},
  {id:'shop',label:'SHOP',caption:'写真を残す'},
  {id:'my',label:'MY',caption:'Family Pass'}
]);

const TAB_IDS=new Set(MEMBER_APP_TABS.map(tab=>tab.id));
const text=v=>v==null?'':String(v).trim();

function escapeHtml(value){
  return text(value)
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

export function normalizeMemberAppTab(value){
  const tab=text(value).toLowerCase();
  return TAB_IDS.has(tab)?tab:'home';
}

function trustedBlackState(familyPass){
  return familyPass?.status==='ok'
    && familyPass?.family_pass?.effective_black===true;
}

export function buildMemberAppShellViewModel({
  active_tab='home',
  family_pass=null
}={}){
  const activeTab=normalizeMemberAppTab(active_tab);
  const black=trustedBlackState(family_pass);

  return {
    status:'ok',
    active_tab:activeTab,
    theme:black?'black':'standard',
    black_member:black,
    brand:{
      eyebrow:'MIZUNO PHOTO',
      title:black?'BLACK FAMILY':'MEMBER',
      subtitle:black
        ?'家族の記憶を、これからも。'
        :'家族の時間を、ひとつの場所に。'
    },
    tabs:MEMBER_APP_TABS.map(tab=>({
      ...tab,
      active:tab.id===activeTab
    })),
    shell_only:true,
    auto_fetch:false,
    auto_write:false,
    production_route_wired:false
  };
}

function navButton(tab,{compact=false}={}){
  return '<button type="button" class="mp-shell__nav-item'+(tab.active?' is-active':'')+'" data-member-tab="'+escapeHtml(tab.id)+'" aria-selected="'+(tab.active?'true':'false')+'" role="tab">'
    +'<span class="mp-shell__nav-label">'+escapeHtml(tab.label)+'</span>'
    +(compact?'':'<small>'+escapeHtml(tab.caption)+'</small>')
    +'</button>';
}

function panel(tab){
  return '<section class="mp-shell__panel" data-member-panel="'+escapeHtml(tab.id)+'" role="tabpanel" aria-label="'+escapeHtml(tab.label)+'"'+(tab.active?'':' hidden')+'>'
    +'<div class="mp-shell__mount" data-member-mount="'+escapeHtml(tab.id)+'"></div>'
    +'</section>';
}

export function renderMemberAppShellMarkup(viewModel){
  if(viewModel?.status!=='ok'){
    return '<main class="mp-shell mp-shell--unavailable"><p>Member appを表示できません。</p></main>';
  }

  const desktopNav=(viewModel.tabs||[]).map(tab=>navButton(tab)).join('');
  const mobileNav=(viewModel.tabs||[]).map(tab=>navButton(tab,{compact:true})).join('');
  const panels=(viewModel.tabs||[]).map(panel).join('');

  return '<main class="mp-shell mp-shell--'+escapeHtml(viewModel.theme)+'" data-member-app-shell data-active-tab="'+escapeHtml(viewModel.active_tab)+'">'
    +'<aside class="mp-shell__side">'
    +'<div class="mp-shell__brand"><span>'+escapeHtml(viewModel.brand.eyebrow)+'</span><strong>'+escapeHtml(viewModel.brand.title)+'</strong><small>'+escapeHtml(viewModel.brand.subtitle)+'</small></div>'
    +'<nav class="mp-shell__desktop-nav" role="tablist" aria-label="Member navigation">'+desktopNav+'</nav>'
    +(viewModel.black_member?'<div class="mp-shell__black-card"><span>BLACK</span><strong>Family Pass</strong><small>Lifetime member</small></div>':'')
    +'</aside>'
    +'<div class="mp-shell__workspace">'+panels+'</div>'
    +'<nav class="mp-shell__mobile-nav" role="tablist" aria-label="Member navigation">'+mobileNav+'</nav>'
    +'</main>';
}

export function setMemberAppShellActiveTab(root,nextTab){
  const tab=normalizeMemberAppTab(nextTab);
  if(!root||typeof root.querySelectorAll!=='function')return {ok:false,error:'shell_root_required'};

  root.dataset.activeTab=tab;

  for(const button of root.querySelectorAll('[data-member-tab]')){
    const active=button.getAttribute('data-member-tab')===tab;
    button.classList?.toggle('is-active',active);
    button.setAttribute?.('aria-selected',active?'true':'false');
  }

  for(const section of root.querySelectorAll('[data-member-panel]')){
    const active=section.getAttribute('data-member-panel')===tab;
    if(active)section.removeAttribute?.('hidden');
    else section.setAttribute?.('hidden','');
  }

  return {ok:true,active_tab:tab};
}

export function bindMemberAppShellNavigation(root,{
  on_tab_change=null
}={}){
  if(!root||typeof root.addEventListener!=='function'){
    return {ok:false,error:'shell_root_required'};
  }
  if(root.dataset?.memberNavBound==='1'){
    return {ok:true,already_bound:true};
  }

  if(root.dataset)root.dataset.memberNavBound='1';

  root.addEventListener('click',event=>{
    const target=event?.target?.closest?.('[data-member-tab]');
    if(!target||!root.contains?.(target))return;

    const requested=target.getAttribute('data-member-tab');
    if(!TAB_IDS.has(text(requested)))return;

    const result=setMemberAppShellActiveTab(root,requested);
    if(result.ok&&typeof on_tab_change==='function'){
      on_tab_change(result.active_tab);
    }

    if(result.ok&&typeof root.dispatchEvent==='function'&&typeof CustomEvent==='function'){
      root.dispatchEvent(new CustomEvent('member:tab-change',{
        detail:{tab:result.active_tab},
        bubbles:false
      }));
    }
  });

  return {ok:true,already_bound:false};
}

export function memberAppShellCss(){
  return [
    ':root{--mp-ivory:#f7f4ec;--mp-paper:#fffdf8;--mp-ink:#222520;--mp-muted:#7b7f76;--mp-green:#315d4f;--mp-line:rgba(34,37,32,.10)}',
    '.mp-shell{min-height:100dvh;background:var(--mp-ivory);color:var(--mp-ink);font-family:ui-serif,"Hiragino Mincho ProN","Yu Mincho",serif;box-sizing:border-box}',
    '.mp-shell *{box-sizing:border-box}.mp-shell__side{display:none}.mp-shell__workspace{min-height:100dvh;padding:0 0 88px}.mp-shell__panel[hidden]{display:none!important}.mp-shell__panel{min-height:100dvh}.mp-shell__mount{min-height:100dvh}',
    '.mp-shell__mobile-nav{position:fixed;left:10px;right:10px;bottom:max(10px,env(safe-area-inset-bottom));z-index:30;display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:4px;padding:7px;border:1px solid rgba(49,93,79,.12);border-radius:22px;background:rgba(255,253,248,.94);backdrop-filter:blur(18px);box-shadow:0 16px 42px rgba(34,37,32,.14)}',
    '.mp-shell__nav-item{appearance:none;border:0;background:transparent;color:var(--mp-muted);min-width:0;border-radius:16px;padding:10px 4px;font:700 10px/1.1 ui-sans-serif,-apple-system,BlinkMacSystemFont,"Noto Sans JP",sans-serif;letter-spacing:.06em;cursor:pointer}.mp-shell__nav-item.is-active{background:#fff;color:var(--mp-green);box-shadow:0 6px 16px rgba(49,93,79,.08)}',
    '.mp-shell--black{--mp-ivory:#111310;--mp-paper:#1a1d19;--mp-ink:#f3efe6;--mp-muted:#aaa99f;--mp-green:#d6c6a3;--mp-line:rgba(255,255,255,.10);background:#111310}.mp-shell--black .mp-shell__mobile-nav{background:rgba(26,29,25,.94);border-color:rgba(255,255,255,.08)}.mp-shell--black .mp-shell__nav-item.is-active{background:#252922;color:#eadbb8}',
    '@media(min-width:820px){.mp-shell{display:grid;grid-template-columns:240px minmax(0,1fr)}.mp-shell__side{position:sticky;top:0;height:100dvh;display:flex;flex-direction:column;padding:34px 22px 24px;border-right:1px solid var(--mp-line);background:var(--mp-paper)}.mp-shell__brand{display:grid;gap:7px;padding:0 8px 32px}.mp-shell__brand>span{font:700 10px/1.2 ui-sans-serif,sans-serif;letter-spacing:.2em;color:var(--mp-green)}.mp-shell__brand>strong{font-size:28px;font-weight:500;letter-spacing:-.03em}.mp-shell__brand>small{color:var(--mp-muted);line-height:1.7}.mp-shell__desktop-nav{display:grid;gap:6px}.mp-shell__desktop-nav .mp-shell__nav-item{text-align:left;padding:13px 14px;font-size:12px}.mp-shell__desktop-nav .mp-shell__nav-item span,.mp-shell__desktop-nav .mp-shell__nav-item small{display:block}.mp-shell__desktop-nav .mp-shell__nav-item small{margin-top:4px;font-weight:500;letter-spacing:0;color:var(--mp-muted)}.mp-shell__mobile-nav{display:none}.mp-shell__workspace{min-width:0;padding:0}.mp-shell__black-card{margin-top:auto;padding:18px;border:1px solid rgba(214,198,163,.28);border-radius:20px;background:#111310;color:#f1e8d6;display:grid;gap:5px}.mp-shell__black-card span{font:800 10px ui-sans-serif,sans-serif;letter-spacing:.18em;color:#d6c6a3}.mp-shell__black-card strong{font-size:18px}.mp-shell__black-card small{color:#aaa99f}.mp-shell__panel,.mp-shell__mount{min-height:100dvh}}',
    '@media(min-width:1200px){.mp-shell{grid-template-columns:270px minmax(0,1fr)}.mp-shell__side{padding-left:30px;padding-right:30px}}'
  ].join('');
}

export function memberAppShellUiHealth(){
  return {
    member_app_shell_ui:true,
    build:BUILD,
    source_only:true,
    navigation_tabs:MEMBER_APP_TABS.map(tab=>tab.id),
    mobile_bottom_navigation:true,
    desktop_side_navigation:true,
    responsive_breakpoint_px:820,
    black_theme_source:'authorized_family_pass_effective_black',
    client_tier_override:false,
    customer_id_exposed:false,
    family_id_exposed:false,
    auto_fetch:false,
    auto_write:false,
    line_send:false,
    production_route_wired:false,
    production_write:false
  };
}

export const __test={
  TAB_IDS,
  escapeHtml,
  trustedBlackState
};
