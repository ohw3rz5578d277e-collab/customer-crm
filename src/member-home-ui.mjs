import { setMemberAppShellActiveTab } from './member-app-shell-ui.mjs';

const BUILD='member-home-ui-20260925-01';
const SAFE_TAB_IDS=new Set(['home','memories','create','shop','my']);
const text=v=>v==null?'':String(v).trim();

function escapeHtml(value){
  return text(value)
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

function safeMemberAssetPath(value){
  const path=text(value);
  return /^\/member-assets\/[A-Za-z0-9._~!$&'()*+,;=:@%\/-]+$/.test(path)
    && !path.includes('..')
    && !path.includes('\\')
    ?path
    :'';
}

function safeDate(value){
  const v=text(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(v)?v:'';
}

function normalizeMemory(memory){
  if(!memory||typeof memory!=='object')return null;
  return {
    title:text(memory.title)||'MEMORY',
    genre:text(memory.genre),
    shoot_date:safeDate(memory.shoot_date),
    favorite:memory.favorite===true,
    years_ago:Number.isFinite(Number(memory?.anniversary?.years_ago))
      ?Math.max(0,Number(memory.anniversary.years_ago))
      :null
  };
}

function normalizeToday(home){
  const section=home?.today_memory?.today_memory;
  if(!section||typeof section!=='object')return null;
  const primary=normalizeMemory(section.primary);
  if(!primary)return null;
  return {
    headline:text(section.headline)||'この日の思い出',
    mode:text(section.mode),
    primary
  };
}

function normalizeFamilyPass(home){
  const p=home?.family_pass?.family_pass;
  if(!p||typeof p!=='object')return null;
  const ratio=Number(p.progress_ratio);
  return {
    current_tier:text(p.current_tier)||'FAMILY',
    memory_count:Math.max(0,Number(p.memory_count)||0),
    next_tier:text(p.next_tier)||null,
    memories_to_next:Math.max(0,Number(p.memories_to_next)||0),
    progress_ratio:Number.isFinite(ratio)?Math.max(0,Math.min(1,ratio)):0,
    effective_black:p.effective_black===true
  };
}

function normalizePassport(home){
  const p=home?.family_passport;
  if(!p||typeof p!=='object')return null;
  return {
    achieved_count:Math.max(0,Number(p.achieved_count)||0),
    total_milestones:Math.max(0,Number(p.total_milestones)||0),
    milestones:(Array.isArray(p.milestones)?p.milestones:[]).slice(0,8).map(m=>({
      label:text(m?.label),
      achieved:m?.achieved===true
    })).filter(m=>m.label)
  };
}

function normalizeNextMemory(home){
  const n=home?.next_memory?.next_memory;
  if(!n||typeof n!=='object')return null;
  return {
    label:text(n.label)||'次の記念日',
    target_date:safeDate(n.target_date)||null,
    days_until:Number.isFinite(Number(n.days_until))?Math.max(0,Number(n.days_until)):null,
    child_name:text(n?.child?.display_name)||null,
    candidate_only:n.candidate_only===true
  };
}

function normalizeCreative(home){
  const c=home?.creative;
  if(!c||typeof c!=='object')return null;
  return {
    available_count:Math.max(0,Number(c.available_count)||0),
    eligible_count:Math.max(0,Number(c.eligible_count)||0),
    generation_ready:c.generation_ready===true,
    templates:(Array.isArray(c.featured_templates)?c.featured_templates:[]).slice(0,3).map(t=>({
      title:text(t?.title)||'Creative',
      description:text(t?.description),
      eligible:t?.eligibility?.eligible===true,
      memories_needed:Math.max(0,Number(t?.eligibility?.memories_needed)||0),
      preview_path:safeMemberAssetPath(t?.asset_ref?.preview_public_asset?.public_path)
    }))
  };
}

function normalizeShop(home){
  const s=home?.shop_pickup;
  if(!s||typeof s!=='object')return null;
  return {
    available_count:Math.max(0,Number(s.available_count)||0),
    pricing_authoritative:s.pricing_authoritative===true,
    checkout_ready:s.checkout_ready===true,
    products:(Array.isArray(s.products)?s.products:[]).slice(0,3).map(p=>({
      title:text(p?.title)||'Photo Item',
      description:text(p?.description),
      hero_path:safeMemberAssetPath(p?.hero_asset_ref?.public_asset?.public_path),
      cta_label:text(p?.navigation?.cta_label)||'商品を見る'
    }))
  };
}

function normalizeNews(home){
  const n=home?.news;
  if(!n||typeof n!=='object')return null;
  return {
    items:(Array.isArray(n.items)?n.items:[]).slice(0,3).map(item=>({
      title:text(item?.title),
      summary:text(item?.summary),
      published_at:safeDate(item?.published_at),
      hero_path:safeMemberAssetPath(item?.hero_asset_ref?.public_asset?.public_path)
    })).filter(item=>item.title)
  };
}

export function buildMemberHomeViewModel(homeResult){
  if(homeResult?.status!=='ok'||!homeResult?.home){
    return {
      status:text(homeResult?.status)||'home_unavailable',
      source_only:true,
      identity_exposed:false
    };
  }

  const home=homeResult.home;
  return {
    status:'ok',
    hero:{
      eyebrow:'MIZUNO PHOTO MEMBER',
      title:'家族の時間を、\nひとつの物語に。',
      subtitle:'撮った日も、その先の日々も。写真と記憶がつながっていく場所。'
    },
    today_memory:normalizeToday(home),
    recent_memories:(Array.isArray(home.recent_memories)?home.recent_memories:[]).slice(0,3).map(normalizeMemory).filter(Boolean),
    visible_memory_count:Math.max(0,Number(home.visible_memory_count)||0),
    family_pass:normalizeFamilyPass(home),
    family_passport:normalizePassport(home),
    next_memory:normalizeNextMemory(home),
    creative:normalizeCreative(home),
    shop:normalizeShop(home),
    news:normalizeNews(home),
    partial:home.partial===true,
    unavailable_sections:(Array.isArray(home.unavailable_sections)?home.unavailable_sections:[])
      .slice(0,8)
      .map(x=>text(x?.section))
      .filter(Boolean),
    source_only:true,
    auto_fetch:false,
    auto_write:false,
    automatic_contact:false,
    production_route_wired:false
  };
}

function asset(path,alt=''){
  return path
    ?'<img src="'+escapeHtml(path)+'" alt="'+escapeHtml(alt)+'" loading="lazy" decoding="async">'
    :'<div class="mp-home__photo-placeholder" aria-hidden="true"><span>PHOTO</span></div>';
}

function renderToday(vm){
  if(!vm.today_memory)return '';
  const m=vm.today_memory.primary;
  return '<section class="mp-home__today">'
    +'<div class="mp-home__section-kicker">TODAY\'S MEMORY</div>'
    +'<div class="mp-home__today-card">'
    +'<div class="mp-home__today-photo"><div class="mp-home__photo-placeholder mp-home__photo-placeholder--hero"><span>MEMORY</span></div></div>'
    +'<div class="mp-home__today-copy">'
    +(m.years_ago!=null?'<p class="mp-home__today-years">'+escapeHtml(m.years_ago)+' years ago</p>':'')
    +'<h2>'+escapeHtml(m.title)+'</h2>'
    +'<p>'+escapeHtml([m.shoot_date,m.genre].filter(Boolean).join(' · '))+'</p>'
    +'<button type="button" data-home-go-tab="memories">MEMORIESを見る</button>'
    +'</div></div></section>';
}

function renderRecent(vm){
  if(!vm.recent_memories.length)return '';
  const cards=vm.recent_memories.map(m=>
    '<article class="mp-home__memory-card"><div class="mp-home__photo-placeholder"><span>PHOTO</span></div>'
    +'<div><small>'+escapeHtml(m.shoot_date||'MEMORY')+'</small><strong>'+escapeHtml(m.title)+'</strong>'
    +'<span>'+escapeHtml(m.genre)+'</span></div></article>'
  ).join('');
  return '<section class="mp-home__section"><div class="mp-home__section-head"><div><p>MEMORIES</p><h2>最近の記憶</h2></div>'
    +'<button type="button" data-home-go-tab="memories">すべて見る</button></div>'
    +'<div class="mp-home__memory-grid">'+cards+'</div></section>';
}

function renderPass(vm){
  const pass=vm.family_pass;
  if(!pass)return '';
  const pct=Math.round(pass.progress_ratio*100);
  const next=pass.effective_black
    ?'BLACK MEMBER'
    :(pass.next_tier?'あと '+escapeHtml(pass.memories_to_next)+' MEMORYで '+escapeHtml(pass.next_tier):'Family Pass');
  return '<section class="mp-home__section"><div class="mp-home__pass'+(pass.effective_black?' is-black':'')+'">'
    +'<div><p>FAMILY PASS</p><h2>'+escapeHtml(pass.current_tier)+'</h2><span>'+next+'</span></div>'
    +'<div class="mp-home__pass-count"><strong>'+escapeHtml(pass.memory_count)+'</strong><small>MEMORIES</small></div>'
    +'<div class="mp-home__progress"><span style="width:'+pct+'%"></span></div>'
    +'<button type="button" data-home-go-tab="my">Family Passを見る</button>'
    +'</div></section>';
}

function renderNext(vm){
  const n=vm.next_memory;
  if(!n)return '';
  const date=n.target_date?escapeHtml(n.target_date):'時期を確認中';
  const days=n.days_until!=null?'あと '+escapeHtml(n.days_until)+'日':'';
  return '<section class="mp-home__section"><div class="mp-home__next">'
    +'<p>NEXT MEMORY</p><h2>'+escapeHtml(n.label)+'</h2>'
    +(n.child_name?'<strong>'+escapeHtml(n.child_name)+'</strong>':'')
    +'<span>'+[date,days].filter(Boolean).join(' · ')+'</span>'
    +'<small>次の撮影機会の候補です。予約や連絡は自動では行いません。</small>'
    +'</div></section>';
}

function renderCreative(vm){
  const c=vm.creative;
  if(!c||!c.templates.length)return '';
  const cards=c.templates.map(t=>
    '<article class="mp-home__creative-card">'+asset(t.preview_path,t.title)
    +'<div><strong>'+escapeHtml(t.title)+'</strong><p>'+escapeHtml(t.description)+'</p>'
    +'<small>'+(t.eligible?'つくれます':'あと '+escapeHtml(t.memories_needed)+' MEMORY')+'</small></div></article>'
  ).join('');
  return '<section class="mp-home__section"><div class="mp-home__section-head"><div><p>CREATE</p><h2>写真を、暮らしの中へ。</h2></div>'
    +'<button type="button" data-home-go-tab="create">CREATEへ</button></div>'
    +'<div class="mp-home__creative-grid">'+cards+'</div></section>';
}

function renderPassport(vm){
  const p=vm.family_passport;
  if(!p||!p.milestones.length)return '';
  const items=p.milestones.map(m=>
    '<li class="'+(m.achieved?'is-achieved':'')+'"><span></span><strong>'+escapeHtml(m.label)+'</strong></li>'
  ).join('');
  return '<section class="mp-home__section"><div class="mp-home__passport"><div><p>FAMILY PASSPORT</p><h2>'
    +escapeHtml(p.achieved_count)+' / '+escapeHtml(p.total_milestones)+'</h2><span>家族の節目</span></div>'
    +'<ol>'+items+'</ol></div></section>';
}

function renderShop(vm){
  const s=vm.shop;
  if(!s||!s.products.length)return '';
  const cards=s.products.map(p=>
    '<article class="mp-home__shop-card">'+asset(p.hero_path,p.title)
    +'<div><strong>'+escapeHtml(p.title)+'</strong><p>'+escapeHtml(p.description)+'</p><span>'+escapeHtml(p.cta_label)+'</span></div></article>'
  ).join('');
  return '<section class="mp-home__section"><div class="mp-home__section-head"><div><p>SHOP</p><h2>写真を、かたちに残す。</h2></div>'
    +'<button type="button" data-home-go-tab="shop">SHOPへ</button></div>'
    +'<div class="mp-home__shop-grid">'+cards+'</div>'
    +(!s.pricing_authoritative||!s.checkout_ready?'<small class="mp-home__quiet">商品情報は準備中です。購入処理はまだ有効ではありません。</small>':'')
    +'</section>';
}

function renderNews(vm){
  const n=vm.news;
  if(!n||!n.items.length)return '';
  return '<section class="mp-home__section"><div class="mp-home__section-head"><div><p>NEWS</p><h2>お知らせ</h2></div></div>'
    +'<div class="mp-home__news">'+n.items.map(item=>
      '<article>'+asset(item.hero_path,item.title)+'<div><small>'+escapeHtml(item.published_at)+'</small><strong>'+escapeHtml(item.title)+'</strong><p>'+escapeHtml(item.summary)+'</p></div></article>'
    ).join('')+'</div></section>';
}

export function renderMemberHomeMarkup(vm){
  if(vm?.status!=='ok'){
    return '<section class="mp-home mp-home--unavailable"><p>HOMEを表示できません。</p></section>';
  }

  return '<section class="mp-home" data-member-home-ui>'
    +'<header class="mp-home__hero"><p>'+escapeHtml(vm.hero.eyebrow)+'</p><h1>'+escapeHtml(vm.hero.title).replaceAll('\n','<br>')+'</h1><span>'+escapeHtml(vm.hero.subtitle)+'</span>'
    +'<button type="button" data-home-go-tab="memories">'+escapeHtml(vm.visible_memory_count)+' MEMORIES</button></header>'
    +renderToday(vm)
    +renderRecent(vm)
    +renderPass(vm)
    +renderNext(vm)
    +renderCreative(vm)
    +renderPassport(vm)
    +renderShop(vm)
    +renderNews(vm)
    +(vm.partial?'<p class="mp-home__partial">一部の情報は現在準備中です。</p>':'')
    +'</section>';
}

export function memberHomeCss(){
  return [
    '.mp-home{--home-paper:#f7f4ec;--home-card:#fffdf8;--home-ink:#242620;--home-muted:#777a72;--home-green:#315d4f;max-width:1180px;margin:0 auto;padding:34px 18px 120px;color:var(--home-ink);font-family:ui-serif,"Hiragino Mincho ProN","Yu Mincho",serif}',
    '.mp-home button{font:700 11px/1.2 ui-sans-serif,-apple-system,BlinkMacSystemFont,"Noto Sans JP",sans-serif;letter-spacing:.05em;cursor:pointer}.mp-home__hero{padding:28px 4px 46px;max-width:780px}.mp-home__hero>p,.mp-home__section-kicker,.mp-home__section-head p,.mp-home__pass p,.mp-home__next>p,.mp-home__passport p{margin:0 0 10px;font:700 10px/1.3 ui-sans-serif,sans-serif;letter-spacing:.2em;color:var(--home-green)}.mp-home__hero h1{margin:0 0 18px;font-size:clamp(38px,8vw,74px);font-weight:500;line-height:1.06;letter-spacing:-.045em}.mp-home__hero>span{display:block;max-width:560px;color:var(--home-muted);line-height:1.9}.mp-home__hero>button,.mp-home__section-head button,.mp-home__today-copy button,.mp-home__pass button{margin-top:24px;border:0;border-bottom:1px solid currentColor;background:transparent;padding:6px 0;color:var(--home-green)}',
    '.mp-home__section,.mp-home__today{margin:0 0 48px}.mp-home__section-head{display:flex;align-items:end;justify-content:space-between;gap:18px;margin:0 2px 15px}.mp-home__section-head h2{margin:0;font-size:clamp(24px,4vw,36px);font-weight:500;letter-spacing:-.025em}.mp-home__section-head button{margin:0;white-space:nowrap}',
    '.mp-home__today-card{overflow:hidden;border-radius:30px;background:var(--home-card);box-shadow:0 18px 50px rgba(36,38,32,.08)}.mp-home__today-photo{min-height:260px}.mp-home__photo-placeholder{display:grid;place-items:center;width:100%;aspect-ratio:4/3;background:linear-gradient(145deg,#e8e4d9,#d8d5cc);color:#8d8d85;font:800 10px ui-sans-serif,sans-serif;letter-spacing:.18em}.mp-home__photo-placeholder--hero{min-height:260px;aspect-ratio:auto}.mp-home__today-copy{padding:26px}.mp-home__today-years{margin:0 0 6px;color:var(--home-green);font-size:13px}.mp-home__today-copy h2{margin:0 0 8px;font-size:30px;font-weight:500}.mp-home__today-copy>p:last-of-type{color:var(--home-muted)}',
    '.mp-home__memory-grid,.mp-home__creative-grid,.mp-home__shop-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.mp-home__memory-card,.mp-home__creative-card,.mp-home__shop-card{overflow:hidden;border-radius:22px;background:var(--home-card);box-shadow:0 10px 28px rgba(36,38,32,.06)}.mp-home__memory-card>div:last-child,.mp-home__creative-card>div,.mp-home__shop-card>div{display:grid;gap:5px;padding:14px 15px 16px}.mp-home__memory-card small,.mp-home__memory-card span,.mp-home__creative-card p,.mp-home__creative-card small,.mp-home__shop-card p,.mp-home__quiet,.mp-home__news small,.mp-home__news p{color:var(--home-muted);font-size:12px;line-height:1.6}.mp-home__creative-card small{color:var(--home-green)}',
    '.mp-home__pass{display:grid;grid-template-columns:1fr auto;gap:20px;padding:28px;border-radius:28px;background:#e7efe9}.mp-home__pass h2{margin:0;font-size:40px;font-weight:500}.mp-home__pass>div>span{color:var(--home-muted)}.mp-home__pass-count{text-align:right}.mp-home__pass-count strong{display:block;font-size:38px;font-weight:500}.mp-home__pass-count small{color:var(--home-muted)}.mp-home__progress{grid-column:1/-1;height:3px;border-radius:999px;background:rgba(49,93,79,.12);overflow:hidden}.mp-home__progress span{display:block;height:100%;background:var(--home-green)}.mp-home__pass button{grid-column:1/-1;justify-self:start;margin:2px 0 0}.mp-home__pass.is-black{background:#171a16;color:#f1ecdf}.mp-home__pass.is-black p,.mp-home__pass.is-black button{color:#d5c5a2}.mp-home__pass.is-black .mp-home__progress{background:rgba(255,255,255,.12)}.mp-home__pass.is-black .mp-home__progress span{background:#d5c5a2}',
    '.mp-home__next{padding:28px;border:1px solid rgba(49,93,79,.14);border-radius:28px;background:transparent}.mp-home__next h2{margin:0 0 10px;font-size:34px;font-weight:500}.mp-home__next strong,.mp-home__next span,.mp-home__next small{display:block;margin-top:7px}.mp-home__next small{color:var(--home-muted);line-height:1.7}',
    '.mp-home__passport{display:grid;gap:24px;padding:28px;border-radius:28px;background:var(--home-card)}.mp-home__passport h2{margin:0;font-size:42px;font-weight:500}.mp-home__passport>div>span{color:var(--home-muted)}.mp-home__passport ol{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:0;padding:0;list-style:none}.mp-home__passport li{display:flex;gap:9px;align-items:center;color:var(--home-muted)}.mp-home__passport li>span{width:10px;height:10px;border-radius:999px;border:1px solid var(--home-muted)}.mp-home__passport li.is-achieved{color:var(--home-ink)}.mp-home__passport li.is-achieved>span{border-color:var(--home-green);background:var(--home-green)}',
    '.mp-home__news{display:grid;gap:10px}.mp-home__news article{display:grid;grid-template-columns:100px 1fr;gap:14px;align-items:center;padding:10px;border-radius:18px;background:var(--home-card)}.mp-home__news article>img,.mp-home__news article>.mp-home__photo-placeholder{width:100px;height:78px;aspect-ratio:auto;border-radius:12px;object-fit:cover}.mp-home__news article>div:last-child{display:grid;gap:3px}.mp-home__partial{padding:16px;border-radius:16px;background:rgba(49,93,79,.06);color:var(--home-muted);font-size:12px}.mp-home--unavailable{padding:30px}',
    '@media(min-width:760px){.mp-home{padding:48px 34px 110px}.mp-home__today-card{display:grid;grid-template-columns:minmax(0,1.4fr) minmax(280px,.8fr)}.mp-home__today-photo,.mp-home__photo-placeholder--hero{min-height:420px}.mp-home__today-copy{display:flex;flex-direction:column;justify-content:end;padding:38px}.mp-home__memory-grid{grid-template-columns:repeat(3,minmax(0,1fr))}.mp-home__creative-grid,.mp-home__shop-grid{grid-template-columns:repeat(3,minmax(0,1fr))}.mp-home__passport{grid-template-columns:220px 1fr;align-items:center}.mp-home__passport ol{grid-template-columns:repeat(4,minmax(0,1fr))}}',
    '.mp-shell--black .mp-home{--home-paper:#111310;--home-card:#1a1d19;--home-ink:#f3efe6;--home-muted:#aaa99f;--home-green:#d6c6a3}.mp-shell--black .mp-home__pass{background:#20251f}.mp-shell--black .mp-home__photo-placeholder{background:linear-gradient(145deg,#282b26,#1c1f1b);color:#8e9189}.mp-shell--black .mp-home__next{border-color:rgba(214,198,163,.18)}'
  ].join('');
}

export function resolveMemberHomeMount(shellRoot){
  if(!shellRoot||typeof shellRoot.querySelector!=='function')return null;
  return shellRoot.querySelector('[data-member-mount="home"]')||null;
}

export function mountMemberHomeUi(shellRoot,homeResult,{
  on_tab_change=null
}={}){
  const mount=resolveMemberHomeMount(shellRoot);
  if(!mount||typeof mount.addEventListener!=='function'){
    return {ok:false,error:'member_home_mount_required'};
  }

  const vm=buildMemberHomeViewModel(homeResult);
  mount.innerHTML='<style data-member-home-style>'+memberHomeCss()+'</style>'+renderMemberHomeMarkup(vm);

  const onClick=event=>{
    const target=event?.target?.closest?.('[data-home-go-tab]');
    if(!target||!mount.contains?.(target))return;
    const tab=text(target.getAttribute('data-home-go-tab')).toLowerCase();
    if(!SAFE_TAB_IDS.has(tab))return;
    const result=setMemberAppShellActiveTab(shellRoot,tab);
    if(result.ok&&typeof on_tab_change==='function')on_tab_change(tab);
  };

  mount.addEventListener('click',onClick);

  return {
    ok:true,
    source_only:true,
    view_model:vm,
    destroy(){
      if(typeof mount.removeEventListener==='function')mount.removeEventListener('click',onClick);
      return {ok:true};
    }
  };
}

export function memberHomeUiHealth(){
  return {
    member_home_ui:true,
    build:BUILD,
    source_only:true,
    design_direction:'luxury_minimal_family_story',
    canonical_mount:'home',
    home_read_model_only:true,
    private_memory_binary_rendering:false,
    public_asset_local_path_only:true,
    external_navigation:false,
    shell_tab_navigation_only:true,
    client_identity_input:false,
    customer_id_exposed:false,
    family_id_exposed:false,
    auto_fetch:false,
    auto_write:false,
    automatic_contact:false,
    reservation_creation:false,
    line_send:false,
    production_route_wired:false,
    production_write:false
  };
}

export const __test={
  escapeHtml,
  safeMemberAssetPath,
  safeDate,
  normalizeMemory,
  normalizeToday,
  normalizeFamilyPass,
  normalizePassport,
  normalizeNextMemory,
  normalizeCreative,
  normalizeShop,
  normalizeNews
};
