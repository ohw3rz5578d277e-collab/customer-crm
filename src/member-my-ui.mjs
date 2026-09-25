import { setMemberAppShellActiveTab } from './member-app-shell-ui.mjs';

const BUILD='member-my-ui-20260925-01';
const text=v=>v==null?'':String(v).trim();

function escapeHtml(value){
  return text(value)
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

function safeDate(value){
  const v=text(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(v)?v:'';
}

function normalizeFamily(my){
  const family=my?.family;
  if(!family||typeof family!=='object')return null;
  return {
    display_name:text(family.display_name)||'FAMILY',
    relation:text(family.relation)||null,
    access_role:text(family.access_role)||null,
    linked_member_count:Math.max(0,Number(family.linked_member_count)||0),
    family_profile_edit_ready:family.family_profile_edit_ready===true,
    member_identity_edit_ready:family.member_identity_edit_ready===true
  };
}

function normalizeFamilyPass(my){
  const section=my?.family_pass;
  const pass=section?.family_pass;
  if(!pass||typeof pass!=='object')return null;
  const ratio=Number(pass.progress_ratio);
  const benefit=section?.benefit_contract||{};
  return {
    memory_count:Math.max(0,Number(pass.memory_count)||Number(my?.memory_count)||0),
    current_tier:text(pass.current_tier)||text(my?.current_tier)||'FAMILY',
    next_tier:text(pass.next_tier)||null,
    memories_to_next:Math.max(0,Number(pass.memories_to_next)||0),
    progress_ratio:Number.isFinite(ratio)?Math.max(0,Math.min(1,ratio)):0,
    effective_black:pass.effective_black===true||my?.effective_black===true,
    durable_black:section?.entitlement?.durable_black===true,
    black_goods_discount_percent:Number.isFinite(Number(benefit.black_photo_goods_discount_percent))
      ?Math.max(0,Number(benefit.black_photo_goods_discount_percent))
      :0,
    shooting_fee_discount:benefit.applies_to_shooting_fee===true,
    benefit_enforcement_ready:benefit.enforcement_ready===true
  };
}

function normalizePassport(my){
  const passport=my?.family_passport;
  if(!passport||typeof passport!=='object')return null;
  return {
    achieved_count:Math.max(0,Number(passport.achieved_count)||0),
    total_milestones:Math.max(0,Number(passport.total_milestones)||0),
    completion_ratio:Number.isFinite(Number(passport.completion_ratio))
      ?Math.max(0,Math.min(1,Number(passport.completion_ratio)))
      :0,
    milestones:(Array.isArray(passport.milestones)?passport.milestones:[]).slice(0,12).map(m=>({
      code:text(m?.code),
      label:text(m?.label),
      achieved:m?.achieved===true
    })).filter(m=>m.label)
  };
}

function normalizeNextMemory(my){
  const section=my?.next_memory;
  const next=section?.next_memory;
  if(!next||typeof next!=='object')return null;
  return {
    type:text(next.type),
    label:text(next.label)||'次の記念日',
    target_date:safeDate(next.target_date)||null,
    days_until:Number.isFinite(Number(next.days_until))?Math.max(0,Number(next.days_until)):null,
    child_name:text(next?.child?.display_name)||null,
    candidate_only:next.candidate_only!==false,
    consultation_channel:text(section?.consultation_cta?.channel),
    consultation_intent:text(section?.consultation_cta?.intent),
    automatic_send:section?.consultation_cta?.automatic_send===true
  };
}

function normalizeSettings(my){
  const settings=my?.settings||{};
  return {
    family_profile_edit_ready:settings.family_profile_edit_ready===true,
    member_profile_edit_ready:settings.member_profile_edit_ready===true,
    notification_settings_ready:settings.notification_settings_ready===true,
    line_link_settings_ready:settings.line_link_settings_ready===true,
    account_deletion_ready:settings.account_deletion_ready===true
  };
}

export function buildMemberMyViewModel(result){
  if(result?.status!=='ok'||!result?.my){
    return {
      status:text(result?.status)||'my_unavailable',
      source_only:true
    };
  }

  const my=result.my;
  const unavailable=(Array.isArray(my.unavailable_sections)?my.unavailable_sections:[])
    .slice(0,12)
    .map(x=>({section:text(x?.section),error:text(x?.error)}))
    .filter(x=>x.section);

  return {
    status:'ok',
    family:normalizeFamily(my),
    family_pass:normalizeFamilyPass(my),
    family_passport:normalizePassport(my),
    favorite_count:my.favorite_count==null?null:Math.max(0,Number(my.favorite_count)||0),
    next_memory:normalizeNextMemory(my),
    settings:normalizeSettings(my),
    partial:my.partial===true,
    unavailable_sections:unavailable,
    source_only:true,
    profile_edit:false,
    family_edit:false,
    favorites_mutation:false,
    notification_settings_mutation:false,
    line_link_mutation:false,
    account_deletion:false,
    reservation_creation:false,
    automatic_contact:false,
    line_send:false,
    production_route_wired:false
  };
}

function renderFamily(vm){
  const family=vm.family;
  if(!family)return '';
  const relation=[family.relation,family.access_role].filter(Boolean).join(' · ');
  return '<section class="mp-my__section mp-my__family">'
    +'<div><p>FAMILY</p><h2>'+escapeHtml(family.display_name)+'</h2>'
    +(relation?'<span>'+escapeHtml(relation)+'</span>':'')
    +'</div><div class="mp-my__family-count"><strong>'+escapeHtml(family.linked_member_count)+'</strong><small>MEMBERS</small></div>'
    +'</section>';
}

function renderPass(vm){
  const pass=vm.family_pass;
  if(!pass)return '';
  const pct=Math.round(pass.progress_ratio*100);
  const next=pass.effective_black
    ?'BLACK MEMBER'
    :(pass.next_tier?'あと '+escapeHtml(pass.memories_to_next)+' MEMORYで '+escapeHtml(pass.next_tier):'Family Pass');

  return '<section class="mp-my__section"><div class="mp-my__pass'+(pass.effective_black?' is-black':'')+'">'
    +'<div class="mp-my__pass-head"><div><p>FAMILY PASS</p><h2>'+escapeHtml(pass.current_tier)+'</h2><span>'+next+'</span></div>'
    +'<div class="mp-my__pass-count"><strong>'+escapeHtml(pass.memory_count)+'</strong><small>MEMORIES</small></div></div>'
    +'<div class="mp-my__progress"><span style="width:'+pct+'%"></span></div>'
    +'<div class="mp-my__benefit"><strong>BLACK BENEFIT</strong><span>PHOTO GOODS '+escapeHtml(pass.black_goods_discount_percent)+'% OFF</span>'
    +'<small>'+(pass.benefit_enforcement_ready?'特典連携済み':'割引適用はまだ有効ではありません。撮影料金には適用されません。')+'</small></div>'
    +'</div></section>';
}

function renderPassport(vm){
  const p=vm.family_passport;
  if(!p)return '';
  const milestones=p.milestones.map(m=>
    '<li class="'+(m.achieved?'is-achieved':'')+'"><span></span><strong>'+escapeHtml(m.label)+'</strong></li>'
  ).join('');

  return '<section class="mp-my__section"><div class="mp-my__section-head"><div><p>FAMILY PASSPORT</p><h2>家族の節目</h2></div><strong>'+escapeHtml(p.achieved_count)+' / '+escapeHtml(p.total_milestones)+'</strong></div>'
    +'<ol class="mp-my__passport">'+milestones+'</ol></section>';
}

function renderFavorites(vm){
  const available=vm.favorite_count!=null;
  return '<section class="mp-my__section"><div class="mp-my__tile">'
    +'<div><p>FAVORITES</p><h2>'+(available?escapeHtml(vm.favorite_count):'—')+'</h2>'
    +'<span>'+(available?'お気に入りのMEMORY':'お気に入り情報は準備中です')+'</span></div>'
    +(available?'<button type="button" data-my-go-tab="memories">MEMORIESを見る</button>':'')
    +'</div></section>';
}

function renderNext(vm){
  const n=vm.next_memory;
  if(!n){
    return '<section class="mp-my__section"><div class="mp-my__tile"><div><p>NEXT MEMORY</p><h2>まだ候補はありません</h2><span>家族情報が揃うと次の撮影候補を表示できます。</span></div></div></section>';
  }
  const meta=[
    n.target_date,
    n.days_until!=null?'あと '+n.days_until+'日':''
  ].filter(Boolean).join(' · ');

  return '<section class="mp-my__section"><div class="mp-my__next"><p>NEXT MEMORY</p><h2>'+escapeHtml(n.label)+'</h2>'
    +(n.child_name?'<strong>'+escapeHtml(n.child_name)+'</strong>':'')
    +(meta?'<span>'+escapeHtml(meta)+'</span>':'')
    +'<small>次の撮影機会の候補です。LINE送信・予約作成は自動では行いません。</small>'
    +'</div></section>';
}

function renderSettings(vm){
  const rows=[
    ['家族プロフィール','family_profile_edit_ready'],
    ['会員プロフィール','member_profile_edit_ready'],
    ['通知設定','notification_settings_ready'],
    ['LINE連携','line_link_settings_ready'],
    ['アカウント削除','account_deletion_ready']
  ];
  return '<section class="mp-my__section"><div class="mp-my__section-head"><div><p>SETTINGS</p><h2>設定</h2></div></div>'
    +'<div class="mp-my__settings">'+rows.map(([label,key])=>
      '<div><span>'+escapeHtml(label)+'</span><small>'+(vm.settings[key]?'利用可能':'準備中')+'</small></div>'
    ).join('')+'</div></section>';
}

export function renderMemberMyMarkup(vm){
  if(vm?.status!=='ok'){
    return '<section class="mp-my mp-my--unavailable"><p>MYを表示できません。</p></section>';
  }

  return '<section class="mp-my" data-member-my-ui>'
    +'<header class="mp-my__hero"><p>MY</p><h1>家族の物語を、<br>ひとつの場所に。</h1><span>MEMORIES、Family Pass、これからの記念日。家族の写真体験をまとめて確認できます。</span></header>'
    +renderFamily(vm)
    +renderPass(vm)
    +renderPassport(vm)
    +renderFavorites(vm)
    +renderNext(vm)
    +renderSettings(vm)
    +(vm.partial?'<p class="mp-my__partial">一部の情報は現在準備中です。</p>':'')
    +'</section>';
}

export function memberMyCss(){
  return [
    '.mp-my{--my-card:#fffdf8;--my-ink:#242620;--my-muted:#74786f;--my-green:#315d4f;max-width:1050px;margin:0 auto;padding:34px 18px 120px;color:var(--my-ink);font-family:ui-serif,"Hiragino Mincho ProN","Yu Mincho",serif}',
    '.mp-my__hero{padding:28px 4px 38px;max-width:760px}.mp-my__hero>p,.mp-my__section-head p,.mp-my__family p,.mp-my__pass p,.mp-my__tile p,.mp-my__next>p{margin:0 0 10px;font:700 10px/1.3 ui-sans-serif,sans-serif;letter-spacing:.2em;color:var(--my-green)}.mp-my__hero h1{margin:0 0 16px;font-size:clamp(38px,8vw,70px);font-weight:500;line-height:1.06;letter-spacing:-.045em}.mp-my__hero>span{display:block;max-width:580px;color:var(--my-muted);line-height:1.9}.mp-my__section{margin-bottom:18px}',
    '.mp-my__family,.mp-my__tile,.mp-my__next,.mp-my__pass,.mp-my__passport,.mp-my__settings{border-radius:28px;background:var(--my-card);box-shadow:0 12px 34px rgba(36,38,32,.06)}.mp-my__family{display:flex;justify-content:space-between;gap:20px;align-items:end;padding:28px}.mp-my__family h2{margin:0 0 5px;font-size:34px;font-weight:500}.mp-my__family span,.mp-my__family-count small{color:var(--my-muted)}.mp-my__family-count{text-align:right}.mp-my__family-count strong{display:block;font-size:40px;font-weight:500}.mp-my__family-count small{font:700 9px ui-sans-serif,sans-serif;letter-spacing:.12em}',
    '.mp-my__pass{padding:28px;background:#e7efe9}.mp-my__pass-head{display:flex;justify-content:space-between;gap:18px}.mp-my__pass h2{margin:0;font-size:44px;font-weight:500}.mp-my__pass-head span{color:var(--my-muted)}.mp-my__pass-count{text-align:right}.mp-my__pass-count strong{display:block;font-size:42px;font-weight:500}.mp-my__pass-count small{color:var(--my-muted)}.mp-my__progress{height:3px;margin:22px 0;background:rgba(49,93,79,.14);border-radius:999px;overflow:hidden}.mp-my__progress span{display:block;height:100%;background:var(--my-green)}.mp-my__benefit{display:grid;gap:5px;padding-top:2px}.mp-my__benefit strong{font:700 9px ui-sans-serif,sans-serif;letter-spacing:.12em;color:var(--my-green)}.mp-my__benefit small{color:var(--my-muted);line-height:1.6}.mp-my__pass.is-black{background:#171a16;color:#f1ecdf}.mp-my__pass.is-black p,.mp-my__pass.is-black .mp-my__benefit strong{color:#d6c6a3}.mp-my__pass.is-black .mp-my__progress{background:rgba(255,255,255,.12)}.mp-my__pass.is-black .mp-my__progress span{background:#d6c6a3}',
    '.mp-my__section-head{display:flex;justify-content:space-between;align-items:end;padding:12px 2px}.mp-my__section-head h2{margin:0;font-size:28px;font-weight:500}.mp-my__section-head>strong{font-size:24px;font-weight:500}.mp-my__passport{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:0;padding:24px;list-style:none}.mp-my__passport li{display:flex;align-items:center;gap:9px;color:var(--my-muted)}.mp-my__passport li>span{width:10px;height:10px;border:1px solid var(--my-muted);border-radius:999px}.mp-my__passport li.is-achieved{color:var(--my-ink)}.mp-my__passport li.is-achieved>span{border-color:var(--my-green);background:var(--my-green)}',
    '.mp-my__tile,.mp-my__next{padding:28px}.mp-my__tile{display:flex;justify-content:space-between;gap:18px;align-items:end}.mp-my__tile h2,.mp-my__next h2{margin:0 0 7px;font-size:36px;font-weight:500}.mp-my__tile span,.mp-my__next span,.mp-my__next small{color:var(--my-muted)}.mp-my__tile button{border:0;border-bottom:1px solid currentColor;background:transparent;padding:6px 0;color:var(--my-green);font-weight:700}.mp-my__next strong,.mp-my__next span,.mp-my__next small{display:block;margin-top:7px}.mp-my__next small{line-height:1.7}',
    '.mp-my__settings{overflow:hidden}.mp-my__settings>div{display:flex;justify-content:space-between;gap:20px;padding:17px 20px;border-bottom:1px solid rgba(36,38,32,.08)}.mp-my__settings>div:last-child{border-bottom:0}.mp-my__settings small{color:var(--my-muted)}.mp-my__partial{padding:16px;border-radius:16px;background:rgba(49,93,79,.06);color:var(--my-muted);font-size:12px}',
    '@media(min-width:760px){.mp-my{padding:48px 34px 110px}.mp-my__passport{grid-template-columns:repeat(4,minmax(0,1fr))}.mp-my__section{margin-bottom:22px}}',
    '.mp-shell--black .mp-my{--my-card:#1a1d19;--my-ink:#f3efe6;--my-muted:#aaa99f;--my-green:#d6c6a3}.mp-shell--black .mp-my__pass{background:#20251f}.mp-shell--black .mp-my__settings>div{border-color:rgba(255,255,255,.08)}'
  ].join('');
}

export function resolveMemberMyMount(shellRoot){
  if(!shellRoot||typeof shellRoot.querySelector!=='function')return null;
  return shellRoot.querySelector('[data-member-mount="my"]')||null;
}

export function mountMemberMyUi(shellRoot,result,{on_tab_change=null}={}){
  const mount=resolveMemberMyMount(shellRoot);
  if(!mount||typeof mount.addEventListener!=='function'){
    return {ok:false,error:'member_my_mount_required'};
  }

  const vm=buildMemberMyViewModel(result);
  mount.innerHTML='<style data-member-my-style>'+memberMyCss()+'</style>'+renderMemberMyMarkup(vm);

  const handleClick=event=>{
    const target=event?.target?.closest?.('[data-my-go-tab]');
    if(!target||!mount.contains?.(target))return;
    const tab=text(target.getAttribute('data-my-go-tab')).toLowerCase();
    if(tab!=='memories')return;
    const changed=setMemberAppShellActiveTab(shellRoot,tab);
    if(changed.ok&&typeof on_tab_change==='function')on_tab_change(tab);
  };

  mount.addEventListener('click',handleClick);

  return {
    ok:true,
    source_only:true,
    view_model:vm,
    destroy(){
      if(typeof mount.removeEventListener==='function')mount.removeEventListener('click',handleClick);
      return {ok:true};
    }
  };
}

export function memberMyUiHealth(){
  return {
    member_my_ui:true,
    build:BUILD,
    source_only:true,
    design_direction:'luxury_minimal_family_story',
    canonical_mount:'my',
    my_read_model_only:true,
    family_pass_core:true,
    family_passport_optional:true,
    favorites_optional:true,
    next_memory_optional:true,
    customer_id_exposed:false,
    family_id_exposed:false,
    linked_customer_ids_exposed:false,
    child_id_exposed:false,
    profile_edit:false,
    family_edit:false,
    favorites_mutation:false,
    notification_settings_mutation:false,
    line_link_mutation:false,
    account_deletion:false,
    reservation_creation:false,
    automatic_contact:false,
    line_send:false,
    production_route_wired:false,
    production_write:false
  };
}

export const __test={
  escapeHtml,
  safeDate,
  normalizeFamily,
  normalizeFamilyPass,
  normalizePassport,
  normalizeNextMemory,
  normalizeSettings
};
