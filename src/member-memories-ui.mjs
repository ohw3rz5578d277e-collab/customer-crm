const BUILD='member-memories-ui-20260925-01';
const MAX_MEMORY_ID=160;
const SAFE_MEMORY_ID_RE=/^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$/;
const text=v=>v==null?'':String(v).trim();

function escapeHtml(value){
  return text(value)
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

function safeMemoryId(value){
  const id=text(value);
  return id.length>0&&id.length<=MAX_MEMORY_ID&&SAFE_MEMORY_ID_RE.test(id)?id:'';
}

function safeDate(value){
  const v=text(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(v)?v:'';
}

function normalizeCover(cover){
  if(!cover||typeof cover!=='object')return null;
  return {
    media_type:text(cover.media_type||'image'),
    role:text(cover.role||'cover'),
    width:Number.isFinite(Number(cover.width))?Number(cover.width):null,
    height:Number.isFinite(Number(cover.height))?Number(cover.height):null,
    private_delivery_required:cover.private_delivery_required!==false
  };
}

function normalizeMemoryItem(memory){
  const memoryId=safeMemoryId(memory?.memory_id);
  if(!memoryId)return null;
  return {
    memory_id:memoryId,
    title:text(memory?.title)||'MEMORY',
    shoot_date:safeDate(memory?.shoot_date),
    genre:text(memory?.genre),
    cover:normalizeCover(memory?.cover),
    preview_count:Math.max(0,Number(memory?.preview_count)||0),
    amazon_photos_available:memory?.amazon_photos_available===true,
    favorite:memory?.favorite===true,
    favorite_mutable:memory?.favorite_mutable===true
  };
}

function normalizeDetail(detailResult,selectedMemoryId){
  if(detailResult?.status!=='ok'||!detailResult?.memory)return null;
  const memoryId=safeMemoryId(detailResult.memory.memory_id);
  if(!memoryId||memoryId!==safeMemoryId(selectedMemoryId))return null;

  const media=(Array.isArray(detailResult.media)?detailResult.media:[]).slice(0,100).map(item=>({
    media_type:text(item?.media_type||'image'),
    role:text(item?.role),
    width:Number.isFinite(Number(item?.width))?Number(item.width):null,
    height:Number.isFinite(Number(item?.height))?Number(item.height):null,
    private_delivery_required:item?.private_delivery_required!==false
  }));

  return {
    memory_id:memoryId,
    title:text(detailResult.memory.title)||'MEMORY',
    shoot_date:safeDate(detailResult.memory.shoot_date),
    genre:text(detailResult.memory.genre),
    media,
    favorite:detailResult.favorite===true,
    favorite_mutable:detailResult.favorite_mutable===true,
    amazon_photos_available:detailResult?.amazon_link?.provider==='amazon_photos'
      && /^https:\/\//i.test(text(detailResult?.amazon_link?.url)),
    create_available:detailResult.create_available===true,
    shop_available:detailResult.shop_available===true
  };
}

export function buildMemberMemoriesViewModel(listResult,{
  selected_memory_id='',
  detail_result=null,
  detail_loading=false,
  detail_error=''
}={}){
  if(listResult?.status!=='ok'){
    return {
      status:text(listResult?.status)||'memories_unavailable',
      memories:[],
      source_only:true
    };
  }

  const memories=(Array.isArray(listResult.memories)?listResult.memories:[])
    .map(normalizeMemoryItem)
    .filter(Boolean);

  const selectedId=safeMemoryId(selected_memory_id);
  const selected=selectedId?memories.find(item=>item.memory_id===selectedId)||null:null;
  const detail=selected?normalizeDetail(detail_result,selectedId):null;

  return {
    status:'ok',
    memories,
    memory_count:memories.length,
    favorites_available:listResult.favorites_available===true,
    favorite_mutation_ready:listResult.favorite_mutation_ready===true,
    selected_memory:selected,
    detail,
    detail_loading:selected?detail_loading===true:false,
    detail_error:selected?text(detail_error):'',
    source_only:true,
    private_media_delivery_active:false,
    auto_detail_fetch:false,
    favorite_write:false,
    production_route_wired:false
  };
}

function placeholder({hero=false,label='PHOTO'}={}){
  return '<div class="mp-memories__photo-placeholder'+(hero?' is-hero':'')+'" aria-hidden="true"><span>'+escapeHtml(label)+'</span></div>';
}

function favoriteMark(memory){
  if(!memory.favorite)return '';
  return '<span class="mp-memories__favorite" aria-label="お気に入り">♥</span>';
}

function renderTimeline(vm){
  if(!vm.memories.length){
    return '<div class="mp-memories__empty"><p>まだMEMORYはありません。</p><span>撮影した家族の時間が、ここに少しずつ増えていきます。</span></div>';
  }

  return '<div class="mp-memories__grid">'+vm.memories.map(memory=>
    '<button type="button" class="mp-memories__card" data-memory-open="'+escapeHtml(memory.memory_id)+'">'
      +'<div class="mp-memories__card-photo">'+placeholder({label:memory.preview_count?'PHOTO '+memory.preview_count:'PHOTO'})+favoriteMark(memory)+'</div>'
      +'<div class="mp-memories__card-copy"><small>'+escapeHtml(memory.shoot_date||'MEMORY')+'</small><strong>'+escapeHtml(memory.title)+'</strong><span>'+escapeHtml(memory.genre)+'</span></div>'
    +'</button>'
  ).join('')+'</div>';
}

function renderDetail(vm){
  if(!vm.selected_memory)return '';
  const memory=vm.selected_memory;
  if(vm.detail_loading){
    return '<div class="mp-memories__detail" data-memory-detail><div class="mp-memories__detail-head"><button type="button" data-memory-close>閉じる</button></div><div class="mp-memories__loading">MEMORYを読み込んでいます。</div></div>';
  }
  if(vm.detail_error){
    return '<div class="mp-memories__detail" data-memory-detail><div class="mp-memories__detail-head"><button type="button" data-memory-close>閉じる</button></div><div class="mp-memories__loading">MEMORYを表示できません。</div></div>';
  }
  if(!vm.detail){
    return '<div class="mp-memories__detail" data-memory-detail><div class="mp-memories__detail-head"><button type="button" data-memory-close>閉じる</button></div></div>';
  }

  const d=vm.detail;
  const mediaCount=d.media.length;
  const photos=mediaCount
    ?d.media.slice(0,12).map((item,index)=>'<div class="mp-memories__detail-photo">'+placeholder({hero:index===0,label:item.role==='cover'?'COVER':'PHOTO'})+'</div>').join('')
    :'<div class="mp-memories__detail-photo">'+placeholder({hero:true,label:'PHOTO'})+'</div>';

  return '<div class="mp-memories__detail" data-memory-detail>'
    +'<div class="mp-memories__detail-head"><button type="button" data-memory-close>← MEMORIES</button>'
      +'<div>'+favoriteMark(d)+(d.amazon_photos_available?'<span class="mp-memories__delivery">Amazon Photosあり</span>':'')+'</div></div>'
    +'<header class="mp-memories__detail-title"><p>'+escapeHtml(d.shoot_date||'MEMORY')+'</p><h2>'+escapeHtml(d.title)+'</h2><span>'+escapeHtml(d.genre)+'</span></header>'
    +'<div class="mp-memories__detail-grid">'+photos+'</div>'
    +'<div class="mp-memories__detail-note"><strong>'+escapeHtml(mediaCount)+' photos</strong><span>写真表示はPrivate Media配信のProduction有効化後に接続されます。</span></div>'
    +'</div>';
}

export function renderMemberMemoriesMarkup(vm){
  if(vm?.status!=='ok'){
    return '<section class="mp-memories mp-memories--unavailable"><p>MEMORIESを表示できません。</p></section>';
  }

  return '<section class="mp-memories" data-member-memories-ui>'
    +'<header class="mp-memories__hero"><p>MEMORIES</p><h1>家族の時間を、<br>何度でも。</h1><span>撮影した日の記憶を、季節と一緒に振り返る場所。</span><small>'+escapeHtml(vm.memory_count)+' MEMORIES</small></header>'
    +'<div class="mp-memories__toolbar"><span>NEWEST FIRST</span>'
      +(vm.favorites_available?'<span>♥ お気に入り表示対応</span>':'<span>お気に入りは準備中</span>')
      +'</div>'
    +renderTimeline(vm)
    +renderDetail(vm)
    +'</section>';
}

export function memberMemoriesCss(){
  return [
    '.mp-memories{--mm-card:#fffdf8;--mm-ink:#232622;--mm-muted:#74786f;--mm-green:#315d4f;max-width:1180px;margin:0 auto;padding:34px 18px 120px;color:var(--mm-ink);font-family:ui-serif,"Hiragino Mincho ProN","Yu Mincho",serif}',
    '.mp-memories__hero{padding:28px 4px 34px}.mp-memories__hero>p{margin:0 0 10px;font:700 10px/1.3 ui-sans-serif,sans-serif;letter-spacing:.2em;color:var(--mm-green)}.mp-memories__hero h1{margin:0 0 16px;font-size:clamp(38px,8vw,70px);font-weight:500;line-height:1.07;letter-spacing:-.045em}.mp-memories__hero>span{display:block;color:var(--mm-muted);line-height:1.9}.mp-memories__hero>small{display:block;margin-top:18px;color:var(--mm-green);font:700 11px ui-sans-serif,sans-serif;letter-spacing:.12em}',
    '.mp-memories__toolbar{display:flex;justify-content:space-between;gap:14px;margin:0 2px 14px;color:var(--mm-muted);font:700 10px ui-sans-serif,sans-serif;letter-spacing:.08em}.mp-memories__grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.mp-memories__card{position:relative;appearance:none;border:0;padding:0;overflow:hidden;border-radius:24px;background:var(--mm-card);color:inherit;text-align:left;box-shadow:0 12px 32px rgba(35,38,34,.07);cursor:pointer}.mp-memories__card-photo{position:relative}.mp-memories__photo-placeholder{display:grid;place-items:center;width:100%;aspect-ratio:4/3;background:linear-gradient(145deg,#e7e3d9,#d7d4ca);color:#8b8e86;font:800 10px ui-sans-serif,sans-serif;letter-spacing:.15em}.mp-memories__photo-placeholder.is-hero{aspect-ratio:5/4}.mp-memories__favorite{position:absolute;top:12px;right:12px;display:grid;place-items:center;width:30px;height:30px;border-radius:999px;background:rgba(255,253,248,.92);color:#315d4f;font-size:15px;box-shadow:0 4px 12px rgba(35,38,34,.08)}.mp-memories__card-copy{display:grid;gap:4px;padding:14px 15px 17px}.mp-memories__card-copy small,.mp-memories__card-copy span{color:var(--mm-muted);font-size:11px}.mp-memories__card-copy strong{font-size:17px;font-weight:600}',
    '.mp-memories__empty{padding:40px 24px;border-radius:24px;background:var(--mm-card);text-align:center}.mp-memories__empty p{font-size:22px;margin:0 0 8px}.mp-memories__empty span{color:var(--mm-muted);line-height:1.8}',
    '.mp-memories__detail{position:fixed;inset:0;z-index:45;overflow:auto;background:#f7f4ec;padding:18px 16px max(110px,env(safe-area-inset-bottom))}.mp-memories__detail-head{position:sticky;top:0;z-index:2;display:flex;justify-content:space-between;align-items:center;padding:10px 0 16px;background:linear-gradient(#f7f4ec 72%,rgba(247,244,236,0))}.mp-memories__detail-head button{border:0;background:transparent;padding:8px 0;color:var(--mm-green);font-weight:700}.mp-memories__delivery{display:inline-block;margin-left:8px;padding:6px 9px;border-radius:999px;background:#e8eee9;color:var(--mm-green);font:700 10px ui-sans-serif,sans-serif}.mp-memories__detail-head .mp-memories__favorite{position:static;display:inline-grid}.mp-memories__detail-title{max-width:850px;margin:12px auto 24px}.mp-memories__detail-title p{margin:0 0 8px;color:var(--mm-green);font-size:12px}.mp-memories__detail-title h2{margin:0 0 7px;font-size:clamp(34px,7vw,62px);font-weight:500;letter-spacing:-.04em}.mp-memories__detail-title span{color:var(--mm-muted)}.mp-memories__detail-grid{max-width:1080px;margin:0 auto;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.mp-memories__detail-photo:first-child{grid-column:1/-1}.mp-memories__detail-note{max-width:850px;margin:24px auto 0;display:flex;justify-content:space-between;gap:20px;color:var(--mm-muted);font-size:12px;line-height:1.6}.mp-memories__detail-note strong{color:var(--mm-ink)}.mp-memories__loading{max-width:850px;margin:80px auto;color:var(--mm-muted)}',
    '@media(min-width:760px){.mp-memories{padding:48px 34px 110px}.mp-memories__grid{grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}.mp-memories__detail{padding:26px 34px 80px}.mp-memories__detail-grid{grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.mp-memories__detail-photo:first-child{grid-column:span 2;grid-row:span 2}.mp-memories__detail-photo:first-child .mp-memories__photo-placeholder{height:100%;aspect-ratio:auto;min-height:520px}}',
    '.mp-shell--black .mp-memories{--mm-card:#1a1d19;--mm-ink:#f3efe6;--mm-muted:#aaa99f;--mm-green:#d6c6a3}.mp-shell--black .mp-memories__photo-placeholder{background:linear-gradient(145deg,#292c27,#1c1f1b);color:#8d9188}.mp-shell--black .mp-memories__detail{background:#111310}.mp-shell--black .mp-memories__detail-head{background:linear-gradient(#111310 72%,rgba(17,19,16,0))}.mp-shell--black .mp-memories__favorite{background:#252922;color:#d6c6a3}.mp-shell--black .mp-memories__delivery{background:#252922;color:#d6c6a3}'
  ].join('');
}

export function resolveMemberMemoriesMount(shellRoot){
  if(!shellRoot||typeof shellRoot.querySelector!=='function')return null;
  return shellRoot.querySelector('[data-member-mount="memories"]')||null;
}

export function createMemberMemoriesController({
  shell_root,
  list_result,
  load_detail,
  on_error=null
}={}){
  const mount=resolveMemberMemoriesMount(shell_root);
  if(!mount||typeof mount.addEventListener!=='function'){
    return {ok:false,error:'member_memories_mount_required'};
  }

  const state={
    selected_memory_id:'',
    detail_result:null,
    detail_loading:false,
    detail_error:'',
    destroyed:false
  };

  const vm=()=>buildMemberMemoriesViewModel(list_result,state);
  const render=()=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    const current=vm();
    mount.innerHTML='<style data-member-memories-style>'+memberMemoriesCss()+'</style>'+renderMemberMemoriesMarkup(current);
    return {ok:true,view_model:current};
  };

  const close=()=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    state.selected_memory_id='';
    state.detail_result=null;
    state.detail_loading=false;
    state.detail_error='';
    return render();
  };

  const open=async memoryId=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    const id=safeMemoryId(memoryId);
    const base=vm();
    if(!id||!base.memories.some(item=>item.memory_id===id)){
      return {ok:false,error:'memory_not_visible'};
    }
    if(typeof load_detail!=='function'){
      return {ok:false,error:'memory_detail_loader_unavailable'};
    }

    state.selected_memory_id=id;
    state.detail_result=null;
    state.detail_error='';
    state.detail_loading=true;
    render();

    let result;
    try{
      result=await load_detail(id);
    }catch(error){
      result={status:'memory_detail_load_failed'};
    }

    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    if(state.selected_memory_id!==id)return {ok:false,error:'stale_memory_detail_ignored'};

    state.detail_loading=false;
    if(result?.status!=='ok'){
      state.detail_result=null;
      state.detail_error=text(result?.status)||'memory_detail_load_failed';
      render();
      if(typeof on_error==='function')on_error({status:state.detail_error});
      return {ok:false,error:state.detail_error};
    }

    const detailMemoryId=safeMemoryId(result?.memory?.memory_id);
    if(detailMemoryId!==id){
      state.detail_result=null;
      state.detail_error='memory_detail_mismatch';
      render();
      if(typeof on_error==='function')on_error({status:'memory_detail_mismatch'});
      return {ok:false,error:'memory_detail_mismatch'};
    }

    state.detail_result=result;
    state.detail_error='';
    const rendered=render();
    return {ok:true,view_model:rendered.view_model};
  };

  const handleClick=event=>{
    const openTarget=event?.target?.closest?.('[data-memory-open]');
    if(openTarget&&mount.contains?.(openTarget)){
      void open(openTarget.getAttribute('data-memory-open'));
      return;
    }
    const closeTarget=event?.target?.closest?.('[data-memory-close]');
    if(closeTarget&&mount.contains?.(closeTarget))close();
  };

  mount.addEventListener('click',handleClick);
  const initial=render();

  return {
    ok:true,
    source_only:true,
    get_view_model:vm,
    open_memory:open,
    close_memory:close,
    render,
    initial_view_model:initial.view_model,
    destroy(){
      if(state.destroyed)return {ok:true,already_destroyed:true};
      state.destroyed=true;
      if(typeof mount.removeEventListener==='function')mount.removeEventListener('click',handleClick);
      return {ok:true,already_destroyed:false};
    }
  };
}

export function memberMemoriesUiHealth(){
  return {
    member_memories_ui:true,
    build:BUILD,
    source_only:true,
    design_direction:'luxury_minimal_family_story',
    canonical_mount:'memories',
    list_read_model_reused:true,
    detail_read_model_reused:true,
    detail_load_user_triggered:true,
    auto_detail_fetch:false,
    private_memory_binary_rendering:false,
    private_storage_key_exposed:false,
    signed_private_url_exposed:false,
    amazon_photos_url_exposed:false,
    favorite_state_visible:true,
    favorite_mutation_active:false,
    favorite_optimistic_state:false,
    customer_id_exposed:false,
    family_id_exposed:false,
    automatic_contact:false,
    line_send:false,
    production_route_wired:false,
    production_write:false
  };
}

export const __test={
  escapeHtml,
  safeMemoryId,
  safeDate,
  normalizeCover,
  normalizeMemoryItem,
  normalizeDetail
};
