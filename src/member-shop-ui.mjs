const BUILD='member-shop-ui-20260925-01';
const PRODUCT_TYPES=new Set(['album','canvas','frame','print','kotobuki']);
const SAFE_ID_RE=/^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$/;
const text=v=>v==null?'':String(v).trim();

function escapeHtml(value){
  return text(value)
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

function safeProductId(value){
  const id=text(value);
  return SAFE_ID_RE.test(id)?id:'';
}

function safeMemberAssetPath(value){
  const path=text(value);
  return /^\/member-assets\/[A-Za-z0-9._~!$&'()*+,;=:@%\/-]+$/.test(path)
    && !path.includes('..')
    && !path.includes('\\')
    ?path
    :'';
}

function safeLocalShopPath(value){
  const path=text(value);
  if(!path||path.length>256)return '';
  if(!path.startsWith('/')||path.startsWith('//'))return '';
  if(/[\u0000-\u001f\u007f]/.test(path))return '';
  if(/^[a-z][a-z0-9+.-]*:/i.test(path))return '';
  if(path.includes('\\'))return '';
  return path;
}

function normalizeProduct(product){
  const productId=safeProductId(product?.product_id);
  const type=text(product?.product_type).toLowerCase();
  if(!productId||!PRODUCT_TYPES.has(type))return null;

  const publicPath=safeMemberAssetPath(product?.hero_asset_ref?.public_asset?.public_path);
  const localPath=safeLocalShopPath(product?.navigation?.shop_path);

  return {
    product_id:productId,
    product_type:type,
    title:text(product?.title)||'Photo Item',
    description:text(product?.description),
    season_tag:text(product?.season_tag)||'evergreen',
    hero_path:publicPath,
    local_shop_path:localPath,
    cta_label:text(product?.navigation?.cta_label)||'商品を見る',
    featured_home:product?.featured_home===true,
    pricing_authoritative:product?.pricing?.authoritative===true,
    price_amount:Number.isFinite(Number(product?.pricing?.amount))
      ?Number(product.pricing.amount)
      :null,
    checkout_ready:product?.checkout?.ready===true,
    discount_enforcement_ready:product?.family_pass_benefit?.enforcement_ready===true
  };
}

export function buildMemberShopViewModel(shopResult,{
  category='all',
  selected_product_id=''
}={}){
  if(shopResult?.status!=='ok'){
    return {
      status:text(shopResult?.status)||'shop_unavailable',
      products:[],
      source_only:true
    };
  }

  const products=(Array.isArray(shopResult.products)?shopResult.products:[])
    .map(normalizeProduct)
    .filter(Boolean);

  const normalizedCategory=category==='all'||PRODUCT_TYPES.has(text(category).toLowerCase())
    ?text(category).toLowerCase()||'all'
    :'all';

  const visibleProducts=normalizedCategory==='all'
    ?products
    :products.filter(product=>product.product_type===normalizedCategory);

  const selectedId=safeProductId(selected_product_id);
  const selectedProduct=selectedId
    ?products.find(product=>product.product_id===selectedId)||null
    :null;

  return {
    status:'ok',
    category:normalizedCategory,
    products,
    visible_products:visibleProducts,
    available_count:Math.max(0,Number(shopResult.available_count)||products.length),
    selected_product:selectedProduct,
    pricing_authoritative:shopResult.pricing_authoritative===true,
    checkout_ready:shopResult.checkout_ready===true,
    discount_enforcement_ready:shopResult.discount_enforcement_ready===true,
    source_only:true,
    presentation_catalog_only:true,
    order_creation:false,
    payment_execution:false,
    production_route_wired:false
  };
}

function productImage(product,{hero=false}={}){
  if(product.hero_path){
    return '<img class="mp-shop__image'+(hero?' is-hero':'')+'" src="'+escapeHtml(product.hero_path)+'" alt="'+escapeHtml(product.title)+'" loading="lazy" decoding="async">';
  }
  return '<div class="mp-shop__placeholder'+(hero?' is-hero':'')+'" aria-hidden="true"><span>PHOTO GOODS</span></div>';
}

function categoryLabel(type){
  return ({
    album:'ALBUM',
    canvas:'CANVAS',
    frame:'FRAME',
    print:'PRINT',
    kotobuki:'KOTOBUKI'
  })[type]||'SHOP';
}

function renderCategories(vm){
  const categories=[
    ['all','ALL'],
    ['album','ALBUM'],
    ['canvas','CANVAS'],
    ['frame','FRAME'],
    ['print','PRINT'],
    ['kotobuki','KOTOBUKI']
  ];
  return '<div class="mp-shop__categories" role="tablist" aria-label="商品カテゴリ">'
    +categories.map(([id,label])=>
      '<button type="button" data-shop-category="'+id+'" class="'+(vm.category===id?'is-active':'')+'" aria-selected="'+(vm.category===id?'true':'false')+'">'+label+'</button>'
    ).join('')
    +'</div>';
}

function renderGrid(vm){
  if(!vm.visible_products.length){
    return '<div class="mp-shop__empty"><p>このカテゴリの商品は準備中です。</p></div>';
  }
  return '<div class="mp-shop__grid">'+vm.visible_products.map(product=>
    '<button type="button" class="mp-shop__card" data-shop-product="'+escapeHtml(product.product_id)+'">'
      +productImage(product)
      +'<div class="mp-shop__card-copy"><small>'+escapeHtml(categoryLabel(product.product_type))+'</small><strong>'+escapeHtml(product.title)+'</strong><p>'+escapeHtml(product.description)+'</p>'
      +'<span>詳細を見る</span></div>'
    +'</button>'
  ).join('')+'</div>';
}

function renderDetail(vm){
  const product=vm.selected_product;
  if(!product)return '';

  const commerceReady=
    product.pricing_authoritative===true
    && product.checkout_ready===true
    && vm.pricing_authoritative===true
    && vm.checkout_ready===true;

  return '<div class="mp-shop__detail" data-shop-detail>'
    +'<div class="mp-shop__detail-head"><button type="button" data-shop-close>← SHOP</button><span>'+escapeHtml(categoryLabel(product.product_type))+'</span></div>'
    +'<div class="mp-shop__detail-layout"><div>'+productImage(product,{hero:true})+'</div>'
    +'<div class="mp-shop__detail-copy"><p>PHOTO GOODS</p><h2>'+escapeHtml(product.title)+'</h2><span>'+escapeHtml(product.description)+'</span>'
    +'<dl><div><dt>TYPE</dt><dd>'+escapeHtml(categoryLabel(product.product_type))+'</dd></div><div><dt>SEASON</dt><dd>'+escapeHtml(product.season_tag)+'</dd></div></dl>'
    +(commerceReady
      ?'<div class="mp-shop__commerce-ready"><strong>Commerce source connected</strong></div>'
      :'<div class="mp-shop__commerce-note"><strong>価格・購入は準備中です。</strong><span>表示価格の推測や注文処理は行いません。</span></div>')
    +(!product.discount_enforcement_ready?'<small>Family Pass特典の割引適用はまだ有効ではありません。</small>':'')
    +'<button type="button" class="mp-shop__disabled-cta" disabled aria-disabled="true">'+escapeHtml(product.cta_label)+'</button>'
    +'</div></div></div>';
}

export function renderMemberShopMarkup(vm){
  if(vm?.status!=='ok'){
    return '<section class="mp-shop mp-shop--unavailable"><p>SHOPを表示できません。</p></section>';
  }

  return '<section class="mp-shop" data-member-shop-ui>'
    +'<header class="mp-shop__hero"><p>SHOP</p><h1>写真を、<br>暮らしの中へ。</h1><span>アルバム、フレーム、キャンバス。家族の記憶を、手に触れられるかたちに。</span><small>'+escapeHtml(vm.available_count)+' ITEMS</small></header>'
    +renderCategories(vm)
    +renderGrid(vm)
    +renderDetail(vm)
    +'</section>';
}

export function memberShopCss(){
  return [
    '.mp-shop{--ms-card:#fffdf8;--ms-ink:#242620;--ms-muted:#74786f;--ms-green:#315d4f;max-width:1180px;margin:0 auto;padding:34px 18px 120px;color:var(--ms-ink);font-family:ui-serif,"Hiragino Mincho ProN","Yu Mincho",serif}',
    '.mp-shop__hero{padding:28px 4px 34px;max-width:800px}.mp-shop__hero>p{margin:0 0 10px;font:700 10px/1.3 ui-sans-serif,sans-serif;letter-spacing:.2em;color:var(--ms-green)}.mp-shop__hero h1{margin:0 0 16px;font-size:clamp(38px,8vw,72px);font-weight:500;line-height:1.06;letter-spacing:-.045em}.mp-shop__hero>span{display:block;max-width:580px;color:var(--ms-muted);line-height:1.9}.mp-shop__hero>small{display:block;margin-top:18px;color:var(--ms-green);font:700 11px ui-sans-serif,sans-serif;letter-spacing:.12em}',
    '.mp-shop__categories{display:flex;gap:8px;overflow-x:auto;padding:0 2px 14px;scrollbar-width:none}.mp-shop__categories::-webkit-scrollbar{display:none}.mp-shop__categories button{flex:0 0 auto;border:1px solid rgba(49,93,79,.14);border-radius:999px;background:transparent;padding:9px 13px;color:var(--ms-muted);font:700 10px ui-sans-serif,sans-serif;letter-spacing:.08em}.mp-shop__categories button.is-active{background:var(--ms-green);border-color:var(--ms-green);color:white}',
    '.mp-shop__grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.mp-shop__card{appearance:none;border:0;padding:0;overflow:hidden;border-radius:24px;background:var(--ms-card);color:inherit;text-align:left;box-shadow:0 12px 30px rgba(36,38,32,.07);cursor:pointer}.mp-shop__image,.mp-shop__placeholder{display:block;width:100%;aspect-ratio:4/5;object-fit:cover}.mp-shop__placeholder{display:grid;place-items:center;background:linear-gradient(145deg,#e6e2d7,#d5d1c8);color:#898b84;font:800 10px ui-sans-serif,sans-serif;letter-spacing:.16em}.mp-shop__card-copy{display:grid;gap:5px;padding:15px}.mp-shop__card-copy small{color:var(--ms-green);font:700 9px ui-sans-serif,sans-serif;letter-spacing:.12em}.mp-shop__card-copy strong{font-size:18px;font-weight:600}.mp-shop__card-copy p{margin:0;color:var(--ms-muted);font-size:12px;line-height:1.6}.mp-shop__card-copy span{margin-top:4px;color:var(--ms-green);font:700 10px ui-sans-serif,sans-serif}.mp-shop__empty{padding:40px 22px;border-radius:24px;background:var(--ms-card);color:var(--ms-muted);text-align:center}',
    '.mp-shop__detail{position:fixed;inset:0;z-index:45;overflow:auto;background:#f7f4ec;padding:18px 16px max(110px,env(safe-area-inset-bottom))}.mp-shop__detail-head{position:sticky;top:0;z-index:2;display:flex;justify-content:space-between;align-items:center;padding:10px 0 18px;background:linear-gradient(#f7f4ec 72%,rgba(247,244,236,0));color:var(--ms-muted);font:700 10px ui-sans-serif,sans-serif;letter-spacing:.08em}.mp-shop__detail-head button{border:0;background:transparent;padding:8px 0;color:var(--ms-green);font-weight:700}.mp-shop__detail-layout{max-width:1060px;margin:0 auto;display:grid;gap:26px}.mp-shop__image.is-hero,.mp-shop__placeholder.is-hero{aspect-ratio:4/5;border-radius:28px}.mp-shop__detail-copy>p{margin:0 0 9px;color:var(--ms-green);font:700 10px ui-sans-serif,sans-serif;letter-spacing:.18em}.mp-shop__detail-copy h2{margin:0 0 12px;font-size:clamp(38px,7vw,64px);font-weight:500;letter-spacing:-.04em}.mp-shop__detail-copy>span{display:block;color:var(--ms-muted);line-height:1.9}.mp-shop__detail-copy dl{margin:28px 0;display:grid;gap:1px;border-top:1px solid rgba(36,38,32,.1)}.mp-shop__detail-copy dl>div{display:flex;justify-content:space-between;gap:20px;padding:13px 0;border-bottom:1px solid rgba(36,38,32,.1)}.mp-shop__detail-copy dt{color:var(--ms-muted);font:700 9px ui-sans-serif,sans-serif;letter-spacing:.12em}.mp-shop__detail-copy dd{margin:0}.mp-shop__commerce-note{display:grid;gap:6px;padding:18px;border-radius:18px;background:rgba(49,93,79,.07)}.mp-shop__commerce-note span,.mp-shop__detail-copy>small{color:var(--ms-muted);font-size:12px;line-height:1.7}.mp-shop__detail-copy>small{display:block;margin:14px 2px}.mp-shop__disabled-cta{width:100%;border:0;border-radius:16px;padding:16px;background:#deddd7;color:#8e9089;font-weight:700}.mp-shop__commerce-ready{padding:16px;border-radius:16px;background:#e7efe9;color:var(--ms-green)}',
    '@media(min-width:760px){.mp-shop{padding:48px 34px 110px}.mp-shop__grid{grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}.mp-shop__detail{padding:28px 34px 80px}.mp-shop__detail-layout{grid-template-columns:minmax(0,1.15fr) minmax(320px,.85fr);align-items:start}.mp-shop__detail-copy{position:sticky;top:78px;padding:20px 0}}',
    '.mp-shell--black .mp-shop{--ms-card:#1a1d19;--ms-ink:#f3efe6;--ms-muted:#aaa99f;--ms-green:#d6c6a3}.mp-shell--black .mp-shop__categories button.is-active{color:#171912}.mp-shell--black .mp-shop__placeholder{background:linear-gradient(145deg,#292c27,#1c1f1b);color:#8d9188}.mp-shell--black .mp-shop__detail{background:#111310}.mp-shell--black .mp-shop__detail-head{background:linear-gradient(#111310 72%,rgba(17,19,16,0))}.mp-shell--black .mp-shop__commerce-note{background:#20251f}'
  ].join('');
}

export function resolveMemberShopMount(shellRoot){
  if(!shellRoot||typeof shellRoot.querySelector!=='function')return null;
  return shellRoot.querySelector('[data-member-mount="shop"]')||null;
}

export function createMemberShopController({
  shell_root,
  shop_result
}={}){
  const mount=resolveMemberShopMount(shell_root);
  if(!mount||typeof mount.addEventListener!=='function'){
    return {ok:false,error:'member_shop_mount_required'};
  }

  const state={
    category:'all',
    selected_product_id:'',
    destroyed:false
  };

  const vm=()=>buildMemberShopViewModel(shop_result,state);
  const render=()=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    const current=vm();
    mount.innerHTML='<style data-member-shop-style>'+memberShopCss()+'</style>'+renderMemberShopMarkup(current);
    return {ok:true,view_model:current};
  };

  const setCategory=value=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    const category=text(value).toLowerCase();
    state.category=category==='all'||PRODUCT_TYPES.has(category)?category:'all';
    state.selected_product_id='';
    return render();
  };

  const openProduct=value=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    const id=safeProductId(value);
    const current=vm();
    if(!id||!current.products.some(product=>product.product_id===id)){
      return {ok:false,error:'shop_product_not_visible'};
    }
    state.selected_product_id=id;
    return render();
  };

  const closeProduct=()=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    state.selected_product_id='';
    return render();
  };

  const handleClick=event=>{
    const categoryTarget=event?.target?.closest?.('[data-shop-category]');
    if(categoryTarget&&mount.contains?.(categoryTarget)){
      setCategory(categoryTarget.getAttribute('data-shop-category'));
      return;
    }
    const productTarget=event?.target?.closest?.('[data-shop-product]');
    if(productTarget&&mount.contains?.(productTarget)){
      openProduct(productTarget.getAttribute('data-shop-product'));
      return;
    }
    const closeTarget=event?.target?.closest?.('[data-shop-close]');
    if(closeTarget&&mount.contains?.(closeTarget))closeProduct();
  };

  mount.addEventListener('click',handleClick);
  const initial=render();

  return {
    ok:true,
    source_only:true,
    get_view_model:vm,
    set_category:setCategory,
    open_product:openProduct,
    close_product:closeProduct,
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

export function memberShopUiHealth(){
  return {
    member_shop_ui:true,
    build:BUILD,
    source_only:true,
    design_direction:'luxury_minimal_family_story',
    canonical_mount:'shop',
    presentation_catalog_only:true,
    public_asset_local_path_only:true,
    arbitrary_external_asset_url:false,
    local_shop_path_navigation_active:false,
    authoritative_pricing:false,
    inventory_claim:false,
    cart:false,
    checkout:false,
    order_creation:false,
    payment_execution:false,
    family_pass_discount_enforcement:false,
    customer_id_exposed:false,
    family_id_exposed:false,
    auto_write:false,
    automatic_contact:false,
    line_send:false,
    production_route_wired:false,
    production_write:false
  };
}

export const __test={
  escapeHtml,
  safeProductId,
  safeMemberAssetPath,
  safeLocalShopPath,
  normalizeProduct
};
