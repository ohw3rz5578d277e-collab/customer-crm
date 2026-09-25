import {
  buildMemberShopViewModel,
  renderMemberShopMarkup,
  memberShopCss,
  resolveMemberShopMount,
  createMemberShopController,
  memberShopUiHealth,
  __test
} from '../src/member-shop-ui.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const shopResult={
  status:'ok',
  family_id:'family-secret',
  customer_id:'26000001',
  products:[
    {
      product_id:'prod-album-01',
      product_type:'album',
      title:'KAHO',
      description:'家族の物語を一冊に。',
      season_tag:'evergreen',
      hero_asset_ref:{
        asset_id:'asset:shop:album:01',
        public_asset:{public_path:'/member-assets/shop/album-01.jpg'},
        storage_key:'must-not-leak'
      },
      navigation:{
        shop_path:'/shop/album/',
        local_path_only:true,
        cta_label:'アルバムを見る'
      },
      featured_home:true,
      pricing:{authoritative:false,amount:null,currency:'JPY',source_connected:false},
      checkout:{ready:false,provider:null},
      family_pass_benefit:{enforcement_ready:false,discount_amount:null}
    },
    {
      product_id:'prod-canvas-01',
      product_type:'canvas',
      title:'CANVAS',
      description:'お気に入りの一枚を飾る。',
      season_tag:'evergreen',
      hero_asset_ref:{
        asset_id:'asset:shop:canvas:01',
        public_asset:{public_path:'https://evil.example/canvas.jpg'}
      },
      navigation:{shop_path:'/shop/canvas/',local_path_only:true,cta_label:'商品を見る'},
      pricing:{authoritative:false,amount:999999,currency:'JPY'},
      checkout:{ready:false},
      family_pass_benefit:{enforcement_ready:false}
    },
    {
      product_id:'bad/id',
      product_type:'album',
      title:'BAD',
      hero_asset_ref:{public_asset:{public_path:'/member-assets/x.jpg'}},
      navigation:{shop_path:'/bad/'}
    }
  ],
  available_count:2,
  pricing_authoritative:false,
  checkout_ready:false,
  discount_enforcement_ready:false,
  read_only:true
};

const vm=buildMemberShopViewModel(shopResult);
pass('SHOP view model builds',vm.status==='ok'&&vm.products.length===2);
pass('identity is removed from SHOP UI model',!JSON.stringify(vm).includes('family-secret')&&!JSON.stringify(vm).includes('26000001'));
pass('unsafe product id is excluded',!vm.products.some(x=>x.title==='BAD'));
pass('trusted public asset path survives',vm.products[0].hero_path==='/member-assets/shop/album-01.jpg');
pass('external public asset URL is dropped',vm.products[1].hero_path==='');
pass('local shop path remains local presentation metadata',vm.products[0].local_shop_path==='/shop/album/');
pass('authoritative price remains disabled',vm.pricing_authoritative===false&&vm.products[0].pricing_authoritative===false);
pass('non-authoritative product amount is not commerce-ready',vm.products[1].checkout_ready===false&&vm.checkout_ready===false);
pass('Family Pass discount enforcement remains disabled',vm.discount_enforcement_ready===false&&vm.products.every(x=>x.discount_enforcement_ready===false));

const albumVm=buildMemberShopViewModel(shopResult,{category:'album'});
pass('category filter is local and exact',albumVm.visible_products.length===1&&albumVm.visible_products[0].product_type==='album');

const selectedVm=buildMemberShopViewModel(shopResult,{selected_product_id:'prod-album-01'});
pass('visible product can be selected for source-only detail',selectedVm.selected_product?.title==='KAHO');

const markup=renderMemberShopMarkup(selectedVm);
pass('SHOP markup is discovery-first',markup.includes('写真を、<br>暮らしの中へ。')&&markup.includes('KAHO')&&markup.includes('PHOTO GOODS'));
pass('SHOP markup does not invent a price',!markup.includes('999999')&&!markup.includes('¥')&&!markup.includes('￥'));
pass('checkout CTA stays disabled',markup.includes('disabled aria-disabled="true"'));
pass('discount boundary is visible',markup.includes('割引適用はまだ有効ではありません'));
pass('external URL is absent from markup',!markup.includes('evil.example')&&!markup.includes('https://'));

const css=memberShopCss();
pass('SHOP is responsive',css.includes('@media(min-width:760px)')&&css.includes('grid-template-columns:repeat(3'));
pass('SHOP supports BLACK shell',css.includes('.mp-shell--black .mp-shop'));

pass('safe product id accepts opaque id',__test.safeProductId('prod-album-01')==='prod-album-01');
pass('safe product id rejects slash',__test.safeProductId('bad/id')==='');
pass('asset guard rejects traversal',__test.safeMemberAssetPath('/member-assets/../secret.jpg')==='');
pass('asset guard rejects external URL',__test.safeMemberAssetPath('https://example.com/x.jpg')==='');
pass('local shop path accepts relative-origin path',__test.safeLocalShopPath('/寿-kotobuki/')==='/寿-kotobuki/');
pass('local shop path rejects protocol-relative URL',__test.safeLocalShopPath('//evil.example/x')==='');

const mount={
  innerHTML:'',
  listener:null,
  addEventListener(type,fn){if(type==='click')this.listener=fn},
  removeEventListener(type,fn){if(type==='click'&&this.listener===fn)this.listener=null},
  contains(){return true}
};
const shell={
  querySelector(selector){return selector==='[data-member-mount="shop"]'?mount:null}
};

pass('SHOP mount resolves canonical shell destination',resolveMemberShopMount(shell)===mount);
const controller=createMemberShopController({shell_root:shell,shop_result:shopResult});
pass('controller mounts catalog source-only',controller.ok===true&&mount.innerHTML.includes('data-member-shop-ui'));
const category=controller.set_category('canvas');
pass('controller category filtering is local',category.ok===true&&category.view_model.visible_products.length===1);
const open=controller.open_product('prod-canvas-01');
pass('controller opens only visible catalog product',open.ok===true&&open.view_model.selected_product.product_id==='prod-canvas-01');
const hidden=controller.open_product('unknown-product');
pass('unknown product fails closed',hidden.ok===false&&hidden.error==='shop_product_not_visible');
controller.close_product();
pass('detail closes without navigation',controller.get_view_model().selected_product===null);
const destroyed=controller.destroy();
pass('destroy removes click listener',destroyed.ok===true&&mount.listener===null);
pass('destroyed controller refuses actions',controller.open_product('prod-album-01').error==='controller_destroyed');

const noMount=createMemberShopController({shell_root:{querySelector(){return null}},shop_result:shopResult});
pass('controller fails closed without canonical mount',noMount.ok===false&&noMount.error==='member_shop_mount_required');

const health=memberShopUiHealth();
pass('health records source-only SHOP',health.source_only===true&&health.canonical_mount==='shop'&&health.presentation_catalog_only===true);
pass('health keeps pricing/checkout/order/payment disabled',health.authoritative_pricing===false&&health.checkout===false&&health.order_creation===false&&health.payment_execution===false);
pass('health keeps BLACK benefit enforcement inactive',health.family_pass_discount_enforcement===false);
pass('health keeps local path navigation inactive',health.local_shop_path_navigation_active===false);
pass('health keeps identity/contact/Production writes disabled',health.customer_id_exposed===false&&health.family_id_exposed===false&&health.production_route_wired===false&&health.production_write===false&&health.line_send===false);

console.log(`MEMBER_SHOP_UI=${n}/${n} PASS`);
