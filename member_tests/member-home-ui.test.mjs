import {
  buildMemberHomeViewModel,
  renderMemberHomeMarkup,
  memberHomeCss,
  resolveMemberHomeMount,
  mountMemberHomeUi,
  memberHomeUiHealth,
  __test
} from '../src/member-home-ui.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const homeResult={
  status:'ok',
  family_id:'family-secret',
  customer_id:'26000001',
  home:{
    recent_memories:[
      {memory_id:'m1',title:'七五三',genre:'七五三',shoot_date:'2025-11-15',favorite:true},
      {memory_id:'m2',title:'Family Day',genre:'ファミリー',shoot_date:'2026-05-03'}
    ],
    visible_memory_count:2,
    family_pass:{
      family_pass:{
        current_tier:'GOLD',
        memory_count:6,
        next_tier:'BLACK',
        memories_to_next:4,
        progress_ratio:.2,
        effective_black:false
      }
    },
    family_passport:{
      achieved_count:2,
      total_milestones:4,
      milestones:[
        {label:'お宮参り',achieved:true},
        {label:'1st Birthday',achieved:true},
        {label:'七五三',achieved:false},
        {label:'入学',achieved:false}
      ]
    },
    today_memory:{
      today_memory:{
        headline:'この日の思い出',
        mode:'exact_anniversary',
        primary:{
          memory_id:'m1',
          title:'七五三',
          genre:'七五三',
          shoot_date:'2025-11-15',
          anniversary:{years_ago:1}
        }
      }
    },
    next_memory:{
      next_memory:{
        label:'4歳バースデー',
        target_date:'2026-12-20',
        days_until:86,
        child:{child_id:'child-secret',display_name:'はるくん'},
        candidate_only:true
      }
    },
    creative:{
      available_count:2,
      eligible_count:1,
      generation_ready:false,
      featured_templates:[
        {
          title:'Family Wallpaper',
          description:'スマホ壁紙',
          eligibility:{eligible:true,memories_needed:0},
          asset_ref:{preview_public_asset:{public_path:'/member-assets/creative/wallpaper.jpg'}}
        },
        {
          title:'Then & Now',
          eligibility:{eligible:false,memories_needed:2},
          asset_ref:{preview_public_asset:{public_path:'https://evil.example/x.jpg'}}
        }
      ]
    },
    shop_pickup:{
      available_count:1,
      pricing_authoritative:false,
      checkout_ready:false,
      products:[
        {
          title:'KOTOBUKI',
          description:'家族の写真を一冊に。',
          hero_asset_ref:{public_asset:{public_path:'/member-assets/shop/kotobuki.jpg'}},
          navigation:{cta_label:'商品を見る'}
        }
      ]
    },
    news:{
      items:[
        {
          title:'秋の撮影について',
          summary:'ご予約枠のお知らせです。',
          published_at:'2026-09-20',
          hero_asset_ref:{public_asset:{public_path:'/member-assets/news/autumn.jpg'}}
        }
      ]
    },
    partial:true,
    unavailable_sections:[{section:'some_optional',error:'not_applied'}]
  }
};

const vm=buildMemberHomeViewModel(homeResult);
pass('HOME view model builds',vm.status==='ok');
pass('identity is omitted from HOME UI model',!JSON.stringify(vm).includes('26000001')&&!JSON.stringify(vm).includes('family-secret')&&!JSON.stringify(vm).includes('child-secret'));
pass('recent MEMORY data is presentation-only',vm.recent_memories.length===2&&vm.recent_memories[0].title==='七五三');
pass('TODAYS MEMORY preserves honest headline',vm.today_memory.headline==='この日の思い出'&&vm.today_memory.primary.years_ago===1);
pass('Family Pass is normalized',vm.family_pass.current_tier==='GOLD'&&vm.family_pass.memories_to_next===4);
pass('NEXT MEMORY remains candidate-only',vm.next_memory.label==='4歳バースデー'&&vm.next_memory.candidate_only===true);
pass('trusted public asset path survives',vm.creative.templates[0].preview_path==='/member-assets/creative/wallpaper.jpg');
pass('external Creative asset path is dropped',vm.creative.templates[1].preview_path==='');
pass('SHOP keeps disabled commerce state',vm.shop.pricing_authoritative===false&&vm.shop.checkout_ready===false);
pass('partial HOME remains presentational',vm.partial===true&&vm.unavailable_sections[0]==='some_optional');

const markup=renderMemberHomeMarkup(vm);
pass('HOME markup contains adopted content modules',[
  'TODAY','MEMORIES','FAMILY PASS','NEXT MEMORY','CREATE','FAMILY PASSPORT','SHOP','NEWS'
].every(term=>markup.includes(term)));
pass('HOME markup has no raw IDs',!markup.includes('26000001')&&!markup.includes('family-secret')&&!markup.includes('child-secret'));
pass('HOME CTA only targets canonical shell tabs',!markup.includes('http://')&&!markup.includes('https://')&&markup.includes('data-home-go-tab="create"'));
pass('no private MEMORY image URL is invented',markup.includes('mp-home__photo-placeholder'));
pass('disabled commerce messaging is explicit',markup.includes('購入処理はまだ有効ではありません'));

const css=memberHomeCss();
pass('HOME uses responsive editorial layout',css.includes('@media(min-width:760px)')&&css.includes('clamp(38px,8vw,74px)'));
pass('HOME inherits BLACK shell theme',css.includes('.mp-shell--black .mp-home'));

pass('safe asset accepts Member local path',__test.safeMemberAssetPath('/member-assets/x/a.jpg')==='/member-assets/x/a.jpg');
pass('safe asset rejects traversal',__test.safeMemberAssetPath('/member-assets/../secret.jpg')==='');
pass('safe asset rejects external URL',__test.safeMemberAssetPath('https://example.com/a.jpg')==='');

const mount={
  innerHTML:'',
  listener:null,
  addEventListener(type,fn){if(type==='click')this.listener=fn},
  removeEventListener(type,fn){if(type==='click'&&this.listener===fn)this.listener=null},
  contains(){return true}
};
const buttons=[
  {tab:'home',classList:{toggle(){}},getAttribute(k){return k==='data-member-tab'?this.tab:null},setAttribute(){}},
  {tab:'create',classList:{toggle(){}},getAttribute(k){return k==='data-member-tab'?this.tab:null},setAttribute(){}}
];
const panels=[
  {tab:'home',hidden:false,getAttribute(k){return k==='data-member-panel'?this.tab:null},setAttribute(k){if(k==='hidden')this.hidden=true},removeAttribute(k){if(k==='hidden')this.hidden=false}},
  {tab:'create',hidden:true,getAttribute(k){return k==='data-member-panel'?this.tab:null},setAttribute(k){if(k==='hidden')this.hidden=true},removeAttribute(k){if(k==='hidden')this.hidden=false}}
];
const shell={
  dataset:{},
  querySelector(selector){return selector==='[data-member-mount="home"]'?mount:null},
  querySelectorAll(selector){return selector==='[data-member-tab]'?buttons:panels}
};

pass('HOME mount resolves canonical destination',resolveMemberHomeMount(shell)===mount);
const mounted=mountMemberHomeUi(shell,homeResult);
pass('HOME mounts into shell',mounted.ok===true&&mount.innerHTML.includes('data-member-home-ui'));
pass('HOME mount is source-only',mounted.source_only===true);

const fakeTarget={
  closest(){return this},
  getAttribute(k){return k==='data-home-go-tab'?'create':null}
};
mount.listener({target:fakeTarget});
pass('HOME CTA changes only canonical shell tab',shell.dataset.activeTab==='create'&&panels[1].hidden===false);

mounted.destroy();
pass('HOME destroy removes listener',mount.listener===null);

const bad=mountMemberHomeUi({querySelector(){return null}},homeResult);
pass('HOME integration fails closed without canonical mount',bad.ok===false&&bad.error==='member_home_mount_required');

const health=memberHomeUiHealth();
pass('health records source-only HOME',health.source_only===true&&health.canonical_mount==='home');
pass('health keeps identity/network/write boundaries closed',health.customer_id_exposed===false&&health.family_id_exposed===false&&health.auto_fetch===false&&health.auto_write===false);
pass('health keeps contact and Production disabled',health.automatic_contact===false&&health.line_send===false&&health.production_route_wired===false&&health.production_write===false);

console.log(`MEMBER_HOME_UI=${n}/${n} PASS`);
