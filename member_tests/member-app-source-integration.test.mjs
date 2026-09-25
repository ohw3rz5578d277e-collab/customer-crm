import {
  createMemberAppSourceIntegration,
  memberAppSourceAcceptanceHealth,
  __test
} from '../src/member-app-source-integration.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

function makeMount(){
  return {
    innerHTML:'',
    listener:null,
    addEventListener(type,fn){if(type==='click')this.listener=fn},
    removeEventListener(type,fn){if(type==='click'&&this.listener===fn)this.listener=null},
    contains(){return true}
  };
}

const mounts={
  home:makeMount(),
  memories:makeMount(),
  create:makeMount(),
  shop:makeMount(),
  my:makeMount()
};

const buttons=__test.TABS.map(tab=>({
  tab,
  classList:{toggle(){}},
  getAttribute(key){return key==='data-member-tab'?this.tab:null},
  setAttribute(){}
}));

const panels=__test.TABS.map(tab=>({
  tab,
  hidden:tab!=='home',
  getAttribute(key){return key==='data-member-panel'?this.tab:null},
  setAttribute(key){if(key==='hidden')this.hidden=true},
  removeAttribute(key){if(key==='hidden')this.hidden=false}
}));

const shell={
  dataset:{},
  querySelector(selector){
    const match=selector.match(/^\[data-member-mount="([^"]+)"\]$/);
    return match?mounts[match[1]]||null:null;
  },
  querySelectorAll(selector){
    if(selector==='[data-member-tab]')return buttons;
    if(selector==='[data-member-panel]')return panels;
    return [];
  }
};

const homeResult={
  status:'ok',
  home:{
    recent_memories:[],
    visible_memory_count:0,
    family_pass:{
      family_pass:{
        current_tier:'FAMILY',
        memory_count:1,
        next_tier:'WELCOME_BACK',
        memories_to_next:1,
        progress_ratio:0
      }
    },
    family_passport:null,
    today_memory:null,
    next_memory:null,
    creative:null,
    shop_pickup:null,
    news:null,
    partial:false,
    unavailable_sections:[]
  }
};

const memoriesResult={
  status:'ok',
  memories:[],
  favorites_available:false,
  favorite_mutation_ready:false,
  read_only:true
};

const creativeCatalog={
  status:'ok',
  templates:[
    {
      template_id:'wallpaper-1',
      creative_type:'wallpaper',
      title:'Family Wallpaper',
      description:'1枚の写真でつくる',
      composition:{mode:'single_photo',photo_slots:1,output_mime:'image/jpeg'},
      eligibility:{eligible:true,memories_needed:0},
      asset_ref:{preview_public_asset:{public_path:'/member-assets/creative/wallpaper-1.jpg'}}
    }
  ]
};

const shopResult={
  status:'ok',
  products:[],
  available_count:0,
  pricing_authoritative:false,
  checkout_ready:false,
  discount_enforcement_ready:false
};

const myResult={
  status:'ok',
  my:{
    family:{
      display_name:'YAMADA FAMILY',
      relation:'owner',
      access_role:'owner',
      linked_member_count:1,
      family_profile_edit_ready:false,
      member_identity_edit_ready:false
    },
    memory_count:1,
    current_tier:'FAMILY',
    effective_black:false,
    favorite_count:null,
    family_pass:{
      family_pass:{
        memory_count:1,
        current_tier:'FAMILY',
        next_tier:'WELCOME_BACK',
        memories_to_next:1,
        progress_ratio:0,
        effective_black:false
      },
      entitlement:{durable_black:false},
      benefit_contract:{
        black_photo_goods_discount_percent:10,
        applies_to_shooting_fee:false,
        enforcement_ready:false
      }
    },
    family_passport:null,
    next_memory:null,
    settings:{
      family_profile_edit_ready:false,
      member_profile_edit_ready:false,
      notification_settings_ready:false,
      line_link_settings_ready:false,
      account_deletion_ready:false
    },
    partial:true,
    unavailable_sections:[
      {section:'favorites',error:'favorites_schema_not_applied'}
    ]
  }
};

let detailCalls=0;
let planCalls=0;
let renderCalls=0;
const app=createMemberAppSourceIntegration({
  shell_root:shell,
  home_result:homeResult,
  memories_result:memoriesResult,
  creative_catalog:creativeCatalog,
  creative_memory_details:[],
  creative_plan_composition:async()=>{planCalls++;return {status:'blocked'}},
  creative_render_composition:async()=>{renderCalls++;return {status:'blocked'}},
  creative_runtime:{},
  creative_document_ref:{
    body:{appendChild(){}},
    createElement(){return {style:{},click(){},remove(){}}}
  },
  creative_url_ref:{revokeObjectURL(){}},
  memory_detail_loader:async()=>{detailCalls++;return {status:'memory_not_found'}},
  shop_result:shopResult,
  my_result:myResult,
  initial_tab:'home'
});

pass('five-tab source integration succeeds',app.ok===true&&app.tabs.join(',')==='home,memories,create,shop,my');
pass('HOME mounted',mounts.home.innerHTML.includes('data-member-home-ui'));
pass('MEMORIES mounted',mounts.memories.innerHTML.includes('data-member-memories-ui'));
pass('CREATE mounted',mounts.create.innerHTML.includes('data-member-creative-create'));
pass('SHOP mounted',mounts.shop.innerHTML.includes('data-member-shop-ui'));
pass('MY mounted',mounts.my.innerHTML.includes('data-member-my-ui'));

pass('integration performs no automatic MEMORY detail fetch',detailCalls===0);
pass('integration performs no automatic Creative planning',planCalls===0);
pass('integration performs no automatic Creative rendering',renderCalls===0);

pass('initial canonical tab is HOME',shell.dataset.activeTab==='home'&&panels.find(x=>x.tab==='home').hidden===false);
const moved=app.set_active_tab('shop');
pass('app controller can switch canonical tabs locally',moved.ok===true&&shell.dataset.activeTab==='shop'&&panels.find(x=>x.tab==='shop').hidden===false);
const unknown=app.set_active_tab('unknown-tab');
pass('unknown tab fails closed to HOME through shell normalization',unknown.ok===true&&unknown.active_tab==='home'&&shell.dataset.activeTab==='home');

const health=memberAppSourceAcceptanceHealth();
pass('acceptance sees all five UI foundations',health.all_five_ui_foundations_present===true&&health.canonical_tabs.length===5);
pass('acceptance keeps automatic actions disabled',health.auto_fetch===false&&health.auto_memory_detail_fetch===false&&health.auto_creative_preview===false&&health.auto_creative_download===false);
pass('acceptance keeps identity/private media boundaries closed',health.customer_id_exposed===false&&health.family_id_exposed===false&&health.child_id_exposed===false&&health.private_storage_key_exposed===false&&health.private_media_binary_rendering===false);
pass('acceptance keeps mutations and commerce disabled',health.favorite_mutation_active===false&&health.profile_edit_active===false&&health.settings_mutation_active===false&&health.shop_checkout_active===false&&health.shop_payment_active===false&&health.black_discount_enforcement_active===false);
pass('acceptance keeps contact/write/Production disabled',health.reservation_creation===false&&health.automatic_contact===false&&health.line_send===false&&health.generated_output_server_write===false&&health.production_route_wired===false&&health.production_private_media_delivery_active===false&&health.production_write===false);

const destroyed=app.destroy();
pass('destroy succeeds across all five UI handles',destroyed.ok===true&&destroyed.results.length===5);
pass('destroy removes HOME listener',mounts.home.listener===null);
pass('destroy removes MEMORIES listener',mounts.memories.listener===null);
pass('destroy removes CREATE listener',mounts.create.listener===null);
pass('destroy removes SHOP listener',mounts.shop.listener===null);
pass('destroy removes MY listener',mounts.my.listener===null);
pass('destroyed app refuses tab actions',app.set_active_tab('home').error==='controller_destroyed');

const missingMountShell={
  querySelector(selector){
    if(selector==='[data-member-mount="create"]')return null;
    return makeMount();
  }
};
const validation=__test.validateCanonicalMounts(missingMountShell);
pass('missing canonical mount fails before partial integration',validation.ok===false&&validation.error==='member_app_mount_required'&&validation.missing_tab==='create');

const missingApp=createMemberAppSourceIntegration({
  shell_root:missingMountShell,
  home_result:homeResult,
  memories_result:memoriesResult,
  creative_catalog:creativeCatalog,
  shop_result:shopResult,
  my_result:myResult
});
pass('integration fails closed when one canonical mount is absent',missingApp.ok===false&&missingApp.missing_tab==='create');

console.log(`MEMBER_APP_SOURCE_INTEGRATION=${n}/${n} PASS`);
