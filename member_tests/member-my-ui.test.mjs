import {
  buildMemberMyViewModel,
  renderMemberMyMarkup,
  memberMyCss,
  resolveMemberMyMount,
  mountMemberMyUi,
  memberMyUiHealth
} from '../src/member-my-ui.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const result={
  status:'ok',
  identity:{family_id:'family-secret',customer_id:'26000123'},
  my:{
    family:{
      display_name:'YAMADA FAMILY',
      relation:'owner',
      access_role:'owner',
      linked_member_count:2,
      family_profile_edit_ready:false,
      member_identity_edit_ready:false,
      member_customer_ids:['26000123','26000456']
    },
    memory_count:6,
    current_tier:'GOLD',
    effective_black:false,
    favorite_count:2,
    family_pass:{
      family_pass:{
        memory_count:6,
        current_tier:'GOLD',
        next_tier:'BLACK',
        memories_to_next:4,
        progress_ratio:.2,
        effective_black:false
      },
      entitlement:{durable_black:false},
      benefit_contract:{
        black_photo_goods_discount_percent:10,
        applies_to_shooting_fee:false,
        enforcement_ready:false
      }
    },
    family_passport:{
      achieved_count:2,
      total_milestones:4,
      completion_ratio:.5,
      milestones:[
        {code:'OMIYAMAIRI',label:'お宮参り',achieved:true,memory_ids:['secret-memory-id']},
        {code:'FIRST_BIRTHDAY',label:'1st Birthday',achieved:true},
        {code:'SHICHIGOSAN',label:'七五三',achieved:false},
        {code:'SCHOOL_ENTRANCE',label:'入学',achieved:false}
      ]
    },
    next_memory:{
      next_memory:{
        type:'birthday',
        label:'4歳バースデー',
        target_date:'2026-10-10',
        days_until:15,
        child:{child_id:'child-secret',display_name:'はるくん'},
        candidate_only:true
      },
      canonical_child_count:1,
      consultation_cta:{channel:'line',intent:'consultation',automatic_send:false}
    },
    settings:{
      family_profile_edit_ready:false,
      member_profile_edit_ready:false,
      notification_settings_ready:false,
      line_link_settings_ready:false,
      account_deletion_ready:false
    },
    partial:false,
    unavailable_sections:[],
    capabilities:{
      family_profile_edit:false,
      member_profile_edit:false,
      favorites_mutation:false,
      notification_settings_mutation:false,
      reservation_creation:false,
      automatic_contact:false,
      line_send:false
    }
  },
  read_only:true
};

const vm=buildMemberMyViewModel(result);
pass('MY view model builds',vm.status==='ok');
pass('public identity is stripped from MY UI model',!JSON.stringify(vm).includes('family-secret')&&!JSON.stringify(vm).includes('26000123')&&!JSON.stringify(vm).includes('26000456'));
pass('child identity is stripped but display name remains',!JSON.stringify(vm).includes('child-secret')&&vm.next_memory.child_name==='はるくん');
pass('Passport evidence MEMORY IDs are stripped',!JSON.stringify(vm).includes('secret-memory-id'));
pass('Family summary remains safe presentation data',vm.family.display_name==='YAMADA FAMILY'&&vm.family.linked_member_count===2);
pass('Family Pass remains authoritative presentation source',vm.family_pass.memory_count===6&&vm.family_pass.current_tier==='GOLD');
pass('BLACK benefit contract is shown but not enforced',vm.family_pass.black_goods_discount_percent===10&&vm.family_pass.benefit_enforcement_ready===false);
pass('Favorite count is composed read-only',vm.favorite_count===2&&vm.favorites_mutation===false);
pass('NEXT MEMORY remains candidate-only/non-automatic',vm.next_memory.candidate_only===true&&vm.next_memory.automatic_send===false);
pass('settings remain disabled',Object.values(vm.settings).every(v=>v===false));

const markup=renderMemberMyMarkup(vm);
pass('MY markup includes adopted sections',[
  'FAMILY','FAMILY PASS','FAMILY PASSPORT','FAVORITES','NEXT MEMORY','SETTINGS'
].every(x=>markup.includes(x)));
pass('MY markup contains no Customer/Family/child ids',!markup.includes('family-secret')&&!markup.includes('26000123')&&!markup.includes('child-secret'));
pass('MY states BLACK benefit is not active yet',markup.includes('割引適用はまだ有効ではありません'));
pass('MY states no automatic LINE/reservation',markup.includes('LINE送信・予約作成は自動では行いません'));
pass('MY only local CTA points to MEMORIES',markup.includes('data-my-go-tab="memories"')&&!markup.includes('https://'));

const partialVm=buildMemberMyViewModel({
  status:'ok',
  my:{
    ...result.my,
    favorite_count:null,
    family_passport:null,
    next_memory:null,
    partial:true,
    unavailable_sections:[
      {section:'favorites',error:'favorites_schema_not_applied'},
      {section:'family_passport',error:'passport_unavailable'}
    ]
  }
});
pass('optional degradation remains renderable',partialVm.status==='ok'&&partialVm.partial===true&&partialVm.favorite_count===null);
pass('partial reason list is presentation-safe',partialVm.unavailable_sections.length===2);
pass('partial markup keeps core MY visible',renderMemberMyMarkup(partialVm).includes('FAMILY PASS'));

const blackVm=buildMemberMyViewModel({
  status:'ok',
  my:{
    ...result.my,
    effective_black:true,
    family_pass:{
      ...result.my.family_pass,
      family_pass:{
        ...result.my.family_pass.family_pass,
        current_tier:'BLACK',
        effective_black:true,
        memory_count:11
      },
      entitlement:{durable_black:true}
    }
  }
});
pass('authorized BLACK state is reflected',blackVm.family_pass.effective_black===true&&blackVm.family_pass.current_tier==='BLACK');

const css=memberMyCss();
pass('MY is responsive',css.includes('@media(min-width:760px)'));
pass('MY supports BLACK shell',css.includes('.mp-shell--black .mp-my'));

const mount={
  innerHTML:'',
  listener:null,
  addEventListener(type,fn){if(type==='click')this.listener=fn},
  removeEventListener(type,fn){if(type==='click'&&this.listener===fn)this.listener=null},
  contains(){return true}
};
const buttons=[
  {tab:'my',classList:{toggle(){}},getAttribute(k){return k==='data-member-tab'?this.tab:null},setAttribute(){}},
  {tab:'memories',classList:{toggle(){}},getAttribute(k){return k==='data-member-tab'?this.tab:null},setAttribute(){}}
];
const panels=[
  {tab:'my',hidden:false,getAttribute(k){return k==='data-member-panel'?this.tab:null},setAttribute(k){if(k==='hidden')this.hidden=true},removeAttribute(k){if(k==='hidden')this.hidden=false}},
  {tab:'memories',hidden:true,getAttribute(k){return k==='data-member-panel'?this.tab:null},setAttribute(k){if(k==='hidden')this.hidden=true},removeAttribute(k){if(k==='hidden')this.hidden=false}}
];
const shell={
  dataset:{},
  querySelector(selector){return selector==='[data-member-mount="my"]'?mount:null},
  querySelectorAll(selector){return selector==='[data-member-tab]'?buttons:panels}
};

pass('MY mount resolves canonical shell destination',resolveMemberMyMount(shell)===mount);
const mounted=mountMemberMyUi(shell,result);
pass('MY mounts source-only',mounted.ok===true&&mounted.source_only===true&&mount.innerHTML.includes('data-member-my-ui'));

const target={
  closest(){return this},
  getAttribute(k){return k==='data-my-go-tab'?'memories':null}
};
mount.listener({target});
pass('Favorites CTA changes only to canonical MEMORIES tab',shell.dataset.activeTab==='memories'&&panels[1].hidden===false);

mounted.destroy();
pass('destroy removes listener',mount.listener===null);

const noMount=mountMemberMyUi({querySelector(){return null}},result);
pass('MY fails closed without canonical mount',noMount.ok===false&&noMount.error==='member_my_mount_required');

const health=memberMyUiHealth();
pass('health records source-only canonical MY UI',health.source_only===true&&health.canonical_mount==='my');
pass('health strips all internal identity ids',health.customer_id_exposed===false&&health.family_id_exposed===false&&health.linked_customer_ids_exposed===false&&health.child_id_exposed===false);
pass('health keeps all mutation/settings actions disabled',health.profile_edit===false&&health.family_edit===false&&health.favorites_mutation===false&&health.notification_settings_mutation===false&&health.line_link_mutation===false&&health.account_deletion===false);
pass('health keeps reservation/contact/Production disabled',health.reservation_creation===false&&health.automatic_contact===false&&health.line_send===false&&health.production_route_wired===false&&health.production_write===false);

console.log(`MEMBER_MY_UI=${n}/${n} PASS`);
