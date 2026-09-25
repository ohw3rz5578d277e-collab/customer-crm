import {
  buildMemberMemoriesViewModel,
  renderMemberMemoriesMarkup,
  memberMemoriesCss,
  resolveMemberMemoriesMount,
  createMemberMemoriesController,
  memberMemoriesUiHealth,
  __test
} from '../src/member-memories-ui.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const listResult={
  status:'ok',
  family_id:'family-secret',
  memories:[
    {
      memory_id:'mem-2026-01',
      shoot_date:'2026-09-01',
      genre:'ファミリー',
      title:'Summer Family',
      cover:{
        media_id:'media-secret',
        media_type:'image',
        role:'cover',
        width:1800,
        height:1200,
        private_delivery_required:true,
        storage_key:'must-not-leak'
      },
      preview_count:12,
      amazon_photos_available:true,
      favorite:true,
      favorite_mutable:false
    },
    {
      memory_id:'mem-2025-02',
      shoot_date:'2025-11-15',
      genre:'七五三',
      title:'七五三',
      cover:null,
      preview_count:0,
      amazon_photos_available:false,
      favorite:false,
      favorite_mutable:false
    }
  ],
  favorites_available:true,
  favorite_mutation_ready:false
};

const detailResult={
  status:'ok',
  family_id:'family-secret',
  memory:{
    memory_id:'mem-2026-01',
    shoot_date:'2026-09-01',
    genre:'ファミリー',
    title:'Summer Family'
  },
  media:[
    {media_id:'media-1',media_type:'image',role:'cover',width:1800,height:1200,private_delivery_required:true,storage_key:'secret-r2-key'},
    {media_id:'media-2',media_type:'image',role:'preview',width:1200,height:1800,private_delivery_required:true}
  ],
  amazon_link:{provider:'amazon_photos',url:'https://www.amazon.example/private-share'},
  favorite:true,
  favorite_mutable:false,
  create_available:false,
  shop_available:false
};

const vm=buildMemberMemoriesViewModel(listResult);
pass('MEMORIES list view model builds',vm.status==='ok'&&vm.memory_count===2);
pass('Family identity is removed from UI model',!JSON.stringify(vm).includes('family-secret'));
pass('private storage key is removed from list UI model',!JSON.stringify(vm).includes('must-not-leak'));
pass('cover retains only non-secret presentation metadata',vm.memories[0].cover.private_delivery_required===true&&vm.memories[0].cover.width===1800);
pass('favorite state is visible but immutable',vm.memories[0].favorite===true&&vm.memories[0].favorite_mutable===false&&vm.favorite_mutation_ready===false);

const detailVm=buildMemberMemoriesViewModel(listResult,{
  selected_memory_id:'mem-2026-01',
  detail_result:detailResult
});
pass('detail normalizes matching visible MEMORY',detailVm.detail?.memory_id==='mem-2026-01'&&detailVm.detail.media.length===2);
pass('detail strips media ids/storage internals',!JSON.stringify(detailVm.detail).includes('media-1')&&!JSON.stringify(detailVm.detail).includes('secret-r2-key'));
pass('Amazon availability may be shown without URL',detailVm.detail.amazon_photos_available===true&&!JSON.stringify(detailVm.detail).includes('amazon.example'));

const mismatch=buildMemberMemoriesViewModel(listResult,{
  selected_memory_id:'mem-2026-01',
  detail_result:{...detailResult,memory:{...detailResult.memory,memory_id:'mem-other'}}
});
pass('mismatched detail is not composed',mismatch.detail===null);

const markup=renderMemberMemoriesMarkup(detailVm);
pass('markup is photo-led timeline/detail UI',markup.includes('MEMORIES')&&markup.includes('家族の時間を')&&markup.includes('data-memory-detail'));
pass('favorite renders as state indicator',markup.includes('お気に入り'));
pass('private image URL is not invented',!markup.includes('storage_key')&&!markup.includes('blob:')&&!markup.includes('https://'));
pass('detail explains private media activation boundary',markup.includes('Private Media配信のProduction有効化後'));

const css=memberMemoriesCss();
pass('MEMORIES is responsive',css.includes('@media(min-width:760px)')&&css.includes('grid-template-columns:repeat(3'));
pass('MEMORIES supports BLACK shell',css.includes('.mp-shell--black .mp-memories'));

pass('safe MEMORY id accepts canonical opaque id',__test.safeMemoryId('mem-2026-01')==='mem-2026-01');
pass('safe MEMORY id rejects path injection',__test.safeMemoryId('../mem')==='');

let loadCalls=0;
const mount={
  innerHTML:'',
  listener:null,
  addEventListener(type,fn){if(type==='click')this.listener=fn},
  removeEventListener(type,fn){if(type==='click'&&this.listener===fn)this.listener=null},
  contains(){return true}
};
const shell={
  querySelector(selector){return selector==='[data-member-mount="memories"]'?mount:null}
};

pass('MEMORIES mount resolves canonical shell destination',resolveMemberMemoriesMount(shell)===mount);
const controller=createMemberMemoriesController({
  shell_root:shell,
  list_result:listResult,
  load_detail:async id=>{loadCalls++;return id==='mem-2026-01'?detailResult:{status:'memory_not_found'}}
});
pass('controller mounts timeline without fetching detail',controller.ok===true&&loadCalls===0&&mount.innerHTML.includes('data-member-memories-ui'));
const opened=await controller.open_memory('mem-2026-01');
pass('detail load occurs only after explicit open',opened.ok===true&&loadCalls===1);
pass('loaded detail is rendered',controller.get_view_model().detail?.title==='Summer Family');
const hidden=await controller.open_memory('mem-hidden');
pass('non-visible MEMORY cannot be loaded',hidden.ok===false&&hidden.error==='memory_not_visible'&&loadCalls===1);
controller.close_memory();
pass('close returns to timeline state',controller.get_view_model().selected_memory===null);

const destroyed=controller.destroy();
pass('destroy removes click listener',destroyed.ok===true&&mount.listener===null);
const afterDestroy=await controller.open_memory('mem-2026-01');
pass('destroyed controller refuses actions',afterDestroy.ok===false&&afterDestroy.error==='controller_destroyed');

const mismatchMount={
  innerHTML:'',
  listener:null,
  addEventListener(type,fn){if(type==='click')this.listener=fn},
  removeEventListener(type,fn){if(type==='click'&&this.listener===fn)this.listener=null},
  contains(){return true}
};
const mismatchShell={
  querySelector(selector){return selector==='[data-member-mount="memories"]'?mismatchMount:null}
};
const mismatchController=createMemberMemoriesController({
  shell_root:mismatchShell,
  list_result:listResult,
  load_detail:async()=>({...detailResult,memory:{...detailResult.memory,memory_id:'mem-other'}})
});
const mismatchOpen=await mismatchController.open_memory('mem-2026-01');
pass('controller fails closed on detail MEMORY mismatch',mismatchOpen.ok===false&&mismatchOpen.error==='memory_detail_mismatch');
mismatchController.destroy();

const noMount=createMemberMemoriesController({shell_root:{querySelector(){return null}},list_result:listResult});
pass('controller fails closed without canonical mount',noMount.ok===false&&noMount.error==='member_memories_mount_required');

const health=memberMemoriesUiHealth();
pass('health records source-only MEMORIES shell UI',health.source_only===true&&health.canonical_mount==='memories');
pass('health keeps private media URLs and storage keys hidden',health.private_memory_binary_rendering===false&&health.private_storage_key_exposed===false&&health.signed_private_url_exposed===false);
pass('health keeps Favorite mutation inactive',health.favorite_state_visible===true&&health.favorite_mutation_active===false&&health.favorite_optimistic_state===false);
pass('health keeps Production/contact writes disabled',health.production_route_wired===false&&health.production_write===false&&health.line_send===false&&health.automatic_contact===false);

console.log(`MEMBER_MEMORIES_UI=${n}/${n} PASS`);
