import {
  buildMemberCreativeCreateViewModel,
  buildMemberCreativePlanInput,
  toggleMemberCreativeMediaSelection,
  runMemberCreativeCreatePreview,
  saveMemberCreativeOutputFromUserGesture,
  renderMemberCreativeCreateMarkup,
  memberCreativeCreateCss,
  memberCreativeCreateUiHealth,
  __test
} from '../src/member-creative-create-ui.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const catalog={
  status:'ok',
  templates:[
    {
      template_id:'tpl_pair',
      creative_type:'then_and_now',
      title:'Then & Now',
      description:'ふたつの時間を、ひとつの記憶に。',
      composition:{mode:'pair_photo',photo_slots:2,output_mime:'image/png'},
      eligibility:{eligible:true,memories_needed:0},
      asset_ref:{
        public_asset:{public_path:'/member-assets/creative/pair.png'},
        preview_public_asset:{public_path:'/member-assets/creative/pair-preview.webp'}
      }
    },
    {
      template_id:'tpl_locked',
      creative_type:'collage',
      title:'Family Collage',
      description:'家族の時間を一枚に。',
      composition:{mode:'multi_photo',photo_slots:4,output_mime:'image/jpeg'},
      eligibility:{eligible:false,memories_needed:2},
      asset_ref:{public_asset:{public_path:'/member-assets/creative/grid.png'}}
    },
    {
      template_id:'tpl_movie',
      creative_type:'memory_movie',
      title:'Movie',
      description:'not yet',
      composition:{mode:'sequence',photo_slots:3,output_mime:'video/mp4'},
      eligibility:{eligible:true,memories_needed:0},
      asset_ref:{public_asset:{public_path:'/member-assets/creative/movie.mp4'}}
    }
  ]
};

const details=[
  {
    memory:{memory_id:'mem_1',title:'七五三',shoot_date:'2025-11-03'},
    media:[
      {media_id:'media_a',media_type:'image',role:'cover',width:2000,height:3000},
      {media_id:'media_video',media_type:'video',role:'preview'}
    ]
  },
  {
    memory:{memory_id:'mem_2',title:'Family',shoot_date:'2026-04-12'},
    media:[
      {media_id:'media_b',media_type:'image',role:'cover',width:3000,height:2000}
    ]
  }
];

const ready=buildMemberCreativeCreateViewModel({
  catalog,
  memory_details:details,
  selected_template_id:'tpl_pair',
  selected_media_ids:['media_a','media_b']
});

pass('CREATE view model builds',ready.status==='ok');
pass('non-image template output is hidden from initial image CREATE UI',ready.templates.length===2&&!ready.templates.some(x=>x.template_id==='tpl_movie'));
pass('only image media are selectable',ready.media.length===2&&!ready.media.some(x=>x.media_id==='media_video'));
pass('pair template becomes plan-ready with two distinct MEMORY photos',ready.plan_ready===true&&ready.selection_status==='ready_to_plan');
pass('view model exposes no Customer/Family identity',!JSON.stringify(ready).includes('customer_id')&&!JSON.stringify(ready).includes('family_id'));
pass('view model exposes no storage key',!JSON.stringify(ready).includes('storage_key'));

const input=buildMemberCreativePlanInput(ready);
pass('plan input is exact template_id + media_ids only',JSON.stringify(Object.keys(input).sort())===JSON.stringify(['media_ids','template_id'])&&input.template_id==='tpl_pair'&&input.media_ids.join(',')==='media_a,media_b');

const sameMemory=buildMemberCreativeCreateViewModel({
  catalog,
  memory_details:details,
  selected_template_id:'tpl_pair',
  selected_media_ids:['media_a']
});
const forgedSameMemory={
  ...sameMemory,
  selected_media:[
    sameMemory.media[0],
    {...sameMemory.media[0],media_id:'media_c'}
  ]
};
const sameMemoryStatus=__test.selectionStatus(forgedSameMemory.selected_template,forgedSameMemory.selected_media);
pass('pair-photo requires distinct MEMORY IDs',sameMemoryStatus.status==='pair_photo_requires_distinct_memories'&&sameMemoryStatus.plan_ready===false);

const oneSelected=buildMemberCreativeCreateViewModel({
  catalog,
  memory_details:details,
  selected_template_id:'tpl_pair',
  selected_media_ids:['media_a']
});
pass('incomplete media selection does not plan',oneSelected.plan_ready===false&&oneSelected.required_photo_slots===2);
pass('selection toggler adds a permitted media item',toggleMemberCreativeMediaSelection(oneSelected,'media_b').join(',')==='media_a,media_b');

const locked=buildMemberCreativeCreateViewModel({
  catalog,
  memory_details:details,
  selected_template_id:'tpl_locked',
  selected_media_ids:[]
});
pass('locked template never becomes plan-ready',locked.plan_ready===false&&locked.selection_status==='template_locked');

let rendererCalls=0;
const blocked=await runMemberCreativeCreatePreview({
  view_model:ready,
  plan_composition:async()=>({
    status:'ok',
    plan:{browser_execution:{readiness:{runtime_ready:false,blockers:['private_media_delivery_not_active']}}}
  }),
  render_composition:async()=>{rendererCalls++;return {status:'ok',rendered:true}}
});
pass('runtime blocker prevents renderer execution',blocked.status==='creative_runtime_not_ready'&&rendererCalls===0&&blocked.blockers.includes('private_media_delivery_not_active'));

const rendered=await runMemberCreativeCreatePreview({
  view_model:ready,
  plan_composition:async input=>({
    status:'ok',
    plan:{
      browser_execution:{
        readiness:{runtime_ready:true,blockers:[]},
        echo:input
      }
    }
  }),
  render_composition:async contract=>{
    rendererCalls++;
    return {
      status:'ok',
      rendered:true,
      output:{
        object_url:'blob:https://member.example/output',
        filename:'Then and Now.png',
        mime_type:'image/png',
        width:1290,
        height:2796
      },
      contract
    };
  },
  runtime:{}
});
pass('ready runtime invokes renderer only after explicit preview action',rendered.status==='ok'&&rendered.rendered===true&&rendererCalls===1);

let clicks=0;
const fakeDocument={
  body:{appendChild(){}},
  createElement(tag){
    assert(tag==='a','download creates anchor');
    return {
      href:'',
      download:'',
      rel:'',
      style:{},
      click(){clicks++;},
      remove(){}
    };
  }
};
const saved=saveMemberCreativeOutputFromUserGesture(rendered.output,{document_ref:fakeDocument});
pass('explicit save action performs one local-only download click',saved.ok===true&&saved.local_only===true&&saved.server_write===false&&clicks===1);
pass('non-blob download URL rejected',saveMemberCreativeOutputFromUserGesture({...rendered.output,object_url:'https://example.com/file.png'},{document_ref:fakeDocument}).ok===false);

const markup=renderMemberCreativeCreateMarkup(ready);
pass('markup contains CREATE controls without raw private media URL',markup.includes('data-creative-template="tpl_pair"')&&markup.includes('data-creative-media="media_a"')&&!markup.includes('storage_key'));
pass('markup is escaped',__test.escapeHtml('<img onerror=alert(1)>')==='&lt;img onerror=alert(1)&gt;');
pass('only local member asset paths are accepted',__test.safeMemberAssetPath('/member-assets/creative/a.webp')==='/member-assets/creative/a.webp'&&__test.safeMemberAssetPath('https://evil.example/a.webp')==='');
pass('Luxury Minimal CREATE stylesheet is source-only and responsive',memberCreativeCreateCss().includes('--paper:#f7f4ec')&&memberCreativeCreateCss().includes('@media(min-width:760px)'));

const health=memberCreativeCreateUiHealth();
pass('health records source-only CREATE UI',health.ui_source_implemented===true&&health.source_only===true&&health.design_direction==='luxury_minimal_family_story');
pass('health keeps Production and auto actions off',health.production_route_wired===false&&health.production_write===false&&health.auto_execute===false&&health.auto_download===false&&health.generated_output_persistence===false);
pass('health keeps identity and private storage hidden',health.client_identity_input===false&&health.customer_id_exposed===false&&health.family_id_exposed===false&&health.private_storage_key_exposed===false);

console.log(`MEMBER_CREATIVE_CREATE_UI=${n}/${n} PASS`);
