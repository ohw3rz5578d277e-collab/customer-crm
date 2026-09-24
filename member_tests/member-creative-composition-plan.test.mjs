import {
  planMemberCreativeComposition,
  handleMemberCreativePlanRequest,
  memberCreativeCompositionPlanHealth,
  __test
} from '../src/member-creative-composition-plan.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

function template(overrides={}){
  return {
    template_id:'tpl_single',
    creative_type:'wallpaper',
    title:'Wallpaper',
    description:'',
    season_tag:'evergreen',
    schedule:{starts_on:null,ends_on:null},
    composition:{
      mode:'single_photo',
      photo_slots:1,
      canvas_width:1290,
      canvas_height:2796,
      output_mime:'image/png',
      execution_ready:false,
      browser_side_preferred:true
    },
    asset_ref:{
      asset_id:'asset:creative:single',
      preview_asset_id:'asset:creative:single:preview',
      storage_key_exposed:false,
      arbitrary_url_exposed:false
    },
    eligibility:{
      eligible:true,
      minimum_memory_count:1,
      visible_memory_count:2,
      memories_needed:0
    },
    sort_order:1,
    ...overrides
  };
}

function media(overrides={}){
  return {
    media_id:'media_1',
    memory_id:'mem_1',
    media_type:'image',
    role:'cover',
    width:1200,
    height:800,
    ...overrides
  };
}

pass('valid selection accepts template plus media IDs',__test.normalizeSelection({
  template_id:'tpl_single',
  media_ids:['media_1']
}).ok===true);
pass('duplicate media IDs are rejected',__test.normalizeSelection({
  template_id:'tpl_single',
  media_ids:['media_1','media_1']
}).error==='duplicate_media_id');
pass('missing media array is rejected',__test.normalizeSelection({
  template_id:'tpl_single'
}).error==='invalid_media_selection');
pass('empty media selection is rejected',__test.normalizeSelection({
  template_id:'tpl_single',
  media_ids:[]
}).error==='invalid_media_selection');
pass('oversized media selection is rejected',__test.normalizeSelection({
  template_id:'tpl_single',
  media_ids:Array.from({length:13},(_,i)=>`media_${i}`)
}).error==='invalid_media_selection');
pass('control characters in opaque IDs are rejected',__test.validOpaqueId('media\n1',160)===false);

const safeDescriptor=__test.safeMediaDescriptor({
  media:{
    ...media(),
    storage_key:'member/fam_A/mem_1/private.jpg'
  }
});
pass('safe descriptor strips private storage key',!('storage_key' in safeDescriptor));
pass('safe descriptor keeps only composition metadata',safeDescriptor.media_id==='media_1'&&safeDescriptor.memory_id==='mem_1'&&safeDescriptor.media_type==='image');

const single=__test.buildCompositionPlan(template(),[media()]);
pass('single-photo composition plan succeeds',single.status==='ok'&&single.plan.template.composition.photo_slots===1);
pass('composition remains non-executable',single.plan.execution.ready===false&&single.plan.execution.browser_composition_implemented===false);
pass('plan explicitly hides private storage keys',single.plan.privacy.private_storage_key_exposed===false);
pass('plan contains no raw photo binary',single.plan.privacy.raw_photo_binary_in_plan===false);

const mismatch=__test.buildCompositionPlan(template(),[]);
pass('photo slot mismatch fails before execution',mismatch.status==='creative_media_count_mismatch'&&mismatch.required_photo_slots===1);

const video=__test.buildCompositionPlan(template(),[media({media_type:'video'})]);
pass('video input is not accepted by initial Creative composition',video.status==='unsupported_creative_media_type');

const pairTemplate=template({
  template_id:'tpl_pair',
  creative_type:'then_and_now',
  composition:{
    mode:'pair_photo',
    photo_slots:2,
    canvas_width:1600,
    canvas_height:1200,
    output_mime:'image/jpeg',
    execution_ready:false,
    browser_side_preferred:true
  },
  asset_ref:{
    asset_id:'asset:creative:pair',
    preview_asset_id:null,
    storage_key_exposed:false,
    arbitrary_url_exposed:false
  },
  eligibility:{
    eligible:true,
    minimum_memory_count:2,
    visible_memory_count:2,
    memories_needed:0
  }
});
const sameMemoryPair=__test.buildCompositionPlan(pairTemplate,[
  media({media_id:'media_1',memory_id:'mem_1'}),
  media({media_id:'media_2',memory_id:'mem_1'})
]);
pass('pair-photo requires two distinct MEMORY sources',sameMemoryPair.status==='pair_photo_requires_distinct_memories');

const distinctPair=__test.buildCompositionPlan(pairTemplate,[
  media({media_id:'media_1',memory_id:'mem_1'}),
  media({media_id:'media_2',memory_id:'mem_2'})
]);
pass('pair-photo accepts distinct MEMORY sources',distinctPair.status==='ok'&&distinctPair.plan.selected_memory_count===2);

const locked=__test.buildCompositionPlan(template({
  eligibility:{
    eligible:false,
    minimum_memory_count:3,
    visible_memory_count:2,
    memories_needed:1
  }
}),[media()]);
pass('ineligible template cannot produce composition plan',locked.status==='creative_template_not_eligible');

pass('missing/invalid media is mapped to non-enumerating availability error',__test.mapMediaAuthorizationFailure({status:'media_not_found'}).status==='creative_media_not_available');
pass('invalid private storage key becomes review-required',__test.mapMediaAuthorizationFailure({status:'invalid_private_storage_key'}).review_required===true);

function makeDb(){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    'member_memories',
    'member_memory_media',
    'member_creative_templates',
    'member_public_assets'
  ]);
  const writes=[];
  const seenSql=[];

  const memories=[
    {
      memory_id:'mem_2',
      family_id:familyId,
      shoot_date:'2026-08-01',
      genre:'七五三',
      title:'七五三',
      amazon_photos_url:'',
      published:1,
      created_at:'2026-08-01',
      updated_at:'2026-08-01',
      deleted_at:''
    },
    {
      memory_id:'mem_1',
      family_id:familyId,
      shoot_date:'2025-08-01',
      genre:'1歳バースデー',
      title:'1st',
      amazon_photos_url:'',
      published:1,
      created_at:'2025-08-01',
      updated_at:'2025-08-01',
      deleted_at:''
    }
  ];

  const mediaRows=[
    {
      media_id:'media_1',
      memory_id:'mem_1',
      family_id:familyId,
      storage_key:'member/fam_A/mem_1/cover.jpg',
      media_type:'image',
      role:'cover',
      width:1200,
      height:800,
      deleted_at:''
    },
    {
      media_id:'media_2',
      memory_id:'mem_2',
      family_id:familyId,
      storage_key:'member/fam_A/mem_2/cover.jpg',
      media_type:'image',
      role:'cover',
      width:1600,
      height:1000,
      deleted_at:''
    },
    {
      media_id:'media_same_2',
      memory_id:'mem_1',
      family_id:familyId,
      storage_key:'member/fam_A/mem_1/preview.jpg',
      media_type:'image',
      role:'preview',
      width:1200,
      height:800,
      deleted_at:''
    },
    {
      media_id:'media_video',
      memory_id:'mem_2',
      family_id:familyId,
      storage_key:'member/fam_A/mem_2/clip.mp4',
      media_type:'video',
      role:'preview',
      width:1920,
      height:1080,
      deleted_at:''
    },
    {
      media_id:'media_bad_key',
      memory_id:'mem_1',
      family_id:familyId,
      storage_key:'https://public.example/file.jpg',
      media_type:'image',
      role:'preview',
      width:1200,
      height:800,
      deleted_at:''
    },
    {
      media_id:'media_other_family',
      memory_id:'mem_B',
      family_id:'fam_B',
      storage_key:'member/fam_B/mem_B/cover.jpg',
      media_type:'image',
      role:'cover',
      width:1200,
      height:800,
      deleted_at:''
    }
  ];

  const templates=[
    {
      template_id:'tpl_single',
      creative_type:'wallpaper',
      title:'Wallpaper',
      description:'',
      season_tag:'evergreen',
      starts_on:null,
      ends_on:null,
      photo_slots:1,
      composition_mode:'single_photo',
      canvas_width:1290,
      canvas_height:2796,
      output_mime:'image/png',
      asset_id:'asset:creative:single',
      preview_asset_id:'asset:creative:single:preview',
      minimum_memory_count:1,
      sort_order:1
    },
    {
      template_id:'tpl_pair',
      creative_type:'then_and_now',
      title:'Then & Now',
      description:'',
      season_tag:'evergreen',
      starts_on:null,
      ends_on:null,
      photo_slots:2,
      composition_mode:'pair_photo',
      canvas_width:1600,
      canvas_height:1200,
      output_mime:'image/jpeg',
      asset_id:'asset:creative:pair',
      preview_asset_id:null,
      minimum_memory_count:2,
      sort_order:2
    },
    {
      template_id:'tpl_locked',
      creative_type:'collage',
      title:'Locked',
      description:'',
      season_tag:'evergreen',
      starts_on:null,
      ends_on:null,
      photo_slots:2,
      composition_mode:'multi_photo',
      canvas_width:1600,
      canvas_height:1200,
      output_mime:'image/jpeg',
      asset_id:'asset:creative:locked',
      preview_asset_id:null,
      minimum_memory_count:3,
      sort_order:3
    }
  ];

  return {
    writes,
    seenSql,
    prepare(sql){
      seenSql.push(sql);
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')){
            return tables.has(state.params[0])?{name:state.params[0]}:null;
          }
          if(sql.includes('FROM customer_family_groups')){
            return state.params[0]===familyId
              ?{family_id:familyId,display_name:'TEST FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          if(sql.includes('FROM member_memory_media mm')){
            const [mediaId,requestedFamily]=state.params;
            const row=mediaRows.find(x=>
              x.media_id===mediaId
              && x.family_id===requestedFamily
              && !x.deleted_at
            );
            if(!row)return null;
            const parent=memories.find(x=>
              x.memory_id===row.memory_id
              && x.family_id===row.family_id
              && x.published===1
              && !x.deleted_at
            );
            return parent?{...row}:null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            return state.params[0]===customerId
              ?{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return state.params[0]===familyId
              ?{results:[{customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          if(sql.includes('FROM member_memory_media')&&!sql.includes(' mm')){
            return {results:[]};
          }
          if(sql.includes('FROM member_memories')){
            return state.params[0]===familyId?{results:memories}:{results:[]};
          }
          if(sql.includes('FROM member_creative_templates')){
            return {results:templates};
          }
          if(sql.includes('FROM member_public_assets')){
            const assets=[
              {asset_id:'asset:creative:single',asset_kind:'image',local_path:'/member-assets/creative/single.png',mime_type:'image/png',width:1290,height:2796},
              {asset_id:'asset:creative:single:preview',asset_kind:'image',local_path:'/member-assets/creative/single-preview.png',mime_type:'image/png',width:645,height:1398},
              {asset_id:'asset:creative:pair',asset_kind:'image',local_path:'/member-assets/creative/pair.jpg',mime_type:'image/jpeg',width:1600,height:1200},
              {asset_id:'asset:creative:locked',asset_kind:'image',local_path:'/member-assets/creative/locked.jpg',mime_type:'image/jpeg',width:1600,height:1200}
            ];
            return {results:assets.filter(asset=>state.params.includes(asset.asset_id))};
          }
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('Creative composition plan must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const db=makeDb();

const integrated=await planMemberCreativeComposition(
  {DB:db},
  {family_id:familyId,customer_id:customerId},
  {template_id:'tpl_single',media_ids:['media_1']},
  {as_of:'2026-09-24'}
);
pass('authorized Member can build Creative composition plan',integrated.status==='ok'&&integrated.family_id===familyId&&integrated.customer_id===customerId);
pass('integrated plan preserves exact selected media metadata',integrated.plan.selected_media[0].media_id==='media_1'&&integrated.plan.selected_media[0].memory_id==='mem_1');
pass('integrated selected media carries source-only private delivery contract',integrated.plan.selected_media[0].delivery?.source_contract_ready===true&&integrated.plan.selected_media[0].delivery?.delivery_ready===false);
pass('integrated plan resolves trusted local template asset',integrated.plan.template.asset_ref.public_asset?.public_path==='/member-assets/creative/single.png');
pass('integrated plan attaches browser execution contract',integrated.browser_execution_status==='ok'&&integrated.plan.browser_execution?.execution_environment==='browser');
pass('browser execution contract uses fixed single-photo recipe',integrated.plan.browser_execution?.layout?.recipe==='single_full_bleed_v1'&&integrated.plan.browser_execution?.photo_layers?.length===1);
pass('browser execution contract remains runtime-blocked',integrated.plan.browser_execution?.readiness?.runtime_ready===false&&integrated.plan.browser_execution?.readiness?.blockers?.includes('private_media_delivery_not_active')&&integrated.plan.browser_execution?.readiness?.blockers?.includes('browser_renderer_not_implemented'));
pass('local download contract exists but is not active',integrated.plan.browser_execution?.local_download?.mechanism==='browser_blob_object_url'&&integrated.plan.browser_execution?.local_download?.ready===false);
pass('integrated plan never exposes private storage key',!JSON.stringify(integrated).includes('member/fam_A/'));
pass('integrated plan remains generation-disabled',integrated.generation_executed===false&&integrated.generated_output_write===false);
pass('integrated plan performs zero writes',db.writes.length===0);

const wrongCount=await planMemberCreativeComposition(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId},
  {template_id:'tpl_pair',media_ids:['media_1']},
  {as_of:'2026-09-24'}
);
pass('integrated plan rejects wrong slot count',wrongCount.status==='creative_media_count_mismatch'&&wrongCount.required_photo_slots===2);

const lockedPlan=await planMemberCreativeComposition(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId},
  {template_id:'tpl_locked',media_ids:['media_1','media_2']},
  {as_of:'2026-09-24'}
);
pass('locked template cannot be planned even with valid media',lockedPlan.status==='creative_template_not_eligible'&&lockedPlan.eligibility.memories_needed===1);

const unavailableTemplate=await planMemberCreativeComposition(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId},
  {template_id:'tpl_missing',media_ids:['media_1']},
  {as_of:'2026-09-24'}
);
pass('unknown template fails without fallback',unavailableTemplate.status==='creative_template_not_available');

const otherFamilyMedia=await planMemberCreativeComposition(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId},
  {template_id:'tpl_single',media_ids:['media_other_family']},
  {as_of:'2026-09-24'}
);
pass('other-Family media is hidden as unavailable',otherFamilyMedia.status==='creative_media_not_available');

const videoPlan=await planMemberCreativeComposition(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId},
  {template_id:'tpl_single',media_ids:['media_video']},
  {as_of:'2026-09-24'}
);
pass('integrated video input is rejected',videoPlan.status==='unsupported_creative_media_type');

const badStorage=await planMemberCreativeComposition(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId},
  {template_id:'tpl_single',media_ids:['media_bad_key']},
  {as_of:'2026-09-24'}
);
pass('invalid private storage descriptor fails for review',badStorage.status==='creative_media_review_required'&&badStorage.review_required===true);

const sameMemoryIntegrated=await planMemberCreativeComposition(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId},
  {template_id:'tpl_pair',media_ids:['media_1','media_same_2']},
  {as_of:'2026-09-24'}
);
pass('integrated Then & Now requires distinct MEMORY IDs',sameMemoryIntegrated.status==='pair_photo_requires_distinct_memories');

const distinctMemoryIntegrated=await planMemberCreativeComposition(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId},
  {template_id:'tpl_pair',media_ids:['media_1','media_2']},
  {as_of:'2026-09-24'}
);
pass('integrated Then & Now accepts two authorized distinct MEMORIES',distinctMemoryIntegrated.status==='ok'&&distinctMemoryIntegrated.plan.selected_memory_count===2);

const crossFamily=await planMemberCreativeComposition(
  {DB:makeDb()},
  {family_id:'fam_B',customer_id:customerId},
  {template_id:'tpl_single',media_ids:['media_1']},
  {as_of:'2026-09-24'}
);
pass('cross-Family Member session fails closed',crossFamily.status==='family_access_denied');

const duplicateIntegrated=await planMemberCreativeComposition(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId},
  {template_id:'tpl_pair',media_ids:['media_1','media_1']},
  {as_of:'2026-09-24'}
);
pass('duplicate selected media fails before authorization',duplicateIntegrated.status==='duplicate_media_id');

const noSession=await handleMemberCreativePlanRequest(
  new Request('https://example.test/api/internal/member/creative/plan',{
    method:'POST',
    body:JSON.stringify({template_id:'tpl_single',media_ids:['media_1']})
  }),
  {DB:makeDb()},
  null
);
pass('HTTP Creative plan requires server Member session',noSession.status===401);

const get=await handleMemberCreativePlanRequest(
  new Request('https://example.test/api/internal/member/creative/plan'),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
pass('HTTP Creative plan is POST only',get.status===405);

const identityInjection=await handleMemberCreativePlanRequest(
  new Request('https://example.test/api/internal/member/creative/plan',{
    method:'POST',
    body:JSON.stringify({
      template_id:'tpl_single',
      media_ids:['media_1'],
      family_id:'fam_B'
    })
  }),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
const identityInjectionBody=await identityInjection.json();
pass('HTTP body cannot inject Family identity',identityInjection.status===400&&identityInjectionBody.error==='unsupported_request_field');

const clientClock=await handleMemberCreativePlanRequest(
  new Request('https://example.test/api/internal/member/creative/plan',{
    method:'POST',
    body:JSON.stringify({
      template_id:'tpl_single',
      media_ids:['media_1'],
      as_of:'1999-01-01'
    })
  }),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
const clientClockBody=await clientClock.json();
pass('HTTP body cannot control catalog clock',clientClock.status===400&&clientClockBody.error==='unsupported_request_field');

const successHttp=await handleMemberCreativePlanRequest(
  new Request('https://example.test/api/internal/member/creative/plan',{
    method:'POST',
    body:JSON.stringify({template_id:'tpl_single',media_ids:['media_1']})
  }),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
const successHttpBody=await successHttp.json();
pass('HTTP plan returns safe non-executable plan',successHttp.status===200&&successHttpBody.ok===true&&successHttpBody.plan.execution.ready===false);
pass('HTTP plan response contains no private storage key',!JSON.stringify(successHttpBody).includes('member/fam_A/'));

const health=memberCreativeCompositionPlanHealth();
pass('health records exact input contract',health.request_template_id_input===true&&health.request_media_ids_input===true&&health.request_customer_id_input===false&&health.request_family_id_input===false);
pass('health requires exact slots and rejects duplicates',health.exact_photo_slot_count_required===true&&health.duplicate_media_rejected===true);
pass('health records per-media reauthorization and cross-Family hiding',health.private_media_reauthorized_individually===true&&health.cross_family_media_hidden===true);
pass('health records image-only input and distinct pair MEMORY rule',health.input_media_type==='image_only'&&health.pair_photo_distinct_memory_required===true);
pass('health records zero storage/URL/raw-binary exposure',health.private_storage_key_exposed===false&&health.template_storage_key_exposed===false&&health.arbitrary_external_url_exposed===false&&health.raw_photo_binary_in_plan===false);
pass('health records browser contract ready but renderer/generation inactive',health.generation_executed===false&&health.browser_composition_implemented===false&&health.browser_execution_contract_ready===true&&health.local_download_contract_ready===true&&health.memory_movie_execution_supported===false);
pass('health records no photo/output write or Production route',health.customer_photo_write===false&&health.generated_output_write===false&&health.production_route_wired===false&&health.production_write===false);

console.log(`MEMBER_CREATIVE_COMPOSITION_PLAN=${n}/${n} PASS`);
