import {
  buildMemberCreativeBrowserExecutionContract,
  memberCreativeBrowserExecutionHealth,
  __test
} from '../src/member-creative-browser-execution.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const delivery={
  source_contract_ready:true,
  delivery_ready:false,
  grant:{
    method:'POST',
    path:'/api/internal/member/media/grant'
  },
  content:{
    method:'POST',
    path:'/api/internal/member/media/content'
  }
};

function media(id='media_1'){
  return {
    media_id:id,
    memory_id:'mem_1',
    media_type:'image',
    role:'cover',
    width:1200,
    height:800,
    delivery
  };
}

function plan(overrides={}){
  return {
    template:{
      template_id:'tpl_single',
      creative_type:'wallpaper',
      title:'Family Wallpaper',
      composition:{
        mode:'single_photo',
        photo_slots:1,
        canvas_width:1290,
        canvas_height:2796,
        output_mime:'image/png',
        browser_side_preferred:true
      },
      asset_ref:{
        asset_id:'asset:creative:single',
        preview_asset_id:null,
        public_asset:{
          asset_id:'asset:creative:single',
          asset_kind:'image',
          public_path:'/member-assets/creative/single.png',
          mime_type:'image/png',
          width:1290,
          height:2796
        }
      }
    },
    selected_media:[media()],
    selected_memory_count:1,
    ...overrides
  };
}

pass('trusted Member public asset path accepted',__test.validLocalAssetPath('/member-assets/creative/single.png')===true);
pass('external URL rejected as template asset path',__test.validLocalAssetPath('https://evil.example/x.png')===false);
pass('protocol-relative path rejected',__test.validLocalAssetPath('//evil.example/x.png')===false);
pass('traversal rejected',__test.validLocalAssetPath('/member-assets/../secret.png')===false);
pass('encoded path material rejected',__test.validLocalAssetPath('/member-assets/%2e%2e/secret.png')===false);
pass('backslash path rejected',__test.validLocalAssetPath('/member-assets\\secret.png')===false);
pass('query string rejected',__test.validLocalAssetPath('/member-assets/a.png?x=1')===false);
pass('fragment rejected',__test.validLocalAssetPath('/member-assets/a.png#x')===false);

const safeAsset=__test.safePublicAsset(plan().template.asset_ref.public_asset);
pass('safe public image asset normalizes',safeAsset?.public_path==='/member-assets/creative/single.png'&&safeAsset.same_origin_required===true);
pass('unsafe public asset does not normalize',__test.safePublicAsset({...plan().template.asset_ref.public_asset,public_path:'https://evil.example/a.png'})===null);
pass('video template asset is rejected',__test.safePublicAsset({...plan().template.asset_ref.public_asset,asset_kind:'video',mime_type:'video/mp4'})===null);

const split=__test.splitAxis(100,3,1);
pass('splitAxis produces deterministic bounded segment',split.start===33&&split.size===33);

const verticalPair=__test.fixedLayoutRecipe('pair_photo',2,1000,1600);
pass('portrait pair stacks vertically',verticalPair.recipe==='pair_split_v1'&&verticalPair.slots[0].width===1000&&verticalPair.slots[0].height===800&&verticalPair.slots[1].y===800);

const horizontalPair=__test.fixedLayoutRecipe('pair_photo',2,1600,1000);
pass('landscape pair splits side-by-side',horizontalPair.slots[0].width===800&&horizontalPair.slots[1].x===800);

const grid=__test.fixedLayoutRecipe('multi_photo',4,1600,1200);
pass('multi-photo uses fixed balanced grid',grid.recipe==='balanced_grid_v1'&&grid.slots.length===4&&grid.slots.every(slot=>slot.fit==='cover'&&slot.focal_point==='center'));

const sequence=__test.fixedLayoutRecipe('sequence',3,1200,1600);
pass('sequence uses deterministic grid recipe',sequence.recipe==='sequence_grid_v1'&&sequence.slots.length===3);

const single=buildMemberCreativeBrowserExecutionContract(plan());
pass('single-photo browser contract succeeds',single.status==='ok'&&single.read_only===true);
pass('contract is browser-side and never server-rendered',single.contract.execution_environment==='browser'&&single.contract.server_rendering===false);
pass('canvas dimensions and output MIME are exact',single.contract.canvas.width===1290&&single.contract.canvas.height===2796&&single.contract.canvas.output_mime==='image/png');
pass('template layer uses trusted local public asset only',single.contract.template_layer.asset.public_path==='/member-assets/creative/single.png'&&single.contract.privacy.same_origin_assets_only===true);
pass('photo layer uses private grant/content contract only',single.contract.photo_layers[0].media.grant.path==='/api/internal/member/media/grant'&&single.contract.photo_layers[0].media.content.path==='/api/internal/member/media/content');
pass('photo layer does not contain grant value or storage key',!('grant_value' in single.contract.photo_layers[0].media)&&!('storage_key' in single.contract.photo_layers[0].media));
pass('browser renderer remains explicitly unimplemented',single.contract.readiness.browser_renderer_implemented===false&&single.contract.readiness.runtime_ready===false);
pass('private media inactive blocks runtime',single.contract.readiness.blockers.includes('private_media_delivery_not_active'));
pass('renderer absence blocks runtime',single.contract.readiness.blockers.includes('browser_renderer_not_implemented'));
pass('local download is browser-local contract only',single.contract.local_download.mechanism==='browser_blob_object_url'&&single.contract.local_download.ready===false&&single.contract.local_download.server_upload===false&&single.contract.local_download.generated_output_persistence===false);
pass('download filename is safe and MIME-specific',single.contract.local_download.filename==='Family Wallpaper.png');
pass('image processing does not copy EXIF/metadata',single.contract.image_processing.exif_preservation===false&&single.contract.image_processing.metadata_copy===false);
pass('contract forbids arbitrary executable presentation inputs',single.contract.layout.arbitrary_layout_json===false&&single.contract.layout.arbitrary_html===false&&single.contract.layout.arbitrary_css===false&&single.contract.layout.arbitrary_javascript===false);
pass('contract records zero customer/output server writes',single.contract.privacy.customer_photo_server_write===false&&single.contract.privacy.generated_output_server_write===false);
pass('contract exposes no storage key/signed URL/external URL',single.contract.privacy.storage_key_exposed===false&&single.contract.privacy.signed_storage_url_exposed===false&&single.contract.privacy.arbitrary_external_url===false);

const jpeg=buildMemberCreativeBrowserExecutionContract(plan({
  template:{
    ...plan().template,
    title:'Then / Now:*?',
    composition:{
      mode:'pair_photo',
      photo_slots:2,
      canvas_width:1600,
      canvas_height:1200,
      output_mime:'image/jpeg'
    },
    asset_ref:{
      asset_id:'asset:creative:pair',
      public_asset:{
        asset_id:'asset:creative:pair',
        asset_kind:'image',
        public_path:'/member-assets/creative/pair.jpg',
        mime_type:'image/jpeg',
        width:1600,
        height:1200
      }
    }
  },
  selected_media:[media('media_1'),{...media('media_2'),memory_id:'mem_2'}],
  selected_memory_count:2
}));
pass('JPEG pair browser contract succeeds',jpeg.status==='ok'&&jpeg.contract.layout.recipe==='pair_split_v1');
pass('JPEG download filename strips unsafe filename chars',jpeg.contract.local_download.filename==='Then - Now-.jpg');

const webp=buildMemberCreativeBrowserExecutionContract(plan({
  template:{
    ...plan().template,
    composition:{
      ...plan().template.composition,
      output_mime:'image/webp'
    },
    asset_ref:{
      asset_id:'asset:creative:webp',
      public_asset:{
        asset_id:'asset:creative:webp',
        asset_kind:'image',
        public_path:'/member-assets/creative/a.webp',
        mime_type:'image/webp',
        width:1290,
        height:2796
      }
    }
  }
}));
pass('WebP output contract supported',webp.status==='ok'&&webp.contract.local_download.filename.endsWith('.webp'));

const video=buildMemberCreativeBrowserExecutionContract(plan({
  template:{
    ...plan().template,
    creative_type:'memory_movie',
    composition:{
      ...plan().template.composition,
      output_mime:'video/mp4'
    }
  }
}));
pass('video/memory movie output is not in initial browser executor',video.status==='creative_browser_output_not_supported'&&video.contract===null);

const missingTemplateAsset=buildMemberCreativeBrowserExecutionContract(plan({
  template:{
    ...plan().template,
    asset_ref:{
      asset_id:'asset:creative:single',
      public_asset:null
    }
  }
}));
pass('missing public template asset keeps source contract but blocks runtime',missingTemplateAsset.status==='ok'&&missingTemplateAsset.contract.readiness.template_public_asset_ready===false&&missingTemplateAsset.contract.readiness.blockers.includes('template_public_asset_unavailable'));

const missingDelivery=buildMemberCreativeBrowserExecutionContract(plan({
  selected_media:[{...media(),delivery:null}]
}));
pass('missing private delivery source contract fails browser plan',missingDelivery.status==='creative_private_delivery_contract_missing'&&missingDelivery.contract===null);

const badMediaType=buildMemberCreativeBrowserExecutionContract(plan({
  selected_media:[{...media(),media_type:'video'}]
}));
pass('non-image selected media fails browser plan',badMediaType.status==='creative_browser_media_invalid');

const badDimensions=buildMemberCreativeBrowserExecutionContract(plan({
  template:{
    ...plan().template,
    composition:{
      ...plan().template.composition,
      canvas_width:100
    }
  }
}));
pass('unsafe canvas dimensions fail browser plan',badDimensions.status==='creative_browser_plan_invalid');

const badSlotCount=buildMemberCreativeBrowserExecutionContract(plan({
  selected_media:[media(),media('media_2')]
}));
pass('slot-count mismatch fails browser plan',badSlotCount.status==='creative_browser_plan_invalid');

const health=memberCreativeBrowserExecutionHealth();
pass('health records source-only browser contract',health.member_creative_browser_execution===true&&health.source_only===true&&health.browser_side_only===true&&health.browser_execution_contract_ready===true);
pass('health keeps actual renderer inactive',health.browser_renderer_implemented===false&&health.server_rendering===false);
pass('health limits outputs to image formats and excludes memory movie',health.supported_output_mimes.includes('image/png')&&health.supported_output_mimes.includes('image/jpeg')&&health.supported_output_mimes.includes('image/webp')&&health.video_output_supported===false&&health.memory_movie_supported===false);
pass('health records no arbitrary HTML/CSS/JS/layout JSON',health.arbitrary_layout_json===false&&health.arbitrary_html===false&&health.arbitrary_css===false&&health.arbitrary_javascript===false);
pass('health records local-only download and no server writes',health.local_download_mechanism==='browser_blob_object_url'&&health.customer_photo_server_write===false&&health.generated_output_server_write===false);
pass('health records no Production route/write',health.production_route_wired===false&&health.production_write===false);

console.log(`MEMBER_CREATIVE_BROWSER_EXECUTION=${n}/${n} PASS`);
