import {
  buildMemberCreativeBrowserExecutionContract
} from '../src/member-creative-browser-execution.mjs';
import {
  validateMemberCreativeBrowserRenderContract,
  computeCoverCrop,
  renderMemberCreativeBrowserContract,
  createDefaultMemberCreativeBrowserRuntime,
  memberCreativeBrowserRendererHealth,
  __test
} from '../src/member-creative-browser-renderer.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const readyDelivery={
  source_contract_ready:true,
  delivery_ready:true,
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
    memory_id:id==='media_1'?'mem_1':'mem_2',
    media_type:'image',
    role:'cover',
    width:1200,
    height:800,
    delivery:readyDelivery
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
        canvas_width:1200,
        canvas_height:1600,
        output_mime:'image/png',
        browser_side_preferred:true
      },
      asset_ref:{
        asset_id:'asset:creative:single',
        public_asset:{
          asset_id:'asset:creative:single',
          asset_kind:'image',
          public_path:'/member-assets/creative/single.png',
          mime_type:'image/png',
          width:1200,
          height:1600
        }
      }
    },
    selected_media:[media()],
    selected_memory_count:1,
    ...overrides
  };
}

const built=buildMemberCreativeBrowserExecutionContract(plan());
assert(built.status==='ok','test execution contract build failed');
const contract=built.contract;

pass('ready delivery + template produces runtime-ready contract',contract.readiness.browser_renderer_implemented===true&&contract.readiness.runtime_ready===true&&contract.local_download.ready===true);
pass('renderer contract uses photo then overlay roles',contract.photo_layers[0].role==='photo'&&contract.template_layer.role==='overlay');

const validation=validateMemberCreativeBrowserRenderContract(contract);
pass('renderer accepts exact generated browser contract',validation.ok===true&&validation.width===1200&&validation.height===1600);

const runtimeBlocked=structuredClone(contract);
runtimeBlocked.readiness.runtime_ready=false;
runtimeBlocked.local_download.ready=false;
pass('renderer rejects contract not runtime-ready',validateMemberCreativeBrowserRenderContract(runtimeBlocked).error==='creative_runtime_not_ready');

const unsafeLayout=structuredClone(contract);
unsafeLayout.layout.arbitrary_javascript=true;
pass('renderer rejects arbitrary JavaScript contract',validateMemberCreativeBrowserRenderContract(unsafeLayout).error==='unsafe_layout_contract');

const externalTemplate=structuredClone(contract);
externalTemplate.template_layer.asset.public_path='https://evil.example/a.png';
pass('renderer rejects external template asset',validateMemberCreativeBrowserRenderContract(externalTemplate).error==='invalid_template_asset_path');

const queryTemplate=structuredClone(contract);
queryTemplate.template_layer.asset.public_path='/member-assets/a.png?x=1';
pass('renderer rejects query-bearing template asset',validateMemberCreativeBrowserRenderContract(queryTemplate).error==='invalid_template_asset_path');

const wrongGrantPath=structuredClone(contract);
wrongGrantPath.photo_layers[0].media.grant.path='/api/internal/member/media/grant?x=1';
pass('renderer rejects mutated grant route',validateMemberCreativeBrowserRenderContract(wrongGrantPath).error==='invalid_private_media_delivery_contract');

const wrongTransform=structuredClone(contract);
wrongTransform.photo_layers[0].rotation_degrees=1;
pass('renderer rejects arbitrary photo rotation',validateMemberCreativeBrowserRenderContract(wrongTransform).error==='unsupported_photo_transform');

const wrongDownload=structuredClone(contract);
wrongDownload.local_download.server_upload=true;
pass('renderer rejects server-upload download contract',validateMemberCreativeBrowserRenderContract(wrongDownload).error==='unsafe_download_contract');

const landscapeCrop=computeCoverCrop(1600,900,1000,1000);
pass('cover crop centers wide image horizontally',Math.round(landscapeCrop.sx)===350&&landscapeCrop.sy===0&&Math.round(landscapeCrop.sw)===900&&landscapeCrop.sh===900);

const portraitCrop=computeCoverCrop(900,1600,1000,1000);
pass('cover crop centers tall image vertically',portraitCrop.sx===0&&Math.round(portraitCrop.sy)===350&&portraitCrop.sw===900&&Math.round(portraitCrop.sh)===900);

const exactCrop=computeCoverCrop(1000,1000,500,500);
pass('cover crop preserves matching aspect',exactCrop.sx===0&&exactCrop.sy===0&&exactCrop.sw===1000&&exactCrop.sh===1000);

pass('invalid crop dimensions fail closed',computeCoverCrop(0,100,100,100)===null);
pass('canvas dimensions stay within 320-8192',__test.validDimension(320)===true&&__test.validDimension(8192)===true&&__test.validDimension(319)===false&&__test.validDimension(8193)===false);

const drawCalls=[];
const releases=[];
const context={
  globalAlpha:1,
  fillStyle:'',
  save(){drawCalls.push({op:'save'})},
  restore(){drawCalls.push({op:'restore'})},
  clearRect(...args){drawCalls.push({op:'clearRect',args})},
  fillRect(...args){drawCalls.push({op:'fillRect',args})},
  drawImage(image,...args){drawCalls.push({op:'drawImage',id:image.id,args})}
};
const canvas={id:'canvas'};

const fakeRuntime={
  async createCanvas(width,height,options){
    drawCalls.push({op:'createCanvas',width,height,options});
    return {canvas,context};
  },
  async loadPrivateImage(mediaDescriptor){
    drawCalls.push({op:'loadPrivate',media_id:mediaDescriptor.media_id});
    return {id:'private',width:1600,height:900};
  },
  async loadPublicImage(asset){
    drawCalls.push({op:'loadPublic',path:asset.public_path});
    return {id:'template',width:1200,height:1600};
  },
  async exportBlob(renderedCanvas,mime,options){
    drawCalls.push({op:'exportBlob',canvas:renderedCanvas.id,mime,options});
    return new Blob([new Uint8Array([1,2,3,4])],{type:mime});
  },
  createObjectURL(blob){
    drawCalls.push({op:'createObjectURL',size:blob.size});
    return 'blob:https://member.example.test/test-output';
  },
  releaseImage(image){
    releases.push(image.id);
  }
};

const rendered=await renderMemberCreativeBrowserContract(contract,fakeRuntime);
pass('renderer returns local output blob/object URL',rendered.status==='ok'&&rendered.rendered===true&&rendered.output.object_url.startsWith('blob:')&&rendered.output.filename==='Family Wallpaper.png');
pass('rendered output remains local-only with no server persistence',rendered.local_only===true&&rendered.server_upload===false&&rendered.generated_output_persistence===false&&rendered.output.revoke_object_url_required===true);

const imageDraws=drawCalls.filter(x=>x.op==='drawImage');
pass('renderer draws exactly private photo then template overlay',imageDraws.length===2&&imageDraws[0].id==='private'&&imageDraws[1].id==='template');
pass('renderer exports after drawing both layers',drawCalls.findIndex(x=>x.op==='exportBlob')>drawCalls.findIndex(x=>x.op==='drawImage'&&x.id==='template'));
pass('renderer releases loaded browser images',releases.join(',')==='private,template');
pass('renderer never auto-downloads',!drawCalls.some(x=>x.op==='click'||x.op==='download'));

const missingRuntime=await renderMemberCreativeBrowserContract(contract,{});
pass('missing runtime adapter fails closed',missingRuntime.status==='creative_browser_runtime_unavailable'&&missingRuntime.rendered===false);

const badBlobRuntime={
  ...fakeRuntime,
  async exportBlob(){
    return new Blob([],{type:'image/png'});
  }
};
const badBlob=await renderMemberCreativeBrowserContract(contract,badBlobRuntime);
pass('empty exported Blob fails closed',badBlob.status==='creative_blob_export_failed'&&badBlob.rendered===false);

const wrongMimeRuntime={
  ...fakeRuntime,
  async exportBlob(){
    return new Blob([new Uint8Array([1])],{type:'image/jpeg'});
  }
};
const wrongMime=await renderMemberCreativeBrowserContract(contract,wrongMimeRuntime);
pass('export MIME mismatch fails closed',wrongMime.status==='creative_blob_mime_mismatch'&&wrongMime.rendered===false);

pass('same-origin public asset helper accepts exact local path',__test.ensureSameOriginPath('/member-assets/a.png','https://member.example.test','/member-assets/')==='/member-assets/a.png');
pass('same-origin public asset helper rejects absolute external URL',__test.ensureSameOriginPath('https://evil.example/a.png','https://member.example.test','/member-assets/')==='');
pass('same-origin public asset helper rejects query-bearing local path',__test.ensureSameOriginPath('/member-assets/a.png?x=1','https://member.example.test','/member-assets/')==='');

const fetchCalls=[];
const imageBlob=new Blob([new Uint8Array([7,8,9])],{type:'image/png'});
const grantValue='v1.2000000000.2000000120.ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef0123456789_';

async function fakeFetch(path,options={}){
  fetchCalls.push({path,options});

  if(path==='/member-assets/creative/single.png'){
    return new Response(imageBlob,{
      status:200,
      headers:{
        'content-type':'image/png',
        'content-length':String(imageBlob.size)
      }
    });
  }

  if(path==='/api/internal/member/media/grant'){
    return new Response(JSON.stringify({ok:true,grant:grantValue}),{
      status:200,
      headers:{'content-type':'application/json'}
    });
  }

  if(path==='/api/internal/member/media/content'){
    return new Response(imageBlob,{
      status:200,
      headers:{
        'content-type':'image/png',
        'content-length':String(imageBlob.size)
      }
    });
  }

  return new Response('not found',{status:404});
}

const fakeUrlApi={
  createObjectURL(){return 'blob:https://member.example.test/runtime'},
  revokeObjectURL(){}
};

const defaultRuntime=createDefaultMemberCreativeBrowserRuntime({
  fetch_impl:fakeFetch,
  create_image_bitmap:async blob=>({id:'bitmap',width:1000,height:1000,blob,close(){}}),
  document_ref:null,
  offscreen_canvas:null,
  url_api:fakeUrlApi,
  location_origin:'https://member.example.test'
});

pass('default browser runtime can be constructed only with required browser capabilities',!!defaultRuntime);

const publicImage=await defaultRuntime.loadPublicImage({
  public_path:'/member-assets/creative/single.png'
});
pass('default runtime loads public image from same-origin local asset path',publicImage.id==='bitmap');

const privateImage=await defaultRuntime.loadPrivateImage(contract.photo_layers[0].media);
pass('default runtime performs grant then private content fetch',privateImage.id==='bitmap'&&fetchCalls.some(x=>x.path==='/api/internal/member/media/grant')&&fetchCalls.some(x=>x.path==='/api/internal/member/media/content'));

const grantCall=fetchCalls.find(x=>x.path==='/api/internal/member/media/grant');
const contentCall=fetchCalls.find(x=>x.path==='/api/internal/member/media/content');
pass('grant/content fetches use POST + same-origin credentials + no-store + redirect error',grantCall.options.method==='POST'&&grantCall.options.credentials==='same-origin'&&grantCall.options.cache==='no-store'&&grantCall.options.redirect==='error'&&contentCall.options.method==='POST'&&contentCall.options.redirect==='error');
pass('grant is never placed in URL',!String(contentCall.path).includes(grantValue)&&JSON.parse(contentCall.options.body).grant===grantValue);

const oversizedResponse={
  ok:true,
  headers:{
    get(name){
      const key=String(name).toLowerCase();
      if(key==='content-type')return 'image/png';
      if(key==='content-length')return String(__test.MAX_IMAGE_BYTES+1);
      return null;
    }
  },
  async blob(){return imageBlob}
};
const oversized=await __test.readImageBlob(oversizedResponse);
pass('browser runtime rejects oversized image metadata before decode',oversized.ok===false&&oversized.error==='image_size_not_allowed');

const htmlResponse=new Response(new Blob(['<html>'],{type:'text/html'}),{
  status:200,
  headers:{'content-type':'text/html'}
});
const htmlImage=await __test.readImageBlob(htmlResponse);
pass('browser runtime rejects non-image MIME',htmlImage.ok===false&&htmlImage.error==='image_mime_not_allowed');

const unavailableRuntime=createDefaultMemberCreativeBrowserRuntime({
  fetch_impl:null,
  create_image_bitmap:null,
  url_api:null,
  location_origin:'https://member.example.test'
});
pass('default runtime fails closed without required browser capabilities',unavailableRuntime===null);

const health=memberCreativeBrowserRendererHealth();
pass('health records renderer source implemented but not auto-executed',health.member_creative_browser_renderer===true&&health.renderer_implemented===true&&health.auto_execute===false&&health.final_ui_wired===false);
pass('health records same-origin no-redirect no-store browser fetch',health.same_origin_fetch_only===true&&health.fetch_redirects_allowed===false&&health.fetch_credentials==='same-origin'&&health.fetch_cache==='no-store');
pass('health records no grant in URL and user-gesture download expectation',health.grant_in_url===false&&health.user_gesture_download_expected===true&&health.auto_download===false);
pass('health records 50MiB input cap and fixed draw order',health.max_input_image_bytes===50*1024*1024&&health.draw_order==='photos_then_template_overlay'&&health.fit==='cover_center');
pass('health records no server writes and no Production activation',health.customer_photo_server_write===false&&health.generated_output_server_write===false&&health.production_private_media_delivery_active===false&&health.production_route_wired===false&&health.production_write===false);

console.log(`MEMBER_CREATIVE_BROWSER_RENDERER=${n}/${n} PASS`);
