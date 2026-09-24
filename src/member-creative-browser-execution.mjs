const BUILD='member-creative-browser-execution-20260925-01';
const IMAGE_OUTPUT_MIMES=new Set([
  'image/jpeg',
  'image/png',
  'image/webp'
]);
const MAX_SLOTS=12;
const MAX_DIMENSION=8192;

const text=v=>v==null?'':String(v).trim();

function validDimension(value){
  const n=Number(value);
  return Number.isInteger(n)&&n>=320&&n<=MAX_DIMENSION;
}

function validMediaId(value){
  const raw=value==null?'':String(value);
  if(!raw||raw.length>160)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  return true;
}

function validLocalAssetPath(value){
  const raw=value==null?'':String(value);
  if(!raw||raw.length>320)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  if(!raw.startsWith('/member-assets/'))return false;
  if(raw.includes('..')||raw.includes('\\')||raw.includes('%'))return false;
  if(raw.includes('?')||raw.includes('#'))return false;
  if(/^[a-z][a-z0-9+.-]*:/i.test(raw))return false;
  return true;
}

function safePublicAsset(asset){
  if(!asset||typeof asset!=='object'||Array.isArray(asset))return null;
  const path=text(asset.public_path);
  const mime=text(asset.mime_type).toLowerCase();
  const kind=text(asset.asset_kind);
  const width=asset.width==null?null:Number(asset.width);
  const height=asset.height==null?null:Number(asset.height);

  if(!validLocalAssetPath(path))return null;
  if(kind!=='image')return null;
  if(!IMAGE_OUTPUT_MIMES.has(mime))return null;
  if(!Number.isInteger(width)||width<1||width>MAX_DIMENSION)return null;
  if(!Number.isInteger(height)||height<1||height>MAX_DIMENSION)return null;

  return {
    public_path:path,
    mime_type:mime,
    width,
    height,
    same_origin_required:true
  };
}

function splitAxis(total,count,index){
  const start=Math.floor(total*index/count);
  const end=Math.floor(total*(index+1)/count);
  return {start,size:end-start};
}

function buildGridSlots(count,width,height,{pair=false}={}){
  if(!Number.isInteger(count)||count<1||count>MAX_SLOTS)return null;
  if(!validDimension(width)||!validDimension(height))return null;

  let columns;
  let rows;

  if(pair&&count===2){
    if(height>width){
      columns=1;
      rows=2;
    }else{
      columns=2;
      rows=1;
    }
  }else{
    const aspect=width/height;
    columns=Math.min(
      count,
      Math.max(1,Math.ceil(Math.sqrt(count*Math.max(0.5,Math.min(2,aspect)))))
    );
    rows=Math.ceil(count/columns);
  }

  const slots=[];
  for(let index=0;index<count;index++){
    const col=index%columns;
    const row=Math.floor(index/columns);
    const x=splitAxis(width,columns,col);
    const y=splitAxis(height,rows,row);
    slots.push({
      index,
      x:x.start,
      y:y.start,
      width:x.size,
      height:y.size,
      fit:'cover',
      focal_point:'center',
      rotation_degrees:0,
      opacity:1
    });
  }
  return slots;
}

function fixedLayoutRecipe(mode,count,width,height){
  if(mode==='single_photo'&&count===1){
    return {
      recipe:'single_full_bleed_v1',
      slots:[{
        index:0,
        x:0,
        y:0,
        width,
        height,
        fit:'cover',
        focal_point:'center',
        rotation_degrees:0,
        opacity:1
      }]
    };
  }

  if(mode==='pair_photo'&&count===2){
    return {
      recipe:'pair_split_v1',
      slots:buildGridSlots(count,width,height,{pair:true})
    };
  }

  if(mode==='multi_photo'){
    return {
      recipe:'balanced_grid_v1',
      slots:buildGridSlots(count,width,height)
    };
  }

  if(mode==='sequence'){
    return {
      recipe:'sequence_grid_v1',
      slots:buildGridSlots(count,width,height)
    };
  }

  return null;
}

function extensionForMime(mime){
  if(mime==='image/jpeg')return 'jpg';
  if(mime==='image/png')return 'png';
  if(mime==='image/webp')return 'webp';
  return '';
}

function safeDownloadName(template){
  const raw=text(template?.title)
    .normalize('NFKC')
    .replace(/[\\/:*?"<>|\u0000-\u001f\u007f]+/g,'-')
    .replace(/\s+/g,' ')
    .trim()
    .slice(0,80);
  return raw||'mizuno-photo-memory';
}

function deliveryDescriptor(media){
  const delivery=media?.delivery;
  if(!delivery||typeof delivery!=='object')return null;
  if(delivery.source_contract_ready!==true)return null;

  const grant=delivery.grant||{};
  const content=delivery.content||{};

  if(
    grant.method!=='POST'
    || grant.path!=='/api/internal/member/media/grant'
    || content.method!=='POST'
    || content.path!=='/api/internal/member/media/content'
  )return null;

  return {
    media_id:text(media.media_id),
    grant:{
      method:'POST',
      path:'/api/internal/member/media/grant',
      body:{media_id:text(media.media_id)},
      same_origin_required:true,
      signed_member_session_required:true
    },
    content:{
      method:'POST',
      path:'/api/internal/member/media/content',
      body_keys:['media_id','grant'],
      same_origin_required:true,
      signed_member_session_required:true
    },
    source_contract_ready:true,
    delivery_ready:delivery.delivery_ready===true
  };
}

export function buildMemberCreativeBrowserExecutionContract(plan){
  if(!plan||typeof plan!=='object'){
    return {status:'invalid_creative_plan',contract:null};
  }

  const template=plan.template||{};
  const composition=template.composition||{};
  const outputMime=text(composition.output_mime).toLowerCase();
  const width=Number(composition.canvas_width);
  const height=Number(composition.canvas_height);
  const mode=text(composition.mode);
  const slotCount=Number(composition.photo_slots);
  const media=Array.isArray(plan.selected_media)?plan.selected_media:[];

  if(!IMAGE_OUTPUT_MIMES.has(outputMime)){
    return {
      status:'creative_browser_output_not_supported',
      contract:null,
      supported_output_mimes:[...IMAGE_OUTPUT_MIMES]
    };
  }

  if(
    !validDimension(width)
    || !validDimension(height)
    || !Number.isInteger(slotCount)
    || slotCount<1
    || slotCount>MAX_SLOTS
    || media.length!==slotCount
  ){
    return {status:'creative_browser_plan_invalid',contract:null};
  }

  const layout=fixedLayoutRecipe(mode,slotCount,width,height);
  if(!layout||!Array.isArray(layout.slots)||layout.slots.length!==slotCount){
    return {status:'creative_browser_layout_not_supported',contract:null};
  }

  if(media.some(item=>text(item?.media_type)!=='image'||!validMediaId(item?.media_id))){
    return {status:'creative_browser_media_invalid',contract:null};
  }

  const deliveries=media.map(deliveryDescriptor);
  if(deliveries.some(item=>!item)){
    return {status:'creative_private_delivery_contract_missing',contract:null};
  }

  const templateAsset=safePublicAsset(template?.asset_ref?.public_asset);
  const extension=extensionForMime(outputMime);
  const downloadName=`${safeDownloadName(template)}.${extension}`;
  const blockers=[];

  if(!templateAsset)blockers.push('template_public_asset_unavailable');
  if(deliveries.some(item=>item.delivery_ready!==true)){
    blockers.push('private_media_delivery_not_active');
  }
  blockers.push('browser_renderer_not_implemented');

  return {
    status:'ok',
    contract:{
      build:BUILD,
      execution_environment:'browser',
      server_rendering:false,
      canvas:{
        width,
        height,
        output_mime:outputMime,
        color_space:'srgb',
        alpha:outputMime!=='image/jpeg'
      },
      template_layer:{
        required:true,
        asset:templateAsset,
        draw:{
          x:0,
          y:0,
          width,
          height,
          fit:'cover',
          opacity:1
        }
      },
      photo_layers:layout.slots.map((slot,index)=>({
        ...slot,
        media:deliveries[index]
      })),
      layout:{
        mode,
        recipe:layout.recipe,
        arbitrary_layout_json:false,
        arbitrary_css:false,
        arbitrary_html:false,
        arbitrary_javascript:false
      },
      image_processing:{
        fit:'cover',
        focal_point:'center',
        filters:[],
        exif_preservation:false,
        metadata_copy:false
      },
      local_download:{
        ready:false,
        filename:downloadName,
        mime_type:outputMime,
        mechanism:'browser_blob_object_url',
        server_upload:false,
        generated_output_persistence:false
      },
      readiness:{
        source_contract_ready:true,
        browser_engine_contract_ready:true,
        template_public_asset_ready:!!templateAsset,
        private_media_delivery_ready:deliveries.every(item=>item.delivery_ready===true),
        browser_renderer_implemented:false,
        runtime_ready:false,
        blockers
      },
      privacy:{
        same_origin_assets_only:true,
        customer_photo_server_write:false,
        generated_output_server_write:false,
        storage_key_exposed:false,
        signed_storage_url_exposed:false,
        arbitrary_external_url:false
      }
    },
    read_only:true
  };
}

export function memberCreativeBrowserExecutionHealth(){
  return {
    member_creative_browser_execution:true,
    build:BUILD,
    source_only:true,
    browser_side_only:true,
    server_rendering:false,
    browser_renderer_implemented:false,
    browser_execution_contract_ready:true,
    supported_output_mimes:[...IMAGE_OUTPUT_MIMES],
    video_output_supported:false,
    memory_movie_supported:false,
    built_in_layout_recipes:[
      'single_full_bleed_v1',
      'pair_split_v1',
      'balanced_grid_v1',
      'sequence_grid_v1'
    ],
    arbitrary_layout_json:false,
    arbitrary_html:false,
    arbitrary_css:false,
    arbitrary_javascript:false,
    same_origin_assets_only:true,
    local_download_mechanism:'browser_blob_object_url',
    customer_photo_server_write:false,
    generated_output_server_write:false,
    production_route_wired:false,
    production_write:false
  };
}

export const __test={
  validDimension,
  validMediaId,
  validLocalAssetPath,
  safePublicAsset,
  splitAxis,
  buildGridSlots,
  fixedLayoutRecipe,
  extensionForMime,
  safeDownloadName,
  deliveryDescriptor,
  MAX_SLOTS,
  MAX_DIMENSION
};
