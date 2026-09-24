const BUILD='member-creative-browser-renderer-20260925-01';
const MAX_DIMENSION=8192;
const MAX_SLOTS=12;
const MAX_IMAGE_BYTES=50*1024*1024;
const IMAGE_MIMES=new Set([
  'image/jpeg',
  'image/png',
  'image/webp'
]);

const text=v=>v==null?'':String(v).trim();

function validDimension(value){
  const n=Number(value);
  return Number.isInteger(n)&&n>=320&&n<=MAX_DIMENSION;
}

function validLocalMemberAssetPath(value){
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

function validInternalPath(value,expected){
  const raw=value==null?'':String(value);
  return raw===expected;
}

function validMediaId(value){
  const raw=value==null?'':String(value);
  if(!raw||raw.length>160)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  return true;
}

function validRect(rect,canvasWidth,canvasHeight){
  if(!rect||typeof rect!=='object')return false;
  const x=Number(rect.x);
  const y=Number(rect.y);
  const width=Number(rect.width);
  const height=Number(rect.height);

  if(!Number.isInteger(x)||!Number.isInteger(y))return false;
  if(!Number.isInteger(width)||!Number.isInteger(height))return false;
  if(x<0||y<0||width<1||height<1)return false;
  if(x+width>canvasWidth||y+height>canvasHeight)return false;

  return true;
}

function validateDelivery(media){
  if(!media||typeof media!=='object')return false;
  if(!validMediaId(media.media_id))return false;

  const grant=media.grant||{};
  const content=media.content||{};

  return grant.method==='POST'
    && validInternalPath(grant.path,'/api/internal/member/media/grant')
    && content.method==='POST'
    && validInternalPath(content.path,'/api/internal/member/media/content')
    && grant.same_origin_required===true
    && grant.signed_member_session_required===true
    && content.same_origin_required===true
    && content.signed_member_session_required===true;
}

export function validateMemberCreativeBrowserRenderContract(contract){
  if(!contract||typeof contract!=='object'){
    return {ok:false,error:'invalid_render_contract'};
  }

  if(contract.execution_environment!=='browser'||contract.server_rendering!==false){
    return {ok:false,error:'invalid_execution_environment'};
  }

  const canvas=contract.canvas||{};
  const width=Number(canvas.width);
  const height=Number(canvas.height);
  const outputMime=text(canvas.output_mime).toLowerCase();

  if(!validDimension(width)||!validDimension(height)){
    return {ok:false,error:'invalid_canvas_dimensions'};
  }
  if(!IMAGE_MIMES.has(outputMime)){
    return {ok:false,error:'unsupported_output_mime'};
  }

  if(contract.readiness?.browser_renderer_implemented!==true){
    return {ok:false,error:'browser_renderer_not_enabled'};
  }
  if(contract.readiness?.runtime_ready!==true){
    return {ok:false,error:'creative_runtime_not_ready'};
  }
  if(contract.local_download?.ready!==true){
    return {ok:false,error:'creative_download_not_ready'};
  }

  const template=contract.template_layer||{};
  const asset=template.asset||{};
  const draw=template.draw||{};

  if(template.required!==true){
    return {ok:false,error:'template_layer_required'};
  }
  if(template.role!=='overlay'||Number(template.z_index)!==1000){
    return {ok:false,error:'invalid_template_layer_role'};
  }
  if(!validLocalMemberAssetPath(asset.public_path)){
    return {ok:false,error:'invalid_template_asset_path'};
  }
  if(!IMAGE_MIMES.has(text(asset.mime_type).toLowerCase())){
    return {ok:false,error:'invalid_template_asset_mime'};
  }
  if(!validRect(draw,width,height)){
    return {ok:false,error:'invalid_template_draw_rect'};
  }
  if(draw.fit!=='cover'||Number(draw.opacity)!==1){
    return {ok:false,error:'unsupported_template_transform'};
  }

  const layers=Array.isArray(contract.photo_layers)?contract.photo_layers:[];
  if(layers.length<1||layers.length>MAX_SLOTS){
    return {ok:false,error:'invalid_photo_layer_count'};
  }

  for(let index=0;index<layers.length;index++){
    const layer=layers[index];
    if(Number(layer.index)!==index){
      return {ok:false,error:'invalid_photo_layer_index'};
    }
    if(layer.role!=='photo'||Number(layer.z_index)!==index+1){
      return {ok:false,error:'invalid_photo_layer_role'};
    }
    if(!validRect(layer,width,height)){
      return {ok:false,error:'invalid_photo_layer_rect'};
    }
    if(layer.fit!=='cover'||layer.focal_point!=='center'){
      return {ok:false,error:'unsupported_photo_fit'};
    }
    if(Number(layer.rotation_degrees)!==0||Number(layer.opacity)!==1){
      return {ok:false,error:'unsupported_photo_transform'};
    }
    if(!validateDelivery(layer.media)){
      return {ok:false,error:'invalid_private_media_delivery_contract'};
    }
  }

  const layout=contract.layout||{};
  if(
    layout.arbitrary_layout_json!==false
    || layout.arbitrary_html!==false
    || layout.arbitrary_css!==false
    || layout.arbitrary_javascript!==false
  ){
    return {ok:false,error:'unsafe_layout_contract'};
  }

  const processing=contract.image_processing||{};
  if(
    processing.fit!=='cover'
    || processing.focal_point!=='center'
    || !Array.isArray(processing.filters)
    || processing.filters.length!==0
    || processing.exif_preservation!==false
    || processing.metadata_copy!==false
  ){
    return {ok:false,error:'unsafe_image_processing_contract'};
  }

  const download=contract.local_download||{};
  if(
    download.mechanism!=='browser_blob_object_url'
    || download.server_upload!==false
    || download.generated_output_persistence!==false
  ){
    return {ok:false,error:'unsafe_download_contract'};
  }

  const filename=text(download.filename);
  if(!filename||filename.length>120||/[\\/:*?"<>|\u0000-\u001f\u007f]/.test(filename)){
    return {ok:false,error:'invalid_download_filename'};
  }
  if(text(download.mime_type).toLowerCase()!==outputMime){
    return {ok:false,error:'download_mime_mismatch'};
  }

  return {
    ok:true,
    width,
    height,
    output_mime:outputMime,
    filename,
    template,
    layers
  };
}

export function computeCoverCrop(sourceWidth,sourceHeight,targetWidth,targetHeight){
  const sw=Number(sourceWidth);
  const sh=Number(sourceHeight);
  const tw=Number(targetWidth);
  const th=Number(targetHeight);

  if(
    !Number.isFinite(sw)||!Number.isFinite(sh)
    || !Number.isFinite(tw)||!Number.isFinite(th)
    || sw<=0||sh<=0||tw<=0||th<=0
  )return null;

  const sourceAspect=sw/sh;
  const targetAspect=tw/th;

  if(sourceAspect>targetAspect){
    const cropWidth=sh*targetAspect;
    return {
      sx:(sw-cropWidth)/2,
      sy:0,
      sw:cropWidth,
      sh
    };
  }

  const cropHeight=sw/targetAspect;
  return {
    sx:0,
    sy:(sh-cropHeight)/2,
    sw,
    sh:cropHeight
  };
}

function imageDimensions(image){
  const width=Number(
    image?.naturalWidth
    ?? image?.displayWidth
    ?? image?.width
    ?? 0
  );
  const height=Number(
    image?.naturalHeight
    ?? image?.displayHeight
    ?? image?.height
    ?? 0
  );

  if(!Number.isFinite(width)||!Number.isFinite(height)||width<=0||height<=0){
    return null;
  }

  return {width,height};
}

function drawCover(context,image,rect){
  const dimensions=imageDimensions(image);
  if(!dimensions)return {ok:false,error:'invalid_source_image_dimensions'};

  const crop=computeCoverCrop(
    dimensions.width,
    dimensions.height,
    rect.width,
    rect.height
  );
  if(!crop)return {ok:false,error:'invalid_cover_crop'};

  context.drawImage(
    image,
    crop.sx,
    crop.sy,
    crop.sw,
    crop.sh,
    rect.x,
    rect.y,
    rect.width,
    rect.height
  );

  return {ok:true};
}

function validRuntime(runtime){
  return !!runtime
    && typeof runtime.createCanvas==='function'
    && typeof runtime.loadPublicImage==='function'
    && typeof runtime.loadPrivateImage==='function'
    && typeof runtime.exportBlob==='function'
    && typeof runtime.createObjectURL==='function';
}

export async function renderMemberCreativeBrowserContract(contract,runtime){
  const validation=validateMemberCreativeBrowserRenderContract(contract);
  if(!validation.ok){
    return {
      status:validation.error,
      rendered:false
    };
  }

  if(!validRuntime(runtime)){
    return {
      status:'creative_browser_runtime_unavailable',
      rendered:false
    };
  }

  let canvasBundle;
  try{
    canvasBundle=await runtime.createCanvas(
      validation.width,
      validation.height,
      {
        alpha:contract.canvas?.alpha===true,
        color_space:'srgb'
      }
    );
  }catch{
    return {
      status:'creative_canvas_unavailable',
      rendered:false
    };
  }

  const canvas=canvasBundle?.canvas??canvasBundle;
  const context=canvasBundle?.context
    ?? canvas?.getContext?.('2d',{
      alpha:contract.canvas?.alpha===true,
      colorSpace:'srgb'
    });

  if(!canvas||!context||typeof context.drawImage!=='function'){
    return {
      status:'creative_canvas_context_unavailable',
      rendered:false
    };
  }

  if(contract.canvas?.alpha!==true&&typeof context.fillRect==='function'){
    context.save?.();
    context.globalAlpha=1;
    context.fillStyle='#ffffff';
    context.fillRect(0,0,validation.width,validation.height);
    context.restore?.();
  }else if(typeof context.clearRect==='function'){
    context.clearRect(0,0,validation.width,validation.height);
  }

  const loaded=[];
  try{
    for(const layer of validation.layers){
      const image=await runtime.loadPrivateImage(layer.media);
      loaded.push(image);

      context.save?.();
      context.globalAlpha=1;
      const drawn=drawCover(context,image,layer);
      context.restore?.();

      if(!drawn.ok){
        return {
          status:drawn.error,
          rendered:false
        };
      }
    }

    const templateImage=await runtime.loadPublicImage(validation.template.asset);
    loaded.push(templateImage);

    context.save?.();
    context.globalAlpha=1;
    const templateDraw=drawCover(
      context,
      templateImage,
      validation.template.draw
    );
    context.restore?.();

    if(!templateDraw.ok){
      return {
        status:templateDraw.error,
        rendered:false
      };
    }

    const blob=await runtime.exportBlob(
      canvas,
      validation.output_mime,
      {
        quality:validation.output_mime==='image/jpeg'?0.92:undefined
      }
    );

    if(!blob||Number(blob.size)<=0){
      return {
        status:'creative_blob_export_failed',
        rendered:false
      };
    }

    const blobType=text(blob.type).toLowerCase();
    if(blobType&&blobType!==validation.output_mime){
      return {
        status:'creative_blob_mime_mismatch',
        rendered:false
      };
    }

    const objectUrl=runtime.createObjectURL(blob);
    if(!text(objectUrl)){
      return {
        status:'creative_object_url_failed',
        rendered:false
      };
    }

    return {
      status:'ok',
      rendered:true,
      output:{
        blob,
        object_url:objectUrl,
        filename:validation.filename,
        mime_type:validation.output_mime,
        width:validation.width,
        height:validation.height,
        revoke_object_url_required:true
      },
      local_only:true,
      server_upload:false,
      generated_output_persistence:false
    };
  }catch(error){
    return {
      status:'creative_browser_render_failed',
      rendered:false,
      error_class:text(error?.name)||'Error'
    };
  }finally{
    if(typeof runtime.releaseImage==='function'){
      for(const image of loaded){
        try{runtime.releaseImage(image)}catch{}
      }
    }
  }
}

function ensureSameOriginPath(path,locationOrigin,expectedPrefix){
  const raw=text(path);
  if(!raw.startsWith(expectedPrefix))return '';
  if(raw.startsWith('//')||raw.includes('\\'))return '';
  try{
    const url=new URL(raw,locationOrigin);
    if(url.origin!==locationOrigin)return '';
    return url.pathname===raw?raw:'';
  }catch{
    return '';
  }
}

async function readImageBlob(response){
  if(!response?.ok)return {ok:false,error:'image_fetch_failed'};

  const mime=text(response.headers?.get?.('content-type')).toLowerCase().split(';')[0];
  if(!IMAGE_MIMES.has(mime)){
    return {ok:false,error:'image_mime_not_allowed'};
  }

  const lengthHeader=text(response.headers?.get?.('content-length'));
  if(lengthHeader){
    const declared=Number(lengthHeader);
    if(!Number.isInteger(declared)||declared<1||declared>MAX_IMAGE_BYTES){
      return {ok:false,error:'image_size_not_allowed'};
    }
  }

  const blob=await response.blob();
  if(!blob||blob.size<1||blob.size>MAX_IMAGE_BYTES){
    return {ok:false,error:'image_size_not_allowed'};
  }

  const blobMime=text(blob.type).toLowerCase().split(';')[0];
  if(blobMime&&blobMime!==mime){
    return {ok:false,error:'image_mime_mismatch'};
  }

  return {ok:true,blob,mime};
}

export function createDefaultMemberCreativeBrowserRuntime({
  fetch_impl=globalThis.fetch,
  create_image_bitmap=globalThis.createImageBitmap,
  document_ref=globalThis.document,
  offscreen_canvas=globalThis.OffscreenCanvas,
  url_api=globalThis.URL,
  location_origin=globalThis.location?.origin||''
}={}){
  const origin=text(location_origin);

  if(
    !origin
    || typeof fetch_impl!=='function'
    || typeof create_image_bitmap!=='function'
    || !url_api
    || typeof url_api.createObjectURL!=='function'
  ){
    return null;
  }

  async function fetchImage(path,options={}){
    const response=await fetch_impl(path,{
      credentials:'same-origin',
      cache:'no-store',
      redirect:'error',
      ...options
    });
    const parsed=await readImageBlob(response);
    if(!parsed.ok)throw Object.assign(new Error(parsed.error),{name:'CreativeImageFetchError'});
    return create_image_bitmap(parsed.blob);
  }

  return {
    async createCanvas(width,height,{alpha=true}={}){
      if(typeof offscreen_canvas==='function'){
        const canvas=new offscreen_canvas(width,height);
        const context=canvas.getContext('2d',{alpha,colorSpace:'srgb'});
        if(!context)throw new Error('2d context unavailable');
        return {canvas,context};
      }

      if(document_ref&&typeof document_ref.createElement==='function'){
        const canvas=document_ref.createElement('canvas');
        canvas.width=width;
        canvas.height=height;
        const context=canvas.getContext('2d',{alpha,colorSpace:'srgb'});
        if(!context)throw new Error('2d context unavailable');
        return {canvas,context};
      }

      throw new Error('canvas unavailable');
    },

    async loadPublicImage(asset){
      const path=ensureSameOriginPath(
        asset?.public_path,
        origin,
        '/member-assets/'
      );
      if(!path)throw new Error('invalid public asset path');
      return fetchImage(path);
    },

    async loadPrivateImage(media){
      if(!validMediaId(media?.media_id))throw new Error('invalid media id');

      const grantPath=ensureSameOriginPath(
        media?.grant?.path,
        origin,
        '/api/internal/member/media/grant'
      );
      const contentPath=ensureSameOriginPath(
        media?.content?.path,
        origin,
        '/api/internal/member/media/content'
      );

      if(
        grantPath!=='/api/internal/member/media/grant'
        || contentPath!=='/api/internal/member/media/content'
      ){
        throw new Error('invalid private media path');
      }

      const grantResponse=await fetch_impl(grantPath,{
        method:'POST',
        credentials:'same-origin',
        cache:'no-store',
        redirect:'error',
        headers:{
          'content-type':'application/json'
        },
        body:JSON.stringify({
          media_id:media.media_id
        })
      });

      if(!grantResponse?.ok){
        throw Object.assign(new Error('grant request failed'),{name:'CreativeGrantError'});
      }

      const grantJson=await grantResponse.json();
      const grant=text(grantJson?.grant);
      if(!grant||grant.length>512||/[\u0000-\u001f\u007f]/.test(grant)){
        throw Object.assign(new Error('invalid grant response'),{name:'CreativeGrantError'});
      }

      const contentResponse=await fetch_impl(contentPath,{
        method:'POST',
        credentials:'same-origin',
        cache:'no-store',
        redirect:'error',
        headers:{
          'content-type':'application/json'
        },
        body:JSON.stringify({
          media_id:media.media_id,
          grant
        })
      });

      const parsed=await readImageBlob(contentResponse);
      if(!parsed.ok){
        throw Object.assign(new Error(parsed.error),{name:'CreativePrivateMediaError'});
      }

      return create_image_bitmap(parsed.blob);
    },

    async exportBlob(canvas,mime,{quality}={}){
      if(typeof canvas?.convertToBlob==='function'){
        return canvas.convertToBlob({
          type:mime,
          ...(quality==null?{}:{quality})
        });
      }

      if(typeof canvas?.toBlob==='function'){
        return new Promise((resolve,reject)=>{
          canvas.toBlob(
            blob=>blob?resolve(blob):reject(new Error('toBlob failed')),
            mime,
            quality
          );
        });
      }

      throw new Error('blob export unavailable');
    },

    createObjectURL(blob){
      return url_api.createObjectURL(blob);
    },

    revokeObjectURL(url){
      if(typeof url_api.revokeObjectURL==='function'){
        url_api.revokeObjectURL(url);
      }
    },

    releaseImage(image){
      if(typeof image?.close==='function')image.close();
    }
  };
}

export function memberCreativeBrowserRendererHealth(){
  return {
    member_creative_browser_renderer:true,
    build:BUILD,
    source_only:true,
    browser_runtime_module:true,
    renderer_implemented:true,
    auto_execute:false,
    final_ui_wired:false,
    same_origin_fetch_only:true,
    grant_in_url:false,
    fetch_redirects_allowed:false,
    fetch_credentials:'same-origin',
    fetch_cache:'no-store',
    max_input_image_bytes:MAX_IMAGE_BYTES,
    supported_output_mimes:[...IMAGE_MIMES],
    draw_order:'photos_then_template_overlay',
    fit:'cover_center',
    user_gesture_download_expected:true,
    auto_download:false,
    local_blob_object_url:true,
    customer_photo_server_write:false,
    generated_output_server_write:false,
    production_private_media_delivery_active:false,
    production_route_wired:false,
    production_write:false
  };
}

export const __test={
  validDimension,
  validLocalMemberAssetPath,
  validInternalPath,
  validMediaId,
  validRect,
  validateDelivery,
  computeCoverCrop,
  imageDimensions,
  drawCover,
  validRuntime,
  ensureSameOriginPath,
  readImageBlob,
  MAX_DIMENSION,
  MAX_SLOTS,
  MAX_IMAGE_BYTES
};
