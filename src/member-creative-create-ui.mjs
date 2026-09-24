const BUILD='member-creative-create-ui-20260925-01';
const MAX_ID=160;
const MAX_SELECTION=12;
const SAFE_ID_RE=/^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$/;
const IMAGE_MIMES=new Set(['image/jpeg','image/png','image/webp']);

const text=v=>v==null?'':String(v).trim();

function escapeHtml(value){
  return text(value)
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

function validId(value){
  const id=text(value);
  return id.length>0&&id.length<=MAX_ID&&SAFE_ID_RE.test(id);
}

function safeMemberAssetPath(value){
  const path=text(value);
  return /^\/member-assets\/[A-Za-z0-9._~!$&'()*+,;=:@%\/-]+$/.test(path)
    && !path.includes('..')
    && !path.includes('\\')
    ?path
    :'';
}

function normalizeTemplate(template){
  const templateId=text(template?.template_id);
  if(!validId(templateId))return null;

  const photoSlots=Number(template?.composition?.photo_slots);
  if(!Number.isInteger(photoSlots)||photoSlots<1||photoSlots>MAX_SELECTION)return null;

  const outputMime=text(template?.composition?.output_mime).toLowerCase();
  if(!IMAGE_MIMES.has(outputMime))return null;

  const publicPath=safeMemberAssetPath(
    template?.asset_ref?.preview_public_asset?.public_path
    || template?.asset_ref?.public_asset?.public_path
  );

  return {
    template_id:templateId,
    creative_type:text(template?.creative_type),
    title:text(template?.title)||'Creative',
    description:text(template?.description),
    composition_mode:text(template?.composition?.mode),
    photo_slots:photoSlots,
    output_mime:outputMime,
    eligible:template?.eligibility?.eligible===true,
    memories_needed:Math.max(0,Number(template?.eligibility?.memories_needed)||0),
    preview_public_path:publicPath||null,
    public_asset_ready:!!safeMemberAssetPath(template?.asset_ref?.public_asset?.public_path)
  };
}

function normalizeMediaFromDetails(memoryDetails=[]){
  const out=[];
  const seen=new Set();

  for(const detail of Array.isArray(memoryDetails)?memoryDetails:[]){
    const memoryId=text(detail?.memory?.memory_id);
    if(!validId(memoryId))continue;

    for(const media of Array.isArray(detail?.media)?detail.media:[]){
      const mediaId=text(media?.media_id);
      if(!validId(mediaId)||seen.has(mediaId))continue;
      if(text(media?.media_type||'image')!=='image')continue;

      seen.add(mediaId);
      out.push({
        media_id:mediaId,
        memory_id:memoryId,
        memory_title:text(detail?.memory?.title)||'MEMORY',
        shoot_date:text(detail?.memory?.shoot_date),
        role:text(media?.role)||'preview',
        width:media?.width==null?null:Number(media.width),
        height:media?.height==null?null:Number(media.height),
        private_delivery_required:true
      });
    }
  }

  return out;
}

function selectedMedia(media,selectedIds){
  const byId=new Map(media.map(item=>[item.media_id,item]));
  const selected=[];
  for(const id of selectedIds){
    if(byId.has(id))selected.push(byId.get(id));
  }
  return selected;
}

function selectionStatus(template,selected){
  if(!template)return {status:'template_required',plan_ready:false};
  if(!template.eligible)return {status:'template_locked',plan_ready:false};
  if(selected.length!==template.photo_slots){
    return {
      status:'media_selection_incomplete',
      plan_ready:false,
      required_photo_slots:template.photo_slots,
      selected_media_count:selected.length
    };
  }

  if(template.composition_mode==='pair_photo'){
    const memories=new Set(selected.map(item=>item.memory_id));
    if(memories.size!==template.photo_slots){
      return {
        status:'pair_photo_requires_distinct_memories',
        plan_ready:false,
        required_distinct_memories:template.photo_slots,
        selected_distinct_memories:memories.size
      };
    }
  }

  return {
    status:'ready_to_plan',
    plan_ready:true,
    required_photo_slots:template.photo_slots,
    selected_media_count:selected.length
  };
}

export function buildMemberCreativeCreateViewModel({
  catalog,
  memory_details=[],
  selected_template_id='',
  selected_media_ids=[],
  preview=null
}={}){
  if(catalog?.status!=='ok'){
    return {
      status:text(catalog?.status)||'creative_catalog_unavailable',
      templates:[],
      media:[],
      selected_template:null,
      selected_media:[],
      plan_ready:false,
      preview_ready:false,
      source_only:true
    };
  }

  const templates=(catalog.templates||[]).map(normalizeTemplate).filter(Boolean);
  const media=normalizeMediaFromDetails(memory_details);
  const selectedTemplate=templates.find(item=>item.template_id===text(selected_template_id))||null;

  const selectedIds=(Array.isArray(selected_media_ids)?selected_media_ids:[])
    .map(text)
    .filter((id,index,list)=>validId(id)&&list.indexOf(id)===index)
    .slice(0,MAX_SELECTION);

  const selected=selectedMedia(media,selectedIds);
  const selection=selectionStatus(selectedTemplate,selected);

  const previewOutput=preview?.status==='ok'&&preview?.rendered===true&&preview?.output
    ?{
      object_url:text(preview.output.object_url),
      filename:text(preview.output.filename),
      mime_type:text(preview.output.mime_type),
      width:Number(preview.output.width)||null,
      height:Number(preview.output.height)||null
    }
    :null;

  const previewReady=!!(
    previewOutput
    && previewOutput.object_url.startsWith('blob:')
    && IMAGE_MIMES.has(previewOutput.mime_type)
  );

  return {
    status:'ok',
    templates,
    media,
    selected_template:selectedTemplate,
    selected_media:selected,
    selection_status:selection.status,
    plan_ready:selection.plan_ready===true,
    required_photo_slots:selection.required_photo_slots||selectedTemplate?.photo_slots||0,
    selected_media_count:selected.length,
    preview_ready:previewReady,
    preview:previewReady?previewOutput:null,
    runtime_note:selection.plan_ready
      ?'写真の配信準備が整った環境でプレビューできます。'
      :'テンプレートと必要枚数の写真を選択してください。',
    source_only:true,
    auto_execute:false,
    auto_download:false,
    server_write:false
  };
}

export function buildMemberCreativePlanInput(viewModel){
  if(viewModel?.status!=='ok'||viewModel?.plan_ready!==true)return null;
  const templateId=text(viewModel?.selected_template?.template_id);
  const mediaIds=(viewModel?.selected_media||[]).map(item=>text(item.media_id));
  if(!validId(templateId)||!mediaIds.length||mediaIds.some(id=>!validId(id)))return null;
  return {
    template_id:templateId,
    media_ids:mediaIds
  };
}

export function toggleMemberCreativeMediaSelection(viewModel,mediaId){
  const id=text(mediaId);
  if(viewModel?.status!=='ok'||!validId(id))return [];
  if(!(viewModel.media||[]).some(item=>item.media_id===id)){
    return (viewModel.selected_media||[]).map(item=>item.media_id);
  }

  const current=(viewModel.selected_media||[]).map(item=>item.media_id);
  if(current.includes(id))return current.filter(item=>item!==id);

  const max=Math.min(
    MAX_SELECTION,
    Math.max(1,Number(viewModel?.selected_template?.photo_slots)||1)
  );
  if(current.length>=max)return current;
  return [...current,id];
}

export async function runMemberCreativeCreatePreview({
  view_model,
  plan_composition,
  render_composition,
  runtime
}={}){
  const input=buildMemberCreativePlanInput(view_model);
  if(!input)return {status:'creative_selection_not_ready',rendered:false};

  if(typeof plan_composition!=='function'){
    return {status:'creative_planner_unavailable',rendered:false};
  }

  const planned=await plan_composition(input);
  if(planned?.status!=='ok'||!planned?.plan){
    return {
      status:text(planned?.status)||'creative_plan_failed',
      rendered:false
    };
  }

  const execution=planned.plan.browser_execution;
  if(execution?.readiness?.runtime_ready!==true){
    return {
      status:'creative_runtime_not_ready',
      rendered:false,
      blockers:Array.isArray(execution?.readiness?.blockers)
        ?execution.readiness.blockers.slice(0,10).map(text)
        :[]
    };
  }

  if(typeof render_composition!=='function'){
    return {status:'creative_renderer_unavailable',rendered:false};
  }

  return render_composition(execution,runtime);
}

export function saveMemberCreativeOutputFromUserGesture(output,{
  document_ref=globalThis.document
}={}){
  const objectUrl=text(output?.object_url);
  const filename=text(output?.filename);
  const mime=text(output?.mime_type).toLowerCase();

  if(!objectUrl.startsWith('blob:'))return {ok:false,error:'invalid_local_object_url'};
  if(!filename||filename.length>120||/[\\/:*?"<>|\u0000-\u001f\u007f]/.test(filename)){
    return {ok:false,error:'invalid_download_filename'};
  }
  if(!IMAGE_MIMES.has(mime))return {ok:false,error:'invalid_download_mime'};
  if(!document_ref||typeof document_ref.createElement!=='function'){
    return {ok:false,error:'document_unavailable'};
  }

  const anchor=document_ref.createElement('a');
  if(!anchor||typeof anchor.click!=='function')return {ok:false,error:'download_anchor_unavailable'};

  anchor.href=objectUrl;
  anchor.download=filename;
  anchor.rel='noopener';
  anchor.style && (anchor.style.display='none');

  const body=document_ref.body;
  if(body&&typeof body.appendChild==='function')body.appendChild(anchor);
  anchor.click();
  if(typeof anchor.remove==='function')anchor.remove();

  return {
    ok:true,
    local_only:true,
    server_write:false,
    object_url_revoke_required:true
  };
}

export function renderMemberCreativeCreateMarkup(viewModel){
  if(viewModel?.status!=='ok'){
    return '<section class="mp-create mp-create--unavailable"><p>CREATEを利用できません。</p></section>';
  }

  const templates=(viewModel.templates||[]).map(template=>{
    const selected=viewModel.selected_template?.template_id===template.template_id;
    const locked=!template.eligible;
    const image=template.preview_public_path
      ?'<img class="mp-create__template-image" src="'+escapeHtml(template.preview_public_path)+'" alt="">'
      :'';
    const sub=locked
      ?'あと'+escapeHtml(template.memories_needed)+' MEMORY'
      :escapeHtml(template.description||'写真から新しい思い出をつくる');
    return '<button type="button" class="mp-create__template'+(selected?' is-selected':'')+'" data-creative-template="'+escapeHtml(template.template_id)+'" aria-pressed="'+(selected?'true':'false')+'"'+(locked?' disabled':'')+'>'+image+'<span class="mp-create__template-copy"><strong>'+escapeHtml(template.title)+'</strong><small>'+sub+'</small></span></button>';
  }).join('');

  const media=(viewModel.media||[]).map(item=>{
    const selected=(viewModel.selected_media||[]).some(x=>x.media_id===item.media_id);
    return '<button type="button" class="mp-create__media'+(selected?' is-selected':'')+'" data-creative-media="'+escapeHtml(item.media_id)+'" aria-pressed="'+(selected?'true':'false')+'"><span class="mp-create__media-placeholder">PHOTO</span><span><strong>'+escapeHtml(item.memory_title)+'</strong><small>'+escapeHtml(item.shoot_date||'MEMORY')+'</small></span></button>';
  }).join('');

  const selected=viewModel.selected_template;
  const footer=selected
    ?'<div class="mp-create__summary"><span>'+escapeHtml(viewModel.selected_media_count)+' / '+escapeHtml(viewModel.required_photo_slots)+' photos</span><button type="button" data-creative-preview'+(viewModel.plan_ready?'':' disabled')+'>プレビューを作る</button><button type="button" data-creative-save'+(viewModel.preview_ready?'':' disabled')+'>画像を保存</button></div>'
    :'<div class="mp-create__summary"><span>テンプレートを選択してください</span></div>';

  return '<section class="mp-create" data-member-creative-create><header class="mp-create__hero"><p class="mp-create__eyebrow">CREATE</p><h1>思い出を、暮らしの中へ。</h1><p>お気に入りの写真から、壁紙やコラージュをつくれます。</p></header><div class="mp-create__section"><div class="mp-create__section-head"><h2>Template</h2><span>つくりたい形を選ぶ</span></div><div class="mp-create__templates">'+templates+'</div></div><div class="mp-create__section"><div class="mp-create__section-head"><h2>Photos</h2><span>必要な写真を選ぶ</span></div><div class="mp-create__media-grid">'+media+'</div></div><p class="mp-create__note">'+escapeHtml(viewModel.runtime_note)+'</p>'+footer+'</section>';
}

export function memberCreativeCreateCss(){
  return [
    '.mp-create{--paper:#f7f4ec;--ink:#232622;--muted:#74786f;--green:#315d4f;max-width:720px;margin:0 auto;padding:28px 18px 120px;color:var(--ink);background:var(--paper);font-family:ui-serif,"Hiragino Mincho ProN","Yu Mincho",serif;}',
    '.mp-create__hero{padding:22px 4px 34px}.mp-create__eyebrow{font:600 11px/1.4 ui-sans-serif,sans-serif;letter-spacing:.22em;color:var(--green)}',
    '.mp-create__hero h1{margin:10px 0 12px;font-size:clamp(28px,7vw,44px);font-weight:500;letter-spacing:-.03em}.mp-create__hero>p:last-child{margin:0;color:var(--muted);line-height:1.9}',
    '.mp-create__section{margin:0 0 34px}.mp-create__section-head{display:flex;align-items:end;justify-content:space-between;margin:0 2px 12px}.mp-create__section-head h2{margin:0;font-size:17px;font-weight:600}.mp-create__section-head span{font:12px ui-sans-serif,sans-serif;color:var(--muted)}',
    '.mp-create__templates{display:grid;grid-auto-flow:column;grid-auto-columns:minmax(210px,72%);gap:12px;overflow-x:auto;padding:2px 2px 10px;scroll-snap-type:x mandatory}.mp-create__template{scroll-snap-align:start;border:0;border-radius:24px;background:#fff;padding:0;overflow:hidden;text-align:left;box-shadow:0 10px 28px rgba(35,38,34,.08)}',
    '.mp-create__template.is-selected{outline:2px solid var(--green);outline-offset:2px}.mp-create__template:disabled{opacity:.48}.mp-create__template-image{display:block;width:100%;aspect-ratio:4/3;object-fit:cover;background:#ece9df}.mp-create__template-copy{display:grid;gap:6px;padding:15px 16px 17px}.mp-create__template-copy small{color:var(--muted)}',
    '.mp-create__media-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.mp-create__media{border:1px solid rgba(49,93,79,.14);border-radius:18px;background:#fff;padding:10px;text-align:left}.mp-create__media.is-selected{border-color:var(--green);box-shadow:inset 0 0 0 1px var(--green)}.mp-create__media-placeholder{display:grid;place-items:center;aspect-ratio:4/3;border-radius:12px;background:#ebe8df;color:#8b8e86;font:700 10px ui-sans-serif,sans-serif;letter-spacing:.14em}.mp-create__media span:last-child{display:grid;gap:3px;padding:9px 2px 2px}.mp-create__media small{color:var(--muted)}',
    '.mp-create__note{color:var(--muted);font-size:13px;line-height:1.8}.mp-create__summary{position:sticky;bottom:12px;display:grid;grid-template-columns:1fr auto auto;gap:8px;align-items:center;padding:10px 10px 10px 15px;border:1px solid rgba(49,93,79,.15);border-radius:22px;background:rgba(255,255,255,.94);backdrop-filter:blur(16px);box-shadow:0 16px 40px rgba(35,38,34,.12)}',
    '.mp-create__summary button{border:0;border-radius:999px;padding:12px 15px;background:var(--green);color:#fff;font-weight:700}.mp-create__summary button[data-creative-save]{background:#171918}.mp-create__summary button:disabled{opacity:.35}.mp-create--unavailable{padding:28px}',
    '@media(min-width:760px){.mp-create{padding-left:28px;padding-right:28px}.mp-create__templates{grid-auto-columns:minmax(230px,42%)}.mp-create__media-grid{grid-template-columns:repeat(3,minmax(0,1fr))}}'
  ].join('');
}

export function memberCreativeCreateUiHealth(){
  return {
    member_creative_create_ui:true,
    build:BUILD,
    source_only:true,
    ui_source_implemented:true,
    design_direction:'luxury_minimal_family_story',
    template_picker:true,
    media_picker:true,
    preview_action:true,
    user_gesture_download_action:true,
    client_identity_input:false,
    customer_id_exposed:false,
    family_id_exposed:false,
    private_storage_key_exposed:false,
    arbitrary_external_media_url:false,
    auto_execute:false,
    auto_download:false,
    generated_output_persistence:false,
    production_private_media_delivery_active:false,
    production_route_wired:false,
    production_write:false
  };
}

export const __test={
  escapeHtml,
  validId,
  safeMemberAssetPath,
  normalizeTemplate,
  normalizeMediaFromDetails,
  selectedMedia,
  selectionStatus,
  IMAGE_MIMES,
  MAX_SELECTION
};
