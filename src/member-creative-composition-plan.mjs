import { readMemberCreativeCatalogForSession } from './member-creative-catalog-read-model.mjs';
import { authorizeMemberPrivateMediaAccess } from './member-private-media-access.mjs';
import { memberPrivateMediaDeliveryPublicContract } from './member-private-media-delivery-grant.mjs';
import { buildMemberCreativeBrowserExecutionContract } from './member-creative-browser-execution.mjs';

const BUILD='member-creative-composition-plan-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const MAX_TEMPLATE_ID=160;
const MAX_MEDIA_ID=160;
const MAX_MEDIA_SELECTION=12;
const MAX_REQUEST_BODY_BYTES=4096;

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-creative-plan-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer'
    }
  });
}

function validSession(session){
  const familyId=text(session?.family_id);
  const customerId=text(session?.customer_id);
  if(!familyId||familyId.length>MAX_FAMILY_ID)return {ok:false,error:'invalid_member_session'};
  if(!CUSTOMER_ID_RE.test(customerId))return {ok:false,error:'invalid_member_session'};
  return {ok:true,family_id:familyId,customer_id:customerId};
}

function validOpaqueId(value,max=160){
  const id=text(value);
  return !!id
    && id.length<=max
    && !/[\u0000-\u001f\u007f]/.test(id);
}

function normalizeSelection(input){
  const templateId=text(input?.template_id);
  const mediaIds=Array.isArray(input?.media_ids)?input.media_ids.map(text):null;

  if(!validOpaqueId(templateId,MAX_TEMPLATE_ID)){
    return {ok:false,error:'invalid_template_id'};
  }
  if(!mediaIds){
    return {ok:false,error:'invalid_media_selection'};
  }
  if(mediaIds.length<1||mediaIds.length>MAX_MEDIA_SELECTION){
    return {ok:false,error:'invalid_media_selection'};
  }
  if(mediaIds.some(id=>!validOpaqueId(id,MAX_MEDIA_ID))){
    return {ok:false,error:'invalid_media_selection'};
  }
  if(new Set(mediaIds).size!==mediaIds.length){
    return {ok:false,error:'duplicate_media_id'};
  }

  return {
    ok:true,
    template_id:templateId,
    media_ids:mediaIds
  };
}

function safeMediaDescriptor(result){
  return {
    media_id:text(result?.media?.media_id),
    memory_id:text(result?.media?.memory_id),
    media_type:text(result?.media?.media_type)||'image',
    role:text(result?.media?.role)||'preview',
    width:result?.media?.width==null?null:Number(result.media.width),
    height:result?.media?.height==null?null:Number(result.media.height),
    delivery:memberPrivateMediaDeliveryPublicContract()
  };
}

function buildCompositionPlan(template,mediaDescriptors=[]){
  if(!template?.eligibility?.eligible){
    return {
      status:'creative_template_not_eligible',
      plan:null,
      read_only:true
    };
  }

  const requiredSlots=Number(template?.composition?.photo_slots);
  if(!Number.isInteger(requiredSlots)||requiredSlots<1||requiredSlots>MAX_MEDIA_SELECTION){
    return {
      status:'creative_template_invalid',
      plan:null,
      review_required:true,
      read_only:true
    };
  }

  if(mediaDescriptors.length!==requiredSlots){
    return {
      status:'creative_media_count_mismatch',
      plan:null,
      required_photo_slots:requiredSlots,
      selected_media_count:mediaDescriptors.length,
      read_only:true
    };
  }

  if(mediaDescriptors.some(media=>media.media_type!=='image')){
    return {
      status:'unsupported_creative_media_type',
      plan:null,
      read_only:true
    };
  }

  const distinctMemoryIds=new Set(mediaDescriptors.map(media=>media.memory_id).filter(Boolean));

  if(template.composition.mode==='pair_photo'&&distinctMemoryIds.size!==requiredSlots){
    return {
      status:'pair_photo_requires_distinct_memories',
      plan:null,
      required_distinct_memories:requiredSlots,
      selected_distinct_memories:distinctMemoryIds.size,
      read_only:true
    };
  }

  return {
    status:'ok',
    plan:{
      template:{
        template_id:template.template_id,
        creative_type:template.creative_type,
        title:template.title,
        composition:{
          mode:template.composition.mode,
          photo_slots:requiredSlots,
          canvas_width:template.composition.canvas_width,
          canvas_height:template.composition.canvas_height,
          output_mime:template.composition.output_mime,
          browser_side_preferred:true
        },
        asset_ref:{
          asset_id:template.asset_ref.asset_id,
          preview_asset_id:template.asset_ref.preview_asset_id||null,
          public_asset:template.asset_ref.public_asset||null,
          preview_public_asset:template.asset_ref.preview_public_asset||null
        }
      },
      selected_media:mediaDescriptors,
      selected_memory_count:distinctMemoryIds.size,
      execution:{
        ready:false,
        template_asset_delivery_ready:!!template.asset_ref?.public_asset,
        private_media_delivery_ready:false,
        browser_composition_implemented:false,
        browser_execution_contract_ready:false,
        generated_output_persistence_ready:false
      },
      privacy:{
        private_storage_key_exposed:false,
        arbitrary_external_url_exposed:false,
        raw_photo_binary_in_plan:false
      }
    },
    read_only:true
  };
}

function mapMediaAuthorizationFailure(result){
  const status=text(result?.status);
  if(status==='media_not_found'||status==='invalid_media_id'){
    return {
      status:'creative_media_not_available',
      review_required:false
    };
  }
  if(status==='invalid_private_storage_key'){
    return {
      status:'creative_media_review_required',
      review_required:true
    };
  }
  if(status==='family_access_denied'
    || status==='ambiguous_family_identity'
    || status==='schema_not_applied'
    || status==='family_inactive_or_missing'
    || status==='unlinked'
    || status==='member_memory_schema_not_applied'){
    return {
      status,
      review_required:status==='ambiguous_family_identity'
    };
  }
  return {
    status:'creative_media_authorization_failed',
    review_required:true
  };
}

export async function planMemberCreativeComposition(env,session,input,{as_of}={}){
  const normalizedSession=validSession(session);
  if(!normalizedSession.ok){
    return {
      status:'invalid_member_session',
      plan:null,
      read_only:true
    };
  }

  const selection=normalizeSelection(input);
  if(!selection.ok){
    return {
      status:selection.error,
      plan:null,
      read_only:true
    };
  }

  const catalog=await readMemberCreativeCatalogForSession(
    env,
    normalizedSession,
    {as_of}
  );

  if(catalog.status!=='ok'){
    return {
      status:catalog.status,
      plan:null,
      review_required:catalog.review_required===true,
      read_only:true
    };
  }

  if(
    text(catalog.family_id)!==normalizedSession.family_id
    || text(catalog.customer_id)!==normalizedSession.customer_id
  ){
    return {
      status:'component_identity_mismatch',
      plan:null,
      review_required:true,
      read_only:true
    };
  }

  const template=(catalog.templates||[])
    .find(item=>text(item?.template_id)===selection.template_id);

  if(!template){
    return {
      status:'creative_template_not_available',
      plan:null,
      read_only:true
    };
  }

  if(!template.eligibility?.eligible){
    return {
      status:'creative_template_not_eligible',
      plan:null,
      eligibility:template.eligibility,
      read_only:true
    };
  }

  const requiredSlots=Number(template?.composition?.photo_slots);
  if(selection.media_ids.length!==requiredSlots){
    return {
      status:'creative_media_count_mismatch',
      plan:null,
      required_photo_slots:requiredSlots,
      selected_media_count:selection.media_ids.length,
      read_only:true
    };
  }

  const authorized=[];
  for(const mediaId of selection.media_ids){
    const result=await authorizeMemberPrivateMediaAccess(
      env,
      normalizedSession,
      mediaId
    );

    if(result.status!=='ok'||result.authorized!==true){
      const mapped=mapMediaAuthorizationFailure(result);
      return {
        ...mapped,
        plan:null,
        read_only:true
      };
    }

    if(
      text(result.family_id)!==normalizedSession.family_id
      || text(result.customer_id)!==normalizedSession.customer_id
    ){
      return {
        status:'component_identity_mismatch',
        plan:null,
        review_required:true,
        read_only:true
      };
    }

    authorized.push(safeMediaDescriptor(result));
  }

  const built=buildCompositionPlan(template,authorized);
  const browserExecution=built.status==='ok'
    ?buildMemberCreativeBrowserExecutionContract(built.plan)
    :{status:'creative_browser_plan_unavailable',contract:null};

  if(built.status==='ok'){
    built.plan.browser_execution=browserExecution.contract;
    built.plan.execution.browser_execution_contract_ready=browserExecution.status==='ok';
    built.plan.execution.browser_composition_implemented=false;
    built.plan.execution.template_asset_delivery_ready=
      browserExecution.contract?.readiness?.template_public_asset_ready===true;
    built.plan.execution.private_media_delivery_ready=
      browserExecution.contract?.readiness?.private_media_delivery_ready===true;
    built.plan.execution.ready=false;
  }

  return {
    ...built,
    browser_execution_status:browserExecution.status,
    family_id:normalizedSession.family_id,
    customer_id:normalizedSession.customer_id,
    template_id:selection.template_id,
    selected_media_count:authorized.length,
    generation_executed:false,
    customer_photo_write:false,
    generated_output_write:false
  };
}

async function readBoundedJson(request){
  const raw=await request.text();
  if(raw.length>MAX_REQUEST_BODY_BYTES){
    return {ok:false,error:'request_too_large'};
  }
  let parsed;
  try{
    parsed=JSON.parse(raw||'{}');
  }catch{
    return {ok:false,error:'invalid_json'};
  }
  if(!parsed||Array.isArray(parsed)||typeof parsed!=='object'){
    return {ok:false,error:'invalid_request'};
  }

  const keys=Object.keys(parsed);
  if(keys.some(key=>!['template_id','media_ids'].includes(key))){
    return {ok:false,error:'unsupported_request_field'};
  }

  return {ok:true,value:parsed};
}

export async function handleMemberCreativePlanRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/creative/plan')return null;
  if(request.method!=='POST')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  const body=await readBoundedJson(request);
  if(!body.ok){
    const status=body.error==='request_too_large'?413:400;
    return json({ok:false,error:body.error},status);
  }

  const result=await planMemberCreativeComposition(env,session,body.value);

  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='family_access_denied')return json({ok:false,error:'family_access_denied'},403);
  if(result.status==='unlinked'||result.status==='family_inactive_or_missing'){
    return json({ok:false,error:result.status},403);
  }
  if(result.status==='schema_not_applied'){
    return json({ok:false,error:'member_family_schema_not_applied'},409);
  }
  if(result.status==='member_memory_schema_not_applied'
    || result.status==='creative_catalog_schema_not_applied'){
    return json({ok:false,error:result.status},409);
  }
  if(result.review_required===true){
    return json({ok:false,error:result.status,review_required:true},409);
  }

  return json({ok:false,error:result.status||'creative_plan_unavailable'},400);
}

export function memberCreativeCompositionPlanHealth(){
  return {
    member_creative_composition_plan:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    request_as_of_input:false,
    request_template_id_input:true,
    request_media_ids_input:true,
    duplicate_media_rejected:true,
    exact_photo_slot_count_required:true,
    private_media_reauthorized_individually:true,
    cross_family_media_hidden:true,
    input_media_type:'image_only',
    pair_photo_distinct_memory_required:true,
    private_storage_key_exposed:false,
    template_storage_key_exposed:false,
    arbitrary_external_url_exposed:false,
    raw_photo_binary_in_plan:false,
    generation_executed:false,
    browser_composition_implemented:false,
    browser_execution_contract_ready:true,
    browser_output_image_only:true,
    memory_movie_execution_supported:false,
    local_download_contract_ready:true,
    customer_photo_write:false,
    generated_output_write:false,
    production_write:false
  };
}

export const __test={
  validSession,
  validOpaqueId,
  normalizeSelection,
  safeMediaDescriptor,
  buildCompositionPlan,
  mapMediaAuthorizationFailure,
  readBoundedJson
};
