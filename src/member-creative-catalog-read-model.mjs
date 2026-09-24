import { readMemberMemoriesForSession } from './member-memories-read-model.mjs';
import { jstToday } from './crm-customer360-marketing-engine.mjs';

const BUILD='member-creative-catalog-read-model-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const TEMPLATE_LIMIT=100;
const SAFE_ID_RE=/^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$/;
const CREATIVE_TYPES=new Set([
  'wallpaper',
  'calendar',
  'collage',
  'then_and_now',
  'family_card',
  'memory_movie'
]);
const COMPOSITION_MODES=new Set([
  'single_photo',
  'multi_photo',
  'pair_photo',
  'sequence'
]);
const OUTPUT_MIMES=new Set([
  'image/jpeg',
  'image/png',
  'image/webp',
  'video/mp4'
]);

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-creative-catalog-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer'
    }
  });
}

async function first(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  return (await q.first())||null;
}

async function all(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  const result=await q.all();
  return result.results||[];
}

async function tableExists(env,name){
  return !!(await first(
    env,
    "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
    [name]
  ));
}

function validDateOnly(value){
  const raw=text(value);
  if(!raw)return '';
  const m=raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if(!m)return '';
  const y=Number(m[1]);
  const month=Number(m[2]);
  const day=Number(m[3]);
  const d=new Date(Date.UTC(y,month-1,day));
  if(
    d.getUTCFullYear()!==y
    || d.getUTCMonth()+1!==month
    || d.getUTCDate()!==day
  )return '';
  return raw;
}

function validSession(session){
  const familyId=text(session?.family_id);
  const customerId=text(session?.customer_id);
  if(!familyId||familyId.length>MAX_FAMILY_ID)return {ok:false,error:'invalid_member_session'};
  if(!CUSTOMER_ID_RE.test(customerId))return {ok:false,error:'invalid_member_session'};
  return {ok:true,family_id:familyId,customer_id:customerId};
}

function activeSchedule(row,asOf){
  const rawStart=text(row?.starts_on);
  const rawEnd=text(row?.ends_on);
  const start=rawStart?validDateOnly(rawStart):'';
  const end=rawEnd?validDateOnly(rawEnd):'';

  if(rawStart&&!start)return {valid:false,active:false};
  if(rawEnd&&!end)return {valid:false,active:false};
  if(start&&end&&start>end)return {valid:false,active:false};

  return {
    valid:true,
    active:(!start||start<=asOf)&&(!end||end>=asOf),
    starts_on:start||null,
    ends_on:end||null
  };
}

function normalizeTemplate(row,{asOf,memoryCount}){
  const templateId=text(row?.template_id);
  const assetId=text(row?.asset_id);
  const previewAssetId=text(row?.preview_asset_id);
  const creativeType=text(row?.creative_type);
  const compositionMode=text(row?.composition_mode);
  const outputMime=text(row?.output_mime);
  const title=text(row?.title);
  const description=text(row?.description);
  const seasonTag=text(row?.season_tag)||'evergreen';
  const photoSlots=Number(row?.photo_slots);
  const canvasWidth=Number(row?.canvas_width);
  const canvasHeight=Number(row?.canvas_height);
  const minimumMemoryCount=Number(row?.minimum_memory_count);
  const sortOrder=Number(row?.sort_order||0);

  if(!SAFE_ID_RE.test(templateId)||!SAFE_ID_RE.test(assetId))return null;
  if(previewAssetId&&!SAFE_ID_RE.test(previewAssetId))return null;
  if(!CREATIVE_TYPES.has(creativeType))return null;
  if(!COMPOSITION_MODES.has(compositionMode))return null;
  if(!OUTPUT_MIMES.has(outputMime))return null;
  if(!title||title.length>120||description.length>500)return null;
  if(!seasonTag||seasonTag.length>64)return null;
  if(!Number.isInteger(photoSlots)||photoSlots<1||photoSlots>12)return null;
  if(!Number.isInteger(canvasWidth)||canvasWidth<320||canvasWidth>8192)return null;
  if(!Number.isInteger(canvasHeight)||canvasHeight<320||canvasHeight>8192)return null;
  if(!Number.isInteger(minimumMemoryCount)||minimumMemoryCount<1||minimumMemoryCount>12)return null;

  const schedule=activeSchedule(row,asOf);
  if(!schedule.valid||!schedule.active)return null;

  const eligible=memoryCount>=minimumMemoryCount;

  return {
    template_id:templateId,
    creative_type:creativeType,
    title,
    description,
    season_tag:seasonTag,
    schedule:{
      starts_on:schedule.starts_on,
      ends_on:schedule.ends_on
    },
    composition:{
      mode:compositionMode,
      photo_slots:photoSlots,
      canvas_width:canvasWidth,
      canvas_height:canvasHeight,
      output_mime:outputMime,
      execution_ready:false,
      browser_side_preferred:true
    },
    asset_ref:{
      asset_id:assetId,
      preview_asset_id:previewAssetId||null,
      storage_key_exposed:false,
      arbitrary_url_exposed:false
    },
    eligibility:{
      eligible,
      minimum_memory_count:minimumMemoryCount,
      visible_memory_count:memoryCount,
      memories_needed:eligible?0:Math.max(0,minimumMemoryCount-memoryCount)
    },
    sort_order:Number.isFinite(sortOrder)?sortOrder:0
  };
}

export function buildCreativeCatalog(rows=[],{
  as_of=jstToday(),
  visible_memory_count=0
}={}){
  const asOf=validDateOnly(as_of);
  if(!asOf){
    return {
      status:'invalid_as_of',
      templates:[],
      read_only:true
    };
  }

  const memoryCount=Math.max(0,Math.floor(Number(visible_memory_count)||0));
  const normalized=[];
  let invalidOrInactiveCount=0;

  for(const row of rows||[]){
    const template=normalizeTemplate(row,{asOf,memoryCount});
    if(template)normalized.push(template);
    else invalidOrInactiveCount++;
  }

  normalized.sort((a,b)=>
    a.sort_order-b.sort_order
    || a.template_id.localeCompare(b.template_id)
  );

  return {
    status:'ok',
    as_of:asOf,
    templates:normalized,
    available_count:normalized.length,
    eligible_count:normalized.filter(x=>x.eligibility.eligible).length,
    hidden_invalid_or_inactive_count:invalidOrInactiveCount,
    generation_ready:false,
    read_only:true
  };
}

export async function readMemberCreativeCatalogForSession(env,session,{as_of}={}){
  const normalized=validSession(session);
  if(!normalized.ok){
    return {
      status:'invalid_member_session',
      templates:[],
      read_only:true
    };
  }

  const memories=await readMemberMemoriesForSession(env,normalized);
  if(memories.status!=='ok'){
    return {
      status:memories.status,
      templates:[],
      read_only:true
    };
  }
  if(text(memories.family_id)!==normalized.family_id){
    return {
      status:'component_identity_mismatch',
      templates:[],
      review_required:true,
      read_only:true
    };
  }

  if(!(await tableExists(env,'member_creative_templates'))){
    return {
      status:'creative_catalog_schema_not_applied',
      templates:[],
      read_only:true
    };
  }

  const rows=await all(
    env,
    `SELECT
       template_id,creative_type,title,description,season_tag,
       starts_on,ends_on,photo_slots,composition_mode,
       canvas_width,canvas_height,output_mime,
       asset_id,preview_asset_id,minimum_memory_count,
       sort_order
       FROM member_creative_templates
       WHERE published=1
         AND COALESCE(deleted_at,'')=''
       ORDER BY sort_order,template_id
       LIMIT ${TEMPLATE_LIMIT}`
  );

  const built=buildCreativeCatalog(rows,{
    as_of:validDateOnly(as_of)||jstToday(),
    visible_memory_count:(memories.memories||[]).length
  });

  return {
    ...built,
    family_id:normalized.family_id,
    customer_id:normalized.customer_id,
    source:{
      template_table:'member_creative_templates',
      published_only:true,
      deleted_hidden:true,
      family_memory_reader:'member_memories_read_model',
      bounded_memory_count:true
    }
  };
}

export async function handleMemberCreativeCatalogReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/creative/templates')return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  const result=await readMemberCreativeCatalogForSession(env,session);

  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='family_access_denied')return json({ok:false,error:'family_access_denied'},403);
  if(result.status==='unlinked'||result.status==='family_inactive_or_missing'){
    return json({ok:false,error:result.status},403);
  }
  if(result.status==='schema_not_applied'){
    return json({ok:false,error:'member_family_schema_not_applied'},409);
  }
  if(result.status==='member_memory_schema_not_applied'){
    return json({ok:false,error:'member_memory_schema_not_applied'},409);
  }
  if(result.status==='creative_catalog_schema_not_applied'){
    return json({ok:false,error:'creative_catalog_schema_not_applied'},409);
  }
  if(result.status==='ambiguous_family_identity'
    || result.status==='component_identity_mismatch'){
    return json({ok:false,error:result.status,review_required:true},409);
  }
  return json({ok:false,error:result.status||'creative_catalog_unavailable'},409);
}

export function memberCreativeCatalogHealth(){
  return {
    member_creative_catalog_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    production_schema_applied:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    request_as_of_input:false,
    template_source:'member_creative_templates',
    published_only:true,
    deleted_hidden:true,
    arbitrary_template_html:false,
    arbitrary_template_javascript:false,
    arbitrary_external_url:false,
    storage_key_exposed:false,
    asset_ref_is_logical_id_only:true,
    browser_side_composition_preferred:true,
    generation_ready:false,
    upload_ready:false,
    customer_photo_write:false,
    automatic_contact:false,
    line_send:false,
    production_write:false
  };
}

export const __test={
  validDateOnly,
  validSession,
  activeSchedule,
  normalizeTemplate,
  buildCreativeCatalog
};
