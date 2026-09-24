import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';
import { jstToday } from './crm-customer360-marketing-engine.mjs';
import { resolveMemberPublicAssetMapOptional } from './member-public-asset-resolver.mjs';

const BUILD='member-news-read-model-20260925-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const NEWS_LIMIT=100;
const HOME_NEWS_LIMIT=3;
const SAFE_ID_RE=/^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$/;
const NEWS_TYPES=new Set(['news','campaign','service','maintenance']);

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-news-build':BUILD,
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

function validLocalPath(value){
  if(value==null||value==='')return true;
  const raw=String(value);
  if(!raw||raw.length>256)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  if(!raw.startsWith('/')||raw.startsWith('//'))return false;
  if(/^[a-z][a-z0-9+.-]*:/i.test(raw))return false;
  return true;
}

function validPlainText(value,max){
  const raw=value==null?'':String(value);
  if(raw.length>max)return false;
  if(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(raw))return false;
  if(/<\s*\/?\s*(script|iframe|style|html|body)\b/i.test(raw))return false;
  return true;
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

function normalizeNewsItem(row,{asOf}){
  const newsId=text(row?.news_id);
  const newsType=text(row?.news_type);
  const title=text(row?.title);
  const summary=row?.summary==null?'':String(row.summary);
  const bodyText=row?.body_text==null?'':String(row.body_text);
  const heroAssetId=text(row?.hero_asset_id);
  const rawPath=row?.local_path==null?'':String(row.local_path);
  const localPath=rawPath.trim();
  const publishedAt=text(row?.published_at);
  const featuredHome=Number(row?.featured_home)===1;
  const sortOrder=Number(row?.sort_order||0);

  if(!SAFE_ID_RE.test(newsId))return null;
  if(!NEWS_TYPES.has(newsType))return null;
  if(!title||title.length>120)return null;
  if(!validPlainText(title,120)||!validPlainText(summary,280)||!validPlainText(bodyText,4000))return null;
  if(heroAssetId&&!SAFE_ID_RE.test(heroAssetId))return null;
  if(!validLocalPath(rawPath))return null;
  if(publishedAt&&!validDateOnly(publishedAt))return null;

  const schedule=activeSchedule(row,asOf);
  if(!schedule.valid||!schedule.active)return null;

  return {
    news_id:newsId,
    news_type:newsType,
    title,
    summary,
    body_text:bodyText,
    published_at:publishedAt||null,
    schedule:{
      starts_on:schedule.starts_on,
      ends_on:schedule.ends_on
    },
    hero_asset_ref:heroAssetId?{
      asset_id:heroAssetId,
      storage_key_exposed:false,
      arbitrary_external_url_exposed:false
    }:null,
    navigation:{
      local_path:localPath||null,
      local_path_only:true
    },
    featured_home:featuredHome,
    delivery:{
      push_sent:false,
      line_sent:false,
      automatic_contact:false
    },
    sort_order:Number.isFinite(sortOrder)?sortOrder:0
  };
}

export function buildMemberNewsCatalog(rows=[],{as_of=jstToday()}={}){
  const asOf=validDateOnly(as_of);
  if(!asOf){
    return {
      status:'invalid_as_of',
      items:[],
      home_news:[],
      read_only:true
    };
  }

  const items=[];
  let hiddenInvalidOrInactiveCount=0;

  for(const row of rows||[]){
    const item=normalizeNewsItem(row,{asOf});
    if(item)items.push(item);
    else hiddenInvalidOrInactiveCount++;
  }

  items.sort((a,b)=>
    a.sort_order-b.sort_order
    || (b.published_at||'').localeCompare(a.published_at||'')
    || a.news_id.localeCompare(b.news_id)
  );

  return {
    status:'ok',
    as_of:asOf,
    items,
    available_count:items.length,
    home_news:items.filter(item=>item.featured_home).slice(0,HOME_NEWS_LIMIT),
    hidden_invalid_or_inactive_count:hiddenInvalidOrInactiveCount,
    push_delivery_ready:false,
    line_delivery_ready:false,
    automatic_contact:false,
    read_only:true
  };
}

async function authorizeMemberFamily(env,session){
  const normalized=validSession(session);
  if(!normalized.ok)return normalized;

  const family=await readMemberFamilyByCustomer(env,normalized.customer_id);
  if(family.status!=='linked')return {ok:false,error:family.status};
  if(text(family.family?.family_id)!==normalized.family_id){
    return {ok:false,error:'family_access_denied'};
  }

  return {ok:true,...normalized};
}

export async function readMemberNewsForSession(env,session,{as_of}={}){
  const auth=await authorizeMemberFamily(env,session);
  if(!auth.ok){
    return {
      status:auth.error,
      items:[],
      home_news:[],
      read_only:true
    };
  }

  if(!(await tableExists(env,'member_news_items'))){
    return {
      status:'news_catalog_schema_not_applied',
      items:[],
      home_news:[],
      read_only:true
    };
  }

  const rows=await all(
    env,
    `SELECT
       news_id,news_type,title,summary,body_text,
       hero_asset_id,local_path,starts_on,ends_on,published_at,
       featured_home,sort_order
       FROM member_news_items
       WHERE published=1
         AND COALESCE(deleted_at,'')=''
       ORDER BY sort_order,
                COALESCE(published_at,'') DESC,
                news_id
       LIMIT ${NEWS_LIMIT}`
  );

  const built=buildMemberNewsCatalog(rows,{
    as_of:validDateOnly(as_of)||jstToday()
  });

  const assetIds=(built.items||[])
    .map(item=>item.hero_asset_ref?.asset_id)
    .filter(Boolean);
  const assetResolution=await resolveMemberPublicAssetMapOptional(env,assetIds);
  const assetById=assetResolution.status==='ok'
    ?assetResolution.asset_by_id||{}
    :{};

  const items=(built.items||[]).map(item=>({
    ...item,
    hero_asset_ref:item.hero_asset_ref?{
      ...item.hero_asset_ref,
      public_asset:assetById[item.hero_asset_ref.asset_id]||null,
      public_asset_resolution_available:assetResolution.status==='ok'
    }:null
  }));
  const itemById=new Map(items.map(item=>[item.news_id,item]));
  const homeNews=(built.home_news||[])
    .map(item=>itemById.get(item.news_id))
    .filter(Boolean);

  return {
    ...built,
    items,
    home_news:homeNews,
    public_assets_available:assetResolution.status==='ok',
    public_assets_status:assetResolution.status,
    unresolved_public_asset_ids:assetResolution.unresolved_asset_ids||[],
    family_id:auth.family_id,
    customer_id:auth.customer_id,
    source:{
      table:'member_news_items',
      plain_text_only:true,
      published_only:true,
      deleted_hidden:true,
      arbitrary_html:false,
      arbitrary_external_url:false,
      public_asset_resolver:'member_public_asset_resolver',
      public_asset_resolution_optional:true
    }
  };
}

export async function handleMemberNewsReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/news')return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  const result=await readMemberNewsForSession(env,session);

  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='family_access_denied')return json({ok:false,error:'family_access_denied'},403);
  if(result.status==='unlinked'||result.status==='family_inactive_or_missing'){
    return json({ok:false,error:result.status},403);
  }
  if(result.status==='schema_not_applied'){
    return json({ok:false,error:'member_family_schema_not_applied'},409);
  }
  if(result.status==='news_catalog_schema_not_applied'){
    return json({ok:false,error:'news_catalog_schema_not_applied'},409);
  }
  if(result.status==='ambiguous_family_identity'){
    return json({ok:false,error:result.status,review_required:true},409);
  }

  return json({ok:false,error:result.status||'news_unavailable'},409);
}

export function memberNewsHealth(){
  return {
    member_news_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    production_schema_applied:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    request_as_of_input:false,
    plain_text_only:true,
    supported_news_types:[...NEWS_TYPES],
    logical_asset_id_only:true,
    public_asset_resolution_optional:true,
    public_asset_local_path_only:true,
    public_asset_schema_absence_degrades_locally:true,
    storage_key_exposed:false,
    local_navigation_only:true,
    arbitrary_external_url_exposed:false,
    arbitrary_html:false,
    arbitrary_javascript:false,
    home_news_limit:HOME_NEWS_LIMIT,
    push_delivery_ready:false,
    line_delivery_ready:false,
    automatic_contact:false,
    production_write:false
  };
}

export const __test={
  validDateOnly,
  validSession,
  validLocalPath,
  validPlainText,
  activeSchedule,
  normalizeNewsItem,
  buildMemberNewsCatalog
};
