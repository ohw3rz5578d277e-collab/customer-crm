import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';
import { jstToday } from './crm-customer360-marketing-engine.mjs';

const BUILD='member-shop-pickup-read-model-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const PRODUCT_LIMIT=100;
const HOME_PICKUP_LIMIT=3;
const SAFE_ID_RE=/^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$/;
const PRODUCT_TYPES=new Set(['album','canvas','frame','print','kotobuki']);

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-shop-pickup-build':BUILD,
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

function validShopPath(value){
  const raw=value==null?'':String(value);
  if(!raw||raw.length>256)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  const path=raw;
  if(!path.startsWith('/')||path.startsWith('//'))return false;
  if(/^[a-z][a-z0-9+.-]*:/i.test(path))return false;
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

function normalizeProduct(row,{asOf}){
  const productId=text(row?.product_id);
  const productType=text(row?.product_type);
  const title=text(row?.title);
  const description=text(row?.description);
  const seasonTag=text(row?.season_tag)||'evergreen';
  const heroAssetId=text(row?.hero_asset_id);
  const rawShopPath=row?.shop_path==null?'':String(row.shop_path);
  const shopPath=rawShopPath.trim();
  const ctaLabel=text(row?.cta_label)||'商品を見る';
  const featuredHome=Number(row?.featured_home)===1;
  const sortOrder=Number(row?.sort_order||0);

  if(!SAFE_ID_RE.test(productId)||!SAFE_ID_RE.test(heroAssetId))return null;
  if(!PRODUCT_TYPES.has(productType))return null;
  if(!title||title.length>120||description.length>500)return null;
  if(!seasonTag||seasonTag.length>64)return null;
  if(!validShopPath(rawShopPath))return null;
  if(!ctaLabel||ctaLabel.length>40)return null;

  const schedule=activeSchedule(row,asOf);
  if(!schedule.valid||!schedule.active)return null;

  return {
    product_id:productId,
    product_type:productType,
    title,
    description,
    season_tag:seasonTag,
    schedule:{
      starts_on:schedule.starts_on,
      ends_on:schedule.ends_on
    },
    hero_asset_ref:{
      asset_id:heroAssetId,
      storage_key_exposed:false,
      arbitrary_external_url_exposed:false
    },
    navigation:{
      shop_path:shopPath,
      local_path_only:true,
      cta_label:ctaLabel
    },
    featured_home:featuredHome,
    pricing:{
      authoritative:false,
      amount:null,
      currency:'JPY',
      source_connected:false
    },
    checkout:{
      ready:false,
      provider:null
    },
    family_pass_benefit:{
      enforcement_ready:false,
      discount_amount:null
    },
    sort_order:Number.isFinite(sortOrder)?sortOrder:0
  };
}

export function buildMemberShopCatalog(rows=[],{as_of=jstToday()}={}){
  const asOf=validDateOnly(as_of);
  if(!asOf){
    return {
      status:'invalid_as_of',
      products:[],
      home_pickup:[],
      read_only:true
    };
  }

  const products=[];
  let hiddenInvalidOrInactiveCount=0;

  for(const row of rows||[]){
    const product=normalizeProduct(row,{asOf});
    if(product)products.push(product);
    else hiddenInvalidOrInactiveCount++;
  }

  products.sort((a,b)=>
    a.sort_order-b.sort_order
    || a.product_id.localeCompare(b.product_id)
  );

  return {
    status:'ok',
    as_of:asOf,
    products,
    available_count:products.length,
    home_pickup:products.filter(item=>item.featured_home).slice(0,HOME_PICKUP_LIMIT),
    hidden_invalid_or_inactive_count:hiddenInvalidOrInactiveCount,
    pricing_authoritative:false,
    checkout_ready:false,
    discount_enforcement_ready:false,
    read_only:true
  };
}

async function authorizeMemberFamily(env,session){
  const normalized=validSession(session);
  if(!normalized.ok)return normalized;

  const family=await readMemberFamilyByCustomer(env,normalized.customer_id);
  if(family.status!=='linked'){
    return {ok:false,error:family.status};
  }
  if(text(family.family?.family_id)!==normalized.family_id){
    return {ok:false,error:'family_access_denied'};
  }

  return {ok:true,...normalized};
}

export async function readMemberShopCatalogForSession(env,session,{as_of}={}){
  const auth=await authorizeMemberFamily(env,session);
  if(!auth.ok){
    return {
      status:auth.error,
      products:[],
      home_pickup:[],
      read_only:true
    };
  }

  if(!(await tableExists(env,'member_shop_products'))){
    return {
      status:'shop_catalog_schema_not_applied',
      products:[],
      home_pickup:[],
      read_only:true
    };
  }

  const rows=await all(
    env,
    `SELECT
       product_id,product_type,title,description,season_tag,
       starts_on,ends_on,hero_asset_id,shop_path,cta_label,
       featured_home,sort_order
       FROM member_shop_products
       WHERE published=1
         AND COALESCE(deleted_at,'')=''
       ORDER BY sort_order,product_id
       LIMIT ${PRODUCT_LIMIT}`
  );

  const built=buildMemberShopCatalog(rows,{
    as_of:validDateOnly(as_of)||jstToday()
  });

  return {
    ...built,
    family_id:auth.family_id,
    customer_id:auth.customer_id,
    source:{
      table:'member_shop_products',
      presentation_catalog_only:true,
      published_only:true,
      deleted_hidden:true,
      price_source_connected:false,
      checkout_source_connected:false
    }
  };
}

export async function handleMemberShopCatalogReadRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/shop/products')return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);

  const session=validSession(memberSession);
  if(!session.ok)return json({ok:false,error:'member_session_required'},401);

  const result=await readMemberShopCatalogForSession(env,session);

  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='family_access_denied')return json({ok:false,error:'family_access_denied'},403);
  if(result.status==='unlinked'||result.status==='family_inactive_or_missing'){
    return json({ok:false,error:result.status},403);
  }
  if(result.status==='schema_not_applied'){
    return json({ok:false,error:'member_family_schema_not_applied'},409);
  }
  if(result.status==='shop_catalog_schema_not_applied'){
    return json({ok:false,error:'shop_catalog_schema_not_applied'},409);
  }
  if(result.status==='ambiguous_family_identity'){
    return json({ok:false,error:result.status,review_required:true},409);
  }

  return json({ok:false,error:result.status||'shop_catalog_unavailable'},409);
}

export function memberShopPickupHealth(){
  return {
    member_shop_pickup_read_model:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    production_schema_applied:false,
    session_identity_source:'server_verified_member_session',
    request_customer_id_input:false,
    request_family_id_input:false,
    request_as_of_input:false,
    presentation_catalog_only:true,
    supported_product_types:[...PRODUCT_TYPES],
    logical_asset_id_only:true,
    storage_key_exposed:false,
    local_shop_path_only:true,
    arbitrary_external_url_exposed:false,
    pricing_authoritative:false,
    price_source_connected:false,
    checkout_ready:false,
    checkout_provider_connected:false,
    order_creation:false,
    payment_execution:false,
    family_pass_discount_enforcement:false,
    inventory_claim:false,
    home_pickup_limit:HOME_PICKUP_LIMIT,
    automatic_contact:false,
    line_send:false,
    production_write:false
  };
}

export const __test={
  validDateOnly,
  validSession,
  validShopPath,
  activeSchedule,
  normalizeProduct,
  buildMemberShopCatalog
};
