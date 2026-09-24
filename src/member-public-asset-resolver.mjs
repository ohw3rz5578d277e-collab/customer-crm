const BUILD='member-public-asset-resolver-20260925-01';
const MAX_ASSET_IDS=20;
const MAX_ASSET_ID=160;
const SAFE_ID_RE=/^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$/;
const LOCAL_PREFIX='/member-assets/';
const MIME_TYPES=new Set([
  'image/jpeg',
  'image/png',
  'image/webp',
  'video/mp4'
]);
const ASSET_KINDS=new Set(['image','video']);

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-public-assets-build':BUILD,
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

function validAssetId(value){
  const raw=value==null?'':String(value);
  if(!raw||raw.length>MAX_ASSET_ID)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  return SAFE_ID_RE.test(raw);
}

function validLocalAssetPath(value){
  const raw=value==null?'':String(value);
  if(!raw||raw.length>320)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  if(!raw.startsWith(LOCAL_PREFIX))return false;
  if(raw.startsWith('//'))return false;
  if(raw.includes('..'))return false;
  if(raw.includes('\\')||raw.includes('%'))return false;
  if(raw.includes('?')||raw.includes('#'))return false;
  if(/^[a-z][a-z0-9+.-]*:/i.test(raw))return false;
  return true;
}

function normalizeAssetRow(row){
  const assetId=text(row?.asset_id);
  const assetKind=text(row?.asset_kind);
  const localPath=row?.local_path==null?'':String(row.local_path);
  const mimeType=text(row?.mime_type);
  const width=row?.width==null?null:Number(row.width);
  const height=row?.height==null?null:Number(row.height);

  if(!validAssetId(assetId))return null;
  if(!ASSET_KINDS.has(assetKind))return null;
  if(!validLocalAssetPath(localPath))return null;
  if(!MIME_TYPES.has(mimeType))return null;

  if(assetKind==='image'){
    if(!Number.isInteger(width)||width<1||width>8192)return null;
    if(!Number.isInteger(height)||height<1||height>8192)return null;
    if(!mimeType.startsWith('image/'))return null;
  }

  if(assetKind==='video'){
    if(width!==null&&(!Number.isInteger(width)||width<1||width>8192))return null;
    if(height!==null&&(!Number.isInteger(height)||height<1||height>8192))return null;
    if(mimeType!=='video/mp4')return null;
  }

  return {
    asset_id:assetId,
    asset_kind:assetKind,
    public_path:localPath,
    mime_type:mimeType,
    width,
    height,
    source:{
      local_public_asset:true,
      storage_key_exposed:false,
      arbitrary_external_url_exposed:false,
      signed_url_exposed:false,
      private_customer_media:false
    }
  };
}

function normalizeRequestedAssetIds(assetIds){
  if(!Array.isArray(assetIds)||assetIds.length<1||assetIds.length>MAX_ASSET_IDS)return null;
  const normalized=[];
  const seen=new Set();

  for(const value of assetIds){
    if(!validAssetId(value))return null;
    if(seen.has(value))continue;
    seen.add(value);
    normalized.push(value);
  }

  return normalized.length?normalized:null;
}

export async function resolveMemberPublicAssets(env,assetIds){
  const requested=normalizeRequestedAssetIds(assetIds);
  if(!requested){
    return {
      status:'invalid_asset_ids',
      assets:[],
      unresolved_asset_ids:[],
      read_only:true
    };
  }

  if(!(await tableExists(env,'member_public_assets'))){
    return {
      status:'public_asset_schema_not_applied',
      assets:[],
      unresolved_asset_ids:requested,
      read_only:true
    };
  }

  const placeholders=requested.map(()=>'?').join(',');
  const rows=await all(
    env,
    `SELECT
       asset_id,
       asset_kind,
       local_path,
       mime_type,
       width,
       height
       FROM member_public_assets
       WHERE asset_id IN (${placeholders})
         AND published=1
         AND COALESCE(deleted_at,'')=''
       LIMIT ${MAX_ASSET_IDS}`,
    requested
  );

  const byId=new Map();
  let hiddenInvalidCount=0;
  for(const row of rows){
    const asset=normalizeAssetRow(row);
    if(asset)byId.set(asset.asset_id,asset);
    else hiddenInvalidCount++;
  }

  const assets=[];
  const unresolvedAssetIds=[];
  for(const assetId of requested){
    const asset=byId.get(assetId);
    if(asset)assets.push(asset);
    else unresolvedAssetIds.push(assetId);
  }

  return {
    status:'ok',
    assets,
    unresolved_asset_ids:unresolvedAssetIds,
    hidden_invalid_count:hiddenInvalidCount,
    complete:unresolvedAssetIds.length===0,
    read_only:true
  };
}

function exactResolveBody(value){
  if(!value||typeof value!=='object'||Array.isArray(value))return null;
  const keys=Object.keys(value);
  if(keys.length!==1||keys[0]!=='asset_ids')return null;
  const assetIds=normalizeRequestedAssetIds(value.asset_ids);
  if(!assetIds)return null;
  return {asset_ids:assetIds};
}

export async function handleMemberPublicAssetResolveRequest(request,env,memberSession){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/assets/resolve')return null;
  if(request.method!=='POST')return json({ok:false,error:'method_not_allowed'},405);

  const familyId=text(memberSession?.family_id);
  const customerId=text(memberSession?.customer_id);
  if(!familyId||!/^[0-9]{8}$/.test(customerId)){
    return json({ok:false,error:'member_session_required'},401);
  }

  let parsed;
  try{
    parsed=await request.json();
  }catch{
    return json({ok:false,error:'invalid_json_body'},400);
  }

  const body=exactResolveBody(parsed);
  if(!body)return json({ok:false,error:'invalid_asset_ids'},400);

  const result=await resolveMemberPublicAssets(env,body.asset_ids);
  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='public_asset_schema_not_applied'){
    return json({ok:false,error:result.status},409);
  }
  return json({ok:false,error:result.status||'public_asset_unavailable'},400);
}

export function memberPublicAssetResolverHealth(){
  return {
    member_public_asset_resolver:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    production_schema_applied:false,
    registry_table:'member_public_assets',
    asset_id_logical_only:true,
    supported_asset_kinds:[...ASSET_KINDS],
    supported_mime_types:[...MIME_TYPES],
    local_path_prefix:LOCAL_PREFIX,
    max_asset_ids_per_request:MAX_ASSET_IDS,
    published_only:true,
    deleted_hidden:true,
    storage_key_exposed:false,
    arbitrary_external_url_exposed:false,
    signed_url_exposed:false,
    private_customer_media:false,
    customer_photo_delivery:false,
    production_write:false
  };
}

export const __test={
  validAssetId,
  validLocalAssetPath,
  normalizeAssetRow,
  normalizeRequestedAssetIds,
  exactResolveBody,
  MAX_ASSET_IDS,
  LOCAL_PREFIX
};
