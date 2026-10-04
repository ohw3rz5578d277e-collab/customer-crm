const BUILD='member-production-storage-adapter-20261004-01';
const PUBLIC_ASSET_PREFIX='/member-assets/';
const MAX_KEY_LENGTH=512;

const text=value=>value==null?'':String(value).trim();

function validBinding(binding){
  return !!binding&&typeof binding==='object'&&typeof binding.get==='function';
}

function safeKey(raw,{public_asset=false}={}){
  const key=raw==null?'':String(raw);
  if(!key||key.length>MAX_KEY_LENGTH)return false;
  if(key!==key.trim())return false;
  if(/[\u0000-\u001f\u007f]/.test(key))return false;
  if(/^https?:\/\//i.test(key))return false;
  if(key.includes('\\')||key.includes('?')||key.includes('#'))return false;
  if(key.split('/').some(part=>part==='..'))return false;
  if(public_asset){
    return key.startsWith(PUBLIC_ASSET_PREFIX)&&key.length>PUBLIC_ASSET_PREFIX.length;
  }
  return !key.startsWith('/');
}

function contentType(object){
  return text(
    object?.httpMetadata?.contentType
    || object?.contentType
    || object?.content_type
  ).toLowerCase();
}

function normalizeObject(object,{include_etag=false}={}){
  if(!object||typeof object!=='object')return null;
  if(object.body==null)return null;

  const size=Number(object.size);
  if(!Number.isInteger(size)||size<0)return null;

  const normalized={
    body:object.body,
    size,
    content_type:contentType(object)
  };

  if(include_etag){
    const etag=text(object.etag);
    normalized.etag=etag||null;
  }

  return normalized;
}

function createReadOnlyAdapter(binding,{public_asset=false,include_etag=false}={}){
  if(!validBinding(binding))return null;

  return Object.freeze({
    async get(key){
      if(!safeKey(key,{public_asset}))return null;
      const object=await binding.get(String(key));
      if(!object)return null;
      return normalizeObject(object,{include_etag});
    }
  });
}

export function createMemberPublicAssetStorageAdapter(binding){
  return createReadOnlyAdapter(binding,{public_asset:true,include_etag:false});
}

export function createMemberPrivateMediaStorageAdapter(binding){
  return createReadOnlyAdapter(binding,{public_asset:false,include_etag:true});
}

export function memberProductionStorageAdapterHealth(){
  return {
    member_production_storage_adapter:true,
    build:BUILD,
    source_only:true,
    read_only:true,
    explicit_binding_argument_required:true,
    implicit_env_binding:false,
    get_only:true,
    storage_write:false,
    storage_delete:false,
    public_asset_adapter_ready:true,
    private_media_storage_adapter_ready:true,
    production_binding_configured:false,
    production_storage_fetch:false,
    production_route_activated:false,
    production_deploy:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  PUBLIC_ASSET_PREFIX,
  MAX_KEY_LENGTH,
  validBinding,
  safeKey,
  contentType,
  normalizeObject,
  createReadOnlyAdapter
};
