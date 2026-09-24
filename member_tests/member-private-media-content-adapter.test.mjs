import {
  issueMemberSessionForVerifiedCustomer
} from '../src/member-session-foundation.mjs';
import {
  issueMemberPrivateMediaDeliveryGrant
} from '../src/member-private-media-delivery-grant.mjs';
import {
  readAuthorizedMemberPrivateMediaContent,
  handleMemberPrivateMediaContentRequest,
  memberPrivateMediaContentAdapterHealth,
  __test
} from '../src/member-private-media-content-adapter.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const origin='https://member.example.test';
const familyId='fam_A';
const customerId='26000123';
const mediaId='media_A';
const memoryId='mem_A';
const sessionSecret='0123456789abcdef0123456789abcdef';
const deliverySecret='abcdef0123456789abcdef0123456789';

function makeDb(){
  const state={linkedFamily:familyId,writes:[]};
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    'member_memories',
    'member_memory_media'
  ]);

  return {
    state,
    prepare(sql){
      const bound={params:[]};
      const stmt={
        bind(...params){bound.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')){
            return tables.has(bound.params[0])?{name:bound.params[0]}:null;
          }
          if(sql.includes('FROM customer_family_groups')){
            return bound.params[0]===state.linkedFamily
              ?{family_id:state.linkedFamily,display_name:'TEST FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          if(sql.includes('FROM member_memory_media mm')){
            const [requestedMedia,requestedFamily]=bound.params;
            if(requestedMedia===mediaId&&requestedFamily===familyId){
              return {
                media_id:mediaId,
                memory_id:memoryId,
                family_id:familyId,
                storage_key:'member/fam_A/mem_A/cover.jpg',
                media_type:'image',
                role:'cover',
                width:1200,
                height:800
              };
            }
            return null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            return bound.params[0]===customerId
              ?{results:[{
                  family_id:state.linkedFamily,
                  customer_id:customerId,
                  relation:'owner',
                  access_role:'owner'
                }]}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return bound.params[0]===state.linkedFamily
              ?{results:[{
                  family_id:state.linkedFamily,
                  customer_id:customerId,
                  relation:'owner',
                  access_role:'owner'
                }]}
              :{results:[]};
          }
          return {results:[]};
        },
        async run(){
          state.writes.push({sql,params:bound.params});
          throw new Error('private media content adapter must remain read-only');
        }
      };
      return stmt;
    }
  };
}

function makeStorage({
  contentType='image/jpeg',
  size=4,
  body=new Uint8Array([1,2,3,4]),
  etag='etag123',
  fail=false,
  missing=false
}={}){
  const state={keys:[]};
  return {
    state,
    async get(key){
      state.keys.push(key);
      if(fail)throw Object.assign(new Error('forced storage failure'),{name:'StorageError'});
      if(missing)return null;
      return {
        body,
        size,
        content_type:contentType,
        etag
      };
    }
  };
}

async function makeSessionAndGrant(db,now=2_000_000_000){
  const env={
    DB:db,
    MEMBER_SESSION_SECRET:sessionSecret,
    MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:deliverySecret
  };
  const session={family_id:familyId,customer_id:customerId};
  const grant=await issueMemberPrivateMediaDeliveryGrant(
    env,
    session,
    mediaId,
    {now_seconds:now}
  );
  assert(grant.status==='ok'&&grant.issued===true,'grant issue failed');
  return {env,session,grant:grant.grant};
}

async function issueCookie(db){
  const issued=await issueMemberSessionForVerifiedCustomer(
    {DB:db,MEMBER_SESSION_SECRET:sessionSecret},
    {
      customer_id:customerId,
      family_id:familyId,
      now_seconds:Math.floor(Date.now()/1000)
    }
  );
  assert(issued.status==='ok','session issue failed');
  return issued.cookie.split(';')[0];
}

function contentRequest(body,{
  cookie='',
  requestOrigin=origin,
  method='POST',
  contentType='application/json',
  range=null
}={}){
  const headers={};
  if(cookie)headers.cookie=cookie;
  if(requestOrigin!==null)headers.origin=requestOrigin;
  if(contentType!==null)headers['content-type']=contentType;
  if(range!==null)headers.range=range;
  return new Request(origin+'/api/internal/member/media/content',{
    method,
    headers,
    body:method==='POST'&&body!==undefined
      ?(typeof body==='string'?body:JSON.stringify(body))
      :undefined
  });
}

pass('route defaults disabled',__test.routeEnabled({})===false);
pass('route requires exact enabled mode',__test.routeEnabled({MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE:'enabled'})===true);
pass('valid grant shape accepted',__test.validGrant('v1.2000000000.2000000120.ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef0123456789_')===true);
pass('grant whitespace rejected',__test.validGrant(' v1.2000000000.2000000120.ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef0123456789_')===false);
pass('body accepts exact media_id + grant',!!__test.exactContentBody({
  media_id:mediaId,
  grant:'v1.2000000000.2000000120.ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef0123456789_'
}));
pass('body rejects identity injection',__test.exactContentBody({
  media_id:mediaId,
  grant:'v1.2000000000.2000000120.ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef0123456789_',
  family_id:'fam_B'
})===null);
pass('trusted storage adapter requires get()',__test.validStorageAdapter({get(){}})===true&&__test.validStorageAdapter({})===false);
pass('storage object accepts JPEG',!!__test.normalizeStorageObject({
  body:new Uint8Array([1]),size:1,content_type:'image/jpeg'
}));
pass('storage object rejects unsupported MIME',__test.normalizeStorageObject({
  body:new Uint8Array([1]),size:1,content_type:'text/html'
})===null);
pass('storage object rejects oversize payload metadata',__test.normalizeStorageObject({
  body:new Uint8Array([1]),size:__test.MAX_PRIVATE_MEDIA_BYTES+1,content_type:'image/jpeg'
})===null);

const db=makeDb();
const {env,session,grant}=await makeSessionAndGrant(db);
const storage=makeStorage();

const content=await readAuthorizedMemberPrivateMediaContent(
  env,
  session,
  {media_id:mediaId,grant},
  storage,
  {now_seconds:2_000_000_030}
);
pass('authorized grant can fetch private image bytes',content.status==='ok'&&content.delivered===true&&content.storage_fetch_executed===true);
pass('storage adapter receives only internal authorized storage key',storage.state.keys.length===1&&storage.state.keys[0]==='member/fam_A/mem_A/cover.jpg');
pass('content result keeps storage key out of public metadata',content.storage_key_exposed===false&&!('storage_key' in content));
pass('binary content is normalized to safe MIME and size',content.content.content_type==='image/jpeg'&&content.content.size===4);
pass('content adapter performs zero DB writes',db.state.writes.length===0);

const invalidStorage=makeStorage();
const invalidGrant=grant.slice(0,-1)+(grant.endsWith('A')?'B':'A');
const denied=await readAuthorizedMemberPrivateMediaContent(
  env,
  session,
  {media_id:mediaId,grant:invalidGrant},
  invalidStorage,
  {now_seconds:2_000_000_030}
);
pass('invalid grant is rejected before storage fetch',denied.verified!==true&&denied.storage_fetch_executed===false&&invalidStorage.state.keys.length===0);

const noAdapter=await readAuthorizedMemberPrivateMediaContent(
  env,
  session,
  {media_id:mediaId,grant},
  null,
  {now_seconds:2_000_000_030}
);
pass('missing trusted storage adapter fails closed after authorization',noAdapter.status==='private_media_storage_adapter_unavailable'&&noAdapter.storage_fetch_executed===false);

const missingStorage=makeStorage({missing:true});
const missing=await readAuthorizedMemberPrivateMediaContent(
  env,
  session,
  {media_id:mediaId,grant},
  missingStorage,
  {now_seconds:2_000_000_030}
);
pass('missing storage object returns explicit not found',missing.status==='private_media_content_not_found'&&missing.storage_fetch_executed===true);

const badMimeStorage=makeStorage({contentType:'text/html'});
const badMime=await readAuthorizedMemberPrivateMediaContent(
  env,
  session,
  {media_id:mediaId,grant},
  badMimeStorage,
  {now_seconds:2_000_000_030}
);
pass('unsafe storage MIME fails closed',badMime.status==='invalid_private_media_storage_object'&&badMime.review_required===true);

const failureStorage=makeStorage({fail:true});
const failed=await readAuthorizedMemberPrivateMediaContent(
  env,
  session,
  {media_id:mediaId,grant},
  failureStorage,
  {now_seconds:2_000_000_030}
);
pass('storage fetch exception fails closed for review',failed.status==='private_media_storage_fetch_failed'&&failed.review_required===true);

const httpDb=makeDb();
const cookie=await issueCookie(httpDb);
const runtimeNow=Math.floor(Date.now()/1000);
const httpGrant=await issueMemberPrivateMediaDeliveryGrant(
  {
    DB:httpDb,
    MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:deliverySecret
  },
  {family_id:familyId,customer_id:customerId},
  mediaId,
  {now_seconds:runtimeNow}
);
assert(httpGrant.status==='ok','HTTP grant issue failed');

const httpEnv={
  DB:httpDb,
  MEMBER_SESSION_SECRET:sessionSecret,
  MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:deliverySecret,
  MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE:'enabled'
};

const routeOff=await handleMemberPrivateMediaContentRequest(
  contentRequest({media_id:mediaId,grant:httpGrant.grant},{cookie}),
  {
    DB:httpDb,
    MEMBER_SESSION_SECRET:sessionSecret,
    MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:deliverySecret
  },
  makeStorage()
);
pass('disabled content route hides endpoint',routeOff.status===404);

const crossOriginStorage=makeStorage();
const crossOrigin=await handleMemberPrivateMediaContentRequest(
  contentRequest({media_id:mediaId,grant:httpGrant.grant},{cookie,requestOrigin:'https://evil.example'}),
  httpEnv,
  crossOriginStorage
);
pass('cross-origin content request blocked before storage',crossOrigin.status===403&&crossOriginStorage.state.keys.length===0);

const noSessionStorage=makeStorage();
const noSession=await handleMemberPrivateMediaContentRequest(
  contentRequest({media_id:mediaId,grant:httpGrant.grant}),
  httpEnv,
  noSessionStorage
);
pass('signed Member session required before storage',noSession.status===401&&noSessionStorage.state.keys.length===0);

const rangeStorage=makeStorage();
const range=await handleMemberPrivateMediaContentRequest(
  contentRequest({media_id:mediaId,grant:httpGrant.grant},{cookie,range:'bytes=0-99'}),
  httpEnv,
  rangeStorage
);
pass('Range requests rejected before storage fetch',range.status===416&&range.headers.get('accept-ranges')==='none'&&rangeStorage.state.keys.length===0);

const invalidBodyStorage=makeStorage();
const invalidBody=await handleMemberPrivateMediaContentRequest(
  contentRequest({media_id:mediaId,grant:httpGrant.grant,customer_id:customerId},{cookie}),
  httpEnv,
  invalidBodyStorage
);
pass('content body rejects identity injection before storage',invalidBody.status===400&&invalidBodyStorage.state.keys.length===0);

const httpStorage=makeStorage();
const okHttp=await handleMemberPrivateMediaContentRequest(
  contentRequest({media_id:mediaId,grant:httpGrant.grant},{cookie}),
  httpEnv,
  httpStorage
);
pass('authorized HTTP request returns binary image',okHttp.status===200&&okHttp.headers.get('content-type')==='image/jpeg'&&Number(okHttp.headers.get('content-length'))===4);
pass('private binary response is no-store and nosniff',okHttp.headers.get('cache-control').includes('no-store')&&okHttp.headers.get('x-content-type-options')==='nosniff');
pass('private binary response is same-origin resource only',okHttp.headers.get('cross-origin-resource-policy')==='same-origin');
pass('private binary response advertises no range support',okHttp.headers.get('accept-ranges')==='none');
pass('HTTP response does not redirect to external URL',okHttp.status!==301&&okHttp.status!==302&&okHttp.status!==307&&okHttp.status!==308);
pass('HTTP storage adapter fetched exactly once after auth',httpStorage.state.keys.length===1);

const health=memberPrivateMediaContentAdapterHealth({});
pass('health records route default disabled',health.route_default_disabled===true&&health.route_mode==='disabled');
pass('health records trusted adapter and no implicit env binding',health.trusted_storage_adapter_required===true&&health.implicit_env_storage_binding===false);
pass('health limits private media to images',health.supported_media_types.join(',')==='image'&&health.supported_content_types.includes('image/jpeg')&&health.supported_content_types.includes('image/webp'));
pass('health records 50MiB max and no range',health.max_private_media_bytes===50*1024*1024&&health.range_requests_supported===false);
pass('health records no external redirect/storage key response',health.external_url_redirect===false&&health.storage_key_public_response===false);
pass('health records no Production binding/route/fetch/write',health.production_storage_binding===false&&health.production_route_wired===false&&health.production_storage_fetch===false&&health.production_write===false);

console.log(`MEMBER_PRIVATE_MEDIA_CONTENT_ADAPTER=${n}/${n} PASS`);
