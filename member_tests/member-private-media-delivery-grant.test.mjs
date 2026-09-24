import {
  issueMemberSessionForVerifiedCustomer
} from '../src/member-session-foundation.mjs';
import {
  issueMemberPrivateMediaDeliveryGrant,
  verifyMemberPrivateMediaDeliveryGrant,
  handleMemberPrivateMediaGrantRequest,
  memberPrivateMediaDeliveryGrantHealth,
  __test
} from '../src/member-private-media-delivery-grant.mjs';

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
  const state={
    linkedFamily:familyId,
    writes:[]
  };
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    'member_memories',
    'member_memory_media'
  ]);
  const mediaRows=[
    {
      media_id:mediaId,
      memory_id:memoryId,
      family_id:familyId,
      storage_key:'member/fam_A/mem_A/cover.jpg',
      media_type:'image',
      role:'cover',
      width:1200,
      height:800,
      deleted_at:''
    },
    {
      media_id:'media_B',
      memory_id:'mem_B',
      family_id:'fam_B',
      storage_key:'member/fam_B/mem_B/cover.jpg',
      media_type:'image',
      role:'cover',
      width:1200,
      height:800,
      deleted_at:''
    }
  ];
  const memories=[
    {memory_id:memoryId,family_id:familyId,published:1,deleted_at:''},
    {memory_id:'mem_B',family_id:'fam_B',published:1,deleted_at:''}
  ];

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
            const row=mediaRows.find(x=>
              x.media_id===requestedMedia
              && x.family_id===requestedFamily
              && !x.deleted_at
            );
            if(!row)return null;
            const memory=memories.find(x=>
              x.memory_id===row.memory_id
              && x.family_id===row.family_id
              && x.published===1
              && !x.deleted_at
            );
            return memory?{...row}:null;
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
          throw new Error('private media grant foundation must remain read-only');
        }
      };
      return stmt;
    }
  };
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
  assert(issued.status==='ok'&&issued.issued===true,'test session issue failed');
  return issued.cookie.split(';')[0];
}

function grantRequest(body,{cookie='',requestOrigin=origin,method='POST',contentType='application/json'}={}){
  const headers={};
  if(cookie)headers.cookie=cookie;
  if(requestOrigin!==null)headers.origin=requestOrigin;
  if(contentType!==null)headers['content-type']=contentType;
  return new Request(origin+'/api/internal/member/media/grant',{
    method,
    headers,
    body:method==='POST'&&body!==undefined
      ?(typeof body==='string'?body:JSON.stringify(body))
      :undefined
  });
}

pass('delivery secret requires minimum length',__test.deliverySecret({MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:'short'})==='');
pass('delivery secret accepts separate 32-byte secret',__test.deliverySecret({MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:deliverySecret})===deliverySecret);
pass('strict media ID accepts normal ID',__test.validMediaId(mediaId)===true);
pass('strict media ID rejects outer whitespace',__test.validMediaId(' '+mediaId)===false);
pass('strict media ID rejects control characters',__test.validMediaId(mediaId+'\n')===false);
pass('grant body accepts only media_id',!!__test.exactGrantBody({media_id:mediaId}));
pass('grant body rejects identity injection',__test.exactGrantBody({media_id:mediaId,customer_id:'26000999'})===null);
pass('same origin accepted',__test.sameOrigin(new Request(origin+'/x',{headers:{origin}}))===true);
pass('different origin rejected',__test.sameOrigin(new Request(origin+'/x',{headers:{origin:'https://evil.example'}}))===false);

const db=makeDb();
const env={
  DB:db,
  MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:deliverySecret
};
const session={family_id:familyId,customer_id:customerId};

const issued=await issueMemberPrivateMediaDeliveryGrant(
  env,
  session,
  mediaId,
  {now_seconds:2_000_000_000}
);
pass('authorized media receives short-lived grant',issued.status==='ok'&&issued.issued===true&&issued.expires_in_seconds===120);
pass('grant has fixed compact four-part structure',issued.grant.split('.').length===4&&issued.grant.startsWith('v1.'));
pass('grant token contains no Customer ID',!issued.grant.includes(customerId));
pass('grant token contains no Family ID',!issued.grant.includes(familyId));
pass('grant token contains no storage key',!issued.grant.includes('member/fam_A/mem_A/cover.jpg'));
pass('public grant response descriptor omits actual storage key',!('storage_key' in issued.media)&&issued.media.storage_key_exposed===false);
pass('delivery contract keeps grant out of URL and requires POST body',issued.delivery_contract.method==='POST'&&issued.delivery_contract.path==='/api/internal/member/media/content'&&issued.delivery_contract.body_keys.join(',')==='media_id,grant');
pass('grant issue performs zero writes',db.state.writes.length===0&&issued.read_only===true);

const verified=await verifyMemberPrivateMediaDeliveryGrant(
  env,
  session,
  mediaId,
  issued.grant,
  {now_seconds:2_000_000_030}
);
pass('valid grant verifies and reauthorizes private media',verified.status==='ok'&&verified.verified===true&&verified.media.storage_key==='member/fam_A/mem_A/cover.jpg');
pass('verified storage key remains internal-only function output',verified.read_only===true);

const wrongMedia=await verifyMemberPrivateMediaDeliveryGrant(
  env,
  session,
  'media_other',
  issued.grant,
  {now_seconds:2_000_000_030}
);
pass('grant cannot be replayed for another media ID',wrongMedia.status==='invalid_private_media_grant_signature'&&wrongMedia.verified===false);

const wrongCustomer=await verifyMemberPrivateMediaDeliveryGrant(
  env,
  {family_id:familyId,customer_id:'26000999'},
  mediaId,
  issued.grant,
  {now_seconds:2_000_000_030}
);
pass('grant is bound to exact Customer session',wrongCustomer.status==='invalid_private_media_grant_signature'&&wrongCustomer.verified===false);

const wrongFamily=await verifyMemberPrivateMediaDeliveryGrant(
  env,
  {family_id:'fam_B',customer_id:customerId},
  mediaId,
  issued.grant,
  {now_seconds:2_000_000_030}
);
pass('grant is bound to exact Family session',wrongFamily.status==='invalid_private_media_grant_signature'&&wrongFamily.verified===false);

const expired=await verifyMemberPrivateMediaDeliveryGrant(
  env,
  session,
  mediaId,
  issued.grant,
  {now_seconds:2_000_000_151}
);
pass('grant expires after short TTL plus skew',expired.status==='private_media_grant_expired'&&expired.verified===false);

const future=await issueMemberPrivateMediaDeliveryGrant(
  env,
  session,
  mediaId,
  {now_seconds:2_000_000_500}
);
const futureCheck=await verifyMemberPrivateMediaDeliveryGrant(
  env,
  session,
  mediaId,
  future.grant,
  {now_seconds:2_000_000_400}
);
pass('grant issued too far in future is rejected',futureCheck.status==='private_media_grant_not_yet_valid'&&futureCheck.verified===false);

const tampered=issued.grant.slice(0,-1)+(issued.grant.endsWith('A')?'B':'A');
const tamperedResult=await verifyMemberPrivateMediaDeliveryGrant(
  env,
  session,
  mediaId,
  tampered,
  {now_seconds:2_000_000_030}
);
pass('tampered HMAC is rejected',tamperedResult.status==='invalid_private_media_grant_signature'&&tamperedResult.verified===false);

const malformed=await verifyMemberPrivateMediaDeliveryGrant(
  env,
  session,
  mediaId,
  'v1.bad.token',
  {now_seconds:2_000_000_030}
);
pass('malformed grant rejected',malformed.status==='invalid_private_media_grant'&&malformed.verified===false);

const missingSecret=await issueMemberPrivateMediaDeliveryGrant(
  {DB:makeDb()},
  session,
  mediaId
);
pass('delivery secret absence fails closed',missingSecret.status==='private_media_delivery_secret_not_configured'&&missingSecret.issued===false);

const staleDb=makeDb();
const staleEnv={DB:staleDb,MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:deliverySecret};
const staleIssued=await issueMemberPrivateMediaDeliveryGrant(
  staleEnv,
  session,
  mediaId,
  {now_seconds:2_000_000_000}
);
staleDb.state.linkedFamily='fam_B';
const staleVerified=await verifyMemberPrivateMediaDeliveryGrant(
  staleEnv,
  session,
  mediaId,
  staleIssued.grant,
  {now_seconds:2_000_000_030}
);
pass('grant verification rechecks current Family authorization',staleVerified.status==='family_access_denied'&&staleVerified.verified===false);

const httpDb=makeDb();
const cookie=await issueCookie(httpDb);
const httpEnv={
  DB:httpDb,
  MEMBER_SESSION_SECRET:sessionSecret,
  MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:deliverySecret
};

const noOrigin=await handleMemberPrivateMediaGrantRequest(
  grantRequest({media_id:mediaId},{cookie,requestOrigin:null}),
  httpEnv
);
pass('HTTP grant issue requires Origin',noOrigin.status===403);

const crossOrigin=await handleMemberPrivateMediaGrantRequest(
  grantRequest({media_id:mediaId},{cookie,requestOrigin:'https://evil.example'}),
  httpEnv
);
pass('HTTP grant issue rejects cross-origin',crossOrigin.status===403);

const noSession=await handleMemberPrivateMediaGrantRequest(
  grantRequest({media_id:mediaId}),
  httpEnv
);
pass('HTTP grant issue requires signed Member session Cookie',noSession.status===401);

const wrongType=await handleMemberPrivateMediaGrantRequest(
  grantRequest({media_id:mediaId},{cookie,contentType:'text/plain'}),
  httpEnv
);
pass('HTTP grant issue requires JSON',wrongType.status===415);

const injected=await handleMemberPrivateMediaGrantRequest(
  grantRequest({media_id:mediaId,family_id:'fam_B'},{cookie}),
  httpEnv
);
pass('HTTP grant body rejects Family/Customer injection',injected.status===400);

const okHttp=await handleMemberPrivateMediaGrantRequest(
  grantRequest({media_id:mediaId},{cookie}),
  httpEnv
);
const okBody=await okHttp.json();
pass('HTTP grant issue succeeds for authorized signed session',okHttp.status===200&&okBody.ok===true&&okBody.media.media_id===mediaId);
pass('HTTP grant response exposes no storage key or identity fields',
  !('storage_key' in okBody.media)
  && !('customer_id' in okBody)
  && !('family_id' in okBody)
);
pass('HTTP grant response is no-store/no-referrer',okHttp.headers.get('cache-control')==='no-store'&&okHttp.headers.get('referrer-policy')==='no-referrer');
pass('HTTP grant issue remains read-only',httpDb.state.writes.length===0);

const get=await handleMemberPrivateMediaGrantRequest(
  grantRequest(undefined,{cookie,method:'GET'}),
  httpEnv
);
pass('HTTP grant issue is POST only',get.status===405);

const health=memberPrivateMediaDeliveryGrantHealth({});
pass('health records short-lived token contract',health.token_version==='v1'&&health.token_ttl_seconds===120&&health.clock_skew_seconds===30);
pass('health records no identity/storage claims inside token',health.token_contains_customer_id===false&&health.token_contains_family_id===false&&health.token_contains_storage_key===false);
pass('health records exact signed binding inputs',health.signed_binding_inputs.join(',')==='family_id,customer_id,media_id,issued_at,expires_at');
pass('health records issue and verify both reauthorize private media',health.private_media_reauthorization_on_issue===true&&health.private_media_reauthorization_on_verify===true);
pass('health keeps grant out of URL and binary route inactive',health.content_delivery_method==='POST'&&health.grant_in_url===false&&health.binary_route_implemented===false);
pass('health records no Production route/storage fetch/write',health.production_route_wired===false&&health.production_storage_fetch===false&&health.production_write===false);

console.log(`MEMBER_PRIVATE_MEDIA_DELIVERY_GRANT=${n}/${n} PASS`);
