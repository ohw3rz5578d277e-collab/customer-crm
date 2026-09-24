import {
  authorizeMemberPrivateMediaAccess,
  memberPrivateMediaAccessHealth,
  __test
} from '../src/member-private-media-access.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

const memories=[
  {memory_id:'mem_A',family_id:familyId,published:1,deleted_at:''},
  {memory_id:'mem_A_draft',family_id:familyId,published:0,deleted_at:''},
  {memory_id:'mem_A_deleted',family_id:familyId,published:1,deleted_at:'2026-09-01'},
  {memory_id:'mem_B',family_id:'fam_B',published:1,deleted_at:''}
];

const media=[
  {media_id:'media_A',memory_id:'mem_A',family_id:familyId,storage_key:'member/fam_A/mem_A/cover.jpg',media_type:'image',role:'cover',width:1200,height:800,deleted_at:''},
  {media_id:'media_A_deleted',memory_id:'mem_A',family_id:familyId,storage_key:'member/fam_A/mem_A/deleted.jpg',media_type:'image',role:'preview',width:1200,height:800,deleted_at:'2026-09-02'},
  {media_id:'media_A_draft',memory_id:'mem_A_draft',family_id:familyId,storage_key:'member/fam_A/mem_A_draft/cover.jpg',media_type:'image',role:'cover',width:1200,height:800,deleted_at:''},
  {media_id:'media_A_parent_deleted',memory_id:'mem_A_deleted',family_id:familyId,storage_key:'member/fam_A/mem_A_deleted/cover.jpg',media_type:'image',role:'cover',width:1200,height:800,deleted_at:''},
  {media_id:'media_B',memory_id:'mem_B',family_id:'fam_B',storage_key:'member/fam_B/mem_B/cover.jpg',media_type:'image',role:'cover',width:1200,height:800,deleted_at:''},
  {media_id:'media_bad_key',memory_id:'mem_A',family_id:familyId,storage_key:'https://public.example.com/file.jpg',media_type:'image',role:'preview',width:1200,height:800,deleted_at:''}
];

function makeDb({familySchema=true,memorySchema=true}={}){
  const tables=new Set([
    ...(familySchema?['customer_family_groups','customer_family_customer_links']:[]),
    ...(memorySchema?['member_memories','member_memory_media']:[])
  ]);

  return {
    prepare(sql){
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')){
            return tables.has(state.params[0])?{name:state.params[0]}:null;
          }
          if(sql.includes('FROM customer_family_groups')){
            return state.params[0]===familyId
              ?{family_id:familyId,display_name:'A FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          if(sql.includes('FROM member_memory_media mm')){
            const [mediaId,requestedFamily]=state.params;
            const row=media.find(x=>x.media_id===mediaId&&x.family_id===requestedFamily&&!x.deleted_at);
            if(!row)return null;
            const memory=memories.find(x=>x.memory_id===row.memory_id&&x.family_id===row.family_id&&x.published===1&&!x.deleted_at);
            return memory?{...row}:null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            return state.params[0]===customerId
              ?{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return state.params[0]===familyId
              ?{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          return {results:[]};
        },
        async run(){throw new Error('private media authorization must remain read-only')}
      };
      return stmt;
    }
  };
}

const env={DB:makeDb()};

const ok=await authorizeMemberPrivateMediaAccess(env,{family_id:familyId,customer_id:customerId},'media_A');
pass('authorized family can resolve private media descriptor',ok.status==='ok'&&ok.authorized===true);
pass('descriptor stays internal and contains opaque storage key',ok.media?.storage_key==='member/fam_A/mem_A/cover.jpg');
pass('authorization path is read-only',ok.read_only===true);

const wrongFamily=await authorizeMemberPrivateMediaAccess(env,{family_id:'fam_B',customer_id:customerId},'media_A');
pass('session family mismatch fails closed',wrongFamily.status==='family_access_denied'&&wrongFamily.authorized===false);

const otherFamilyMedia=await authorizeMemberPrivateMediaAccess(env,{family_id:familyId,customer_id:customerId},'media_B');
pass('other-family media is indistinguishable from missing',otherFamilyMedia.status==='media_not_found'&&otherFamilyMedia.authorized===false);

const deletedMedia=await authorizeMemberPrivateMediaAccess(env,{family_id:familyId,customer_id:customerId},'media_A_deleted');
pass('deleted media is hidden',deletedMedia.status==='media_not_found');

const draftParent=await authorizeMemberPrivateMediaAccess(env,{family_id:familyId,customer_id:customerId},'media_A_draft');
pass('media under unpublished MEMORY is hidden',draftParent.status==='media_not_found');

const deletedParent=await authorizeMemberPrivateMediaAccess(env,{family_id:familyId,customer_id:customerId},'media_A_parent_deleted');
pass('media under deleted MEMORY is hidden',deletedParent.status==='media_not_found');

const badKey=await authorizeMemberPrivateMediaAccess(env,{family_id:familyId,customer_id:customerId},'media_bad_key');
pass('absolute URL storage key fails closed',badKey.status==='invalid_private_storage_key'&&badKey.review_required===true);

const noMemorySchema=await authorizeMemberPrivateMediaAccess(
  {DB:makeDb({memorySchema:false})},
  {family_id:familyId,customer_id:customerId},
  'media_A'
);
pass('missing Member memory schema returns explicit status',noMemorySchema.status==='member_memory_schema_not_applied');

const noFamilySchema=await authorizeMemberPrivateMediaAccess(
  {DB:makeDb({familySchema:false})},
  {family_id:familyId,customer_id:customerId},
  'media_A'
);
pass('missing Family schema is not bypassed',noFamilySchema.status==='schema_not_applied');

pass('storage key validator accepts opaque nested key',__test.validPrivateStorageKey('member/fam_A/mem_A/cover.jpg')===true);
pass('storage key validator rejects traversal',__test.validPrivateStorageKey('member/fam_A/../secret.jpg')===false);
pass('storage key validator rejects absolute path',__test.validPrivateStorageKey('/member/fam_A/file.jpg')===false);
pass('storage key validator rejects public URL',__test.validPrivateStorageKey('https://example.com/file.jpg')===false);

const health=memberPrivateMediaAccessHealth();
pass('health records session and family requirements',health.server_verified_member_session_required===true&&health.explicit_family_link_required===true);
pass('health records no Production route/storage fetch/write',health.production_route_wired===false&&health.production_storage_fetch===false&&health.production_write===false);
pass('health records storage key internal-only contract',health.storage_key_internal_only===true&&health.storage_key_public_response===false);

console.log(`MEMBER_PRIVATE_MEDIA_ACCESS=${n}/${n} PASS`);
