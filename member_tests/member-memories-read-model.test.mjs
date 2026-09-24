import {
  readMemberMemoriesForSession,
  readMemberMemoryDetailForSession,
  handleMemberMemoriesReadRequest,
  memberMemoriesReadHealth
} from '../src/member-memories-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

const memories=[
  {memory_id:'mem_A_new',family_id:familyId,shoot_date:'2026-03-03',genre:'七五三',title:'七五三',amazon_photos_url:'https://example.com/new',published:1,created_at:'2026-03-04',updated_at:'2026-03-04',deleted_at:''},
  {memory_id:'mem_A_old',family_id:familyId,shoot_date:'2025-01-10',genre:'バースデー',title:'1st Birthday',amazon_photos_url:'http://unsafe.example.com/old',published:1,created_at:'2025-01-11',updated_at:'2025-01-11',deleted_at:''},
  {memory_id:'mem_A_draft',family_id:familyId,shoot_date:'2026-04-01',genre:'家族',title:'Draft',amazon_photos_url:'https://example.com/draft',published:0,created_at:'2026-04-01',updated_at:'2026-04-01',deleted_at:''},
  {memory_id:'mem_A_deleted',family_id:familyId,shoot_date:'2024-04-01',genre:'入学',title:'Deleted',amazon_photos_url:'https://example.com/deleted',published:1,created_at:'2024-04-01',updated_at:'2024-04-01',deleted_at:'2026-01-01'},
  {memory_id:'mem_B',family_id:'fam_B',shoot_date:'2026-02-02',genre:'成人',title:'B Family',amazon_photos_url:'https://example.com/b',published:1,created_at:'2026-02-03',updated_at:'2026-02-03',deleted_at:''}
];

const media=[
  {media_id:'media_preview',memory_id:'mem_A_new',family_id:familyId,storage_key:'private/a-preview.jpg',media_type:'image',role:'preview',sort_order:1,width:1200,height:800,deleted_at:''},
  {media_id:'media_cover',memory_id:'mem_A_new',family_id:familyId,storage_key:'private/a-cover.jpg',media_type:'image',role:'cover',sort_order:9,width:1200,height:800,deleted_at:''},
  {media_id:'media_deleted',memory_id:'mem_A_new',family_id:familyId,storage_key:'private/deleted.jpg',media_type:'image',role:'preview',sort_order:0,width:1200,height:800,deleted_at:'2026-02-01'},
  {media_id:'media_b',memory_id:'mem_B',family_id:'fam_B',storage_key:'private/b.jpg',media_type:'image',role:'cover',sort_order:0,width:1200,height:800,deleted_at:''}
];

function makeDb({schema=true}={}){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    ...(schema?['member_memories','member_memory_media']:[])
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
          if(sql.includes('FROM member_memories')){
            const [memoryId,requestedFamily]=state.params;
            return memories.find(row=>
              row.memory_id===memoryId
              && row.family_id===requestedFamily
              && row.published===1
              && !row.deleted_at
            )||null;
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
          if(sql.includes('FROM member_memories')){
            const requestedFamily=state.params[0];
            return {results:memories
              .filter(row=>row.family_id===requestedFamily&&row.published===1&&!row.deleted_at)
              .sort((a,b)=>String(b.shoot_date).localeCompare(String(a.shoot_date)))};
          }
          if(sql.includes('FROM member_memory_media')){
            if(state.params.length===1){
              const requestedFamily=state.params[0];
              return {results:media.filter(row=>row.family_id===requestedFamily&&!row.deleted_at)};
            }
            const [memoryId,requestedFamily]=state.params;
            return {results:media.filter(row=>row.memory_id===memoryId&&row.family_id===requestedFamily&&!row.deleted_at)};
          }
          return {results:[]};
        },
        async run(){throw new Error('Member MEMORIES read model must remain read-only')}
      };
      return stmt;
    }
  };
}

const env={DB:makeDb()};

const list=await readMemberMemoriesForSession(env,{family_id:familyId,customer_id:customerId});
pass('family-scoped MEMORY list succeeds',list.status==='ok'&&list.family_id===familyId);
pass('list includes only published non-deleted family MEMORIES',list.memories.length===2&&list.memories.every(x=>x.memory_id.startsWith('mem_A_')));
pass('list is newest shoot first',list.memories[0].memory_id==='mem_A_new'&&list.memories[1].memory_id==='mem_A_old');
pass('cover prefers cover role over lower sort preview',list.memories[0].cover?.media_id==='media_cover');
pass('deleted media is excluded from preview count',list.memories[0].preview_count===2);
pass('unsafe Amazon URL is not exposed',list.memories.find(x=>x.memory_id==='mem_A_old')?.amazon_photos_available===false);
pass('list exposes no private storage keys',!JSON.stringify(list).includes('storage_key')&&!JSON.stringify(list).includes('private/a-cover.jpg'));

const detail=await readMemberMemoryDetailForSession(env,{family_id:familyId,customer_id:customerId},'mem_A_new');
pass('authorized MEMORY detail succeeds',detail.status==='ok'&&detail.memory.memory_id==='mem_A_new');
pass('detail returns exact-family media only',detail.media.length===2&&detail.media.every(x=>x.media_id!=='media_b'));
pass('detail exposes Amazon Photos only after family authorization',detail.amazon_link?.provider==='amazon_photos'&&detail.amazon_link?.url==='https://example.com/new');
pass('detail exposes no private storage keys',!JSON.stringify(detail).includes('storage_key'));

const otherFamilyMemory=await readMemberMemoryDetailForSession(env,{family_id:familyId,customer_id:customerId},'mem_B');
pass('another family memory is indistinguishable from missing',otherFamilyMemory.status==='memory_not_found');

const wrongFamily=await readMemberMemoriesForSession(env,{family_id:'fam_B',customer_id:customerId});
pass('session family mismatch fails closed before MEMORY rows',wrongFamily.status==='family_access_denied'&&wrongFamily.memories.length===0);

const schemaMissing=await readMemberMemoriesForSession({DB:makeDb({schema:false})},{family_id:familyId,customer_id:customerId});
pass('unapplied Member schema fails without writes',schemaMissing.status==='member_memory_schema_not_applied');

const noSession=await handleMemberMemoriesReadRequest(
  new Request('https://example.test/api/internal/member/memories'),
  env,
  null
);
pass('read API requires server-supplied Member session',noSession.status===401);

const tamperedQuery=await handleMemberMemoriesReadRequest(
  new Request('https://example.test/api/internal/member/memories?customer_id=26000999&family_id=fam_B'),
  env,
  {family_id:familyId,customer_id:customerId}
);
const tamperedBody=await tamperedQuery.json();
pass('request customer/family query parameters do not override server session',tamperedQuery.status===200&&tamperedBody.family_id===familyId&&tamperedBody.memories.every(x=>x.memory_id!=='mem_B'));

const crossFamilyHttp=await handleMemberMemoriesReadRequest(
  new Request('https://example.test/api/internal/member/memories'),
  env,
  {family_id:'fam_B',customer_id:customerId}
);
pass('cross-family session request returns 403',crossFamilyHttp.status===403);

const detailHttp=await handleMemberMemoriesReadRequest(
  new Request('https://example.test/api/internal/member/memories/mem_B'),
  env,
  {family_id:familyId,customer_id:customerId}
);
pass('cross-family MEMORY id returns 404',detailHttp.status===404);

const post=await handleMemberMemoriesReadRequest(
  new Request('https://example.test/api/internal/member/memories',{method:'POST'}),
  env,
  {family_id:familyId,customer_id:customerId}
);
pass('MEMORIES read API is GET-only',post.status===405);

const health=memberMemoriesReadHealth();
pass('health contract records no route wiring or Production write',health.read_only===true&&health.production_route_wired===false&&health.production_write===false);
pass('health contract forbids request-supplied identity',health.request_customer_id_input===false&&health.request_family_id_input===false);
pass('health contract keeps private storage key hidden',health.private_storage_key_exposed===false);

console.log(`MEMBER_MEMORIES_READ_MODEL=${n}/${n} PASS`);
