import fs from 'node:fs';
import {
  readFavoriteRowsForAuthorizedMember,
  readMemberFavoritesForSession,
  handleMemberFavoritesReadRequest,
  memberFavoritesReadHealth
} from '../src/member-favorites-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const otherCustomerId='26000999';
const familyId='fam_A';

const memories=[
  {memory_id:'mem_A1',family_id:familyId,published:1,deleted_at:''},
  {memory_id:'mem_A2',family_id:familyId,published:1,deleted_at:''},
  {memory_id:'mem_A_draft',family_id:familyId,published:0,deleted_at:''},
  {memory_id:'mem_A_deleted',family_id:familyId,published:1,deleted_at:'2026-01-01'},
  {memory_id:'mem_B1',family_id:'fam_B',published:1,deleted_at:''}
];

const favorites=[
  {family_id:familyId,customer_id:customerId,memory_id:'mem_A1',created_at:'2026-09-25T00:00:00Z'},
  {family_id:familyId,customer_id:customerId,memory_id:'mem_A_draft',created_at:'2026-09-24T00:00:00Z'},
  {family_id:familyId,customer_id:customerId,memory_id:'mem_A_deleted',created_at:'2026-09-23T00:00:00Z'},
  {family_id:familyId,customer_id:otherCustomerId,memory_id:'mem_A2',created_at:'2026-09-22T00:00:00Z'},
  {family_id:'fam_B',customer_id:customerId,memory_id:'mem_B1',created_at:'2026-09-21T00:00:00Z'}
];

function makeDb({favoriteSchema=true,memorySchema=true,familySchema=true}={}){
  const tables=new Set([
    ...(familySchema?['customer_family_groups','customer_family_customer_links']:[]),
    ...(memorySchema?['member_memories']:[]),
    ...(favoriteSchema?['member_memory_favorites']:[])
  ]);
  const seenSql=[];
  const writes=[];
  return {
    seenSql,writes,
    prepare(sql){
      seenSql.push(sql);
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master'))return tables.has(state.params[0])?{name:state.params[0]}:null;
          if(sql.includes('FROM customer_family_groups')){
            return state.params[0]===familyId
              ?{family_id:familyId,display_name:'A FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
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
          if(sql.includes('FROM member_memory_favorites')){
            const [requestedFamily,requestedCustomer]=state.params;
            return {results:favorites
              .filter(row=>row.family_id===requestedFamily&&row.customer_id===requestedCustomer)
              .filter(row=>{
                const memory=memories.find(item=>item.memory_id===row.memory_id&&item.family_id===row.family_id);
                return memory?.published===1&&!memory?.deleted_at;
              })
              .map(row=>({memory_id:row.memory_id,created_at:row.created_at}))};
          }
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('Favorites read model must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const db=makeDb();
const result=await readMemberFavoritesForSession(
  {DB:db},
  {family_id:familyId,customer_id:customerId}
);
pass('authorized Member Favorite read succeeds',result.status==='ok'&&result.family_id===familyId&&result.customer_id===customerId);
pass('only this Member customer published visible Favorite remains',result.favorite_count===1&&result.favorites[0].memory_id==='mem_A1');
pass('Favorite read remains immutable',result.mutable===false&&result.read_only===true);
pass('Favorite source scope is family customer memory',result.source.ownership_scope==='family_customer_memory');
pass('Favorite read performs zero writes',db.writes.length===0);

const sql=db.seenSql.find(x=>x.includes('FROM member_memory_favorites'))||'';
pass('Favorite query binds exact Family and Customer',sql.includes('f.family_id=?')&&sql.includes('f.customer_id=?'));
pass('Favorite query joins visible published MEMORY',sql.includes('INNER JOIN member_memories')&&sql.includes('m.published=1')&&sql.includes("COALESCE(m.deleted_at,'')=''"));

const authorizedHelper=await readFavoriteRowsForAuthorizedMember(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
pass('authorized helper returns exact favorite ids',authorizedHelper.status==='ok'&&authorizedHelper.favorites.map(x=>x.memory_id).join(',')==='mem_A1');

const missingFavorite=await readMemberFavoritesForSession(
  {DB:makeDb({favoriteSchema:false})},
  {family_id:familyId,customer_id:customerId}
);
pass('missing Favorite schema explicit',missingFavorite.status==='favorites_schema_not_applied');

const missingMemory=await readMemberFavoritesForSession(
  {DB:makeDb({memorySchema:false})},
  {family_id:familyId,customer_id:customerId}
);
pass('missing MEMORY schema explicit',missingMemory.status==='member_memory_schema_not_applied');

const wrongFamily=await readMemberFavoritesForSession(
  {DB:makeDb()},
  {family_id:'fam_B',customer_id:customerId}
);
pass('session Family mismatch fails closed',wrongFamily.status==='family_access_denied');

const noSession=await handleMemberFavoritesReadRequest(
  new Request('https://example.test/api/internal/member/favorites'),
  {DB:makeDb()},
  null
);
pass('Favorites HTTP requires Member session',noSession.status===401);

const post=await handleMemberFavoritesReadRequest(
  new Request('https://example.test/api/internal/member/favorites',{method:'POST'}),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
pass('Favorites HTTP is GET only',post.status===405);

const override=await handleMemberFavoritesReadRequest(
  new Request('https://example.test/api/internal/member/favorites?customer_id=26000999&family_id=fam_B'),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
const overrideBody=await override.json();
pass('request identity cannot override server session',override.status===200&&overrideBody.family_id===familyId&&overrideBody.customer_id===customerId&&overrideBody.favorite_count===1);

const health=memberFavoritesReadHealth();
pass('health records per-customer preference not shared Family preference',health.per_customer_preferences===true&&health.shared_family_preference===false);
pass('health records exact three-part ownership scope',health.ownership_scope==='family_customer_memory');
pass('health keeps mutation disabled',health.favorite_mutation_ready===false&&health.automatic_write===false);
pass('health records no route/write/send',health.production_route_wired===false&&health.production_write===false&&health.line_send===false);

const migration=fs.readFileSync(
  'migrations_managed/20260924_member_memory_favorites_foundation.sql',
  'utf8'
);
const schemaOnly=migration.replace(/^\s*--.*$/gm,'');
pass('Favorite migration is additive only',!/\b(DROP|ALTER|DELETE|UPDATE|INSERT)\b/i.test(schemaOnly));
pass('Favorite schema keys exact Family Customer MEMORY',schemaOnly.includes('PRIMARY KEY (family_id, customer_id, memory_id)'));
pass('Favorite schema contains no fuzzy identity fields',!/\b(name|phone|email|address|line_user_id)\b/i.test(schemaOnly));

console.log(`MEMBER_FAVORITES_READ_MODEL=${n}/${n} PASS`);
