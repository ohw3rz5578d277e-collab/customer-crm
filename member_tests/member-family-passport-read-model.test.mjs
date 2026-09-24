import {
  readMemberFamilyPassportForSession,
  handleMemberFamilyPassportReadRequest,
  memberFamilyPassportReadHealth,
  __test
} from '../src/member-family-passport-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

function makeDb({rows=[],memorySchema=true,familySchema=true}={}){
  const tables=new Set([
    ...(familySchema?['customer_family_groups','customer_family_customer_links']:[]),
    ...(memorySchema?['member_memories']:[])
  ]);
  const seenSql=[];
  const writes=[];

  return {
    seenSql,
    writes,
    prepare(sql){
      seenSql.push(sql);
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')){
            return tables.has(state.params[0])?{name:state.params[0]}:null;
          }
          if(sql.includes('FROM customer_family_groups')){
            return state.params[0]===familyId
              ?{family_id:familyId,display_name:'YAMADA FAMILY',status:'active',created_at:'',updated_at:''}
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
              ?{results:[{customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          if(sql.includes('FROM member_memories')){
            return {
              results:state.params[0]===familyId?rows.filter(x=>x.family_id===familyId):[]
            };
          }
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('Family Passport read model must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const genreCases=[
  ['お宮参り','OMIYAMAIRI'],
  ['七五三','SHICHIGOSAN'],
  ['1歳','FIRST_BIRTHDAY'],
  ['１歳','FIRST_BIRTHDAY'],
  ['1歳バースデー','FIRST_BIRTHDAY'],
  ['ファーストバースデー','FIRST_BIRTHDAY'],
  ['1st Birthday','FIRST_BIRTHDAY'],
  ['FIRST BIRTHDAY','FIRST_BIRTHDAY'],
  ['入学','SCHOOL_ENTRANCE'],
  ['入学撮影','SCHOOL_ENTRANCE'],
  ['入学記念','SCHOOL_ENTRANCE']
];

for(const [genre,expected] of genreCases){
  pass(`explicit genre alias maps: ${genre}`,__test.milestoneForGenre(genre)===expected);
}

pass('generic バースデー does not imply first birthday',__test.milestoneForGenre('バースデー')===null);
pass('2歳バースデー does not imply first birthday',__test.milestoneForGenre('2歳バースデー')===null);
pass('入学・卒業 does not imply school entrance',__test.milestoneForGenre('入学・卒業')===null);
pass('genre substring does not imply milestone',__test.milestoneForGenre('七五三後のファミリー')===null);

const pure=__test.buildPassport([
  {memory_id:'mem_01',shoot_date:'2024-02-01',genre:'お宮参り'},
  {memory_id:'mem_02',shoot_date:'2025-02-01',genre:'1歳バースデー'},
  {memory_id:'mem_03',shoot_date:'2026-11-15',genre:'七五三'},
  {memory_id:'mem_04',shoot_date:'2027-04-05',genre:'入学'},
  {memory_id:'mem_05',shoot_date:'2028-01-01',genre:'バースデー'}
]);

pass('all four explicit milestones can be achieved',pure.achieved_count===4&&pure.all_achieved===true);
pass('Passport total milestone count is four',pure.total_milestones===4&&pure.completion_ratio===1);
pass('unmatched genre is surfaced rather than inferred',pure.unmatched_genres.length===1&&pure.unmatched_genres[0]==='バースデー');
pass('Passport explicitly remains Family-level not child-specific',pure.child_specific===false);

const duplicated=__test.buildPassport([
  {memory_id:'mem_10',shoot_date:'2024-01-10',genre:'七五三'},
  {memory_id:'mem_11',shoot_date:'2025-01-10',genre:'七五三'}
]);
const shichi=duplicated.milestones.find(x=>x.code==='SHICHIGOSAN');
pass('multiple qualifying MEMORIES preserve evidence count',shichi.evidence_count===2);
pass('first and latest achieved dates are deterministic',shichi.first_achieved_date==='2024-01-10'&&shichi.latest_achieved_date==='2025-01-10');
pass('evidence includes MEMORY IDs only from qualifying rows',shichi.evidence_memory_ids.join(',')==='mem_10,mem_11');

const rows=[
  {memory_id:'mem_a',family_id:familyId,shoot_date:'2024-05-01',genre:'お宮参り'},
  {memory_id:'mem_b',family_id:familyId,shoot_date:'2025-05-01',genre:'1歳'},
  {memory_id:'mem_c',family_id:familyId,shoot_date:'2026-05-01',genre:'ファミリー'}
];
const db=makeDb({rows});
const result=await readMemberFamilyPassportForSession(
  {DB:db},
  {family_id:familyId,customer_id:customerId}
);
pass('authorized Family session reads Passport',result.status==='ok'&&result.family_id===familyId&&result.customer_id===customerId);
pass('read model derives only explicit achieved milestones',result.passport.achieved_count===2);
pass('unknown generic Family genre remains unmatched',result.passport.unmatched_genres.includes('ファミリー'));
pass('read model reports published/deleted source contract',result.source.published_only===true&&result.source.deleted_hidden===true&&result.source.family_scoped===true);
pass('read model executes zero writes',db.writes.length===0&&result.read_only===true);

const memorySql=db.seenSql.find(sql=>sql.includes('FROM member_memories'))||'';
pass('Passport SQL is exact Family scoped',memorySql.includes('WHERE family_id=?'));
pass('Passport SQL requires published MEMORY',memorySql.includes('published=1'));
pass('Passport SQL hides deleted MEMORY',memorySql.includes("COALESCE(deleted_at,'')=''"));

const denied=await readMemberFamilyPassportForSession(
  {DB:makeDb({rows})},
  {family_id:'fam_B',customer_id:customerId}
);
pass('cross-Family session is denied',denied.status==='family_access_denied');

const missingMemory=await readMemberFamilyPassportForSession(
  {DB:makeDb({rows,memorySchema:false})},
  {family_id:familyId,customer_id:customerId}
);
pass('missing MEMORY schema is explicit',missingMemory.status==='member_memory_schema_not_applied');

const missingFamily=await readMemberFamilyPassportForSession(
  {DB:makeDb({rows,familySchema:false})},
  {family_id:familyId,customer_id:customerId}
);
pass('missing Family schema cannot be bypassed',missingFamily.status==='schema_not_applied');

const noSession=await handleMemberFamilyPassportReadRequest(
  new Request('https://example.test/api/internal/member/family-passport'),
  {DB:makeDb({rows})},
  null
);
pass('Passport HTTP contract requires server Member session',noSession.status===401);

const post=await handleMemberFamilyPassportReadRequest(
  new Request('https://example.test/api/internal/member/family-passport',{method:'POST'}),
  {DB:makeDb({rows})},
  {family_id:familyId,customer_id:customerId}
);
pass('Passport HTTP contract is GET only',post.status===405);

const okResponse=await handleMemberFamilyPassportReadRequest(
  new Request('https://example.test/api/internal/member/family-passport'),
  {DB:makeDb({rows})},
  {family_id:familyId,customer_id:customerId}
);
const okBody=await okResponse.json();
pass('Passport HTTP contract returns Family-scoped milestones',okResponse.status===200&&okBody.passport.achieved_count===2&&okBody.family_id===familyId);

const health=memberFamilyPassportReadHealth();
pass('health records explicit alias matching only',health.genre_matching==='explicit_alias_only'&&health.substring_genre_inference===false);
pass('health refuses generic birthday inference',health.generic_birthday_implies_first_birthday===false);
pass('health refuses combined school genre inference',health.combined_school_genre_implies_entrance===false);
pass('health records Family-level not child-specific',health.child_specific===false);
pass('health records read-only no Production route/write',health.read_only===true&&health.production_route_wired===false&&health.production_write===false);

console.log(`MEMBER_FAMILY_PASSPORT_READ_MODEL=${n}/${n} PASS`);
