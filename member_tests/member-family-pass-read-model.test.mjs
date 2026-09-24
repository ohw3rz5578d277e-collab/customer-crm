import {
  computeCurrentFamilyPass,
  readMemberFamilyPassForSession,
  handleMemberFamilyPassReadRequest,
  memberFamilyPassReadHealth
} from '../src/member-family-pass-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

function makeDb({memoryCount=0,memorySchema=true,familySchema=true}={}){
  const tables=new Set([
    ...(familySchema?['customer_family_groups','customer_family_customer_links']:[]),
    ...(memorySchema?['member_memories']:[])
  ]);
  const seenSql=[];

  return {
    seenSql,
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
          if(sql.includes('COUNT(*) AS memory_count')){
            return state.params[0]===familyId?{memory_count:memoryCount}:{memory_count:0};
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
          return {results:[]};
        },
        async run(){throw new Error('Family Pass read model must remain read-only')}
      };
      return stmt;
    }
  };
}

const cases=[
  [0,'UNRANKED','FAMILY',1],
  [1,'FAMILY','WELCOME_BACK',1],
  [2,'WELCOME_BACK','SILVER',1],
  [3,'SILVER','GOLD',2],
  [4,'SILVER','GOLD',1],
  [5,'GOLD','BLACK',5],
  [9,'GOLD','BLACK',1],
  [10,'BLACK',null,0],
  [12,'BLACK',null,0]
];

for(const [count,current,next,toNext] of cases){
  const value=computeCurrentFamilyPass(count);
  pass(`tier boundary count=${count}`,value.current_tier===current&&value.next_tier===next&&value.memories_to_next===toNext);
}

const milestone2=computeCurrentFamilyPass(2);
pass('WELCOME BACK is achieved exactly at second MEMORY',milestone2.current_tier==='WELCOME_BACK'&&milestone2.milestones.find(x=>x.tier==='WELCOME_BACK')?.achieved===true);
pass('SILVER is not achieved before third MEMORY',milestone2.milestones.find(x=>x.tier==='SILVER')?.achieved===false);

const milestone10=computeCurrentFamilyPass(10);
pass('BLACK is currently qualified at ten MEMORIES',milestone10.black_currently_qualified===true&&milestone10.current_tier==='BLACK');
pass('BLACK lifetime persistence is not falsely claimed by count-only read model',milestone10.black_lifetime_persistence_supported===false);
pass('Family Pass entitlement enforcement remains disabled',milestone10.entitlement_enforcement_ready===false);

const db=makeDb({memoryCount:5});
const result=await readMemberFamilyPassForSession(
  {DB:db},
  {family_id:familyId,customer_id:customerId}
);
pass('authorized Family session reads Family Pass',result.status==='ok'&&result.family_pass.current_tier==='GOLD');
pass('Family Pass count is Family scoped',result.family_id===familyId&&result.customer_id===customerId&&result.family_pass.memory_count===5);
pass('count source explicitly uses one MEMORY as one shoot',result.count_source.one_memory_equals_one_shoot===true);
pass('count source is published-only and deleted-hidden',result.count_source.published_only===true&&result.count_source.deleted_hidden===true);
pass('BLACK benefit contract is goods 10 percent only',result.benefit_contract.black_photo_goods_discount_percent===10&&result.benefit_contract.applies_to_shooting_fee===false);
pass('BLACK benefit is not enforced by read model',result.benefit_contract.enforcement_ready===false);
pass('read model executes zero writes',result.read_only===true);

const countSql=db.seenSql.find(sql=>sql.includes('COUNT(*) AS memory_count'))||'';
pass('SQL counts only exact Family ID',countSql.includes('WHERE family_id=?'));
pass('SQL counts only published MEMORY rows',countSql.includes('published=1'));
pass('SQL excludes deleted MEMORY rows',countSql.includes("COALESCE(deleted_at,'')=''"));

const denied=await readMemberFamilyPassForSession(
  {DB:makeDb({memoryCount:10})},
  {family_id:'fam_B',customer_id:customerId}
);
pass('cross-Family session is denied before count is returned',denied.status==='family_access_denied');

const missingMemorySchema=await readMemberFamilyPassForSession(
  {DB:makeDb({memorySchema:false})},
  {family_id:familyId,customer_id:customerId}
);
pass('missing Member MEMORY schema is explicit',missingMemorySchema.status==='member_memory_schema_not_applied');

const missingFamilySchema=await readMemberFamilyPassForSession(
  {DB:makeDb({familySchema:false})},
  {family_id:familyId,customer_id:customerId}
);
pass('missing Family schema cannot be bypassed',missingFamilySchema.status==='schema_not_applied');

const noSession=await handleMemberFamilyPassReadRequest(
  new Request('https://example.test/api/internal/member/family-pass'),
  {DB:makeDb({memoryCount:3})},
  null
);
pass('Family Pass HTTP contract requires server Member session',noSession.status===401);

const post=await handleMemberFamilyPassReadRequest(
  new Request('https://example.test/api/internal/member/family-pass',{method:'POST'}),
  {DB:makeDb({memoryCount:3})},
  {family_id:familyId,customer_id:customerId}
);
pass('Family Pass HTTP contract is GET only',post.status===405);

const okResponse=await handleMemberFamilyPassReadRequest(
  new Request('https://example.test/api/internal/member/family-pass'),
  {DB:makeDb({memoryCount:3})},
  {family_id:familyId,customer_id:customerId}
);
const okBody=await okResponse.json();
pass('Family Pass HTTP contract returns family-scoped tier',okResponse.status===200&&okBody.family_pass.current_tier==='SILVER'&&okBody.family_id===familyId);

const health=memberFamilyPassReadHealth();
pass('health records canonical thresholds',health.thresholds.family===1&&health.thresholds.welcome_back===2&&health.thresholds.silver===3&&health.thresholds.gold===5&&health.thresholds.black===10);
pass('health records server session and explicit Family link requirement',health.session_identity_source==='server_verified_member_session'&&health.explicit_family_link_required===true);
pass('health records count source and one MEMORY equals one shoot',health.count_source==='published_non_deleted_member_memories'&&health.one_memory_equals_one_shoot===true);
pass('health records durable BLACK source support without Production schema apply',health.black_lifetime_persistence_supported===true&&health.production_entitlement_schema_apply_executed===false);
pass('health records BLACK goods benefit without enabling enforcement',health.black_goods_discount_percent===10&&health.black_shooting_fee_discount===false&&health.black_benefit_enforcement_ready===false);
pass('health records read-only and no Production route/write',health.read_only===true&&health.production_route_wired===false&&health.production_write===false);

console.log(`MEMBER_FAMILY_PASS_READ_MODEL=${n}/${n} PASS`);
