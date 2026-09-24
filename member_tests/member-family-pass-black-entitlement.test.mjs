import {
  readMemberFamilyPassForSession,
  computeCurrentFamilyPass,
  memberFamilyPassReadHealth,
  __test as passTest
} from '../src/member-family-pass-read-model.mjs';
import {
  buildMemberFamilyPassBlackAchievementPlan,
  memberFamilyPassBlackAchievementPlanHealth
} from '../src/member-family-pass-black-achievement-plan.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

function makeDb({
  memoryCount=10,
  entitlementSchema=true,
  entitlement=null,
  familyLinked=true
}={}){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    'member_memories',
    ...(entitlementSchema?['member_family_pass_entitlements']:[])
  ]);
  const writes=[];

  return {
    writes,
    prepare(sql){
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')){
            return tables.has(state.params[0])?{name:state.params[0]}:null;
          }
          if(sql.includes('FROM customer_family_groups')){
            return state.params[0]===familyId&&familyLinked
              ?{family_id:familyId,display_name:'YAMADA FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          if(sql.includes('COUNT(*) AS memory_count')){
            return state.params[0]===familyId?{memory_count:memoryCount}:{memory_count:0};
          }
          if(sql.includes('FROM member_family_pass_entitlements')){
            return state.params[0]===familyId&&entitlement?{...entitlement,family_id:familyId}:null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            return familyLinked&&state.params[0]===customerId
              ?{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return familyLinked&&state.params[0]===familyId
              ?{results:[{customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          return {results:[]};
        },
        async run(){writes.push({sql,params:state.params});throw new Error('BLACK entitlement foundation must remain read-only')}
      };
      return stmt;
    }
  };
}

const noSchemaDb=makeDb({memoryCount:10,entitlementSchema:false});
const noSchema=await readMemberFamilyPassForSession(
  {DB:noSchemaDb},
  {family_id:familyId,customer_id:customerId}
);
pass('10 current MEMORIES still compute BLACK without entitlement schema',noSchema.status==='ok'&&noSchema.family_pass.current_tier==='BLACK'&&noSchema.family_pass.black_currently_qualified===true);
pass('missing entitlement schema is honestly reported',noSchema.entitlement.schema_applied===false&&noSchema.family_pass.black_lifetime_persistence_supported===false);
pass('count-only BLACK does not invent durable entitlement',noSchema.family_pass.black_lifetime_entitled===false&&noSchema.family_pass.tier_basis==='current_memory_count');

const blocked=await buildMemberFamilyPassBlackAchievementPlan(
  {DB:noSchemaDb},
  {family_id:familyId,customer_id:customerId}
);
pass('BLACK persistence plan blocks until entitlement schema exists',blocked.status==='black_entitlement_schema_not_applied'&&blocked.ready===false&&blocked.write_required===false);
pass('blocked plan executes zero writes',blocked.write_executed===false&&noSchemaDb.writes.length===0);

const eligibleDb=makeDb({memoryCount:10,entitlementSchema:true,entitlement:null});
const eligible=await buildMemberFamilyPassBlackAchievementPlan(
  {DB:eligibleDb},
  {family_id:familyId,customer_id:customerId}
);
pass('10 MEMORIES with schema and no durable row proposes BLACK award',eligible.status==='ok'&&eligible.action==='award_black_lifetime'&&eligible.write_required===true);
pass('award proposal is exact Family scoped',eligible.proposed_record.family_id===familyId&&eligible.proposed_record.qualifying_memory_count===10);
pass('award proposal uses published MEMORY source',eligible.proposed_record.achievement_source==='published-member-memories');
pass('award plan itself performs zero writes',eligible.write_executed===false&&eligibleDb.writes.length===0);

const belowDb=makeDb({memoryCount:9,entitlementSchema:true,entitlement:null});
const below=await buildMemberFamilyPassBlackAchievementPlan(
  {DB:belowDb},
  {family_id:familyId,customer_id:customerId}
);
pass('below BLACK threshold produces no award',below.status==='ok'&&below.action==='none'&&below.reason==='black_threshold_not_reached'&&below.write_required===false);

const durableRow={
  black_lifetime:1,
  black_achieved_at:'2026-09-01T00:00:00.000Z',
  qualifying_memory_count:10,
  achievement_source:'published-member-memories'
};
const durable9Db=makeDb({memoryCount:9,entitlementSchema:true,entitlement:durableRow});
const durable9=await readMemberFamilyPassForSession(
  {DB:durable9Db},
  {family_id:familyId,customer_id:customerId}
);
pass('durable BLACK remains BLACK when current count falls to 9',durable9.status==='ok'&&durable9.family_pass.current_tier==='BLACK'&&durable9.family_pass.memory_count===9);
pass('durable BLACK distinguishes current qualification from lifetime entitlement',durable9.family_pass.black_currently_qualified===false&&durable9.family_pass.black_lifetime_entitled===true);
pass('durable BLACK uses entitlement tier basis',durable9.family_pass.tier_basis==='durable_black_entitlement'&&durable9.family_pass.effective_black===true);
pass('durable BLACK preserves achieved timestamp',durable9.family_pass.black_achieved_at==='2026-09-01T00:00:00.000Z');
pass('durable BLACK marks all historical milestones achieved',durable9.family_pass.milestones.every(x=>x.achieved===true));

const persistedPlan=await buildMemberFamilyPassBlackAchievementPlan(
  {DB:durable9Db},
  {family_id:familyId,customer_id:customerId}
);
pass('already persisted BLACK becomes idempotent no-op even below current threshold',persistedPlan.status==='ok'&&persistedPlan.action==='none'&&persistedPlan.reason==='black_already_persisted'&&persistedPlan.write_required===false);

const durable5Db=makeDb({memoryCount:5,entitlementSchema:true,entitlement:durableRow});
const durable5=await readMemberFamilyPassForSession(
  {DB:durable5Db},
  {family_id:familyId,customer_id:customerId}
);
pass('lifetime BLACK never ranks down to GOLD after data correction',durable5.family_pass.current_tier==='BLACK'&&durable5.family_pass.black_lifetime_entitled===true&&durable5.family_pass.next_tier===null);

const invalidEntitlementDb=makeDb({
  memoryCount:9,
  entitlementSchema:true,
  entitlement:{...durableRow,qualifying_memory_count:9}
});
const invalidEntitlement=await readMemberFamilyPassForSession(
  {DB:invalidEntitlementDb},
  {family_id:familyId,customer_id:customerId}
);
pass('invalid entitlement evidence is not trusted',invalidEntitlement.family_pass.current_tier==='GOLD'&&invalidEntitlement.family_pass.black_lifetime_entitled===false);

const crossFamily=await readMemberFamilyPassForSession(
  {DB:makeDb({memoryCount:10,entitlementSchema:true,entitlement:durableRow})},
  {family_id:'fam_B',customer_id:customerId}
);
pass('cross-Family session cannot read durable BLACK entitlement',crossFamily.status==='family_access_denied');

const pure=computeCurrentFamilyPass(3,{
  durable_black_entitlement:true,
  entitlement_schema_applied:true,
  black_achieved_at:'2026-08-01'
});
pass('pure tier calculator gives durable BLACK precedence over current count',pure.current_tier==='BLACK'&&pure.memory_count===3&&pure.tier_basis==='durable_black_entitlement');

pass('entitlement normalizer requires qualifying count >=10',passTest.normalizeBlackEntitlement({...durableRow,qualifying_memory_count:9})===null);
pass('entitlement normalizer requires black_lifetime=1',passTest.normalizeBlackEntitlement({...durableRow,black_lifetime:0})===null);

const readHealth=memberFamilyPassReadHealth();
pass('read health supports durable BLACK entitlement source',readHealth.durable_black_entitlement_source_supported===true&&readHealth.entitlement_table==='member_family_pass_entitlements');
pass('read health keeps discount enforcement disabled',readHealth.black_benefit_enforcement_ready===false&&readHealth.production_write===false);

const planHealth=memberFamilyPassBlackAchievementPlanHealth();
pass('plan health is read-only with no entitlement executor',planHealth.read_only===true&&planHealth.entitlement_write_executor===false&&planHealth.production_write===false);
pass('plan health requires threshold 10 and schema before award',planHealth.threshold===10&&planHealth.schema_required_before_award===true);
pass('plan health records no automatic backfill or discount enforcement',planHealth.automatic_backfill===false&&planHealth.discount_enforcement===false);

console.log(`MEMBER_FAMILY_PASS_BLACK_ENTITLEMENT=${n}/${n} PASS`);
