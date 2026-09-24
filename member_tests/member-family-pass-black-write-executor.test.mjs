import {
  executeMemberFamilyPassBlackAward,
  memberFamilyPassBlackWriteExecutorHealth,
  __test
} from '../src/member-family-pass-black-write-executor.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

function makeDb({
  memoryCount=10,
  entitlementSchema=true,
  entitlement=null,
  insertFailure=false,
  concurrentWinner=false
}={}){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    'member_memories',
    ...(entitlementSchema?['member_family_pass_entitlements']:[])
  ]);
  const writes=[];
  let durable=entitlement?{...entitlement}:null;

  return {
    writes,
    get entitlement(){return durable},
    prepare(sql){
      const state={params:[]};
      const stmt={
        sql,
        state,
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
          if(sql.includes('FROM member_family_pass_entitlements')){
            return state.params[0]===familyId&&durable
              ?{...durable,family_id:familyId}
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
          return {results:[]};
        },
        async run(){
          assert(sql.includes('INSERT INTO member_family_pass_entitlements'),'only BLACK entitlement INSERT is allowed');
          assert(!/\bUPDATE\b/i.test(sql),'UPDATE is prohibited');
          assert(!/\bDELETE\b/i.test(sql),'DELETE is prohibited');

          if(insertFailure){
            if(concurrentWinner&&!durable){
              durable={
                black_lifetime:1,
                black_achieved_at:'2026-09-24T11:30:00.000Z',
                qualifying_memory_count:10,
                achievement_source:'published-member-memories'
              };
            }
            const e=new Error('simulated insert failure');
            e.name='D1ConstraintError';
            throw e;
          }

          if(durable){
            const e=new Error('duplicate Family entitlement');
            e.name='D1ConstraintError';
            throw e;
          }

          const [
            rowFamilyId,
            blackLifetime,
            blackAchievedAt,
            qualifyingMemoryCount,
            achievementSource
          ]=state.params;

          durable={
            black_lifetime:blackLifetime,
            black_achieved_at:blackAchievedAt,
            qualifying_memory_count:qualifyingMemoryCount,
            achievement_source:achievementSource
          };
          writes.push({
            family_id:rowFamilyId,
            ...durable
          });
          return {success:true};
        }
      };
      return stmt;
    }
  };
}

const session={family_id:familyId,customer_id:customerId};

const noApprovalDb=makeDb();
const noApproval=await executeMemberFamilyPassBlackAward(
  {DB:noApprovalDb,MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'enabled'},
  session
);
pass('write requires explicit approved flag',noApproval.status==='write_approval_required'&&noApproval.write_executed===false&&noApprovalDb.writes.length===0);

const disabledDb=makeDb();
const disabled=await executeMemberFamilyPassBlackAward(
  {DB:disabledDb},
  session,
  {approved:true}
);
pass('write mode defaults disabled',disabled.status==='black_entitlement_write_disabled'&&disabledDb.writes.length===0);

const belowDb=makeDb({memoryCount:9});
const below=await executeMemberFamilyPassBlackAward(
  {DB:belowDb,MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'enabled'},
  session,
  {approved:true}
);
pass('below ten MEMORIES is idempotent no-op',below.status==='ok'&&below.idempotent_noop===true&&below.reason==='black_threshold_not_reached'&&belowDb.writes.length===0);

const missingSchemaDb=makeDb({entitlementSchema:false});
const missingSchema=await executeMemberFamilyPassBlackAward(
  {DB:missingSchemaDb,MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'enabled'},
  session,
  {approved:true}
);
pass('entitlement schema must exist before write',missingSchema.status==='black_entitlement_schema_not_applied'&&missingSchema.write_executed===false&&missingSchemaDb.writes.length===0);

const writeDb=makeDb({memoryCount:10});
const written=await executeMemberFamilyPassBlackAward(
  {DB:writeDb,MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'enabled'},
  session,
  {approved:true,now:1800800000}
);
pass('qualified Family gets one durable BLACK insert',written.status==='ok'&&written.write_executed===true&&written.black_entitlement_created===true&&writeDb.writes.length===1);
pass('insert is exact Family scoped',writeDb.writes[0].family_id===familyId);
pass('insert persists lifetime BLACK only',writeDb.writes[0].black_lifetime===1);
pass('insert preserves qualifying count at award time',writeDb.writes[0].qualifying_memory_count===10);
pass('insert uses canonical achievement source',writeDb.writes[0].achievement_source==='published-member-memories');
pass('post-write verification returns durable BLACK',written.black_lifetime_entitled===true&&written.qualifying_memory_count===10);
pass('award timestamp is server-generated ISO time',written.black_achieved_at===__test.nowIso(1800800000));

const second=await executeMemberFamilyPassBlackAward(
  {DB:writeDb,MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'enabled'},
  session,
  {approved:true,now:1800900000}
);
pass('second execution never rewrites BLACK timestamp',second.status==='ok'&&second.write_executed===false&&second.idempotent_noop===true&&second.reason==='black_already_persisted'&&writeDb.writes.length===1);
pass('second execution preserves first BLACK achieved timestamp',writeDb.entitlement.black_achieved_at===__test.nowIso(1800800000));

const raceDb=makeDb({memoryCount:10,insertFailure:true,concurrentWinner:true});
const race=await executeMemberFamilyPassBlackAward(
  {DB:raceDb,MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'enabled'},
  session,
  {approved:true}
);
pass('concurrent winner becomes safe idempotent no-op',race.status==='ok'&&race.write_executed===false&&race.concurrent_writer_won===true&&race.black_lifetime_entitled===true);
pass('executor does not blind retry concurrent insert',raceDb.writes.length===0);

const failedDb=makeDb({memoryCount:10,insertFailure:true,concurrentWinner:false});
const failed=await executeMemberFamilyPassBlackAward(
  {DB:failedDb,MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'enabled'},
  session,
  {approved:true}
);
pass('unresolved insert failure requires review',failed.status==='black_entitlement_write_failed'&&failed.review_required===true&&failed.write_executed===false&&failedDb.writes.length===0);

const wrongFamilyDb=makeDb({memoryCount:10});
const wrongFamily=await executeMemberFamilyPassBlackAward(
  {DB:wrongFamilyDb,MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'enabled'},
  {family_id:'fam_B',customer_id:customerId},
  {approved:true}
);
pass('cross-Family session fails before write',wrongFamily.status==='family_access_denied'&&wrongFamilyDb.writes.length===0);

pass('write mode requires exact enabled value',__test.writeEnabled({MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'enabled'})===true&&__test.writeEnabled({MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'true'})===false);
pass('server timestamp helper emits ISO timestamp',/^\d{4}-\d{2}-\d{2}T/.test(__test.nowIso(1800800000)));

const health=memberFamilyPassBlackWriteExecutorHealth({});
pass('health records default-disabled write gate',health.write_default_disabled===true&&health.write_mode==='disabled'&&health.explicit_approved_flag_required===true);
pass('health records INSERT only and no downgrade/delete',health.insert_only===true&&health.update_supported===false&&health.delete_supported===false&&health.downgrade_supported===false);
pass('health records no automatic backfill or discount enforcement',health.automatic_backfill===false&&health.discount_enforcement===false&&health.shooting_fee_discount===false);
pass('health records no canonical CRM write, route, Production write, or LINE send',health.canonical_crm_write===false&&health.production_route_wired===false&&health.production_write_enabled===false&&health.line_send===false);

console.log(`MEMBER_FAMILY_PASS_BLACK_WRITE_EXECUTOR=${n}/${n} PASS`);
