import {
  buildMemberFavoriteMutationPlan,
  memberFavoriteMutationPlanHealth,
  __test as planTest
} from '../src/member-favorites-mutation-plan.mjs';
import {
  executeMemberFavoriteMutation,
  memberFavoritesWriteExecutorHealth,
  __test as writeTest
} from '../src/member-favorites-write-executor.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const familyId='fam_A';
const customerId='26000123';
const memoryId='mem_A1';

function makeDb({
  favorite=false,
  favoriteSchema=true,
  memorySchema=true,
  familySchema=true,
  memoryPublished=true,
  memoryDeleted=false,
  failNextWrite=false
}={}){
  const tables=new Set([
    ...(familySchema?['customer_family_groups','customer_family_customer_links']:[]),
    ...(memorySchema?['member_memories']:[]),
    ...(favoriteSchema?['member_memory_favorites']:[])
  ]);

  const state={
    favorite,
    failNextWrite,
    writes:[],
    seenSql:[]
  };

  return {
    state,
    prepare(sql){
      state.seenSql.push(sql);
      const bound={params:[]};
      const stmt={
        bind(...params){bound.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')){
            return tables.has(bound.params[0])?{name:bound.params[0]}:null;
          }
          if(sql.includes('FROM customer_family_groups')){
            return bound.params[0]===familyId
              ?{family_id:familyId,display_name:'A FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          if(sql.includes('FROM member_memories')){
            const [requestedMemory,requestedFamily]=bound.params;
            return requestedMemory===memoryId
              && requestedFamily===familyId
              && memoryPublished
              && !memoryDeleted
              ?{memory_id:memoryId,family_id:familyId}
              :null;
          }
          if(sql.includes('FROM member_memory_favorites')){
            const [requestedFamily,requestedCustomer,requestedMemory]=bound.params;
            return state.favorite
              && requestedFamily===familyId
              && requestedCustomer===customerId
              && requestedMemory===memoryId
              ?{family_id:familyId,customer_id:customerId,memory_id:memoryId,created_at:'2026-09-25T00:00:00Z'}
              :null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            return bound.params[0]===customerId
              ?{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return bound.params[0]===familyId
              ?{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          return {results:[]};
        },
        async run(){
          state.writes.push({sql,params:bound.params});
          if(state.failNextWrite){
            state.failNextWrite=false;
            throw Object.assign(new Error('forced write failure'),{name:'ForcedWriteError'});
          }
          if(sql.includes('INSERT INTO member_memory_favorites')){
            state.favorite=true;
            return {success:true,meta:{changes:1}};
          }
          if(sql.includes('DELETE FROM member_memory_favorites')){
            state.favorite=false;
            return {success:true,meta:{changes:1}};
          }
          throw new Error('unexpected write');
        }
      };
      return stmt;
    }
  };
}

pass('valid memory id accepted',planTest.validMemoryId(memoryId)===true);
pass('control-character memory id rejected',planTest.validMemoryId('mem_A1\n')===false);
pass('overlong memory id rejected',planTest.validMemoryId('x'.repeat(161))===false);

const addPlan=await buildMemberFavoriteMutationPlan(
  {DB:makeDb({favorite:false})},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true}
);
pass('add plan requires write',addPlan.status==='ok'&&addPlan.action==='add'&&addPlan.write_required===true&&addPlan.current_favorite===false&&addPlan.desired_favorite===true);

const removePlan=await buildMemberFavoriteMutationPlan(
  {DB:makeDb({favorite:true})},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:false}
);
pass('remove plan requires write',removePlan.status==='ok'&&removePlan.action==='remove'&&removePlan.write_required===true&&removePlan.current_favorite===true);

const noopPlan=await buildMemberFavoriteMutationPlan(
  {DB:makeDb({favorite:true})},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true}
);
pass('already desired state is idempotent no-op',noopPlan.status==='ok'&&noopPlan.action==='none'&&noopPlan.write_required===false&&noopPlan.idempotent_noop===true);

const invalidDesired=await buildMemberFavoriteMutationPlan(
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:'true'}
);
pass('desired state must be boolean',invalidDesired.status==='invalid_desired_favorite');

const wrongFamily=await buildMemberFavoriteMutationPlan(
  {DB:makeDb()},
  {family_id:'fam_B',customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true}
);
pass('cross-Family session fails closed',wrongFamily.status==='family_access_denied');

const hiddenMemory=await buildMemberFavoriteMutationPlan(
  {DB:makeDb({memoryPublished:false})},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true}
);
pass('draft MEMORY cannot be favorited',hiddenMemory.status==='memory_not_found');

const deletedMemory=await buildMemberFavoriteMutationPlan(
  {DB:makeDb({memoryDeleted:true})},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true}
);
pass('deleted MEMORY cannot be favorited',deletedMemory.status==='memory_not_found');

const noFavoriteSchema=await buildMemberFavoriteMutationPlan(
  {DB:makeDb({favoriteSchema:false})},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true}
);
pass('missing Favorite schema blocks mutation plan',noFavoriteSchema.status==='favorites_schema_not_applied');

const noMemorySchema=await buildMemberFavoriteMutationPlan(
  {DB:makeDb({memorySchema:false})},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true}
);
pass('missing MEMORY schema blocks mutation plan',noMemorySchema.status==='member_memory_schema_not_applied');

const disabledDb=makeDb({favorite:false});
const disabled=await executeMemberFavoriteMutation(
  {DB:disabledDb,MEMBER_FAVORITES_WRITE_MODE:'disabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true,approved:true}
);
pass('env write mode defaults closed',disabled.status==='favorites_write_disabled'&&disabled.write_executed===false&&disabledDb.state.writes.length===0);

const unapprovedDb=makeDb({favorite:false});
const unapproved=await executeMemberFavoriteMutation(
  {DB:unapprovedDb,MEMBER_FAVORITES_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true,approved:false}
);
pass('explicit approved flag required',unapproved.status==='write_approval_required'&&unapproved.write_executed===false&&unapprovedDb.state.writes.length===0);

const addDb=makeDb({favorite:false});
const added=await executeMemberFavoriteMutation(
  {DB:addDb,MEMBER_FAVORITES_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true,approved:true}
);
pass('guarded add executes exact INSERT',added.status==='ok'&&added.write_executed===true&&added.favorite_changed===true&&added.action==='add'&&added.favorite===true);
pass('add writes exact Family Customer MEMORY key',addDb.state.writes.length===1&&addDb.state.writes[0].params.join(',')===`${familyId},${customerId},${memoryId}`);

const addAgain=await executeMemberFavoriteMutation(
  {DB:addDb,MEMBER_FAVORITES_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true,approved:true}
);
pass('repeat add is no-op',addAgain.status==='ok'&&addAgain.write_executed===false&&addAgain.idempotent_noop===true&&addDb.state.writes.length===1);

const removed=await executeMemberFavoriteMutation(
  {DB:addDb,MEMBER_FAVORITES_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:false,approved:true}
);
pass('guarded remove executes exact DELETE',removed.status==='ok'&&removed.write_executed===true&&removed.favorite_changed===true&&removed.action==='remove'&&removed.favorite===false);
pass('remove remains exact-key scoped',addDb.state.writes.length===2&&addDb.state.writes[1].params.join(',')===`${familyId},${customerId},${memoryId}`);

const removeAgain=await executeMemberFavoriteMutation(
  {DB:addDb,MEMBER_FAVORITES_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:false,approved:true}
);
pass('repeat remove is no-op',removeAgain.status==='ok'&&removeAgain.write_executed===false&&removeAgain.idempotent_noop===true&&addDb.state.writes.length===2);

const concurrentDb=makeDb({favorite:false,failNextWrite:true});
const originalPrepare=concurrentDb.prepare.bind(concurrentDb);
concurrentDb.prepare=(sql)=>{
  const stmt=originalPrepare(sql);
  if(sql.includes('INSERT INTO member_memory_favorites')){
    const originalRun=stmt.run.bind(stmt);
    stmt.run=async()=>{
      concurrentDb.state.favorite=true;
      return originalRun();
    };
  }
  return stmt;
};
const concurrent=await executeMemberFavoriteMutation(
  {DB:concurrentDb,MEMBER_FAVORITES_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:memoryId,desired_favorite:true,approved:true}
);
pass('concurrent writer success is recognized after failed write',concurrent.status==='ok'&&concurrent.write_executed===false&&concurrent.concurrent_writer_won===true&&concurrent.favorite===true);

const planHealth=memberFavoriteMutationPlanHealth();
pass('plan health records declarative actions and no write',planHealth.read_only===true&&planHealth.desired_state_boolean_required===true&&planHealth.automatic_write===false&&planHealth.production_write===false);

pass('writeEnabled requires exact enabled mode',writeTest.writeEnabled({MEMBER_FAVORITES_WRITE_MODE:'enabled'})===true&&writeTest.writeEnabled({})===false);
const writeHealthDisabled=memberFavoritesWriteExecutorHealth({});
pass('executor health defaults disabled',writeHealthDisabled.write_default_disabled===true&&writeHealthDisabled.write_mode==='disabled'&&writeHealthDisabled.production_write_enabled===false);
pass('executor health requires approval and pre/post verification',writeHealthDisabled.explicit_approved_flag_required===true&&writeHealthDisabled.prewrite_plan_rebuild===true&&writeHealthDisabled.postwrite_plan_verification===true);
pass('executor permits only insert/delete not update',writeHealthDisabled.insert_supported===true&&writeHealthDisabled.delete_supported===true&&writeHealthDisabled.update_supported===false);
pass('executor never writes canonical CRM or sends LINE',writeHealthDisabled.canonical_crm_write===false&&writeHealthDisabled.line_send===false&&writeHealthDisabled.production_route_wired===false);

console.log(`MEMBER_FAVORITES_MUTATION=${n}/${n} PASS`);
