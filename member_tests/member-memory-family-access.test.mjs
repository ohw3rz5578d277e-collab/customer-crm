import {buildMemberMemoryPlanForFamily,handleMemberMemoryPlanRequest,memberMemoryPlanHealth} from '../src/crm-member-memory-plan.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';
const lineTables=new Set([
  'customer_family_groups',
  'customer_family_customer_links',
  'customers',
  'customer_reservations',
  'customer_delivery_links',
  'member_memories'
]);

function makeDb(existingMemories=[]){
  return {
    prepare(sql){
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')){
            return lineTables.has(state.params[0])?{name:state.params[0]}:null;
          }
          if(sql.includes('FROM customer_family_groups')){
            return state.params[0]===familyId?{family_id:familyId,display_name:'A FAMILY',status:'active',created_at:'',updated_at:''}:null;
          }
          if(sql.includes('FROM customers')){
            return state.params[0]===customerId?{customer_id:customerId}:null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            return state.params[0]===customerId?{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]}:{results:[]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return state.params[0]===familyId?{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]}:{results:[]};
          }
          if(sql.includes('FROM customer_reservations')){
            return {results:[{reservation_id:'R-A',customer_id:customerId,genre:'お宮参り',shoot_date:'2025-04-01',plan_label:'Normal',place:'神社',status:'撮影済み',source:'reservation',created_at:'2025-04-01',updated_at:'2025-04-02'}]};
          }
          if(sql.includes('FROM customer_delivery_links')){
            return {results:[{link_id:'dl-A',customer_id:customerId,reservation_id:'R-A',provider:'amazon_photos',url:'https://example.com/a',delivered_at:'2025-04-20',created_at:'2025-04-20'}]};
          }
          if(sql.includes('FROM member_memories')){
            const wanted=new Set(state.params);
            return {results:existingMemories.filter(x=>wanted.has(x.source_reservation_id))};
          }
          return {results:[]};
        },
        async run(){throw new Error('Member MEMORY plan must remain read-only')}
      };
      return stmt;
    }
  };
}

const env={DB:makeDb(),CRM_INTERNAL_TOKEN:'secret'};
const ok=await buildMemberMemoryPlanForFamily(env,{family_id:familyId,customer_id:customerId});
pass('same explicit family can build MEMORY plan',ok.status==='ok'&&ok.plan.to_create.length===1);
pass('plan is read-only',ok.write_executed===false&&ok.plan.write_executed===false);
pass('Member Core schema detection works',ok.member_memory_schema_applied===true);

const denied=await buildMemberMemoryPlanForFamily(env,{family_id:'fam_B',customer_id:customerId});
pass('different family is denied before history is returned',denied.status==='family_access_denied'&&denied.write_executed===false);

const existingEnv={
  DB:makeDb([{memory_id:'mem_1',family_id:familyId,source_system:'customer-crm',source_customer_id:customerId,source_reservation_id:'R-A',deleted_at:''}]),
  CRM_INTERNAL_TOKEN:'secret'
};
const idempotent=await buildMemberMemoryPlanForFamily(existingEnv,{family_id:familyId,customer_id:customerId});
pass('existing source reservation is reported already synced',idempotent.status==='ok'&&idempotent.plan.to_create.length===0&&idempotent.plan.already_synced.length===1);

const unauth=await handleMemberMemoryPlanRequest(
  new Request(`https://example.test/api/internal/member-memory-plan/family/${familyId}/customer/${customerId}`),
  env
);
pass('internal MEMORY plan endpoint requires authentication',unauth.status===401);

const wrongFamily=await handleMemberMemoryPlanRequest(
  new Request(`https://example.test/api/internal/member-memory-plan/family/fam_B/customer/${customerId}`,{headers:{'x-internal-token':'secret'}}),
  env
);
pass('cross-family endpoint request returns 403',wrongFamily.status===403);

const auth=await handleMemberMemoryPlanRequest(
  new Request(`https://example.test/api/internal/member-memory-plan/family/${familyId}/customer/${customerId}`,{headers:{'x-internal-token':'secret'}}),
  env
);
const body=await auth.json();
pass('authorized endpoint returns only family-scoped plan',auth.status===200&&body.family_id===familyId&&body.customer_id===customerId);

const health=memberMemoryPlanHealth();
pass('health contract records cross-family fail-closed behavior',health.read_only===true&&health.cross_family_fail_closed===true&&health.production_write===false);

console.log(`MEMBER_MEMORY_FAMILY_ACCESS=${n}/${n} PASS`);
