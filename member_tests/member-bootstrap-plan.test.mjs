import {
  buildMemberBootstrapPlanFromVerifiedLineIdentity,
  memberBootstrapPlanHealth
} from '../src/member-bootstrap-plan.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const lineA='U1234567890abcdef1234567890abcdef';
const lineAmb='U11111111111111111111111111111111';
const customerId='26000123';
const familyId='fam_A';

function makeDb({
  ambiguousLine=false,
  familyLinked=true,
  ambiguousFamily=false,
  includeHistory=true,
  existingMemory=false,
  memoryConflict=false
}={}){
  const tables=new Set([
    'customers',
    'customer_family_groups',
    'customer_family_customer_links',
    'customer_reservations',
    'customer_delivery_links',
    'member_memories'
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
              ?{family_id:familyId,display_name:'YAMADA FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          if(sql.includes('FROM customers')&&!sql.includes('line_user_id=?')){
            return state.params[0]===customerId?{customer_id:customerId}:null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customers')&&sql.includes('line_user_id=?')){
            if(state.params[0]===lineAmb&&ambiguousLine){
              return {results:[
                {customer_id:customerId,line_user_id:lineAmb},
                {customer_id:'26000456',line_user_id:lineAmb}
              ]};
            }
            return state.params[0]===lineA
              ?{results:[{customer_id:customerId,line_user_id:lineA}]}
              :{results:[]};
          }

          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            if(state.params[0]!==customerId||!familyLinked)return {results:[]};
            if(ambiguousFamily){
              return {results:[
                {family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'},
                {family_id:'fam_B',customer_id:customerId,relation:'owner',access_role:'owner'}
              ]};
            }
            return {results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]};
          }

          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return state.params[0]===familyId&&familyLinked
              ?{results:[{customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }

          if(sql.includes('FROM customer_reservations')){
            return includeHistory
              ?{results:[{
                reservation_id:'R-2025-001',
                customer_id:customerId,
                genre:'七五三',
                shoot_date:'2025-11-03',
                plan_label:'Normal',
                place:'神社',
                status:'撮影済み',
                source:'reservation',
                created_at:'2025-11-03',
                updated_at:'2025-11-04'
              }]}
              :{results:[]};
          }

          if(sql.includes('FROM customer_delivery_links')){
            return {results:[{
              link_id:'dl-1',
              customer_id:customerId,
              reservation_id:'R-2025-001',
              provider:'amazon_photos',
              url:'https://example.com/gallery',
              delivered_at:'2025-11-20',
              created_at:'2025-11-20'
            }]};
          }

          if(sql.includes('FROM member_memories')){
            if(!existingMemory&&!memoryConflict)return {results:[]};
            const sourceReservationId='R-2025-001';
            if(!state.params.includes(sourceReservationId))return {results:[]};
            return {results:[{
              memory_id:'mem-1',
              family_id:memoryConflict?'fam_B':familyId,
              source_system:'customer-crm',
              source_customer_id:customerId,
              source_reservation_id:sourceReservationId,
              deleted_at:''
            }]};
          }

          return {results:[]};
        },
        async run(){throw new Error('bootstrap plan must remain read-only')}
      };
      return stmt;
    }
  };
}

const configuredEnv={
  DB:makeDb(),
  MEMBER_SESSION_SECRET:'member-session-secret-for-bootstrap-tests-123456789'
};

const ready=await buildMemberBootstrapPlanFromVerifiedLineIdentity(configuredEnv,{
  verified_line_user_id:lineA
});
pass('verified LINE identity builds ready bootstrap plan',ready.status==='ready'&&ready.ready===true);
pass('bootstrap resolves canonical Customer ID only',ready.customer_id===customerId&&ready.fallback_used===false);
pass('bootstrap resolves explicit Family link',ready.family.family_id===familyId&&ready.family.display_name==='YAMADA FAMILY');
pass('historical MEMORY plan proposes completed shoot',ready.memory.to_create===1&&ready.memory.conflicts===0);
pass('session readiness reflects configured Member secret without issuing token',ready.session.ready_to_issue===true&&ready.session.issued===false&&ready.session_issued===false);
pass('bootstrap executes zero writes',ready.write_executed===false&&ready.memory_write_executed===false);

const noSecret=await buildMemberBootstrapPlanFromVerifiedLineIdentity(
  {DB:makeDb()},
  {verified_line_user_id:lineA}
);
pass('missing session secret does not issue session and reports not ready',noSecret.status==='ready'&&noSecret.session.ready_to_issue===false&&noSecret.session.issued===false);
pass('missing session secret does not block identity/history planning',noSecret.customer_id===customerId&&noSecret.memory.to_create===1);

const unknown=await buildMemberBootstrapPlanFromVerifiedLineIdentity(
  configuredEnv,
  {verified_line_user_id:'U99999999999999999999999999999999'}
);
pass('unknown exact LINE identity remains unlinked',unknown.status==='unlinked'&&unknown.stage==='line_identity'&&unknown.ready===false);

const ambiguousLine=await buildMemberBootstrapPlanFromVerifiedLineIdentity(
  {DB:makeDb({ambiguousLine:true}),MEMBER_SESSION_SECRET:configuredEnv.MEMBER_SESSION_SECRET},
  {verified_line_user_id:lineAmb}
);
pass('duplicate LINE identity fails closed for review',ambiguousLine.status==='ambiguous_line_identity'&&ambiguousLine.review_required===true);

const familyUnlinked=await buildMemberBootstrapPlanFromVerifiedLineIdentity(
  {DB:makeDb({familyLinked:false}),MEMBER_SESSION_SECRET:configuredEnv.MEMBER_SESSION_SECRET},
  {verified_line_user_id:lineA}
);
pass('Customer without explicit Family link cannot bootstrap',familyUnlinked.status==='unlinked'&&familyUnlinked.stage==='family_identity');

const familyAmbiguous=await buildMemberBootstrapPlanFromVerifiedLineIdentity(
  {DB:makeDb({ambiguousFamily:true}),MEMBER_SESSION_SECRET:configuredEnv.MEMBER_SESSION_SECRET},
  {verified_line_user_id:lineA}
);
pass('duplicate active Family link fails closed for review',familyAmbiguous.status==='ambiguous_family_identity'&&familyAmbiguous.review_required===true);

const alreadySynced=await buildMemberBootstrapPlanFromVerifiedLineIdentity(
  {DB:makeDb({existingMemory:true}),MEMBER_SESSION_SECRET:configuredEnv.MEMBER_SESSION_SECRET},
  {verified_line_user_id:lineA}
);
pass('existing MEMORY remains idempotent in bootstrap plan',alreadySynced.status==='ready'&&alreadySynced.memory.to_create===0&&alreadySynced.memory.already_synced===1);

const conflict=await buildMemberBootstrapPlanFromVerifiedLineIdentity(
  {DB:makeDb({memoryConflict:true}),MEMBER_SESSION_SECRET:configuredEnv.MEMBER_SESSION_SECRET},
  {verified_line_user_id:lineA}
);
pass('cross-family existing MEMORY conflict stops bootstrap',conflict.status==='memory_sync_conflict'&&conflict.stage==='memory_plan'&&conflict.review_required===true);

const invalid=await buildMemberBootstrapPlanFromVerifiedLineIdentity(
  configuredEnv,
  {verified_line_user_id:'not-line-id'}
);
pass('invalid LINE identity is rejected before any fallback',invalid.status==='invalid_line_user_id'&&invalid.ready===false);

const health=memberBootstrapPlanHealth();
pass('health requires upstream verified LINE identity',health.verified_line_identity_required_upstream===true);
pass('health records exact identity and no Family inference',health.exact_line_user_id_only===true&&health.exact_customer_id_only===true&&health.explicit_family_link_only===true&&health.family_auto_inference===false);
pass('health records no session issue, no MEMORY write, no LINE send, no Production write',health.session_issue_in_plan===false&&health.historical_memory_write_in_plan===false&&health.line_send===false&&health.production_write===false);

console.log(`MEMBER_BOOTSTRAP_PLAN=${n}/${n} PASS`);
