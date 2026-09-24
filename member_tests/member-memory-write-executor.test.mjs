import {
  executeMemberHistoricalMemorySync,
  memberMemoryWriteExecutorHealth,
  __test
} from '../src/member-memory-write-executor.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

function makeDb({
  initialMemories=[],
  memberMemorySchema=true,
  batchFailure=false,
  concurrentWinner=false
}={}){
  const memories=initialMemories.map(x=>({...x}));
  const writes=[];
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    'customers',
    'customer_reservations',
    'customer_delivery_links',
    ...(memberMemorySchema?['member_memories']:[])
  ]);

  const db={
    memories,
    writes,
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
              ?{family_id:familyId,display_name:'A FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          if(sql.includes('FROM customers')){
            return state.params[0]===customerId?{customer_id:customerId}:null;
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
          if(sql.includes('FROM customer_reservations')){
            return {results:[{
              reservation_id:'R-A',
              customer_id:customerId,
              genre:'七五三',
              shoot_date:'2025-11-03',
              plan_label:'Normal',
              place:'神社',
              status:'撮影済み',
              source:'reservation',
              created_at:'2025-11-03',
              updated_at:'2025-11-04'
            }]};
          }
          if(sql.includes('FROM customer_delivery_links')){
            return {results:[{
              link_id:'dl-A',
              customer_id:customerId,
              reservation_id:'R-A',
              provider:'amazon_photos',
              url:'https://example.com/gallery',
              delivered_at:'2025-11-20',
              created_at:'2025-11-20'
            }]};
          }
          if(sql.includes('FROM member_memories')){
            const wanted=new Set(state.params);
            return {results:memories.filter(x=>wanted.has(x.source_reservation_id))};
          }
          return {results:[]};
        },
        async run(){throw new Error('executor must use atomic DB.batch')}
      };
      return stmt;
    },
    async batch(statements){
      if(batchFailure){
        if(concurrentWinner&&!memories.some(x=>x.source_reservation_id==='R-A')){
          memories.push({
            memory_id:'mem_concurrent',
            family_id:familyId,
            source_system:'customer-crm',
            source_customer_id:customerId,
            source_reservation_id:'R-A',
            deleted_at:''
          });
        }
        const e=new Error('simulated batch failure');
        e.name='D1BatchError';
        throw e;
      }

      const pending=[];
      for(const stmt of statements){
        assert(stmt.sql.includes('INSERT INTO member_memories'),'only member_memories inserts are allowed');
        const [
          memory_id,
          rowFamilyId,
          source_system,
          source_customer_id,
          source_reservation_id,
          shoot_date,
          genre,
          title,
          delivery_link_id,
          amazon_photos_url,
          published
        ]=stmt.state.params;

        if(memories.some(x=>x.source_system===source_system&&x.source_reservation_id===source_reservation_id)){
          const e=new Error('unique source conflict');
          e.name='D1ConstraintError';
          throw e;
        }

        pending.push({
          memory_id,
          family_id:rowFamilyId,
          source_system,
          source_customer_id,
          source_reservation_id,
          shoot_date,
          genre,
          title,
          delivery_link_id,
          amazon_photos_url,
          published,
          deleted_at:''
        });
      }

      memories.push(...pending);
      writes.push(...pending);
      return pending.map(()=>({success:true}));
    }
  };

  return db;
}

const baseEnv={
  DB:makeDb(),
  MEMBER_MEMORY_WRITE_MODE:'enabled'
};

const noApproval=await executeMemberHistoricalMemorySync(baseEnv,{
  family_id:familyId,
  customer_id:customerId
});
pass('write requires explicit approved flag',noApproval.status==='write_approval_required'&&noApproval.write_executed===false&&baseEnv.DB.writes.length===0);

const disabledDb=makeDb();
const disabled=await executeMemberHistoricalMemorySync(
  {DB:disabledDb},
  {family_id:familyId,customer_id:customerId,approved:true}
);
pass('write mode defaults disabled',disabled.status==='member_memory_write_disabled'&&disabledDb.writes.length===0);

const noBatchDb=makeDb();
delete noBatchDb.batch;
const noBatch=await executeMemberHistoricalMemorySync(
  {DB:noBatchDb,MEMBER_MEMORY_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId,approved:true}
);
pass('atomic batch support is mandatory',noBatch.status==='atomic_batch_required'&&noBatchDb.writes.length===0);

const writeDb=makeDb();
const written=await executeMemberHistoricalMemorySync(
  {DB:writeDb,MEMBER_MEMORY_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId,approved:true}
);
pass('approved enabled executor creates missing historical MEMORY',written.status==='ok'&&written.write_executed===true&&written.created_count===1&&writeDb.writes.length===1);
pass('write targets exact Family and Customer IDs',writeDb.writes[0].family_id===familyId&&writeDb.writes[0].source_customer_id===customerId);
pass('write preserves stable source reservation id',writeDb.writes[0].source_reservation_id==='R-A'&&writeDb.writes[0].source_system==='customer-crm');
pass('write preserves exact HTTPS Amazon delivery link',writeDb.writes[0].amazon_photos_url==='https://example.com/gallery'&&writeDb.writes[0].delivery_link_id==='dl-A');
pass('created memory id is deterministic and opaque',/^mem_[a-f0-9]{32}$/.test(writeDb.writes[0].memory_id));

const second=await executeMemberHistoricalMemorySync(
  {DB:writeDb,MEMBER_MEMORY_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId,approved:true}
);
pass('second execution is an idempotent no-op',second.status==='ok'&&second.write_executed===false&&second.created_count===0&&second.idempotent_noop===true&&writeDb.writes.length===1);

const conflictDb=makeDb({initialMemories:[{
  memory_id:'mem_other',
  family_id:'fam_B',
  source_system:'customer-crm',
  source_customer_id:customerId,
  source_reservation_id:'R-A',
  deleted_at:''
}]});
const conflict=await executeMemberHistoricalMemorySync(
  {DB:conflictDb,MEMBER_MEMORY_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId,approved:true}
);
pass('cross-family existing MEMORY conflict fails before write',conflict.status==='memory_sync_conflict'&&conflict.write_executed===false&&conflictDb.writes.length===0);

const deniedDb=makeDb();
const denied=await executeMemberHistoricalMemorySync(
  {DB:deniedDb,MEMBER_MEMORY_WRITE_MODE:'enabled'},
  {family_id:'fam_B',customer_id:customerId,approved:true}
);
pass('wrong Family is denied before write',denied.status==='family_access_denied'&&deniedDb.writes.length===0);

const missingSchemaDb=makeDb({memberMemorySchema:false});
const missingSchema=await executeMemberHistoricalMemorySync(
  {DB:missingSchemaDb,MEMBER_MEMORY_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId,approved:true}
);
pass('Member MEMORY schema must already be applied',missingSchema.status==='member_memory_schema_not_applied'&&missingSchemaDb.writes.length===0);

const failedDb=makeDb({batchFailure:true});
const failed=await executeMemberHistoricalMemorySync(
  {DB:failedDb,MEMBER_MEMORY_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId,approved:true}
);
pass('batch failure does not blind-retry',failed.status==='member_memory_write_failed'&&failed.write_executed===false&&failed.review_required===true&&failedDb.writes.length===0);

const raceDb=makeDb({batchFailure:true,concurrentWinner:true});
const race=await executeMemberHistoricalMemorySync(
  {DB:raceDb,MEMBER_MEMORY_WRITE_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId,approved:true}
);
pass('concurrent same-family writer becomes safe idempotent no-op',race.status==='ok'&&race.write_executed===false&&race.concurrent_writer_won===true&&race.idempotent_noop===true);

const idA=await __test.deterministicMemoryId({source_system:'customer-crm',source_reservation_id:'R-A'});
const idA2=await __test.deterministicMemoryId({source_system:'customer-crm',source_reservation_id:'R-A'});
const idB=await __test.deterministicMemoryId({source_system:'customer-crm',source_reservation_id:'R-B'});
pass('deterministic memory id is stable per source reservation',idA===idA2&&idA!==idB);

pass('unsafe Amazon URL normalizes to empty',__test.safeHttpsUrl('http://example.com/file')==='');
pass('write mode requires exact enabled value',__test.writeEnabled({MEMBER_MEMORY_WRITE_MODE:'enabled'})===true&&__test.writeEnabled({MEMBER_MEMORY_WRITE_MODE:'true'})===false);

const healthDisabled=memberMemoryWriteExecutorHealth({});
pass('health records default-disabled write guard',healthDisabled.write_default_disabled===true&&healthDisabled.write_mode==='disabled');
pass('health records no canonical CRM or media writes',healthDisabled.canonical_crm_write===false&&healthDisabled.member_memory_media_write===false);
pass('health records Production route/write remain disabled',healthDisabled.production_route_wired===false&&healthDisabled.production_write_enabled===false&&healthDisabled.line_send===false);

console.log(`MEMBER_MEMORY_WRITE_EXECUTOR=${n}/${n} PASS`);
