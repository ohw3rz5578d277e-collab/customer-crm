import fs from 'node:fs';
import {readMemberMemorySourceByCustomer} from '../src/crm-member-memory-source.mjs';
import {buildMemorySyncPlan} from '../src/member-memory-sync-plan.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const reservations=[
  {reservation_id:'R-1',customer_id:customerId,genre:'七五三',shoot_date:'2025-11-03',plan_label:'Normal',place:'神社',status:'本納品済み',source:'reservation',created_at:'2025-11-01',updated_at:'2025-11-20'},
  {reservation_id:'R-2',customer_id:customerId,genre:'入学',shoot_date:'2027-04-01',status:'予約確定',source:'reservation',created_at:'2026-09-01',updated_at:'2026-09-01'},
  {reservation_id:'R-3',customer_id:customerId,genre:'家族',shoot_date:'2025-05-01',status:'キャンセル',source:'reservation',created_at:'2025-04-01',updated_at:'2025-04-02'},
  {reservation_id:'R-4',customer_id:customerId,genre:'お宮参り',shoot_date:'2024-03-01',status:'キャンセル',source:'reservation',created_at:'2024-03-01',updated_at:'2024-04-01'},
  {reservation_id:'R-4',customer_id:customerId,genre:'お宮参り',shoot_date:'2024-03-01',status:'撮影済み',source:'reservation',created_at:'2024-03-01',updated_at:'2024-03-02'},
  {reservation_id:'CSV-ABCDEF123456',customer_id:customerId,genre:'バースデー',shoot_date:'2023-08-20',status:'CSV取込',source:'customer_csv_import',created_at:'2026-09-13',updated_at:'2026-09-13'},
  {reservation_id:'R-OTHER',customer_id:'26000999',genre:'成人',shoot_date:'2025-01-01',status:'撮影済み',source:'reservation',created_at:'2025-01-01',updated_at:'2025-01-02'}
];

const deliveryLinks=[
  {link_id:'dl-1',customer_id:customerId,reservation_id:'R-1',provider:'amazon_photos',url:'https://example.com/r1',delivered_at:'2025-11-20',created_at:'2025-11-20'},
  {link_id:'dl-old',customer_id:customerId,reservation_id:'R-1',provider:'amazon_photos',url:'https://example.com/r1-old',delivered_at:'2025-11-10',created_at:'2025-11-10'},
  {link_id:'dl-unassigned',customer_id:customerId,reservation_id:'',provider:'amazon_photos',url:'https://example.com/unassigned',delivered_at:'2025-01-01',created_at:'2025-01-01'},
  {link_id:'dl-wrong-provider',customer_id:customerId,reservation_id:'CSV-ABCDEF123456',provider:'other',url:'https://example.com/other',delivered_at:'2025-01-01',created_at:'2025-01-01'}
];

function db(){
  const tables=new Set(['customers','customer_reservations','customer_delivery_links']);
  return {
    prepare(sql){
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')){
            return tables.has(state.params[0])?{name:state.params[0]}:null;
          }
          if(sql.includes('FROM customers')){
            return state.params[0]===customerId?{customer_id:customerId}:null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_reservations'))return {results:reservations};
          if(sql.includes('FROM customer_delivery_links'))return {results:deliveryLinks};
          return {results:[]};
        },
        async run(){throw new Error('MEMORY source must remain read-only')}
      };
      return stmt;
    }
  };
}

const source=await readMemberMemorySourceByCustomer({DB:db()},customerId);
pass('source read succeeds',source.status==='ok');
pass('only completed and controlled legacy shoots become MEMORY candidates',source.memories.length===2);
pass('future and cancelled reservations are excluded',!source.memories.some(x=>['R-2','R-3','R-4'].includes(x.source_reservation_id)));
pass('legacy CSV completed shoot is retained',source.memories.some(x=>x.source_reservation_id==='CSV-ABCDEF123456'&&x.eligibility==='legacy_completed_import'));
const r1=source.memories.find(x=>x.source_reservation_id==='R-1');
pass('Amazon Photos link uses exact reservation match',r1?.amazon_photos_url==='https://example.com/r1');
pass('cross-customer source row is filtered out',!source.memories.some(x=>x.source_reservation_id==='R-OTHER'));

const plan=buildMemorySyncPlan({
  family_id:'fam_A',
  customer_id:customerId,
  source_memories:source.memories,
  existing_memories:[{family_id:'fam_A',source_system:'customer-crm',source_customer_id:customerId,source_reservation_id:'R-1'}]
});
pass('idempotent planner keeps existing reservation already synced',plan.ok&&plan.already_synced.length===1&&plan.already_synced[0].source_reservation_id==='R-1');
pass('idempotent planner proposes only missing MEMORY',plan.to_create.length===1&&plan.to_create[0].source_reservation_id==='CSV-ABCDEF123456');
pass('planner executes zero writes',plan.write_executed===false);

const wrongSource=buildMemorySyncPlan({
  family_id:'fam_A',
  customer_id:customerId,
  source_memories:[{...source.memories[0],source_customer_id:'26000999'}],
  existing_memories:[]
});
pass('source Customer ID mismatch fails closed',wrongSource.ok===false&&wrongSource.conflicts.some(x=>x.reason==='source_customer_id_mismatch'));

const wrongFamily=buildMemorySyncPlan({
  family_id:'fam_A',
  customer_id:customerId,
  source_memories:[source.memories[0]],
  existing_memories:[{family_id:'fam_B',source_system:'customer-crm',source_customer_id:customerId,source_reservation_id:'R-1'}]
});
pass('existing MEMORY in another family fails closed',wrongFamily.ok===false&&wrongFamily.conflicts.some(x=>x.reason==='existing_memory_family_conflict'));

const wrongCustomer=buildMemorySyncPlan({
  family_id:'fam_A',
  customer_id:customerId,
  source_memories:[source.memories[0]],
  existing_memories:[{family_id:'fam_A',source_system:'customer-crm',source_customer_id:'26000999',source_reservation_id:'R-1'}]
});
pass('existing MEMORY for another customer fails closed',wrongCustomer.ok===false&&wrongCustomer.conflicts.some(x=>x.reason==='existing_memory_customer_conflict'));

const duplicate=buildMemorySyncPlan({
  family_id:'fam_A',
  customer_id:customerId,
  source_memories:[source.memories[0],source.memories[0]],
  existing_memories:[]
});
pass('duplicate source reservation in one batch fails closed',duplicate.ok===false&&duplicate.conflicts.some(x=>x.reason==='duplicate_source_reservation'));

const migration=fs.readFileSync('migrations_managed/20260924_member_memory_core_foundation.sql','utf8');
pass('Member Core schema has source idempotency uniqueness',/UNIQUE INDEX[\s\S]*source_system, source_reservation_id/i.test(migration));
pass('Member Core schema stores media references not photo binaries',migration.includes('storage_key')&&!/BLOB/i.test(migration));
pass('Member Core migration does not mutate canonical CRM tables',!/ALTER\s+TABLE\s+(customers|customer_reservations|customer_delivery_links)/i.test(migration)&&!/INSERT\s+INTO\s+(customers|customer_reservations|customer_delivery_links)/i.test(migration));

console.log(`MEMBER_MEMORY_SOURCE_PLAN=${n}/${n} PASS`);
