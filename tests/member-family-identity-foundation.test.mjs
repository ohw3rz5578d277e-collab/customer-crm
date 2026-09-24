import fs from 'node:fs';
import {readMemberFamilyByCustomer,handleMemberFamilyIdentityReadRequest,memberFamilyIdentityHealth} from '../src/crm-member-family-identity.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const tables=new Set(['customer_family_groups','customer_family_customer_links']);
const family={family_id:'fam_test_family_01',display_name:'YAMADA FAMILY',status:'active',created_at:'2026-09-24',updated_at:'2026-09-24'};
const links=[
  {family_id:family.family_id,customer_id:'26000123',relation:'owner',access_role:'owner',created_at:'2026-09-24'},
  {family_id:family.family_id,customer_id:'26000456',relation:'partner',access_role:'adult',created_at:'2026-09-24'}
];

function db(){
  return {
    prepare(sql){
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')) return tables.has(state.params[0])?{name:state.params[0]}:null;
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            return links.find(x=>x.customer_id===state.params[0])||null;
          }
          if(sql.includes('FROM customer_family_groups')){
            return family.family_id===state.params[0]&&family.status==='active'?family:null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return {results:links.filter(x=>x.family_id===state.params[0])};
          }
          return {results:[]};
        },
        async run(){throw new Error('read-only module must never write')}
      };
      return stmt;
    }
  };
}

const env={DB:db(),CRM_INTERNAL_TOKEN:'secret'};
const linked=await readMemberFamilyByCustomer(env,'26000123');
pass('exact canonical Customer ID resolves family',linked.status==='linked'&&linked.family.family_id===family.family_id);
pass('family response lists explicitly linked customers only',linked.family.member_customer_ids.length===2&&linked.family.member_customer_ids.includes('26000456'));
const invalid=await readMemberFamilyByCustomer(env,'山田 花子');
pass('name cannot be used as identity lookup',invalid.status==='invalid_customer_id');
const unknown=await readMemberFamilyByCustomer(env,'26000999');
pass('unknown exact Customer ID remains unlinked',unknown.status==='unlinked'&&unknown.family===null);

const unauth=await handleMemberFamilyIdentityReadRequest(new Request('https://example.test/api/internal/member-family/customer/26000123'),env);
pass('internal family read requires token',unauth.status===401);
const auth=await handleMemberFamilyIdentityReadRequest(new Request('https://example.test/api/internal/member-family/customer/26000123',{headers:{'x-internal-token':'secret'}}),env);
pass('authorized internal family read succeeds',auth.status===200);

const health=memberFamilyIdentityHealth();
pass('foundation is read-only and not production wired',health.read_only===true&&health.production_route_wired===false&&health.production_write===false);
pass('family auto inference is prohibited',health.family_auto_inference===false&&health.name_match===false&&health.address_match===false);

const migration=fs.readFileSync('migrations_managed/20260924_member_family_identity_foundation.sql','utf8');
pass('migration is additive and does not alter customers',migration.includes('customer_family_groups')&&migration.includes('customer_family_customer_links')&&!/ALTER\s+TABLE\s+customers/i.test(migration));
pass('migration has no automatic backfill from PII',!/INSERT\s+INTO\s+customer_family_customer_links\s+SELECT/i.test(migration));

console.log(`MEMBER_FAMILY_IDENTITY_FOUNDATION=${n}/${n} PASS`);
