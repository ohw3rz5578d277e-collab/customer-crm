import fs from 'node:fs';
import {
  consumeMemberFavoriteMutationRateLimit,
  memberFavoritesRateLimitHealth,
  __test
} from '../src/member-favorites-rate-limit.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const familyId='fam_A';
const customerId='26000123';

function makeDb({schema=true,failWrite=false,hideCounter=false}={}){
  const counters=new Map();
  const writes=[];
  return {
    counters,writes,
    prepare(sql){
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')){
            return schema&&state.params[0]==='member_favorite_mutation_rate_limits'
              ?{name:'member_favorite_mutation_rate_limits'}
              :null;
          }
          if(sql.includes('FROM member_favorite_mutation_rate_limits')){
            if(hideCounter)return null;
            const key=state.params.join('|');
            const count=counters.get(key)||0;
            return count>0?{attempt_count:count}:null;
          }
          return null;
        },
        async run(){
          writes.push({sql,params:state.params});
          if(failWrite)return {success:false};
          if(sql.includes('INSERT INTO member_favorite_mutation_rate_limits')){
            const key=state.params.join('|');
            counters.set(key,(counters.get(key)||0)+1);
            return {success:true,meta:{changes:1}};
          }
          throw new Error('unexpected write');
        }
      };
      return stmt;
    }
  };
}

pass('rate-limit mode defaults disabled',__test.modeEnabled({})===false);
pass('rate-limit mode requires exact enabled',__test.modeEnabled({MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled'})===true);
pass('valid session accepted',__test.validSession({family_id:familyId,customer_id:customerId}).ok===true);
pass('invalid Customer ID rejected',__test.validSession({family_id:familyId,customer_id:'abc'}).ok===false);
pass('valid MEMORY ID accepted',__test.validMemoryId('mem_A1')===true);
pass('outer whitespace MEMORY ID rejected',__test.validMemoryId(' mem_A1')===false);
pass('control-character MEMORY ID rejected',__test.validMemoryId('mem_A1\n')===false);
pass('fixed minute bucket computed',__test.bucketStart(125)===120);
pass('retry-after reaches next bucket',__test.retryAfterSeconds(125,120)===55);

const disabledDb=makeDb();
const disabled=await consumeMemberFavoriteMutationRateLimit(
  {DB:disabledDb},
  {family_id:familyId,customer_id:customerId},
  {memory_id:'mem_A1',now_seconds:120}
);
pass('disabled limiter fails closed without counter write',disabled.status==='rate_limit_disabled'&&disabled.allowed===false&&disabledDb.writes.length===0);

const missingDb=makeDb({schema:false});
const missing=await consumeMemberFavoriteMutationRateLimit(
  {DB:missingDb,MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:'mem_A1',now_seconds:120}
);
pass('missing limiter schema fails closed without write',missing.status==='rate_limit_schema_not_applied'&&missing.allowed===false&&missingDb.writes.length===0);

const invalidDb=makeDb();
const invalid=await consumeMemberFavoriteMutationRateLimit(
  {DB:invalidDb,MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:'bad\n',now_seconds:120}
);
pass('invalid MEMORY still consumes Member-wide quota',invalid.status==='invalid_memory_id'&&invalid.allowed===false&&invalid.counter_write_executed===true&&invalidDb.writes.length===1);

const oneDb=makeDb();
const one=await consumeMemberFavoriteMutationRateLimit(
  {DB:oneDb,MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:'mem_A1',now_seconds:120}
);
pass('first valid mutation attempt allowed',one.status==='ok'&&one.allowed===true&&oneDb.writes.length===2);
pass('one attempt writes Member and MEMORY counters only',oneDb.writes.every(x=>x.sql.includes('member_favorite_mutation_rate_limits')));

const memoryDb=makeDb();
for(let i=1;i<=6;i++){
  const result=await consumeMemberFavoriteMutationRateLimit(
    {DB:memoryDb,MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled'},
    {family_id:familyId,customer_id:customerId},
    {memory_id:'mem_same',now_seconds:130}
  );
  pass(`same MEMORY attempt ${i} allowed within six-per-minute limit`,result.status==='ok'&&result.allowed===true);
}
const memoryLimited=await consumeMemberFavoriteMutationRateLimit(
  {DB:memoryDb,MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:'mem_same',now_seconds:130}
);
pass('seventh same-MEMORY attempt is limited',memoryLimited.status==='rate_limited'&&memoryLimited.allowed===false&&memoryLimited.scope==='memory');
pass('MEMORY limit returns Retry-After seconds',memoryLimited.retry_after_seconds===50);

const customerDb=makeDb();
for(let i=1;i<=20;i++){
  const result=await consumeMemberFavoriteMutationRateLimit(
    {DB:customerDb,MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled'},
    {family_id:familyId,customer_id:customerId},
    {memory_id:`mem_${String(i).padStart(2,'0')}`,now_seconds:180}
  );
  pass(`Member-wide attempt ${i} allowed within twenty-per-minute limit`,result.status==='ok'&&result.allowed===true);
}
const customerLimited=await consumeMemberFavoriteMutationRateLimit(
  {DB:customerDb,MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:'mem_21',now_seconds:180}
);
pass('twenty-first Member-wide attempt is limited',customerLimited.status==='rate_limited'&&customerLimited.allowed===false&&customerLimited.scope==='customer');
pass('Member-wide limit stops before MEMORY counter write',customerDb.writes.length===41);

const failedDb=makeDb({failWrite:true});
const failed=await consumeMemberFavoriteMutationRateLimit(
  {DB:failedDb,MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:'mem_A1',now_seconds:240}
);
pass('counter write failure blocks Favorite mutation',failed.status==='rate_limit_counter_write_failed'&&failed.allowed===false);

const verifyDb=makeDb({hideCounter:true});
const verifyFail=await consumeMemberFavoriteMutationRateLimit(
  {DB:verifyDb,MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled'},
  {family_id:familyId,customer_id:customerId},
  {memory_id:'mem_A1',now_seconds:240}
);
pass('counter post-write verification failure blocks Favorite mutation',verifyFail.status==='rate_limit_counter_verification_failed'&&verifyFail.allowed===false);

const health=memberFavoritesRateLimitHealth({});
pass('health records default-disabled mode',health.default_disabled===true&&health.mode==='disabled');
pass('health records fixed-window limits',health.fixed_window_seconds===60&&health.customer_attempt_limit===20&&health.memory_attempt_limit===6);
pass('health records authenticated exact scopes',health.authenticated_member_scope===true&&health.family_customer_scope===true&&health.exact_memory_scope_when_valid===true);
pass('health records invalid MEMORY consumes Member quota',health.invalid_memory_consumes_customer_quota===true);
pass('health records Retry-After and counter write requirement',health.retry_after_supported===true&&health.counter_write_required_when_enabled===true);
pass('health records no canonical CRM/Favorite/LINE write',health.canonical_crm_write===false&&health.favorite_write===false&&health.line_send===false&&health.production_route_wired===false);

const migration=fs.readFileSync(
  'migrations_managed/20260924_member_favorite_mutation_rate_limit_foundation.sql',
  'utf8'
);
const schemaOnly=migration.replace(/^\s*--.*$/gm,'');
pass('rate-limit migration is additive only',!/\b(DROP|ALTER|DELETE|UPDATE|INSERT)\b/i.test(schemaOnly));
pass('rate-limit schema stores no profile or LINE fields',!/\b(name|phone|email|address|line_user_id)\b/i.test(schemaOnly));
pass('rate-limit schema keys exact Family Customer Scope Window',schemaOnly.includes('PRIMARY KEY (')&&schemaOnly.includes('family_id')&&schemaOnly.includes('customer_id')&&schemaOnly.includes('scope_key')&&schemaOnly.includes('window_started_at'));

console.log(`MEMBER_FAVORITES_RATE_LIMIT=${n}/${n} PASS`);
