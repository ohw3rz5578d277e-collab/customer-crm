import {resolveCanonicalCustomerByVerifiedLineUserId,handleMemberLineIdentityReadRequest,memberLineIdentityHealth} from '../src/crm-member-line-identity.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const lineA='U1234567890abcdef1234567890abcdef';
const lineB='Uabcdef1234567890abcdef1234567890';
const lineAmb='U11111111111111111111111111111111';

const customers=[
  {customer_id:'26000123',line_user_id:lineA,deleted_at:''},
  {customer_id:'legacy-id',line_user_id:lineB,deleted_at:''},
  {customer_id:'26000456',line_user_id:lineAmb,deleted_at:''},
  {customer_id:'26000457',line_user_id:lineAmb,deleted_at:''}
];

function db(){
  return {
    prepare(sql){
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async all(){
          if(sql.includes('FROM customers')&&sql.includes('line_user_id=?')){
            return {results:customers.filter(x=>x.line_user_id===state.params[0]&&!x.deleted_at).slice(0,3)};
          }
          return {results:[]};
        },
        async run(){throw new Error('read-only resolver must never write')},
        async first(){return null}
      };
      return stmt;
    }
  };
}

const env={DB:db(),CRM_INTERNAL_TOKEN:'secret'};

const linked=await resolveCanonicalCustomerByVerifiedLineUserId(env,lineA);
pass('verified exact LINE identity resolves canonical customer',linked.status==='linked'&&linked.customer_id==='26000123');

const unknown=await resolveCanonicalCustomerByVerifiedLineUserId(env,'U99999999999999999999999999999999');
pass('unknown LINE identity remains unlinked',unknown.status==='unlinked'&&unknown.customer_id===null);

const invalid=await resolveCanonicalCustomerByVerifiedLineUserId(env,'not-line-id');
pass('invalid LINE identity is rejected',invalid.status==='invalid_line_user_id');

const noncanonical=await resolveCanonicalCustomerByVerifiedLineUserId(env,lineB);
pass('noncanonical customer id is not returned to Member',noncanonical.status==='noncanonical_customer_id'&&noncanonical.customer_id===null);

const ambiguous=await resolveCanonicalCustomerByVerifiedLineUserId(env,lineAmb);
pass('duplicate LINE identity fails closed',ambiguous.status==='ambiguous_line_identity'&&ambiguous.customer_id===null);

const unauth=await handleMemberLineIdentityReadRequest(
  new Request('https://example.test/api/internal/member-line-identity/resolve',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({line_user_id:lineA})}),
  env
);
pass('internal Member LINE resolver requires authentication',unauth.status===401);

const auth=await handleMemberLineIdentityReadRequest(
  new Request('https://example.test/api/internal/member-line-identity/resolve',{method:'POST',headers:{'content-type':'application/json','x-internal-token':'secret'},body:JSON.stringify({line_user_id:lineA})}),
  env
);
const body=await auth.json();
pass('authenticated exact resolver returns only canonical Customer ID',auth.status===200&&body.customer_id==='26000123'&&body.fallback_used===false);

const health=memberLineIdentityHealth();
pass('resolver is read-only and requires upstream verified LINE token',health.read_only===true&&health.verified_line_token_required_upstream===true&&health.production_write===false);
pass('all PII/name fallbacks are prohibited',health.name_fallback===false&&health.phone_fallback===false&&health.email_fallback===false&&health.line_display_name_fallback===false);

console.log(`MEMBER_LINE_IDENTITY_FOUNDATION=${n}/${n} PASS`);
