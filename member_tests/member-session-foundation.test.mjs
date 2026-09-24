import {
  issueMemberSessionForVerifiedCustomer,
  verifyMemberSessionToken,
  verifyMemberSessionRequest,
  memberSessionFoundationHealth,
  memberSessionCookie,
  __test
} from '../src/member-session-foundation.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';
const secret='member-session-secret-for-tests-1234567890';
const now=1800000000;

function makeDb({familySchema=true,duplicate=false}={}){
  const tables=new Set(familySchema?['customer_family_groups','customer_family_customer_links']:[]);
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
              ?{family_id:familyId,display_name:'A FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            if(state.params[0]!==customerId)return {results:[]};
            return duplicate
              ?{results:[
                {family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'},
                {family_id:'fam_B',customer_id:customerId,relation:'owner',access_role:'owner'}
              ]}
              :{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return state.params[0]===familyId
              ?{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          return {results:[]};
        },
        async run(){throw new Error('Member session foundation must remain read-only')}
      };
      return stmt;
    }
  };
}

const env={DB:makeDb(),MEMBER_SESSION_SECRET:secret};

const issued=await issueMemberSessionForVerifiedCustomer(env,{
  customer_id:customerId,
  family_id:familyId,
  now_seconds:now
});
pass('explicit linked Customer/Family can receive a signed session',issued.status==='ok'&&issued.issued===true&&!!issued.token);
pass('session issuance performs zero D1 writes',issued.write_executed===false);
pass('session contains only canonical identity claims',issued.session.customer_id===customerId&&issued.session.family_id===familyId);
pass('session cookie uses __Host prefix',issued.cookie.startsWith('__Host-mizuno_member_session='));
pass('session cookie is HttpOnly Secure and SameSite=Lax',issued.cookie.includes('HttpOnly')&&issued.cookie.includes('Secure')&&issued.cookie.includes('SameSite=Lax')&&issued.cookie.includes('Path=/'));

const verified=await verifyMemberSessionToken(env,issued.token,{now_seconds:now+60});
pass('fresh signed token verifies',verified.status==='ok'&&verified.verified===true);
pass('verified session is server-marked and contains exact IDs',verified.session.verified===true&&verified.session.customer_id===customerId&&verified.session.family_id===familyId);
pass('verification performs zero writes',verified.write_executed===false);

const requestVerified=await verifyMemberSessionRequest(
  new Request('https://example.test/member',{headers:{cookie:`other=1; ${__test.COOKIE_NAME}=${issued.token}`}}),
  env,
  {now_seconds:now+120}
);
pass('request verifier reads only the dedicated Member cookie',requestVerified.status==='ok'&&requestVerified.session.customer_id===customerId);

const noCookie=await verifyMemberSessionRequest(
  new Request('https://example.test/member'),
  env,
  {now_seconds:now}
);
pass('missing Member cookie requires session',noCookie.status==='member_session_required'&&noCookie.verified===false);

const parts=issued.token.split('.');
const tampered=`${parts[0]}x.${parts[1]}`;
const tamperedResult=await verifyMemberSessionToken(env,tampered,{now_seconds:now});
pass('tampered payload fails signature verification',tamperedResult.status==='invalid_member_session_signature'&&tamperedResult.verified===false);

const wrongSecret=await verifyMemberSessionToken(
  {DB:makeDb(),MEMBER_SESSION_SECRET:'different-member-session-secret-123456789'},
  issued.token,
  {now_seconds:now}
);
pass('token does not verify under a different secret',wrongSecret.status==='invalid_member_session_signature');

const expired=await verifyMemberSessionToken(
  env,
  issued.token,
  {now_seconds:now+__test.SESSION_MAX_AGE_SECONDS+__test.SESSION_MAX_AGE_SECONDS}
);
pass('expired session fails closed',expired.status==='member_session_expired'&&expired.verified===false);

const wrongFamily=await issueMemberSessionForVerifiedCustomer(env,{
  customer_id:customerId,
  family_id:'fam_B',
  now_seconds:now
});
pass('issuer refuses a Family ID not explicitly linked to Customer ID',wrongFamily.status==='family_access_denied'&&wrongFamily.issued===false);

const ambiguous=await issueMemberSessionForVerifiedCustomer(
  {DB:makeDb({duplicate:true}),MEMBER_SESSION_SECRET:secret},
  {customer_id:customerId,family_id:familyId,now_seconds:now}
);
pass('duplicate active Family identity fails closed',ambiguous.status==='ambiguous_family_identity'&&ambiguous.issued===false);

const missingSchema=await issueMemberSessionForVerifiedCustomer(
  {DB:makeDb({familySchema:false}),MEMBER_SESSION_SECRET:secret},
  {customer_id:customerId,family_id:familyId,now_seconds:now}
);
pass('Family schema cannot be bypassed during issuance',missingSchema.status==='schema_not_applied'&&missingSchema.issued===false);

const shortSecret=await issueMemberSessionForVerifiedCustomer(
  {DB:makeDb(),MEMBER_SESSION_SECRET:'short'},
  {customer_id:customerId,family_id:familyId,now_seconds:now}
);
pass('short Member session secret fails closed',shortSecret.status==='member_session_secret_not_configured'&&shortSecret.issued===false);

const badCustomer=await issueMemberSessionForVerifiedCustomer(env,{
  customer_id:'not-id',
  family_id:familyId,
  now_seconds:now
});
pass('issuer accepts only canonical eight-digit Customer ID',badCustomer.status==='invalid_customer_id');

pass('Family ID validation rejects control characters',__test.validFamilyId('fam_A\nother')===false);
pass('clear cookie retains security attributes',memberSessionCookie.clear().includes('Max-Age=0')&&memberSessionCookie.clear().includes('HttpOnly')&&memberSessionCookie.clear().includes('Secure'));

const health=memberSessionFoundationHealth(env);
pass('health records HMAC-SHA256 and configured secret',health.signature==='HMAC-SHA256'&&health.configured===true);
pass('health records exact identity and explicit Family link requirement',health.exact_customer_id_only===true&&health.explicit_family_link_required_at_issue===true);
pass('token contract excludes ordinary PII and LINE user ID',health.names_in_token===false&&health.email_in_token===false&&health.phone_in_token===false&&health.line_user_id_in_token===false);
pass('LINE Login and Production route remain inactive',health.line_login_activated===false&&health.production_route_wired===false&&health.production_write===false);

console.log(`MEMBER_SESSION_FOUNDATION=${n}/${n} PASS`);
