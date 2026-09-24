import {
  issueMemberSessionForVerifiedCustomer
} from '../src/member-session-foundation.mjs';
import {
  handleMemberFavoriteMutationRequest,
  memberFavoritesHttpContractHealth,
  __test
} from '../src/member-favorites-http-contract.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const origin='https://member.example.test';
const path='/api/internal/member/favorites/mutate';
const familyId='fam_A';
const customerId='26000123';
const memoryId='mem_A1';
const secret='0123456789abcdef0123456789abcdef';

function makeDb({favorite=false}={}){
  const state={
    favorite,
    linkedFamily:familyId,
    writes:[],
    seenSql:[]
  };

  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    'member_memories',
    'member_memory_favorites'
  ]);

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
            const requested=bound.params[0];
            return requested===state.linkedFamily
              ?{family_id:requested,display_name:'TEST FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          if(sql.includes('FROM member_memories')){
            const [requestedMemory,requestedFamily]=bound.params;
            return requestedMemory===memoryId&&requestedFamily===familyId
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
              ?{results:[{
                  family_id:state.linkedFamily,
                  customer_id:customerId,
                  relation:'owner',
                  access_role:'owner'
                }]}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return bound.params[0]===state.linkedFamily
              ?{results:[{
                  family_id:state.linkedFamily,
                  customer_id:customerId,
                  relation:'owner',
                  access_role:'owner'
                }]}
              :{results:[]};
          }
          return {results:[]};
        },
        async run(){
          state.writes.push({sql,params:bound.params});
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

async function issuedCookie(db){
  const issued=await issueMemberSessionForVerifiedCustomer(
    {DB:db,MEMBER_SESSION_SECRET:secret},
    {customer_id:customerId,family_id:familyId,now_seconds:Math.floor(Date.now()/1000)}
  );
  assert(issued.status==='ok'&&issued.issued===true,'session issue failed in test');
  return issued.cookie.split(';')[0];
}

function request(body,{
  cookie='',
  requestOrigin=origin,
  method='POST',
  contentType='application/json'
}={}){
  const headers={};
  if(requestOrigin!==null)headers.origin=requestOrigin;
  if(contentType!==null)headers['content-type']=contentType;
  if(cookie)headers.cookie=cookie;
  return new Request(origin+path,{
    method,
    headers,
    body:method==='POST'&&body!==undefined
      ?(typeof body==='string'?body:JSON.stringify(body))
      :undefined
  });
}

pass('route mode defaults disabled',__test.routeEnabled({})===false);
pass('route mode requires exact enabled',__test.routeEnabled({MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled'})===true);
pass('same-origin request accepted',__test.sameOrigin(new Request(origin+path,{headers:{origin}}))===true);
pass('cross-origin request rejected',__test.sameOrigin(new Request(origin+path,{headers:{origin:'https://evil.example'}}))===false);
pass('missing Origin rejected',__test.sameOrigin(new Request(origin+path))===false);
pass('JSON content type accepted',__test.jsonContentType(new Request(origin+path,{headers:{'content-type':'application/json; charset=utf-8'}}))===true);
pass('form content type rejected',__test.jsonContentType(new Request(origin+path,{headers:{'content-type':'application/x-www-form-urlencoded'}}))===false);
pass('exact body accepted',!!__test.exactMutationBody({memory_id:memoryId,desired_favorite:true}));
pass('extra customer identity rejected',__test.exactMutationBody({memory_id:memoryId,desired_favorite:true,customer_id:'26000999'})===null);
pass('extra family identity rejected',__test.exactMutationBody({memory_id:memoryId,desired_favorite:true,family_id:'fam_B'})===null);
pass('client approved flag rejected',__test.exactMutationBody({memory_id:memoryId,desired_favorite:true,approved:true})===null);
pass('non-boolean desired state rejected',__test.exactMutationBody({memory_id:memoryId,desired_favorite:'true'})===null);

const db=makeDb();
const cookie=await issuedCookie(db);

const routeOff=await handleMemberFavoriteMutationRequest(
  request({memory_id:memoryId,desired_favorite:true},{cookie}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
pass('route-disabled contract hides mutation path',routeOff.status===404&&db.state.writes.length===0);

const wrongMethod=await handleMemberFavoriteMutationRequest(
  request(undefined,{cookie,method:'GET'}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
pass('mutation contract is POST only',wrongMethod.status===405&&db.state.writes.length===0);

const crossOrigin=await handleMemberFavoriteMutationRequest(
  request({memory_id:memoryId,desired_favorite:true},{cookie,requestOrigin:'https://evil.example'}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
pass('cross-origin POST blocked before write',crossOrigin.status===403&&db.state.writes.length===0);

const noOrigin=await handleMemberFavoriteMutationRequest(
  request({memory_id:memoryId,desired_favorite:true},{cookie,requestOrigin:null}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
pass('missing Origin blocked before write',noOrigin.status===403&&db.state.writes.length===0);

const wrongType=await handleMemberFavoriteMutationRequest(
  request({memory_id:memoryId,desired_favorite:true},{cookie,contentType:'text/plain'}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
pass('non-JSON body rejected before write',wrongType.status===415&&db.state.writes.length===0);

const noSession=await handleMemberFavoriteMutationRequest(
  request({memory_id:memoryId,desired_favorite:true}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
pass('signed Member session cookie required',noSession.status===401&&db.state.writes.length===0);

const injectedIdentity=await handleMemberFavoriteMutationRequest(
  request({
    memory_id:memoryId,
    desired_favorite:true,
    customer_id:'26000999'
  },{cookie}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
pass('body cannot inject Customer identity',injectedIdentity.status===400&&db.state.writes.length===0);

const injectedApproval=await handleMemberFavoriteMutationRequest(
  request({
    memory_id:memoryId,
    desired_favorite:true,
    approved:true
  },{cookie}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
pass('client cannot inject executor approval',injectedApproval.status===400&&db.state.writes.length===0);

const oversized=await handleMemberFavoriteMutationRequest(
  request(JSON.stringify({
    memory_id:'m'.repeat(__test.MAX_BODY_BYTES+100),
    desired_favorite:true
  }),{cookie}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
pass('oversized request body rejected',oversized.status===413&&db.state.writes.length===0);

const writeOff=await handleMemberFavoriteMutationRequest(
  request({memory_id:memoryId,desired_favorite:true},{cookie}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'disabled'
  }
);
pass('executor write mode remains independent gate',writeOff.status===503&&db.state.writes.length===0);

const add=await handleMemberFavoriteMutationRequest(
  request({memory_id:memoryId,desired_favorite:true},{cookie}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
const addBody=await add.json();
pass('valid signed same-origin request adds Favorite',add.status===200&&addBody.ok===true&&addBody.favorite===true&&addBody.changed===true&&db.state.writes.length===1);
pass('success response does not expose Customer or Family ID',!('customer_id' in addBody)&&!('family_id' in addBody));

const addAgain=await handleMemberFavoriteMutationRequest(
  request({memory_id:memoryId,desired_favorite:true},{cookie}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
const addAgainBody=await addAgain.json();
pass('repeat desired Favorite is HTTP idempotent',addAgain.status===200&&addAgainBody.changed===false&&addAgainBody.idempotent_noop===true&&db.state.writes.length===1);

const remove=await handleMemberFavoriteMutationRequest(
  request({memory_id:memoryId,desired_favorite:false},{cookie}),
  {
    DB:db,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
const removeBody=await remove.json();
pass('valid request removes Favorite',remove.status===200&&removeBody.favorite===false&&removeBody.changed===true&&db.state.writes.length===2);

const staleDb=makeDb();
const staleCookie=await issuedCookie(staleDb);
staleDb.state.linkedFamily='fam_B';
const stale=await handleMemberFavoriteMutationRequest(
  request({memory_id:memoryId,desired_favorite:true},{cookie:staleCookie}),
  {
    DB:staleDb,
    MEMBER_SESSION_SECRET:secret,
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
    MEMBER_FAVORITES_WRITE_MODE:'enabled'
  }
);
pass('stale signed session is re-authorized against current Family link',stale.status===403&&staleDb.state.writes.length===0);

const health=memberFavoritesHttpContractHealth({});
pass('health records route default disabled and not Production-wired',health.route_default_disabled===true&&health.route_mode==='disabled'&&health.production_route_wired===false);
pass('health records exact JSON request contract',health.method==='POST'&&health.content_type==='application/json'&&health.exact_request_keys.join(',')==='memory_id,desired_favorite');
pass('health forbids client approval and identity',health.client_approved_input===false&&health.client_customer_id_input===false&&health.client_family_id_input===false);
pass('health requires signed cookie and same-origin defense',health.signed_member_session_cookie_required===true&&health.same_origin_required===true&&health.same_site_cookie_defense==='Lax');
pass('health preserves executor write gate and no automatic Production write',health.executor_write_mode_still_required===true&&health.automatic_write===false&&health.production_write_enabled===false&&health.line_send===false);

console.log(`MEMBER_FAVORITES_HTTP_CONTRACT=${n}/${n} PASS`);
