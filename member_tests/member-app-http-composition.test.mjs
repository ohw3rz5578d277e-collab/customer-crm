import {
  handleMemberAppHttpRequest,
  memberAppHttpCompositionHealth
} from '../src/member-app-http-composition.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const origin='https://member.example.test';

const outside=await handleMemberAppHttpRequest(
  new Request(origin+'/api/customer360/customers'),
  {},
  {
    line_login_handler:async()=>{throw new Error('outside Member prefix must not call login')},
    private_media_handler:async()=>{throw new Error('outside Member prefix must not call media')},
    read_only_handler:async()=>{throw new Error('outside Member prefix must not call read')}
  }
);
pass('non-Member API is not claimed',outside===null);

const order=[];
let loginOptions=null;
const loginResponse=await handleMemberAppHttpRequest(
  new Request(origin+'/api/member/login/line/start'),
  {},
  {
    line_login_handler:async(_request,_env,options)=>{
      order.push('login');
      loginOptions=options;
      return new Response(null,{status:302,headers:{location:'https://access.line.me/oauth2/v2.1/authorize'}});
    },
    private_media_handler:async()=>{order.push('media');return null},
    read_only_handler:async()=>{order.push('read');return null}
  }
);
pass('LINE Login route is handled first',loginResponse.status===302&&order.join(',')==='login');
pass('LINE Login defaults to no activation approval',loginOptions.approved===false);

let approvedSeen=false;
await handleMemberAppHttpRequest(
  new Request(origin+'/api/member/login/line/callback?code=x&state=y'),
  {},
  {
    line_login_approved:true,
    line_login_handler:async(_request,_env,options)=>{
      approvedSeen=options.approved===true;
      return new Response(null,{status:303,headers:{location:origin+'/member'}});
    },
    private_media_handler:async()=>null,
    read_only_handler:async()=>null
  }
);
pass('explicit trusted caller approval is passed through without being invented',approvedSeen===true);

const adapter={async get(){return null}};
const mediaOrder=[];
let adapterSeen=null;
const mediaResponse=await handleMemberAppHttpRequest(
  new Request(origin+'/api/member/media/content',{
    method:'POST',
    headers:{origin},
    body:'{}'
  }),
  {},
  {
    storage_adapter:adapter,
    line_login_handler:async()=>{mediaOrder.push('login');return null},
    private_media_handler:async(_request,_env,options)=>{
      mediaOrder.push('media');
      adapterSeen=options.storage_adapter;
      return new Response(new Uint8Array([1]),{
        status:200,
        headers:{'content-type':'image/jpeg'}
      });
    },
    read_only_handler:async()=>{mediaOrder.push('read');return null}
  }
);
pass('private media is second router after login fallthrough',mediaResponse.status===200&&mediaOrder.join(',')==='login,media');
pass('composition passes only explicitly supplied storage adapter',adapterSeen===adapter);

const readOrder=[];
let nowSeen=null;
const readResponse=await handleMemberAppHttpRequest(
  new Request(origin+'/api/member/home'),
  {},
  {
    now_seconds:1800000000,
    line_login_handler:async()=>{readOrder.push('login');return null},
    private_media_handler:async()=>{readOrder.push('media');return null},
    read_only_handler:async(_request,_env,options)=>{
      readOrder.push('read');
      nowSeen=options.now_seconds;
      return new Response(JSON.stringify({ok:true}),{
        status:200,
        headers:{'content-type':'application/json'}
      });
    }
  }
);
pass('read-only routes run after login and media fallthrough',readResponse.status===200&&readOrder.join(',')==='login,media,read');
pass('composition passes deterministic now value to read router',nowSeen===1800000000);

const unknownOrder=[];
const unknown=await handleMemberAppHttpRequest(
  new Request(origin+'/api/member/not-a-real-route'),
  {},
  {
    line_login_handler:async()=>{unknownOrder.push('login');return null},
    private_media_handler:async()=>{unknownOrder.push('media');return null},
    read_only_handler:async()=>{unknownOrder.push('read');return null}
  }
);
pass('unknown Member API falls through after all source routers',unknown===null&&unknownOrder.join(',')==='login,media,read');

const missingLogin=await handleMemberAppHttpRequest(
  new Request(origin+'/api/member/home'),
  {},
  {
    line_login_handler:null,
    private_media_handler:async()=>null,
    read_only_handler:async()=>new Response('{}')
  }
);
pass('missing source router fails closed instead of silently bypassing it',missingLogin.status===503&&(await missingLogin.clone().json()).error==='member_line_login_handler_unavailable');

const errorResponse=new Response(JSON.stringify({ok:false,error:'member_session_required'}),{
  status:401,
  headers:{'content-type':'application/json','x-test-marker':'read'}
});
const propagated=await handleMemberAppHttpRequest(
  new Request(origin+'/api/member/home'),
  {},
  {
    line_login_handler:async()=>null,
    private_media_handler:async()=>null,
    read_only_handler:async()=>errorResponse
  }
);
pass('subrouter response is returned without rewriting security semantics',propagated===errorResponse&&propagated.status===401&&propagated.headers.get('x-test-marker')==='read');

const health=memberAppHttpCompositionHealth({});
pass('composition health is source-only',health.member_app_http_composition===true&&health.source_only===true);
pass('all Member source layers are present',health.member_ui_source_ready===true&&health.line_login_source_ready===true&&health.read_only_source_ready===true&&health.private_media_source_ready===true&&health.all_source_layers_present===true);
pass('composition order is deterministic',health.router_order.join(',')==='line_login,private_media,read_only');
pass('LINE Login approval remains default-off',health.line_login_default_approved===false&&health.line_login_explicit_approval_passthrough_only===true);
pass('storage dependency stays explicit',health.private_media_storage_adapter_explicit===true&&health.implicit_env_storage_binding===false);
pass('write and commerce routes are excluded',health.favorite_mutation_route_included===false&&health.memory_write_route_included===false&&health.black_entitlement_write_route_included===false&&health.checkout_route_included===false&&health.payment_route_included===false);
pass('identity mutation and LINE send stay off',health.customer_create===false&&health.customer_id_generation===false&&health.family_create===false&&health.family_auto_link===false&&health.canonical_crm_write===false&&health.line_send===false);
pass('Production entry activation deploy storage and write stay off',health.production_entry_wired===false&&health.production_route_activation===false&&health.production_storage_binding===false&&health.production_storage_fetch===false&&health.production_deploy===false&&health.production_write===false);

console.log(`MEMBER_APP_HTTP_COMPOSITION=${n}/${n} PASS`);
