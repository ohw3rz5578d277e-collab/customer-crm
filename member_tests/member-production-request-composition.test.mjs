import {
  handleMemberProductionRequest,
  memberProductionRouteModeEnabled,
  classifyMemberProductionRequest,
  memberProductionRequestCompositionHealth
} from '../src/member-production-request-composition.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const origin='https://member.example.test';

pass('route mode defaults disabled',memberProductionRouteModeEnabled({})===false);
pass('route mode requires exact enabled',memberProductionRouteModeEnabled({MEMBER_PRODUCTION_ROUTE_MODE:'enabled'})===true&&memberProductionRouteModeEnabled({MEMBER_PRODUCTION_ROUTE_MODE:'ENABLED'})===false);

pass('classifier separates assets browser and api',
  classifyMemberProductionRequest(new Request(origin+'/member-assets/member-app.css'))==='assets'
  && classifyMemberProductionRequest(new Request(origin+'/member'))==='browser'
  && classifyMemberProductionRequest(new Request(origin+'/member/memories'))==='browser'
  && classifyMemberProductionRequest(new Request(origin+'/api/member/home'))==='api'
);
pass('classifier does not claim unrelated CRM routes',
  classifyMemberProductionRequest(new Request(origin+'/admin'))===null
  && classifyMemberProductionRequest(new Request(origin+'/api/customer360/customers'))===null
);

let calls=0;
const disabled=await handleMemberProductionRequest(
  new Request(origin+'/member'),
  {},
  {
    approved:true,
    browser_handler:async()=>{calls++;return new Response('must not run')}
  }
);
pass('disabled route mode preserves existing Production fallthrough',disabled===null&&calls===0);

const noApproval=await handleMemberProductionRequest(
  new Request(origin+'/member'),
  {MEMBER_PRODUCTION_ROUTE_MODE:'enabled'},
  {
    approved:false,
    browser_handler:async()=>{calls++;return new Response('must not run')}
  }
);
pass('enabled mode without exact Owner approval fails closed',noApproval.status===503&&calls===0);

let assetAdapterSeen=null;
const asset=await handleMemberProductionRequest(
  new Request(origin+'/member-assets/member-app.css'),
  {MEMBER_PRODUCTION_ROUTE_MODE:'enabled'},
  {
    approved:true,
    public_asset_adapter:{get(){}},
    asset_handler:async(_request,_env,options)=>{
      calls++;
      assetAdapterSeen=options.asset_adapter;
      return new Response('css',{status:200});
    }
  }
);
pass('asset route dispatches through explicit public adapter',asset.status===200&&assetAdapterSeen&&typeof assetAdapterSeen.get==='function');

let browserNowSeen=null;
const browser=await handleMemberProductionRequest(
  new Request(origin+'/member'),
  {MEMBER_PRODUCTION_ROUTE_MODE:'enabled'},
  {
    approved:true,
    now_seconds:1800000000,
    browser_handler:async(_request,_env,options)=>{
      browserNowSeen=options.now_seconds;
      return new Response('member',{status:200});
    }
  }
);
pass('browser route receives deterministic clock only',browser.status===200&&browserNowSeen===1800000000);

let apiOptions=null;
const api=await handleMemberProductionRequest(
  new Request(origin+'/api/member/login/line/callback?code=x&state=y'),
  {MEMBER_PRODUCTION_ROUTE_MODE:'enabled'},
  {
    approved:true,
    line_login_approved:false,
    private_media_storage_adapter:{get(){}},
    api_handler:async(_request,_env,options)=>{
      apiOptions=options;
      return new Response('api',{status:200});
    }
  }
);
pass('api route keeps LINE external exchange approval false unless separately approved',api.status===200&&apiOptions.line_login_approved===false);
pass('api route passes only explicit private media adapter',apiOptions.storage_adapter&&typeof apiOptions.storage_adapter.get==='function');

let approvedSeen=null;
await handleMemberProductionRequest(
  new Request(origin+'/api/member/login/line/callback?code=x&state=y'),
  {MEMBER_PRODUCTION_ROUTE_MODE:'enabled'},
  {
    approved:true,
    line_login_approved:true,
    api_handler:async(_request,_env,options)=>{
      approvedSeen=options.line_login_approved;
      return new Response('api');
    }
  }
);
pass('separate LINE approval can be passed explicitly by trusted caller',approvedSeen===true);

const health=memberProductionRequestCompositionHealth({});
pass('health is source-only and default-off',health.member_production_request_composition===true&&health.source_only===true&&health.route_mode_default_disabled===true);
pass('health records separate Owner and LINE approval boundaries',health.explicit_owner_approval_required===true&&health.line_login_approval_separate===true&&health.line_login_approval_default_false===true);
pass('health requires explicit public and private storage adapters',health.public_asset_adapter_explicit===true&&health.private_media_storage_adapter_explicit===true&&health.implicit_env_asset_binding===false&&health.implicit_env_private_media_binding===false);
pass('health leaves Production entry route deploy fetch and write off',health.production_entry_wired===false&&health.production_route_activated===false&&health.production_deploy===false&&health.production_public_asset_fetch===false&&health.production_private_media_fetch===false&&health.production_write===false);
pass('health keeps identity mutation and LINE send off',health.customer_create===false&&health.customer_id_generation===false&&health.family_create===false&&health.family_auto_link===false&&health.canonical_crm_write===false&&health.line_send===false);

console.log('MEMBER_PRODUCTION_REQUEST_COMPOSITION='+n+'/'+n+' PASS');
