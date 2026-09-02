import { handleCustomer360Request } from './crm-customer360-runtime.mjs';
import { handleCustomerProfileEnrichmentRequest } from './crm-customer360-profile-enrichment.mjs';

export async function handleCustomer360CombinedDetail(request,env){
  if(request.method!=='GET')return null;
  const url=new URL(request.url),m=url.pathname.match(/^\/api\/customer360\/customer\/([0-9]{8})$/);if(!m)return null;
  const base=await handleCustomer360Request(request,env);if(!base)return null;
  if(!base.ok)return base;
  let baseJson;try{baseJson=await base.clone().json()}catch{return base}
  if(!baseJson?.ok||!baseJson.customer)return base;
  const profileUrl=new URL(request.url);profileUrl.pathname=`/api/customer360/profile/${m[1]}`;profileUrl.search='';
  const profileReq=new Request(profileUrl.toString(),{method:'GET',headers:request.headers});
  const profileRes=await handleCustomerProfileEnrichmentRequest(profileReq,env);
  if(profileRes?.ok){try{const pj=await profileRes.json();if(pj?.ok&&pj.customer)baseJson.customer.profile=pj.customer}catch{}}
  const headers=new Headers(base.headers);headers.delete('content-length');headers.set('content-type','application/json; charset=utf-8');headers.set('cache-control','no-store');
  return new Response(JSON.stringify(baseJson),{status:base.status,headers});
}

export function customer360CombinedDetailHealth(){return{customer360_profile_composed_into_detail:true,customer360_profile_initial_extra_request:false};}
