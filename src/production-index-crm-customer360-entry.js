import app from './production-index-crm-browser-root-entry.js';
import { handleCustomer360Request, customer360Health } from './crm-customer360-runtime.mjs';
import { injectCustomer360Marketing } from './crm-customer360-ui.mjs';
import { injectCustomer360SearchFocus } from './crm-customer360-search-focus.mjs';

const BUILD='customer-crm-customer360-family-marketing-20260828-01';

function headersFrom(response){
  const h=new Headers(response.headers);
  h.delete('content-length');
  h.set('cache-control','no-store, no-cache, must-revalidate, max-age=0');
  h.set('x-crm-customer360-build',BUILD);
  return h;
}

async function patchHealth(response){
  const raw=await response.text();
  let data={};
  try{data=raw?JSON.parse(raw):{}}catch(_){}
  const h=headersFrom(response);
  h.set('content-type','application/json; charset=utf-8');
  return new Response(JSON.stringify({...data,...customer360Health(),customer360_build:BUILD},null,2),{status:response.status,headers:h});
}

async function patchHtml(response){
  const ct=response.headers.get('content-type')||'';
  if(response.status!==200||!ct.includes('text/html'))return response;
  const h=headersFrom(response);
  h.set('content-type','text/html; charset=utf-8');
  const withMarketing=injectCustomer360Marketing(await response.text());
  return new Response(injectCustomer360SearchFocus(withMarketing),{status:response.status,statusText:response.statusText,headers:h});
}

export default {
  async fetch(request,env,ctx){
    const api=await handleCustomer360Request(request,env);
    if(api)return api;

    const url=new URL(request.url);
    let response=await app.fetch(request,env,ctx);
    if(request.method==='GET'&&(url.pathname==='/health'||url.pathname==='/api/crm-health-check'))return patchHealth(response);
    if(request.method==='GET'&&url.pathname==='/admin')return patchHtml(response);
    return response;
  }
};
