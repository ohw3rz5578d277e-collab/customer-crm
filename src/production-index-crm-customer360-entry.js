import app from './production-index-crm-browser-root-entry.js';
import { handleCustomer360Request, customer360Health } from './crm-customer360-runtime.mjs';
import { handleCustomerProfileEnrichmentRequest, customerProfileEnrichmentHealth } from './crm-customer360-profile-enrichment.mjs';
import { injectCustomer360Marketing } from './crm-customer360-ui.mjs';
import { injectCustomerListDailyOperations } from './crm-customer-list-daily-operations.mjs';
import { injectCustomer360SearchFocus } from './crm-customer360-search-focus.mjs';
import { injectMobileOwnerInteractionRecovery } from './crm-mobile-owner-interaction-recovery.mjs';
import { injectMobileOwnerCardSummary } from './crm-mobile-owner-card-summary.mjs';
import { injectCustomer360DirectNavigation } from './crm-customer360-direct-navigation.mjs';
import { injectOwnerViewState } from './crm-owner-view-state.mjs';

const BUILD='customer-crm-customer360-profile-enrichment-20260903-01';
const RAW_SCRIPT_CLOSE='<'+String.fromCharCode(92)+'/script>';

export function normalizeCustomer360InjectedHtml(html){
  return String(html||'').split(RAW_SCRIPT_CLOSE).join('</script>');
}

export function composeCustomer360AdminHtml(html){
  const withMarketing=injectCustomer360Marketing(html);
  const withDailyOperations=injectCustomerListDailyOperations(withMarketing);
  const withSearchFocus=injectCustomer360SearchFocus(withDailyOperations);
  const withCardSummary=injectMobileOwnerCardSummary(withSearchFocus);
  const withRecovery=injectMobileOwnerInteractionRecovery(withCardSummary);
  const withDirectNavigation=injectCustomer360DirectNavigation(withRecovery);
  const withOwnerViewState=injectOwnerViewState(withDirectNavigation);
  return normalizeCustomer360InjectedHtml(withOwnerViewState);
}

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
  return new Response(JSON.stringify({...data,...customer360Health(),...customerProfileEnrichmentHealth(),customer360_build:BUILD},null,2),{status:response.status,headers:h});
}

async function patchHtml(response){
  const ct=response.headers.get('content-type')||'';
  if(response.status!==200||!ct.includes('text/html'))return response;
  const h=headersFrom(response);
  h.set('content-type','text/html; charset=utf-8');
  return new Response(composeCustomer360AdminHtml(await response.text()),{status:response.status,statusText:response.statusText,headers:h});
}

export default {
  async fetch(request,env,ctx){
    const profileApi=await handleCustomerProfileEnrichmentRequest(request,env);
    if(profileApi)return profileApi;
    const api=await handleCustomer360Request(request,env);
    if(api)return api;

    const url=new URL(request.url);
    let response=await app.fetch(request,env,ctx);
    if(request.method==='GET'&&(url.pathname==='/health'||url.pathname==='/api/crm-health-check'))return patchHealth(response);
    if(request.method==='GET'&&url.pathname==='/admin')return patchHtml(response);
    return response;
  }
};
