import app from './production-index-crm-browser-root-entry.js';
import { handleCustomer360Request, customer360Health } from './crm-customer360-runtime.mjs';
import { handleCustomerProfileEnrichmentRequest, customerProfileEnrichmentHealth } from './crm-customer360-profile-enrichment.mjs';
import { handleCustomer360LineProfileExtraction, customer360LineProfileExtractionHealth } from './crm-customer360-line-profile-extraction.mjs';
import { guardCustomer360ProfileWrite, customer360ProfileWriteGuardHealth } from './crm-customer360-profile-write-guard.mjs';
import { handleCustomer360CombinedDetail, customer360CombinedDetailHealth } from './crm-customer360-combined-detail.mjs';
import { injectCustomer360Marketing } from './crm-customer360-ui.mjs';
import { injectCustomerListDailyOperations } from './crm-customer-list-daily-operations.mjs';
import { injectCustomer360SearchFocus } from './crm-customer360-search-focus.mjs';
import { injectMobileOwnerInteractionRecovery } from './crm-mobile-owner-interaction-recovery.mjs';
import { injectMobileOwnerCardSummary } from './crm-mobile-owner-card-summary.mjs';
import { injectCustomer360DirectNavigation } from './crm-customer360-direct-navigation.mjs';
import { injectOwnerViewState } from './crm-owner-view-state.mjs';
import { injectCustomer360ProfileUi } from './crm-customer360-profile-ui.mjs';
import { injectOwnerAppShell } from './crm-owner-app-shell.mjs';

const BUILD='customer-crm-customer360-profile-enrichment-20260903-05';
const RAW_SCRIPT_CLOSE='<'+String.fromCharCode(92)+'/script>';
const CUSTOMER360_PROFILE_TABLES=[
  'customer_profile_enrichment',
  'customer_family_member_metadata',
  'customer_field_evidence',
  'customer_notes_history'
];

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
  const withProfile=injectCustomer360ProfileUi(withOwnerViewState);
  const withAppShell=injectOwnerAppShell(withProfile);
  return normalizeCustomer360InjectedHtml(withAppShell);
}

function headersFrom(response){
  const h=new Headers(response.headers);
  h.delete('content-length');
  h.set('cache-control','no-store, no-cache, must-revalidate, max-age=0');
  h.set('x-crm-customer360-build',BUILD);
  return h;
}

async function customer360SchemaHealth(env){
  const fallback={
    customer360_profile_enrichment_schema_available:false,
    customer360_family_metadata_available:false,
    customer360_field_evidence_available:false,
    customer360_notes_history_available:false
  };
  if(!env?.DB?.prepare)return fallback;
  try{
    const placeholders=CUSTOMER360_PROFILE_TABLES.map(()=>'?').join(',');
    const result=await env.DB.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name IN (${placeholders})`).bind(...CUSTOMER360_PROFILE_TABLES).all();
    const names=new Set((result?.results||[]).map(row=>String(row?.name||'')));
    return{
      customer360_profile_enrichment_schema_available:names.has('customer_profile_enrichment'),
      customer360_family_metadata_available:names.has('customer_family_member_metadata'),
      customer360_field_evidence_available:names.has('customer_field_evidence'),
      customer360_notes_history_available:names.has('customer_notes_history')
    };
  }catch(_){
    return fallback;
  }
}

export async function patchHealth(response,env){
  const inheritedNotFound=response.status===404;
  const raw=await response.text();
  let data={};
  try{data=raw?JSON.parse(raw):{}}catch(_){}
  if(inheritedNotFound){
    const {ok:_staleOk,message:_staleMessage,error:_staleError,...preservedHealth}=data;
    data=preservedHealth;
  }
  const h=headersFrom(response);
  h.set('content-type','application/json; charset=utf-8');
  const schema=await customer360SchemaHealth(env);
  const status=inheritedNotFound?200:response.status;
  return new Response(JSON.stringify({
    ...data,
    ...(inheritedNotFound?{ok:true}:{}),
    service:data.service||'customer-crm-api',
    ...customer360Health(),
    ...customerProfileEnrichmentHealth(),
    ...customer360LineProfileExtractionHealth(),
    ...customer360ProfileWriteGuardHealth(),
    ...customer360CombinedDetailHealth(),
    ...schema,
    customer360_identity_fallback:false,
    customer360_paid_ai_provider_active:false,
    customer360_build:BUILD
  },null,2),{status,headers:h});
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
    const lineProfileApi=await handleCustomer360LineProfileExtraction(request,env);
    if(lineProfileApi)return lineProfileApi;
    const profileWriteGuard=await guardCustomer360ProfileWrite(request,env);
    if(profileWriteGuard)return profileWriteGuard;
    const profileApi=await handleCustomerProfileEnrichmentRequest(request,env);
    if(profileApi)return profileApi;
    const combinedDetail=await handleCustomer360CombinedDetail(request,env);
    if(combinedDetail)return combinedDetail;
    const api=await handleCustomer360Request(request,env);
    if(api)return api;

    const url=new URL(request.url);
    let response=await app.fetch(request,env,ctx);
    if(request.method==='GET'&&(url.pathname==='/health'||url.pathname==='/api/crm-health-check'))return patchHealth(response,env);
    if(request.method==='GET'&&url.pathname==='/admin')return patchHtml(response);
    return response;
  }
};
