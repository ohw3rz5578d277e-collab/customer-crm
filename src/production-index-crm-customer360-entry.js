import app from './production-index-crm-browser-root-entry.js';
import todayReadOnlyApp from './production-index-crm-today-dashboard.js';
import { patchBrowserRootHealth } from './production-index-crm-browser-root-entry.js';
import { handleCustomer360Request, customer360Health } from './crm-customer360-runtime.mjs';
import { handleCustomerProfileEnrichmentRequest, customerProfileEnrichmentHealth } from './crm-customer360-profile-enrichment.mjs';
import { handleCustomer360LineProfileExtraction, customer360LineProfileExtractionHealth } from './crm-customer360-line-profile-extraction.mjs';
import { guardCustomer360ProfileWrite, customer360ProfileWriteGuardHealth } from './crm-customer360-profile-write-guard.mjs';
import { handleCustomer360CombinedDetail, customer360CombinedDetailHealth } from './crm-customer360-combined-detail.mjs';
import { handleCustomer360MediaRequest, customer360MediaHealth } from './crm-customer360-media.mjs';
import { injectCustomer360MediaUi, customer360MediaUiHealth } from './crm-customer360-media-ui.mjs';
import { injectCustomer360ExactEditHandoff, customer360ExactEditHandoffHealth } from './crm-customer360-exact-edit-handoff.mjs';
import { injectCustomer360Marketing } from './crm-customer360-ui.mjs';
import { injectCustomerListDailyOperations } from './crm-customer-list-daily-operations.mjs';
import { injectCustomer360SearchFocus } from './crm-customer360-search-focus.mjs';
import { injectMobileOwnerCardSummary } from './crm-mobile-owner-card-summary.mjs';
import { injectOwnerLineChat } from './crm-owner-line-chat.mjs';
import { injectCustomer360DirectNavigation } from './crm-customer360-direct-navigation.mjs';
import { injectOwnerViewState } from './crm-owner-view-state-v2.mjs';
import { injectCustomer360ProfileUi } from './crm-customer360-profile-ui.mjs';
import { injectOwnerAppShell } from './crm-owner-app-shell.mjs';
import { patchReconciliationHealth } from './crm-reconciliation-review.mjs';
import { handleOwnerPasswordAuth, withOwnerPasswordPrincipal, handleOwnerPasswordBrowserGate, ownerPasswordRequestAuthenticated, ownerPasswordAuthHealth } from './crm-owner-password-auth.mjs';
import { reservationInternalUser } from './crm-reservation-browser-handoff.mjs';
import { handleCustomerCsvImport, customerCsvImportHealth, injectCustomerCsvImport } from './crm-customer-csv-import.mjs';

const BUILD='customer-crm-owner-password-auth-20260911-06';
const OWNER_EMAIL='ohw3rz5578d277e@gmail.com';
const RAW_SCRIPT_CLOSE='<'+String.fromCharCode(92)+'/script>';
const CUSTOMER360_PROFILE_TABLES=[
  'customer_profile_enrichment','customer_family_member_metadata','customer_field_evidence','customer_notes_history','customer_profile_media','customer_delivery_links'
];

export const LEGACY_OWNER_VISUAL_ASSET_IDS=Object.freeze([
  'crm-production-desktop-owner-hotfix-20260825',
  'crm-owner-desktop-layout-hotfix-script',
  'crm-customer-detail-v2-style','crm-customer-detail-v2-script',
  'crm-stable-customer-list-style','crm-stable-customer-list-script',
  'crm-number-format-settings-style','crm-number-format-fix-script',
  'crm-stable-audit-style','crm-stable-audit-script',
  'crm-customer-list-return-style','crm-customer-list-return-script',
  'crm-detail-panel-fix-style','crm-detail-panel-fix-script',
  'crm-fetch-safe-fix-style','crm-fetch-safe-fix-script',
  'crm-stability-ux-fix-style','crm-stability-ux-fix-script',
  'crm-mobile-first-ux-style','crm-mobile-first-ux-script',
  'crm-final-layout-cleanup-style','crm-final-layout-cleanup-script',
  'crm-ui-polish-style','crm-ui-polish-script',
  'crm-home-dashboard-style','crm-home-dashboard-script',
  'crm-unified-ux-style','crm-unified-ux-script',
  'crm-line-ops-style','crm-line-ops-script',
  'crmListSafetyStyle','crmListSafetyScript',
  'crmListWorkbenchStyle','crmListWorkbenchScript',
  'crmCustomerSmartPanelStyle','crmCustomerSmartPanelScript',
  'crmMobileUsabilityStyle','crmMobileUsabilityScript',
  'crmUsabilityHubStyle','crmUsabilityHubScript',
  'crmMarketingSuiteStyle','crmMarketingSuiteScript',
  'crmFinalOpsStyle','crmFinalOpsScript',
  'crmInquiryRowActionStyle','crmInquiryRowActionScript',
  'crmInquiryActionStyle','crmInquiryActionScript',
  'crmOpsScreensStyle','crmOpsScreensScript',
  'crmRoadmapSuiteStyle','crmRoadmapSuiteScript',
  'crmDeliveryDashboardStyle','crmDeliveryDashboardScript',
  'crmFollowTemplateButtonStyle','crmFollowTemplateButtonScript',
  'crmFollowTemplateStyle','crmFollowTemplateScript',
  'crmGrowthSuiteStyle','crmGrowthSuiteScript',
  'crmTodayFilterStyle','crmTodayFilterScript',
  'crmTodayActionStyle','crmTodayActionScript',
  'crmTodayDashboardStyle','crmTodayDashboardScript',
  'crmDailySummaryStyle','crmDailySummaryScript',
  'crmLinkAlertStyle','crmLinkAlertScript',
  'crmLinkResyncStyle','crmLinkResyncScript',
  'crmLinkMonitorStyle','crmLinkMonitorScript',
  'crm-reservation-cancel-sync-style','crm-reservation-cancel-sync-script',
  'crm-reservation-update-sync-style','crm-reservation-update-sync-script',
  'crm-reservation-history-sync-style','crm-reservation-history-sync-script',
  'crm-reservation-status-ui-style','crm-reservation-status-ui-script',
  'crm-reservation-created-sync-style','crm-reservation-created-sync-script',
  'crm-reservation-send-style','crm-reservation-send-script',
  'crm-reservation-bridge-style','crm-reservation-bridge-script',
  'crm-ops-polish-style','crm-ops-polish-script',
  'crm-suite-style','crm-suite-script',
  'crm-line-pending-csv-style','crm-line-pending-csv-script',
  'crm-line-pending-filter-style','crm-line-pending-filter-script',
  'crm-line-pending-badges-style','crm-line-pending-badges-script',
  'crm-line-overview-style','crm-line-overview-script',
  'crm-line-log-style','crm-line-log-script',
  'crm-next-actions-script'
]);

function regexEscape(value){return String(value).replace(/[-/\\^$*+?.()|[\]{}]/g,'\\];

export function handleProductionAccessAuthProbe')}
function stripTaggedAssetById(source,tag,id){
  const escaped=regexEscape(id);
  return source.replace(new RegExp('<'+tag+'\\b[^>]*\\bid=(["\\\'])'+escaped+'\\1[^>]*>[\\s\\S]*?<\\/'+tag+'>','gi'),'');
}
export function stripLegacyOwnerVisualAssets(html){
  let out=String(html||'');
  for(const id of LEGACY_OWNER_VISUAL_ASSET_IDS){
    out=stripTaggedAssetById(out,'script',id);
    out=stripTaggedAssetById(out,'style',id);
  }
  out=out.replace(/<a\b[^>]*\bid=(["'])crmReconciliationLink\1[^>]*>[\s\S]*?<\/a>/gi,'');
  return out;
}

export function handleProductionAccessAuthProbe(request,env){
  const url=new URL(request.url);
  if(request.method!=='GET'||url.pathname!=='/__crm/access-auth-probe')return null;
  const expected=String(env?.ADMIN_TOKEN||'');
  const provided=String(request.headers.get('x-admin-token')||'');
  const headers={'content-type':'application/json; charset=utf-8','cache-control':'no-store, no-cache, must-revalidate, max-age=0','x-crm-access-auth-probe':'read-only'};
  if(!expected)return new Response(JSON.stringify({ok:false,error:'auth_probe_unavailable'}),{status:503,headers});
  if(provided!==expected)return new Response(JSON.stringify({ok:false,error:'unauthorized'}),{status:401,headers});
  return new Response(JSON.stringify({ok:true,service:'customer-crm-api',access_auth_probe:true,production_write:false,customer_id_generation:false,line_send:false}),{status:200,headers});
}

export function withReservationOwnerReadPrincipal(request,env){
  if(request.method!=='GET'||!reservationInternalUser(request,env))return request;
  const headers=new Headers(request.headers);
  headers.set('cf-access-authenticated-user-email',OWNER_EMAIL);
  headers.set('x-crm-owner-auth','reservation-internal-read');
  return new Request(request,{headers});
}

export function normalizeCustomer360InjectedHtml(html){return String(html||'').split(RAW_SCRIPT_CLOSE).join('</script>')}
export function injectOwnerLogoutPostRoute(html){
  const source=String(html||'');
  if(!source||source.includes('crm-owner-logout-post-route'))return source;
  const script=`<script id="crm-owner-logout-post-route">(()=>{if(window.__crmOwnerLogoutPostRoute)return;window.__crmOwnerLogoutPostRoute=1;document.addEventListener('click',e=>{const target=e.target&&e.target.closest?e.target.closest('[data-crm-logout],[data-act="logout"],.crm-logout-btn,button,a'):null;if(!target)return;const text=(target.textContent||'').trim();if(!target.matches('[data-crm-logout],[data-act="logout"],.crm-logout-btn')&&text!=='ログアウト')return;e.preventDefault();e.stopImmediatePropagation();try{localStorage.clear();sessionStorage.clear()}catch(_){}let base=typeof window.__CRM_BASE_PATH__==='string'?window.__CRM_BASE_PATH__:'';if(base.endsWith('/'))base=base.slice(0,-1);const form=document.createElement('form');form.method='POST';form.action=base+'/__crm/owner-logout';form.style.display='none';document.body.appendChild(form);form.submit()},true)})();<\/script>`;
  return source.includes('</body>')?source.replace('</body>',script+'</body>'):source+script;
}
export function composeCustomer360AdminHtml(html){
  const canonicalBase=stripLegacyOwnerVisualAssets(html);
  const withMarketing=injectCustomer360Marketing(canonicalBase);
  const withDailyOperations=injectCustomerListDailyOperations(withMarketing);
  const withSearchFocus=injectCustomer360SearchFocus(withDailyOperations);
  const withCardSummary=injectMobileOwnerCardSummary(withSearchFocus);
  const withDirectNavigation=injectCustomer360DirectNavigation(withCardSummary);
  const withLineChat=injectOwnerLineChat(withDirectNavigation);
  const withOwnerViewState=injectOwnerViewState(withLineChat);
  const withProfile=injectCustomer360ProfileUi(withOwnerViewState);
  const withMedia=injectCustomer360MediaUi(withProfile);
  const withEditHandoff=injectCustomer360ExactEditHandoff(withMedia);
  const withAppShell=injectOwnerAppShell(withEditHandoff);
  const withCsvImport=injectCustomerCsvImport(withAppShell);
  const withLogoutRoute=injectOwnerLogoutPostRoute(withCsvImport);
  return normalizeCustomer360InjectedHtml(withLogoutRoute);
}
function headersFrom(response){const h=new Headers(response.headers);h.delete('content-length');h.set('cache-control','no-store, no-cache, must-revalidate, max-age=0');h.set('x-crm-customer360-build',BUILD);return h}
async function customer360SchemaHealth(env){
  const fallback={customer360_profile_enrichment_schema_available:false,customer360_family_metadata_available:false,customer360_field_evidence_available:false,customer360_notes_history_available:false,customer360_profile_media_schema_available:false,customer360_delivery_links_schema_available:false};
  if(!env?.DB?.prepare)return fallback;
  try{
    const placeholders=CUSTOMER360_PROFILE_TABLES.map(()=>'?').join(',');
    const result=await env.DB.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name IN (${placeholders})`).bind(...CUSTOMER360_PROFILE_TABLES).all();
    const names=new Set((result?.results||[]).map(row=>String(row?.name||'')));
    return{customer360_profile_enrichment_schema_available:names.has('customer_profile_enrichment'),customer360_family_metadata_available:names.has('customer_family_member_metadata'),customer360_field_evidence_available:names.has('customer_field_evidence'),customer360_notes_history_available:names.has('customer_notes_history'),customer360_profile_media_schema_available:names.has('customer_profile_media'),customer360_delivery_links_schema_available:names.has('customer_delivery_links')};
  }catch(_){return fallback}
}
export async function patchHealth(response,env){
  const inheritedNotFound=response.status===404;const raw=await response.text();let data={};try{data=raw?JSON.parse(raw):{}}catch(_){}
  if(inheritedNotFound){const {ok:_staleOk,message:_staleMessage,error:_staleError,...preservedHealth}=data;data=preservedHealth}
  const h=headersFrom(response);h.set('content-type','application/json; charset=utf-8');const schema=await customer360SchemaHealth(env);const status=inheritedNotFound?200:response.status;
  return new Response(JSON.stringify({...data,...(inheritedNotFound?{ok:true}:{}),service:data.service||'customer-crm-api',...customer360Health(),...customerProfileEnrichmentHealth(),...customer360LineProfileExtractionHealth(),...customer360ProfileWriteGuardHealth(),...customer360CombinedDetailHealth(),...customer360MediaHealth(),...customer360MediaUiHealth(),...customer360ExactEditHandoffHealth(),...customerCsvImportHealth(),...ownerPasswordAuthHealth(env),...schema,customer360_identity_fallback:false,customer360_paid_ai_provider_active:false,customer360_build:BUILD},null,2),{status,headers:h});
}
export async function handleProductionHealthRequest(request,env){
  const url=new URL(request.url);if(request.method!=='GET'||url.pathname!=='/health')return null;
  let response=new Response(JSON.stringify({ok:false,error:'route_not_found',message:'Not Found'}),{status:404,headers:{'content-type':'application/json; charset=utf-8'}});
  response=await patchBrowserRootHealth(response,env);response=await patchReconciliationHealth(response);return patchHealth(response,env);
}
async function patchHtml(response){const ct=response.headers.get('content-type')||'';if(response.status!==200||!ct.includes('text/html'))return response;const h=headersFrom(response);h.set('content-type','text/html; charset=utf-8');return new Response(composeCustomer360AdminHtml(await response.text()),{status:response.status,statusText:response.statusText,headers:h})}

export default {
  async fetch(request,env,ctx){
    const accessAuthProbe=handleProductionAccessAuthProbe(request,env);if(accessAuthProbe)return accessAuthProbe;
    const ownerAuth=await handleOwnerPasswordAuth(request,env);if(ownerAuth)return ownerAuth;
    const effectiveRequest=await withOwnerPasswordPrincipal(request,env);
    const ownerBrowserGate=handleOwnerPasswordBrowserGate(effectiveRequest,env);if(ownerBrowserGate)return ownerBrowserGate;
    const csvUrl=new URL(effectiveRequest.url);
    if(csvUrl.pathname==='/api/customer-csv-import/preview'||csvUrl.pathname==='/api/customer-csv-import/commit'){
      const ownerEmail=String(effectiveRequest.headers.get('cf-access-authenticated-user-email')||'').trim().toLowerCase();
      const csvApi=await handleCustomerCsvImport(effectiveRequest,env,{authorized:ownerEmail===OWNER_EMAIL});
      if(csvApi)return csvApi;
    }
    const earlyReadRequest=withReservationOwnerReadPrincipal(effectiveRequest,env);
    const earlyUrl=new URL(effectiveRequest.url);
    if(effectiveRequest.method==='GET'&&(earlyUrl.pathname==='/api/today-dashboard'||earlyUrl.pathname==='/api/today-dashboard.csv'))return todayReadOnlyApp.fetch(earlyReadRequest,env,ctx);
    const ownedHealth=await handleProductionHealthRequest(effectiveRequest,env);if(ownedHealth)return ownedHealth;
    const mediaApi=await handleCustomer360MediaRequest(effectiveRequest,env);if(mediaApi)return mediaApi;
    const lineProfileApi=await handleCustomer360LineProfileExtraction(effectiveRequest,env);if(lineProfileApi)return lineProfileApi;
    if(ownerPasswordRequestAuthenticated(effectiveRequest,env)){
      const profileWriteGuard=await guardCustomer360ProfileWrite(effectiveRequest,env);if(profileWriteGuard)return profileWriteGuard;
    }
    const profileApi=await handleCustomerProfileEnrichmentRequest(effectiveRequest,env);if(profileApi)return profileApi;
    const combinedDetail=await handleCustomer360CombinedDetail(earlyReadRequest,env);if(combinedDetail)return combinedDetail;
    const api=await handleCustomer360Request(earlyReadRequest,env);if(api)return api;
    const url=new URL(effectiveRequest.url);let response=await app.fetch(effectiveRequest,env,ctx);
    if(effectiveRequest.method==='GET'&&url.pathname==='/api/crm-health-check')return patchHealth(response,env);
    if(effectiveRequest.method==='GET'&&url.pathname==='/admin')return patchHtml(response);
    return response;
  }
};