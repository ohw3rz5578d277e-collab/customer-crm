import app from './production-index-crm-customer360-entry.js';
import { handleLineHistorySheetBackfill, lineHistorySheetBackfillHealth } from './crm-line-history-sheet-backfill.mjs';
import { withOwnerPasswordPrincipal, ownerPasswordRequestAuthenticated } from './crm-owner-password-auth.mjs';
import { enforceProductionRequestBoundary, hardenProductionResponse } from './production-index-crm-customer360-entry.js';

const OWNER_EMAIL='ohw3rz5578d277e@gmail.com';
const BACKFILL_PATHS=new Set(['/api/line-history-backfill/preview','/api/line-history-backfill/commit']);

async function handleBackfill(request,env,ctx){
  const effective=await withOwnerPasswordPrincipal(request,env,ctx);
  const ownerEmail=String(effective.headers.get('cf-access-authenticated-user-email')||'').trim().toLowerCase();
  const authorized=ownerPasswordRequestAuthenticated(effective,env)&&ownerEmail===OWNER_EMAIL;
  return handleLineHistorySheetBackfill(effective,env,{authorized});
}
async function patchHealth(response,env){
  const raw=await response.text();let data={};try{data=raw?JSON.parse(raw):{}}catch{return new Response(raw,{status:response.status,headers:response.headers})}
  const headers=new Headers(response.headers);headers.delete('content-length');headers.set('cache-control','no-store, no-cache, must-revalidate, max-age=0');headers.set('content-type','application/json; charset=utf-8');
  return new Response(JSON.stringify({...data,...lineHistorySheetBackfillHealth(env)},null,2),{status:response.status,headers});
}

export default{
  async fetch(request,env,ctx){
    const url=new URL(request.url);
    if(BACKFILL_PATHS.has(url.pathname)){
      const blocked=enforceProductionRequestBoundary(request);if(blocked)return hardenProductionResponse(blocked,request);
      const response=await handleBackfill(request,env,ctx);
      return hardenProductionResponse(response,request);
    }
    const response=await app.fetch(request,env,ctx);
    if(request.method==='GET'&&url.pathname==='/health')return patchHealth(response,env);
    return response;
  }
};
