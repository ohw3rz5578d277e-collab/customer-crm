// TASK-CRM-LINE-FOLLOW-CANONICAL-CUSTOMER-01
// READ ONLY identity damage diagnostics. No repair, no merge, no writes.

const BUILD = "crm-identity-damage-diagnostic-20260820-01";
const FORMAL_LINE_ID = "line_user_id GLOB 'U*' AND length(line_user_id) >= 21";
function text(v){ return v == null ? "" : String(v).trim(); }
function json(data,status=200){ return new Response(JSON.stringify(data,null,2),{status,headers:{"content-type":"application/json; charset=utf-8","cache-control":"no-store","x-crm-identity-damage-diagnostic-build":BUILD}}); }
function bearer(req){ const h=text(req.headers.get("authorization")); return /^Bearer\s+/i.test(h) ? h.replace(/^Bearer\s+/i,"").trim() : ""; }
function internalToken(req){ return text(req.headers.get("x-internal-token")) || bearer(req); }
async function tableExists(db,table){ const r=await db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1").bind(table).first(); return !!r; }
async function count(db,sql){ const r=await db.prepare(sql).first(); return Number(r && r.n || 0); }
async function diagnostic(env){
  if(!env || !env.DB) return {ok:false,error:"db_binding_missing"};
  const db=env.DB;
  const customers = await tableExists(db,"customers");
  const registry = await tableExists(db,"customer_identity_registry");
  if(!customers) return {ok:false,error:"customers_table_missing",build:BUILD};
  const out = {
    ok:true,
    build:BUILD,
    read_only:true,
    repair_executed:false,
    categories:{
      customer_id_missing: await count(db,"SELECT COUNT(*) n FROM customers WHERE customer_id IS NULL OR customer_id=''"),
      name_null_or_empty: await count(db,"SELECT COUNT(*) n FROM customers WHERE name IS NULL OR name=''"),
      name_placeholder: await count(db,"SELECT COUNT(*) n FROM customers WHERE name='名称未設定'"),
      line_user_id_missing: await count(db,"SELECT COUNT(*) n FROM customers WHERE line_user_id IS NULL OR line_user_id=''"),
      line_user_id_invalid_format: await count(db,`SELECT COUNT(*) n FROM customers WHERE line_user_id IS NOT NULL AND line_user_id<>'' AND NOT (${FORMAL_LINE_ID})`),
      line_user_id_equals_customer_id: await count(db,"SELECT COUNT(*) n FROM customers WHERE line_user_id IS NOT NULL AND line_user_id<>'' AND line_user_id=customer_id"),
      customer_id_reservation_prefix: await count(db,"SELECT COUNT(*) n FROM customers WHERE customer_id LIKE 'reservation-%'"),
      same_formal_line_id_multiple_customers: await count(db,`SELECT COUNT(*) n FROM (SELECT line_user_id FROM customers WHERE line_user_id IS NOT NULL AND line_user_id<>'' AND ${FORMAL_LINE_ID} GROUP BY line_user_id HAVING COUNT(*)>1)`),
      same_customer_id_conflicting_line_ids: await count(db,"SELECT COUNT(*) n FROM (SELECT customer_id FROM customers WHERE customer_id IS NOT NULL AND customer_id<>'' GROUP BY customer_id HAVING COUNT(DISTINCT COALESCE(line_user_id,''))>1)")
    },
    registry:{
      table_present: registry
    }
  };
  if(registry){
    out.registry.registry_missing_for_line_linked_customers = await count(db,`SELECT COUNT(*) n FROM customers c WHERE c.line_user_id IS NOT NULL AND c.line_user_id<>'' AND ${FORMAL_LINE_ID.replaceAll("line_user_id","c.line_user_id")} AND NOT EXISTS (SELECT 1 FROM customer_identity_registry r WHERE r.line_user_id=c.line_user_id)`);
    out.registry.registry_customer_id_mismatch = await count(db,"SELECT COUNT(*) n FROM customer_identity_registry r JOIN customers c ON c.line_user_id=r.line_user_id WHERE COALESCE(r.customer_id,'')<>'' AND r.customer_id<>c.customer_id");
  }else{
    out.registry.registry_missing_for_line_linked_customers = null;
    out.registry.registry_customer_id_mismatch = null;
  }
  return out;
}
export async function handleIdentityDamageDiagnostic(req,env){
  const u=new URL(req.url);
  if(u.pathname !== "/api/internal/customer-identity/damage-diagnostic") return null;
  if(req.method !== "GET") return json({ok:false,error:"method_not_allowed"},405);
  const expected=text(env && env.CRM_INTERNAL_TOKEN);
  if(!expected) return json({ok:false,error:"identity_diagnostic_auth_not_configured"},503);
  if(internalToken(req)!==expected) return json({ok:false,error:"unauthorized"},401);
  try{ return json(await diagnostic(env)); }catch(e){ return json({ok:false,error:"identity_diagnostic_failed",detail:text(e&&e.message||e),build:BUILD},500); }
}
export function identityDamageDiagnosticHealth(){
  return {
    identity_damage_diagnostic_enabled:true,
    identity_damage_diagnostic_endpoint:"/api/internal/customer-identity/damage-diagnostic",
    identity_damage_diagnostic_read_only:true,
    identity_damage_diagnostic_repair:false,
    identity_damage_diagnostic_build:BUILD
  };
}
