import assert from "node:assert/strict";
import {DatabaseSync} from "node:sqlite";
import {crmRes15IdentityRead,CRM_RES15_QUERIES,CRM_RES15_ROUTE} from "../src/crm-res15-identity-readonly.mjs";
import {readFileSync} from "node:fs";
const entry=readFileSync(new URL("../src/production-index-crm-customer360-entry.js",import.meta.url),"utf8");
assert.match(entry,/crmRes15IdentityRead/);
assert.match(entry,/const res15IdentityRead=await crmRes15IdentityRead\(request,env\)/);
const C="26001234",L="U0123456789abcdef0123456789abcdef",secret="x".repeat(48);
function fixture(){
 const db=new DatabaseSync(":memory:");db.exec("CREATE TABLE customers(customer_id TEXT,line_user_id TEXT,deleted_at TEXT);"+
  "CREATE TABLE customer_identity_registry(customer_id TEXT,line_user_id TEXT,status TEXT);");
 const observed=[];
 const DB={prepare(sql){assert.ok(Object.values(CRM_RES15_QUERIES).includes(sql),"only four fixed SELECT");
  observed.push(sql);let args=[];return{bind(...v){args=v;return this},async all(){return{results:db.prepare(sql).all(...args)}}};}};
 return {db,DB,observed};
}
const a=fixture();
function req(input={candidate_customer_id:C,blocked_line_user_id:L},more={}){
 return new Request("https://crm.test"+CRM_RES15_ROUTE+(more.query||""),{
  method:more.method||"POST",
  headers:{"content-type":more.contentType||"application/json",
    ...(more.token===null?{}:{"x-res15-read-token":more.token||secret}),
    ...more.headers},
  body:more.method==="GET"?undefined:typeof input==="string"?input:JSON.stringify(input)
 });
}
const env={DB:a.DB,RES15_CRM_IDENTITY_READ_ENABLED:"true",RES15_CRM_IDENTITY_READ_TOKEN:secret};
async function check(request,e=env){const res=await crmRes15IdentityRead(request,e);return{status:res.status,body:await res.json()}}
try{
 a.db.prepare("INSERT INTO customers VALUES(?,?,NULL)").run(C,L);
 a.db.prepare("INSERT INTO customer_identity_registry VALUES(?,?,'active')").run(C,L);
 assert.equal((await check(req(),{...env,RES15_CRM_IDENTITY_READ_ENABLED:"false"})).status,404);
 assert.equal((await check(req(),{...env,RES15_CRM_IDENTITY_READ_TOKEN:undefined})).status,401);
 assert.equal((await check(req({}, {token:"bad"}))).status,401);
 assert.equal((await check(req({}, {token:null}))).status,401);
 assert.equal((await check(req({}, {headers:{"origin":"https://crm.test"}}))).status,403);
 assert.equal((await check(req({}, {headers:{"cookie":"session=abc"}}))).status,403);
 assert.equal((await check(req({}, {headers:{"authorization":"Bearer "+secret}}))).status,403);
 assert.equal((await check(req({}, {headers:{"sec-fetch-site":"same-origin"}}))).status,403);
 assert.equal((await check(req({}, {query:"?token="+secret}))).status,403);
 assert.equal((await check(req({}, {method:"GET"}))).status,405);
 assert.equal((await check(req({}, {contentType:"text/plain"}))).status,415);
 assert.equal((await check(req({candidate_customer_id:C+" ",blocked_line_user_id:L}))).status,400);
 assert.equal((await check(req({candidate_customer_id:[C],blocked_line_user_id:L}))).status,400);
 assert.equal((await check(req({candidate_customer_id:C,blocked_line_user_id:L,extra:"x"}))).status,400);
 assert.equal((await check(req("{"+"x".repeat(600)+"}"))).status,400);
 assert.equal(a.observed.length,0,"unauthorized and invalid requests cannot touch CRM DB");
 console.log("RES15_CRM_API_DISABLED_AUTH_AND_PII_BOUNDARY=PASS");
 const matched=await check(req());
 assert.equal(matched.status,200);assert.equal(matched.body.verdict,"matched_snapshot_only");
 assert.equal(matched.body.write_allowed,false);assert.equal(matched.body.auto_replay_allowed,false);
 assert.equal(matched.body.owner_review_required,true);
 assert.equal(a.observed.length,4);
 for(const value of [C,L,secret])assert.equal(JSON.stringify(matched.body).includes(value),false);
 a.db.prepare("UPDATE customer_identity_registry SET status='disabled'").run();
 assert.equal((await check(req())).body.verdict,"inactive_or_deleted");
 a.db.prepare("UPDATE customer_identity_registry SET status='active'").run();
 a.db.prepare("UPDATE customers SET deleted_at='2026-11-01'").run();
 assert.equal((await check(req())).body.verdict,"inactive_or_deleted");
 a.db.prepare("UPDATE customers SET deleted_at=NULL").run();
 a.db.prepare("INSERT INTO customers VALUES(?,?,NULL)").run("26007777",L);
 assert.equal((await check(req())).body.verdict,"missing_or_ambiguous");
 a.db.prepare("DELETE FROM customers WHERE customer_id='26007777'").run();
 a.db.prepare("UPDATE customer_identity_registry SET line_user_id='Uffffffffffffffffffffffffffffffff'").run();
 assert.equal((await check(req())).body.verdict,"missing_or_ambiguous");
 const failing={...env,DB:{prepare(){throw Error("VERY_PRIVATE_DB_DETAILS")}}};
 const fail=await check(req(),failing);
 assert.equal(fail.status,503);assert.equal(JSON.stringify(fail.body).includes("PRIVATE"),false);
 console.log("RES15_CRM_API_FOUR_SELECT_IDENTITY_STATE_AND_FAIL_CLOSED=PASS");
 assert.equal(await crmRes15IdentityRead(new Request("https://crm.test/health"),env),null);
 console.log("RES15_CRM_API_OTHER_ROUTES_UNTOUCHED=PASS");
 console.log("PRODUCTION_DEPLOY=0 D1_REMOTE=0 SECRETS_CHANGED=0 CRM_WRITE=0");
}finally{a.db.close()}
