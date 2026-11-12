// A06 offline source inventory. This is lexical evidence, not an AST,
 // deployment attestation, proof of every runtime invocation or permission
 // to execute any mutation.
import assert from 'node:assert/strict';
import {readdirSync,readFileSync,existsSync} from 'node:fs';
import {join,relative,resolve,dirname,extname} from 'node:path';
const ROOT=process.cwd();
const service="crm";
const entrypoints=["src/index.js","src/secure-index.js","src/production-index.js"];
const mandatory=["src/customer-identity-resolver.mjs","src/crm-customer-id-autofill-runtime.mjs","src/crm-customer360-profile-enrichment.mjs","src/crm-customer-csv-import.mjs"];
const pth=path=>relative(ROOT,path).replaceAll('\\','/');
function files(dir,extensions){
 if(!existsSync(dir))return[];
 let found=[];
 for(const d of readdirSync(dir,{withFileTypes:true})){
  if(['.git','node_modules','.wrangler'].includes(d.name))continue;
  const name=join(dir,d.name);
  if(d.isDirectory())found=found.concat(files(name,extensions));
  else if(d.isFile()&&extensions.some(x=>d.name.endsWith(x)))found.push(name);
 }
 return found;
}
const sourceFiles=files(join(ROOT,'src'),['.js','.mjs']).sort();
const content=new Map(sourceFiles.map(f=>[pth(f),readFileSync(f,'utf8')]));
const staticSpecifiers=src=>{
 const found=[];
 const patterns=[
  /\b(?:import|export)\s+(?:[\s\S]{0,220}?\sfrom\s*)?(["'])(\.{1,2}\/[^"'\n]+)\1/g,
  /\b(?:require|import)\s*\(\s*(["'])(\.{1,2}\/[^"'\n]+)\1\s*\)/g,
  /\bfrom\s*(["'])(\.{1,2}\/[^"'\n]+)\1/g
 ];
 for(const p of patterns)for(const m of src.matchAll(p))found.push(m[2]);
 return [...new Set(found)];
};
const lookup=(from,spec)=>{
 const base=resolve(dirname(join(ROOT,from)),spec);
 const possible=extname(base)?[base]:[base,base+'.js',base+'.mjs',join(base,'index.js'),join(base,'index.mjs')];
 return possible.map(pth).find(candidate=>content.has(candidate))||null;
};
const edges=[],unknown=[];
for(const [path,source] of content){
 for(const spec of staticSpecifiers(source)){
  const target=lookup(path,spec);
  if(target)edges.push({from:path,to:target});
  else unknown.push({from:path,specifier:spec});
 }
}
const links=new Map();
for(const edge of edges){
 const set=links.get(edge.from)||new Set();set.add(edge.to);links.set(edge.from,set);
}
for(const name of [...entrypoints,...mandatory])
 assert.ok(content.has(name),'A06 pinned real source missing: '+name);
const reachable=new Set(entrypoints),queue=[...entrypoints];
for(const current of queue){
 for(const target of links.get(current)||[]){
  if(!reachable.has(target)){reachable.add(target);queue.push(target)}
 }
}
const statements=[];
const write=/\b(?:INSERT\s+(?:OR\s+[A-Z]+\s+)?INTO|REPLACE\s+INTO|UPDATE\s+|DELETE\s+FROM|ON\s+CONFLICT)\b/ig;
for(const [path,source] of content){
 const sql=[...source.matchAll(write)];
 if(!sql.length)continue;
 const dynamic=/\b(?:UPDATE|INSERT|DELETE|ON\s+CONFLICT)[\s\S]{0,150}\$\{/i.test(source);
 const bulk=/\b(?:WHERE\s+[\w.]+\s+IN\s*\(|\.batch\s*\(|for\s*\([^)]*\)\s*\{[\s\S]{0,320}?\.(?:run|batch)\s*\()/i.test(source);
 const identity=/(customer_id|line_user_id|approved_customer_id|customer_identity_registry)/i.test(source);
 statements.push({path,possible_sql_mutation:sql.length,dynamic,bulk,identity,statically_reachable:reachable.has(path)});
}
const migrationFiles=[...files(join(ROOT,'migrations'),['.sql']),...files(join(ROOT,'migrations_managed'),['.sql'])].sort();
const migrationSummary=migrationFiles.map(path=>{
 const s=readFileSync(path,'utf8');
 const triggers=[...s.matchAll(/\bCREATE\s+(?:TEMP(?:ORARY)?\s+)?TRIGGER\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)/gi)].map(m=>m[1]);
 return{path:pth(path),triggers,identity_text:/(customer_id|customer_identity_registry|app_reservations|ai_reservation_drafts)/i.test(s)};
});
const byPath=new Map(statements.map(x=>[x.path,x]));
for(const name of mandatory)assert.ok(byPath.has(name),
 'known material/delegating writer must stay in inventory: '+name);
assert.ok(statements.length>10,'source inventory unexpectedly empty');
const summary={
 service,source_only:true,production_schema_attested:false,
 exhaustive_function_call_graph:false,unresolved_dynamic_dispatch:true,
 entrypoint_file_candidates:entrypoints,
 src_files:sourceFiles.length,import_edges:edges.length,
 unresolved_relative_imports:unknown.length,
 entrypoint_reachable_source_files:reachable.size,
 sql_lexical_candidate_files:statements.length,
 identity_candidate_files:statements.filter(s=>s.identity).length,
 dynamic_sql_candidate_files:statements.filter(s=>s.dynamic).length,
 bulk_or_batch_candidate_files:statements.filter(s=>s.bulk).length,
 identity_candidates_outside_static_graph:statements.filter(s=>s.identity&&!s.statically_reachable).length,
 priority_paths:mandatory.map(path=>({path,reachable:reachable.has(path),dynamic:byPath.get(path).dynamic,bulk:byPath.get(path).bulk})),
 dynamic_examples:statements.filter(s=>s.dynamic).slice(0,12).map(s=>s.path),
 static_unresolved_examples:unknown.slice(0,12),
 migration_sql_files:migrationFiles.length,
 source_trigger_definitions:migrationSummary.flatMap(x=>x.triggers.map(t=>({path:x.path,name:t,identity_text:x.identity_text}))),
 remote_d1_calls:0,production_writes:0
};
assert.equal(summary.exhaustive_function_call_graph,false);
assert.equal(summary.production_schema_attested,false);
console.log('RES15_'+service.toUpperCase()+'_ENTRY_SQL_BULK_DYNAMIC_TRIGGER_A06=PASS');
console.log('RES15_A06_BOUNDED_CALLGRAPH_EVIDENCE='+JSON.stringify(summary));
console.log('REMOTE_D1=0 SCHEMA_APPLY=0 PRODUCTION_WRITE=0');
