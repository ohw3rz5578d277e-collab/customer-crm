// RES-15 source-only discovery over the actual checked-out service repository.
// Heuristic candidate discovery, not a complete AST/runtime call graph or
// authority to write. No network, DB, environment secrets or schema execution.
import assert from 'node:assert/strict';
import {readdirSync,readFileSync,existsSync} from 'node:fs';
import {join,relative,resolve,dirname} from 'node:path';
const ROOT=process.cwd();
const srcRoot=join(ROOT,'src');
const walk=(dir,exts)=>{
 const output=[];
 if(!existsSync(dir))return output;
 for(const item of readdirSync(dir,{withFileTypes:true})){
  if(item.name==='node_modules'||item.name==='.git')continue;
  const path=join(dir,item.name);
  if(item.isDirectory())output.push(...walk(path,exts));
  else if(item.isFile()&&exts.some(e=>item.name.endsWith(e)))output.push(path);
 }
 return output;
};
const paths=walk(srcRoot,['.js','.mjs']).sort();
const byPath=new Map(paths.map(path=>[relative(ROOT,path).replaceAll('\\','/'),readFileSync(path,'utf8')]));
const mutate=/\b(?:INSERT\s+(?:OR\s+\w+\s+)?INTO|UPDATE\s+|DELETE\s+FROM|REPLACE\s+INTO)\b/i;
const dynamic=/(?:UPDATE|INSERT|DELETE|ON CONFLICT)[\s\S]{0,180}\$\{/i;
const delegated=/\b(?:prepareUpsert|upsert|safeRun|run|db\.batch)\s*\(/i;
const ddl=/\b(?:CREATE\s+(?:TRIGGER|TABLE|INDEX)|ALTER\s+TABLE)\b/i;
const edges=[];
const discovered=[];
for(const [path,source] of byPath){
 const signals={mutation:mutate.test(source),dynamic:dynamic.test(source),
  delegated:delegated.test(source),ddl:ddl.test(source),
  batch:/\.batch\s*\(/.test(source)};
 if(Object.values(signals).some(Boolean))discovered.push({path,signals});
 for(const m of source.matchAll(/\bimport\s+[^'"]{0,240}?\bfrom\s+['"](\.\/[^'"]+)['"]/g)){
  const target=relative(ROOT,resolve(dirname(join(ROOT,path)),m[1])).replaceAll('\\','/');
  edges.push({from:path,to:target});
 }
}
const required=["src/index.js","src/customer-identity-resolver.mjs","src/crm-customer360-profile-enrichment.mjs","src/crm-customer-csv-import.mjs",'src/crm-customer360-line-profile-extraction.mjs'];
for(const path of required){
 assert.ok(byPath.has(path),'required real source is unavailable: '+path);
 assert.ok(discovered.some(x=>x.path===path),'required source must remain a candidate: '+path);
}
assert.ok(discovered.length>=required.length);
 // The synthetic revision planner must remain outside every runtime source
 // import graph. Inspect all app sources, not just the immediate entrypoint.
 const adapterName='crm-res15-synthetic-revision-adapter.mjs';
 // Conservatively reject any literal mention in real app source, including
 // long/multiline static imports, comments, side-effect statements, optional
 // trailing commas and template-literal dynamic imports. False positives
 // require human review, never automatic permission. Constructed module
 // identifiers without a literal basename still need manual call-graph audit.
 const syntheticWiringDetected=source=>typeof source==='string'&&
  (source.includes(adapterName)||source.includes('res15_synthetic_mutation_ids')||
   source.includes('res15-synthetic-revision-adapter'));
 const unexpectedImports=[...byPath.entries()].filter(([path,source])=>
  !path.endsWith('/'+adapterName)&&syntheticWiringDetected(source));
 const tick=String.fromCharCode(96);
 const adversarial=[
  'import "./'+adapterName+'";',
  "import './"+adapterName+"';",
  "import { x }\nfrom\n'./"+adapterName+"';",
  'const x = import("./'+adapterName+'");',
  'export { x } from "../src/'+adapterName+'";',
  "const x=require('./"+adapterName+"');",
  'import /* runtime dependency */ "./'+adapterName+'";',
  'const x=import('+tick+'./'+adapterName+tick+');',
  'import("./'+adapterName+'",);',
  'import { '+Array.from({length:60},(_,i)=>'candidate'+i+' as alias'+i).join(', ')+
    ' } from "./'+adapterName+'";'
 ];
 for(const sample of adversarial){
  assert.equal(syntheticWiringDetected(sample),true,
   'Synthetic planner import spelling must fail closed: '+sample.slice(0,80));
 }
 assert.deepEqual(unexpectedImports.map(([path])=>path),[],
  'Synthetic CAS implementation/DDL must not be wired into live app source');
 assert.ok(byPath.has('src/crm-customer360-line-profile-extraction.mjs'),'the newly traced identity/evidence writer must remain in repo');
 assert.ok(discovered.some(item=>item.path==='src/crm-customer360-line-profile-extraction.mjs'),
  'the new writer must remain discoverable by source scanner');
 console.log('RES15_CRM_SYNTHETIC_ADAPTER_TRANSITIVE_SOURCE_ISOLATION=PASS');
const openEdges=edges.filter(e=>!byPath.has(e.to));
const migrations=[...walk(join(ROOT,'migrations'),['.sql']),...walk(join(ROOT,'migrations_managed'),['.sql'])];
const staticTriggerFiles=migrations.filter(p=>/\bCREATE\s+TRIGGER\b/i.test(readFileSync(p,'utf8')));
const sourceDDL=discovered.filter(x=>x.signals.ddl).length;
const summary={
 mode:'repository_source_only',owner:'crm',complete_writer_callgraph:false,
 production_schema_attested:false,real_data_modified:false,
 observed_source_files:paths.length,heuristic_writer_or_delegate_candidates:discovered.length,
 dynamic_sql_candidate_files:discovered.filter(x=>x.signals.dynamic).length,
 bulk_or_batch_candidate_files:discovered.filter(x=>x.signals.batch).length,
 static_import_edges:edges.length,unresolved_static_import_edges:openEdges.length,
 source_ddl_candidate_files:sourceDDL,static_migration_trigger_files:staticTriggerFiles.length,
 static_migration_trigger_paths:staticTriggerFiles.map(p=>relative(ROOT,p).replaceAll('\\','/')).slice(0,12),
 dynamic_sql_candidate_examples:discovered.filter(x=>x.signals.dynamic).slice(0,12).map(x=>x.path),
 mandatory_candidate_paths:required,
 example_candidates:discovered.slice(0,10).map(x=>x.path),
 unmatched_import_examples:openEdges.slice(0,8)
};
assert.equal(summary.complete_writer_callgraph,false);
assert.equal(summary.production_schema_attested,false);
assert.equal(summary.real_data_modified,false);
console.log('RES15_CRM_REAL_SOURCE_WRITER_CANDIDATE_SCAN=PASS');
console.log('RES15_CRM_SOURCE_WRITER_SCAN_COMPLETENESS_NOT_ATTESTED=PASS');
console.log('RES15_SOURCE_SCAN_SUMMARY='+JSON.stringify(summary));
console.log('REMOTE_D1=0 SCHEMA_APPLY=0 PRODUCTION_WRITE=0 LINE_SEND=0');
