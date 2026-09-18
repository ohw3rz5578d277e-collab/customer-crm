import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';

const SKIP_DIRS=new Set([
  '.git','node_modules','.cache','.npm','.Trash','Library','Pictures','Movies','Music'
]);

function text(v){return v==null?'':String(v).trim()}
function sha256File(p){
  return createHash('sha256').update(fs.readFileSync(p)).digest('hex');
}
function readJson(p){
  try{return JSON.parse(fs.readFileSync(p,'utf8'))}catch{return null}
}
function rows(raw){
  if(Array.isArray(raw)){
    if(raw.length===1&&raw[0]&&Array.isArray(raw[0].results))return raw[0].results;
    return raw;
  }
  if(raw&&Array.isArray(raw.rows))return raw.rows;
  if(raw&&Array.isArray(raw.results))return raw.results;
  if(raw&&raw.result&&Array.isArray(raw.result.results))return raw.result.results;
  return [];
}
function statSafe(p){
  try{return fs.statSync(p)}catch{return null}
}
function walk(root,maxDepth=6,maxEntries=20000){
  const out=[];
  const rootStat=statSafe(root);
  if(!rootStat||!rootStat.isDirectory())return out;
  const stack=[{dir:path.resolve(root),depth:0}];
  let seen=0;
  while(stack.length){
    const {dir,depth}=stack.pop();
    let entries=[];
    try{entries=fs.readdirSync(dir,{withFileTypes:true})}catch{continue}
    for(const entry of entries){
      if(++seen>maxEntries)return out;
      const p=path.join(dir,entry.name);
      if(entry.isDirectory()){
        if(depth<maxDepth&&!SKIP_DIRS.has(entry.name))stack.push({dir:p,depth:depth+1});
      }else if(entry.isFile()){
        out.push(p);
      }
    }
  }
  return out;
}
function candidateSnapshot(p){
  if(path.basename(p)!=='candidate-snapshot.json')return false;
  const r=rows(readJson(p));
  return r.length>0;
}
function customerMaster(p){
  if(!/^customer-master(?:-|\.|$)/i.test(path.basename(p)))return false;
  const r=rows(readJson(p));
  return r.some(x=>x&&typeof x==='object'&&text(x.customer_id));
}
function decisionsFile(p){
  const b=path.basename(p).toLowerCase();
  if(!b.endsWith('.json')||!b.includes('decision'))return false;
  const raw=readJson(p);
  const d=Array.isArray(raw)?raw:Array.isArray(raw?.decisions)?raw.decisions:[];
  return d.length>0&&d.every(x=>x&&typeof x==='object'&&text(x.queue_id)&&text(x.decision));
}
function resumeDirFromFile(p){
  if(path.basename(p)!=='final-triage.json')return '';
  const dir=path.dirname(p);
  if(fs.existsSync(path.join(dir,'baseline','customers.json'))&&fs.existsSync(path.join(dir,'owner-review-queue.json'))){
    return dir;
  }
  return '';
}
function preauthDirFromFile(p){
  if(path.basename(p)!=='decision-plan-private.json')return '';
  const dir=path.dirname(p);
  if(fs.existsSync(path.join(dir,'readonly-preview','owner-backfill-preview-summary.json'))){
    return dir;
  }
  return '';
}
function d1PreviewDirFromFile(p){
  if(path.basename(p)!=='write-authorization-packet.json')return '';
  const raw=readJson(p);
  if(raw?.planner==='line_history_owner_write_authorization_packet_v2')return path.dirname(p);
  return '';
}
function rank(paths){
  return [...new Set(paths)].map(p=>{
    const s=statSafe(p);
    return {path:p,mtime_ms:s?Number(s.mtimeMs||0):0};
  }).sort((a,b)=>b.mtime_ms-a.mtime_ms||a.path.localeCompare(b.path));
}
function fileSelection(paths){
  const ranked=rank(paths);
  if(!ranked.length)return {path:'',count:0,equivalent_count:0,ambiguous:false};
  const selected=ranked[0];
  let same=1;
  try{
    const h=sha256File(selected.path);
    same=ranked.filter(x=>{
      try{return sha256File(x.path)===h}catch{return false}
    }).length;
  }catch{}
  const ambiguous=ranked.length>1&&same!==ranked.length;
  return {
    path:ambiguous?'':selected.path,
    count:ranked.length,
    equivalent_count:same,
    ambiguous
  };
}
function dirSelection(paths){
  const ranked=rank(paths);
  const ambiguous=ranked.length>1;
  return {
    path:ambiguous?'':(ranked[0]?.path||''),
    count:ranked.length,
    equivalent_count:ranked.length?1:0,
    ambiguous
  };
}

export function discoverLineHistoryRecoveryArtifacts({roots=[]}={}){
  const files=[];
  for(const root of roots||[])files.push(...walk(root));

  const candidateFiles=[];
  const masterFiles=[];
  const decisionFiles=[];
  const resumeDirs=[];
  const preauthDirs=[];
  const d1Dirs=[];

  for(const p of [...new Set(files)]){
    if(candidateSnapshot(p))candidateFiles.push(p);
    if(customerMaster(p))masterFiles.push(p);
    if(decisionsFile(p))decisionFiles.push(p);

    const resume=resumeDirFromFile(p);
    if(resume)resumeDirs.push(resume);
    const preauth=preauthDirFromFile(p);
    if(preauth)preauthDirs.push(preauth);
    const d1=d1PreviewDirFromFile(p);
    if(d1)d1Dirs.push(d1);
  }

  return {
    planner:'line_history_recovery_artifact_discovery_v1',
    roots_scanned:(roots||[]).map(x=>path.resolve(x)),
    candidates:fileSelection(candidateFiles),
    customer_master:fileSelection(masterFiles),
    decisions:fileSelection(decisionFiles),
    resume_dir:dirSelection(resumeDirs),
    preauth_dir:dirSelection(preauthDirs),
    d1_preview_dir:dirSelection(d1Dirs),
    approval_file:{
      path:'',
      count:0,
      equivalent_count:0,
      ambiguous:false,
      intentionally_not_discovered:true
    },
    safety:{
      local_filesystem_read_only:true,
      approval_file_auto_discovery:false,
      command_execution:false,
      production_d1_read:0,
      production_d1_write:0,
      line_send:0,
      production_deploy:0
    }
  };
}
