import fs from 'node:fs';
import { discoverLineHistoryRecoveryArtifacts } from '../src/crm-line-history-recovery-discovery.mjs';

const roots=[];
let out='';
for(let i=2;i<process.argv.length;i++){
  const a=String(process.argv[i]||'');
  if(a==='--root'){
    roots.push(String(process.argv[++i]||''));
  }else if(a==='--out'){
    out=String(process.argv[++i]||'');
  }
}

const cleanRoots=[...new Set(roots.filter(Boolean))];
if(!cleanRoots.length){
  console.error('Usage: node scripts/discover-line-history-recovery-artifacts.mjs --root <dir> [--root <dir> ...] --out <json>');
  process.exit(2);
}
if(!out){
  console.error('RESULT=STOP_DISCOVERY_OUTPUT_REQUIRED');
  process.exit(3);
}

const result=discoverLineHistoryRecoveryArtifacts({roots:cleanRoots});
fs.writeFileSync(out,JSON.stringify(result,null,2)+'\n');

function emit(label,key){
  const x=result[key];
  console.log(label+'_FOUND='+(x?.path?'YES':'NO'));
  console.log(label+'_CANDIDATE_COUNT='+Number(x?.count||0));
  console.log(label+'_AMBIGUOUS='+(x?.ambiguous?'YES':'NO'));
}

console.log('RESULT=LINE_HISTORY_RECOVERY_ARTIFACT_DISCOVERY_READY');
emit('CANDIDATES','candidates');
emit('CUSTOMER_MASTER','customer_master');
emit('RESUME_DIR','resume_dir');
emit('DECISIONS','decisions');
emit('PREAUTH_DIR','preauth_dir');
emit('D1_PREVIEW_DIR','d1_preview_dir');
console.log('APPROVAL_FILE_AUTO_DISCOVERY=NO');
console.log('PRIVATE_FILE_CONTENT_PRINTED=0');
console.log('COMMAND_EXECUTION=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('OUTPUT='+out);
