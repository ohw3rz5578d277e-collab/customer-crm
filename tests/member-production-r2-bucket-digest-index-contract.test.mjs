import fs from 'node:fs';

const workflowPath='.github/workflows/member-production-r2-bucket-digest-index.yml';
const bridgePath='.github/workflows/dispatch-member-production-r2-bucket-digest-index-from-issue.yml';
const docsPath='docs/member-app/member-production-r2-bucket-digest-index.md';

const workflow=fs.readFileSync(workflowPath,'utf8');
const bridge=fs.readFileSync(bridgePath,'utf8');
const docs=fs.readFileSync(docsPath,'utf8');

function pass(name,condition){
  if(!condition)throw new Error(`FAIL:${name}`);
  console.log(`PASS:${name}`);
}

pass('workflow dispatch is explicit',workflow.includes('workflow_dispatch:'));
pass('jurisdiction input required',workflow.includes('jurisdictions:')&&workflow.includes('R2_JURISDICTIONS_REQUIRED'));
pass('canonical lowercase guard',workflow.includes('R2_JURISDICTIONS_CASE_NOT_CANONICAL'));
pass('canonical order guard',workflow.includes('R2_JURISDICTIONS_NOT_CANONICAL_ORDER'));
pass('dedicated token verify',workflow.includes("https://api.cloudflare.com/client/v4/user/tokens/verify"));
pass('dedicated R2 read secret only',workflow.includes('CLOUDFLARE_R2_READ_API_TOKEN'));
pass('bucket metadata list only',workflow.includes('/r2/buckets?per_page=1000&order=name&direction=asc'));
pass('digest index emits sha only',workflow.includes('R2_BUCKET_DIGEST_INDEX_${n}_SHA256='));
pass('digest index emits observed jurisdiction',workflow.includes('R2_BUCKET_DIGEST_INDEX_${n}_OBSERVED_JURISDICTION='));
pass('combined inventory digest retained',workflow.includes('R2_BUCKET_INVENTORY_SHA256='));
pass('raw bucket marker no',workflow.includes('RAW_BUCKET_NAME_PRINTED=NO'));
pass('no raw bucket console logging',!workflow.match(/console\.log\([^\n]*\bname\b(?!_sha256)/));
pass('object read remains zero',workflow.includes('R2_OBJECT_READ=0'));
pass('write remains zero',workflow.includes('R2_WRITE=0'));
pass('bucket mutation remains zero',workflow.includes('BUCKET_MUTATION=0'));
pass('binding change remains zero',workflow.includes('PRODUCTION_STORAGE_BINDING_CHANGE=0'));
pass('production fetch remains zero',workflow.includes('PRODUCTION_STORAGE_FETCH=0'));
pass('deploy remains zero',workflow.includes('PRODUCTION_DEPLOY=0'));
pass('traffic remains zero',workflow.includes('PRODUCTION_TRAFFIC_CHANGE=0'));
pass('D1 write remains zero',workflow.includes('PRODUCTION_D1_WRITE=0'));
pass('default-off guard preserved',workflow.includes('MEMBER_CANONICAL_STORAGE_DEFAULT_OFF=PASS'));
pass('binding absence guard preserved',workflow.includes('CANONICAL_R2_BINDING_DECLARED=NO'));
pass('pagination fails closed',workflow.includes('R2_BUCKET_DIGEST_INDEX_PAGINATION_UNSUPPORTED'));
pass('active version drift guard',workflow.includes('ACTIVE_PRODUCTION_VERSION_DRIFT'));
pass('main postflight drift guard',workflow.includes('MAIN_DRIFT_AFTER_DIGEST_INDEX'));
pass('bridge issue 26 only',bridge.includes('github.event.issue.number == 26'));
pass('bridge owner only',bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"));
pass('bridge exact command',bridge.includes('/member-production-r2-bucket-digest-index '));
pass('bridge validates current main',bridge.includes('MAIN_DRIFT expected='));
pass('bridge dispatches dedicated workflow',bridge.includes('member-production-r2-bucket-digest-index.yml/dispatches'));
pass('docs prohibit automatic selection',docs.includes('does not select a bucket'));
pass('docs explain local digest reconciliation',docs.includes('local digest reconciliation'));
pass('docs require separate binding authorization',docs.includes('separate fresh Owner authorization'));

console.log('MEMBER_PRODUCTION_R2_BUCKET_DIGEST_INDEX_CONTRACT=PASS');
