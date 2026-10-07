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
pass('token whitespace guard',workflow.includes('R2_READ_REST_TOKEN_FORMAT_INVALID_WHITESPACE'));
pass('token control-character guard',workflow.includes('R2_READ_REST_TOKEN_FORMAT_INVALID_CONTROL_CHARACTER'));
pass('token value remains hidden',workflow.includes('R2_READ_REST_TOKEN_VALUE_PRINTED=NO'));
pass('legacy token prefix gate removed',!workflow.includes('R2_READ_REST_TOKEN_TYPE_NOT_USER_API_TOKEN')&&!workflow.includes('cfut_*')&&!workflow.includes('cfut_*)'));
pass('bucket metadata list only',workflow.includes('/r2/buckets?per_page=1000&order=name&direction=asc'));
pass('digest index emits sha only',workflow.includes('R2_BUCKET_DIGEST_INDEX_${n}_SHA256='));
pass('digest index emits observed jurisdiction',workflow.includes('R2_BUCKET_DIGEST_INDEX_${n}_OBSERVED_JURISDICTION='));
pass('combined inventory digest retained',workflow.includes('R2_BUCKET_INVENTORY_SHA256='));
pass('raw bucket marker no',workflow.includes('RAW_BUCKET_NAME_PRINTED=NO'));
pass('no raw bucket console logging',!workflow.match(/console\.log\([^\n]*\bname\b(?!_sha256)/));
pass('missing buckets array fails closed',workflow.includes('R2_BUCKET_DIGEST_INDEX_BUCKETS_ARRAY_REQUIRED'));
pass('nameless bucket fails closed',workflow.includes('R2_BUCKET_DIGEST_INDEX_BUCKET_NAME_REQUIRED'));
pass('object read remains zero',workflow.includes('R2_OBJECT_READ=0'));
pass('write remains zero',workflow.includes('R2_WRITE=0'));
pass('bucket mutation remains zero',workflow.includes('BUCKET_MUTATION=0'));
pass('binding change remains zero',workflow.includes('PRODUCTION_STORAGE_BINDING_CHANGE=0'));
pass('production fetch remains zero',workflow.includes('PRODUCTION_STORAGE_FETCH=0'));
pass('deploy remains zero',workflow.includes('PRODUCTION_DEPLOY=0'));
pass('traffic remains zero',workflow.includes('PRODUCTION_TRAFFIC_CHANGE=0'));
pass('D1 write remains zero',workflow.includes('PRODUCTION_D1_WRITE=0'));
pass('default-off runtime guard preserved',workflow.includes('MEMBER_CANONICAL_STORAGE_DEFAULT_OFF=PASS'));
pass('canonical binding declared marker',workflow.includes('CANONICAL_R2_BINDING_DECLARED=PASS'));
pass('canonical binding exact marker',workflow.includes('CANONICAL_R2_BINDING_EXACT=PASS'));
pass('canonical binding count fails closed',workflow.includes('CANONICAL_R2_BINDING_COUNT_INVALID'));
pass('canonical binding name fails closed',workflow.includes('CANONICAL_R2_BINDING_NAME_MISMATCH'));
pass('canonical bucket name fails closed',workflow.includes('CANONICAL_R2_BUCKET_NAME_MISMATCH'));
pass('unexpected binding config fails closed',workflow.includes('CANONICAL_R2_BINDING_UNEXPECTED_CONFIGURATION'));
pass('env-scoped binding fails closed',workflow.includes('NONCANONICAL_ENV_R2_BINDING_DECLARED'));
pass('canonical binding name locked',workflow.includes("binding.binding!=='MEMBER_PRIVATE_MEDIA_BUCKET'"));
pass('canonical bucket name locked',workflow.includes("binding.bucket_name!=='customer-crm-member-private-media'"));
pass('legacy binding absence guard removed',!workflow.includes('CANONICAL_R2_BINDING_ALREADY_DECLARED')&&!workflow.includes('CANONICAL_R2_BINDING_DECLARED=NO'));
pass('null public adapter preserved',workflow.includes('public_asset_adapter:null'));
pass('null private adapter preserved',workflow.includes('private_media_storage_adapter:null'));
pass('member route remains fail-closed',workflow.includes('MEMBER_PRODUCTION_ROUTE_MODE_ENABLED'));
pass('private media route remains fail-closed',workflow.includes('PRIVATE_MEDIA_ROUTE_MODE_ENABLED'));
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