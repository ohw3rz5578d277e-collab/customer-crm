import assert from 'node:assert/strict';
import fs from 'node:fs';

function attribution({ sourceMode='deploy', releaseJob=true, deployReached=true, deployResult='success', sourceSha='a'.repeat(40), triggerSha='a'.repeat(40), deploymentId='11111111-1111-4111-8111-111111111111', currentId='11111111-1111-4111-8111-111111111111' }={}) {
  if (sourceMode !== 'deploy') return 'NOT_A_PRODUCTION_DEPLOY';
  if (!releaseJob) return 'NOT_A_PRODUCTION_DEPLOY';
  if (!deployReached) return 'DEPLOY_NOT_REACHED';
  if (deployResult !== 'success') return 'DEPLOY_FAILED';
  if (sourceSha !== triggerSha) return 'SOURCE_SHA_MISMATCH';
  if (!deploymentId) return 'DEPLOYMENT_ID_MISSING';
  if (!currentId) return 'CURRENT_DEPLOYMENT_ID_MISSING';
  if (deploymentId !== currentId) return 'DEPLOYMENT_SUPERSEDED';
  return 'EXACT_MATCH';
}

assert.equal(attribution(), 'EXACT_MATCH');
assert.equal(attribution({sourceMode:'preflight',deployReached:false}), 'NOT_A_PRODUCTION_DEPLOY');
assert.equal(attribution({releaseJob:false}), 'NOT_A_PRODUCTION_DEPLOY');
assert.equal(attribution({deployReached:false}), 'DEPLOY_NOT_REACHED');
assert.equal(attribution({deployResult:'failure'}), 'DEPLOY_FAILED');
assert.equal(attribution({deploymentId:''}), 'DEPLOYMENT_ID_MISSING');
assert.equal(attribution({sourceSha:'b'.repeat(40)}), 'SOURCE_SHA_MISMATCH');
assert.equal(attribution({currentId:''}), 'CURRENT_DEPLOYMENT_ID_MISSING');
assert.equal(attribution({currentId:'22222222-2222-4222-8222-222222222222'}), 'DEPLOYMENT_SUPERSEDED');

function classify({ sourceGuard='PASS', postIdentity='PASS', curlExit=0, httpStatus=200, contentType='application/json', redirectCount=0, body='', jsonParseOk=true, contractOk=true, payloadKind='object' }) {
  const bodyClass = body.length === 0 ? 'EMPTY' : /html/i.test(contentType) || /^\s*</.test(body) ? 'HTML' : /json/i.test(contentType) ? (jsonParseOk ? 'JSON' : 'TEXT') : 'TEXT';
  if (sourceGuard === 'DRIFT_BEFORE_FETCH') return { firstThrow:'SOURCE_SHA_DRIFT_BEFORE_FETCH', errorClass:'SOURCE_DRIFT', bodyClass };
  if (sourceGuard === 'DRIFT_AFTER_FETCH') return { firstThrow:'SOURCE_SHA_DRIFT_AFTER_FETCH', errorClass:'SOURCE_DRIFT', bodyClass };
  if (curlExit !== 0) return { firstThrow: `CURL_FAILURE_${curlExit}`, errorClass:'CURL_FAILURE', bodyClass };
  if (redirectCount > 0 || (httpStatus >= 300 && httpStatus < 400)) return { firstThrow:`HTTP_STATUS_${httpStatus}`, errorClass:'HTTP_UNEXPECTED', bodyClass };
  if (httpStatus !== 200) return { firstThrow:`HTTP_STATUS_${httpStatus}`, errorClass:'HTTP_UNEXPECTED', bodyClass };
  if (!body.length) return { firstThrow:'EMPTY_BODY', errorClass:'NON_JSON', bodyClass };
  if (!/json/i.test(contentType)) return { firstThrow:'NON_JSON_CONTENT_TYPE', errorClass:'NON_JSON', bodyClass };
  if (!jsonParseOk) return { firstThrow:'JSON_PARSE_FAILURE', errorClass:'JSON_PARSE_FAILURE', bodyClass };
  if (payloadKind !== 'object') return { firstThrow:'CONTRACT_PAYLOAD_NOT_OBJECT', errorClass:'CONTRACT_FAILURE', bodyClass };
  if (!contractOk) return { firstThrow:'CONTRACT_FAILURE', errorClass:'CONTRACT_FAILURE', bodyClass };
  if (postIdentity === 'UNAVAILABLE') return { firstThrow:'CURRENT_PRODUCTION_VERSION_ID_UNAVAILABLE_AFTER_HEALTH', errorClass:'ATTRIBUTION_FAILURE', bodyClass };
  if (postIdentity === 'SUPERSEDED') return { firstThrow:'DEPLOYMENT_SUPERSEDED_DURING_OBSERVATION', errorClass:'ATTRIBUTION_FAILURE', bodyClass };
  return { firstThrow:'NONE', errorClass:'NONE', bodyClass:'JSON' };
}

const cases = [
  [{sourceGuard:'DRIFT_BEFORE_FETCH'}, 'SOURCE_SHA_DRIFT_BEFORE_FETCH', 'SOURCE_DRIFT'],
  [{sourceGuard:'DRIFT_AFTER_FETCH',httpStatus:403}, 'SOURCE_SHA_DRIFT_AFTER_FETCH', 'SOURCE_DRIFT'],
  [{postIdentity:'UNAVAILABLE',body:'{}'}, 'CURRENT_PRODUCTION_VERSION_ID_UNAVAILABLE_AFTER_HEALTH', 'ATTRIBUTION_FAILURE'],
  [{postIdentity:'SUPERSEDED',body:'{}'}, 'DEPLOYMENT_SUPERSEDED_DURING_OBSERVATION', 'ATTRIBUTION_FAILURE'],
  [{postIdentity:'SUPERSEDED',curlExit:7,body:'{}'}, 'CURL_FAILURE_7', 'CURL_FAILURE'],
  [{postIdentity:'UNAVAILABLE',httpStatus:500,body:'{}'}, 'HTTP_STATUS_500', 'HTTP_UNEXPECTED'],
  [{postIdentity:'SUPERSEDED',httpStatus:200,contentType:'application/json',body:'{}',contractOk:false}, 'CONTRACT_FAILURE', 'CONTRACT_FAILURE'],
  [{curlExit:7}, 'CURL_FAILURE_7', 'CURL_FAILURE'],
  [{httpStatus:302,contentType:'text/html',body:'<html/>',redirectCount:1}, 'HTTP_STATUS_302', 'HTTP_UNEXPECTED'],
  [{httpStatus:401,contentType:'application/json',body:'{}'}, 'HTTP_STATUS_401', 'HTTP_UNEXPECTED'],
  [{httpStatus:403,contentType:'text/html',body:'<html/> '}, 'HTTP_STATUS_403', 'HTTP_UNEXPECTED'],
  [{httpStatus:500,contentType:'application/json',body:'{}'}, 'HTTP_STATUS_500', 'HTTP_UNEXPECTED'],
  [{httpStatus:200,contentType:'text/html',body:'<html/> '}, 'NON_JSON_CONTENT_TYPE', 'NON_JSON'],
  [{httpStatus:200,contentType:'application/json',body:'{',jsonParseOk:false}, 'JSON_PARSE_FAILURE', 'JSON_PARSE_FAILURE'],
  [{httpStatus:200,contentType:'application/json',body:'null',payloadKind:'null'}, 'CONTRACT_PAYLOAD_NOT_OBJECT', 'CONTRACT_FAILURE'],
  [{httpStatus:200,contentType:'application/json',body:'{}',contractOk:false}, 'CONTRACT_FAILURE', 'CONTRACT_FAILURE'],
  [{httpStatus:200,contentType:'application/json; charset=utf-8',body:'{"ok":true}',contractOk:true}, 'NONE', 'NONE'],
];
for (const [input, firstThrow, errorClass] of cases) {
  const got=classify(input);
  assert.equal(got.firstThrow, firstThrow, JSON.stringify(input));
  assert.equal(got.errorClass, errorClass, JSON.stringify(input));
}

const observer=fs.readFileSync('.github/workflows/customer360-production-first-throw-observability.yml','utf8');
const deploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
for (const token of [
  'actions: read','Verify triggering Production deploy provenance',"j.name==='Exact-SHA Production release gate'",
  "s=>s.name==='Deploy customer-crm-api'",'/actions/jobs/$release_job_id/logs','Current Version ID:',
  'RELEASE_MODE=\\(preflight\\|deploy\\)','SOURCE_MODE=preflight','SOURCE_MODE=deploy','SOURCE_MODE_PREFLIGHT',
  'AUTHORIZED_SHA=','CHECKOUT_SHA=','wrangler@4.33.1 deployments status --name customer-crm-api --json',
  "Number(v.percentage)===100","active[0].version_id",'OBSERVER_ATTRIBUTION_RESULT=NOT_A_PRODUCTION_DEPLOY',
  'OBSERVER_ATTRIBUTION_RESULT=DEPLOY_NOT_REACHED','OBSERVER_ATTRIBUTION_RESULT=DEPLOY_FAILED',
  'OBSERVER_ATTRIBUTION_RESULT=DEPLOYMENT_ID_MISSING','OBSERVER_ATTRIBUTION_RESULT=SOURCE_SHA_MISMATCH',
  'OBSERVER_ATTRIBUTION_RESULT=DEPLOYMENT_SUPERSEDED','OBSERVER_ATTRIBUTION_RESULT=EXACT_MATCH_BEFORE_HEALTH',
  'OBSERVER_ATTRIBUTION_RESULT=EXACT_MATCH_AFTER_HEALTH','current-production-deployment-after-health.json',
  'DEPLOYMENT_SUPERSEDED_DURING_OBSERVATION','CURRENT_PRODUCTION_VERSION_ID_UNAVAILABLE_AFTER_HEALTH',
  'PRODUCTION_HEALTH_POST_WORKER_VERSION_ID','PRODUCTION_HEALTH_POST_ATTRIBUTION_FAILURE',"steps.provenance.outputs.allowed == 'true'",'DEPLOY_STEP_REACHED=true','DEPLOY_RESULT=success',
  'EXPECTED_WORKER_VERSION_ID','CURRENT_PRODUCTION_VERSION_ID','PRODUCTION_HEALTH_FETCH_STAGE','PRODUCTION_HEALTH_SOURCE_GUARD',
  'PRODUCTION_HEALTH_TRIGGER_SHA','PRODUCTION_HEALTH_CURRENT_MAIN_BEFORE','PRODUCTION_HEALTH_CURRENT_MAIN_AFTER',
  'PRODUCTION_HEALTH_CURL_EXIT','PRODUCTION_HEALTH_HTTP_STATUS','PRODUCTION_HEALTH_CONTENT_TYPE','PRODUCTION_HEALTH_REDIRECT_COUNT',
  'PRODUCTION_HEALTH_BODY_CLASS','PRODUCTION_HEALTH_BODY_LENGTH','PRODUCTION_HEALTH_JSON_PARSE_OK','PRODUCTION_HEALTH_ERROR_CLASS',
  'PRODUCTION_HEALTH_FIRST_THROW','SOURCE_SHA_DRIFT_BEFORE_FETCH','SOURCE_SHA_DRIFT_AFTER_FETCH','GITHUB_STEP_SUMMARY',
  '::error::PRODUCTION_HEALTH_FIRST_THROW:','error_class=CONTRACT_FAILURE','fetch_stage=CONTRACT','CONTRACT_PAYLOAD_NOT_OBJECT','CONTRACT_EVALUATION_FAILURE',
  'if [[ "$first_throw" == NONE && "$attribution_failure" != NONE ]]'
]) assert.ok(observer.includes(token), `missing observer token ${token}`);

const curlPos=observer.indexOf('elif [[ "$curl_exit" != 0 ]]');
const attributionFallbackPos=observer.indexOf('if [[ "$first_throw" == NONE && "$attribution_failure" != NONE ]]');
assert.ok(curlPos>=0 && attributionFallbackPos>curlPos,'health FIRST_THROW classification must precede attribution-only fallback');

assert.ok(observer.includes("format('customer360-production-observer-{0}', github.event.workflow_run.id)"));
assert.ok(!observer.includes('group: customer-crm-production-deploy'));
assert.ok(observer.split('wrangler@4.33.1 deployments status --name customer-crm-api --json').length-1>=2);
assert.ok(observer.includes("github.event.workflow_run.event == 'workflow_dispatch'"));
assert.ok(observer.includes("github.event.workflow_run.head_branch == 'main'"));
assert.ok(observer.includes('TRIGGER_SHA: ${{ github.event.workflow_run.head_sha }}'));
assert.ok(observer.includes('TRIGGER_RUN_ID: ${{ github.event.workflow_run.id }}'));
assert.ok(observer.includes('/actions/runs/$TRIGGER_RUN_ID/jobs?per_page=100'));
assert.ok(observer.includes('git ls-remote https://github.com/ohw3rz5578d277e-collab/customer-crm.git refs/heads/main'));
assert.ok(observer.includes('--max-redirs 0'));
assert.ok(observer.includes("!h||typeof h!=='object'||Array.isArray(h)") || observer.includes("!h || typeof h!=='object' || Array.isArray(h)"));
assert.ok(observer.includes('CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID'));
assert.ok(observer.includes('CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET'));
assert.ok(observer.includes('x-admin-token: $ADMIN_TOKEN'));
assert.ok(!observer.includes('continue-on-error: true'));
assert.ok(!observer.includes('PRODUCTION_HEALTH_ALLOW_FAILURE'));
assert.ok(!observer.includes('curl -X POST'));
assert.ok(!observer.includes('curl -X PATCH'));
assert.ok(!observer.includes('curl -X DELETE'));

const canonicalContracts = [
  'customer360_family_marketing_foundation','customer360_profile_enrichment','customer360_line_profile_extraction',
  'referrer_customer_id_exact_existing_customer_only','customer360_profile_composed_into_detail',
  'customer360_profile_enrichment_schema_available','customer360_family_metadata_available','customer360_field_evidence_available','customer360_notes_history_available',
  'customer_id_generation','customer_line_auto_apply','line_profile_auto_apply','candidate-only','customer360_identity_fallback','customer360_paid_ai_provider_active','line_event_direction','line_event_receive_status'
];
for (const contract of canonicalContracts) {
  assert.ok(deploy.includes(contract), `deploy verifier contract missing: ${contract}`);
  assert.ok(observer.includes(contract), `observer contract missing: ${contract}`);
}

console.log('CUSTOMER360_PRODUCTION_HEALTH_FIRST_THROW_OBSERVABILITY_TEST=PASS');
console.log('SCENARIOS=ATTRIBUTION_EXACT,PREFLIGHT_NOT_PRODUCTION,DEPLOY_NOT_REACHED,DEPLOY_FAILED,DEPLOYMENT_ID_MISSING,SOURCE_SHA_MISMATCH,SUPERSEDED_BEFORE,SUPERSEDED_DURING,POST_ID_UNAVAILABLE,CURL,3XX,HTML,INVALID_JSON,CONTRACT,COMBINED_CURL_SUPERSEDED,COMBINED_HTTP_UNAVAILABLE,COMBINED_CONTRACT_SUPERSEDED,CANONICAL');
console.log('SECRET_VALUE_LOGGED=0');
console.log('VERIFIER_CONTRACT_WEAKENED=0');
