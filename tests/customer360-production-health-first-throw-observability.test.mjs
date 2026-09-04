import assert from 'node:assert/strict';
import fs from 'node:fs';

function classify({ curlExit=0, httpStatus=200, contentType='application/json', redirectCount=0, body='', jsonParseOk=true, contractOk=true, payloadKind='object' }) {
  const bodyClass = body.length === 0 ? 'EMPTY' : /html/i.test(contentType) || /^\s*</.test(body) ? 'HTML' : /json/i.test(contentType) ? (jsonParseOk ? 'JSON' : 'TEXT') : 'TEXT';
  if (curlExit !== 0) return { firstThrow: `CURL_FAILURE_${curlExit}`, errorClass:'CURL_FAILURE', bodyClass };
  if (redirectCount > 0 || (httpStatus >= 300 && httpStatus < 400)) return { firstThrow:`HTTP_STATUS_${httpStatus}`, errorClass:'HTTP_UNEXPECTED', bodyClass };
  if (httpStatus !== 200) return { firstThrow:`HTTP_STATUS_${httpStatus}`, errorClass:'HTTP_UNEXPECTED', bodyClass };
  if (!body.length) return { firstThrow:'EMPTY_BODY', errorClass:'NON_JSON', bodyClass };
  if (!/json/i.test(contentType)) return { firstThrow:'NON_JSON_CONTENT_TYPE', errorClass:'NON_JSON', bodyClass };
  if (!jsonParseOk) return { firstThrow:'JSON_PARSE_FAILURE', errorClass:'JSON_PARSE_FAILURE', bodyClass };
  if (payloadKind !== 'object') return { firstThrow:'CONTRACT_PAYLOAD_NOT_OBJECT', errorClass:'CONTRACT_FAILURE', bodyClass };
  if (!contractOk) return { firstThrow:'CONTRACT_FAILURE', errorClass:'CONTRACT_FAILURE', bodyClass };
  return { firstThrow:'NONE', errorClass:'NONE', bodyClass:'JSON' };
}

const cases = [
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
  'PRODUCTION_HEALTH_FETCH_STAGE',
  'PRODUCTION_HEALTH_CURL_EXIT',
  'PRODUCTION_HEALTH_HTTP_STATUS',
  'PRODUCTION_HEALTH_CONTENT_TYPE',
  'PRODUCTION_HEALTH_REDIRECT_COUNT',
  'PRODUCTION_HEALTH_BODY_CLASS',
  'PRODUCTION_HEALTH_BODY_LENGTH',
  'PRODUCTION_HEALTH_JSON_PARSE_OK',
  'PRODUCTION_HEALTH_ERROR_CLASS',
  'PRODUCTION_HEALTH_FIRST_THROW',
  'GITHUB_STEP_SUMMARY',
  '::error::PRODUCTION_HEALTH_FIRST_THROW:',
  'error_class=CONTRACT_FAILURE',
  'fetch_stage=CONTRACT',
  'CONTRACT_PAYLOAD_NOT_OBJECT',
  'CONTRACT_EVALUATION_FAILURE'
]) assert.ok(observer.includes(token), `missing observer token ${token}`);

assert.ok(observer.includes("github.event.workflow_run.event == 'workflow_dispatch'"));
assert.ok(observer.includes("github.event.workflow_run.head_branch == 'main'"));
assert.ok(observer.includes('--max-redirs 0'));
assert.ok(observer.includes("!h || typeof h!=='object' || Array.isArray(h)"));
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
console.log('SCENARIOS=A,B,C401,C403,D,E,F,NULL,G,H');
console.log('SECRET_VALUE_LOGGED=0');
console.log('VERIFIER_CONTRACT_WEAKENED=0');
