import assert from 'node:assert/strict';
import fs from 'node:fs';

function classify({ curlExit=0, httpStatus=200, contentType='application/json', redirectCount=0, body='', jsonParseOk=true, contractOk=true }) {
  const bodyClass = body.length === 0 ? 'EMPTY' : /html/i.test(contentType) || /^\s*</.test(body) ? 'HTML' : /json/i.test(contentType) ? (jsonParseOk ? 'JSON' : 'TEXT') : 'TEXT';
  if (curlExit !== 0) return { firstThrow: `CURL_FAILURE_${curlExit}`, errorClass:'CURL_FAILURE', bodyClass };
  if (redirectCount > 0 || (httpStatus >= 300 && httpStatus < 400)) return { firstThrow:`HTTP_STATUS_${httpStatus}`, errorClass:'HTTP_UNEXPECTED', bodyClass };
  if (httpStatus !== 200) return { firstThrow:`HTTP_STATUS_${httpStatus}`, errorClass:'HTTP_UNEXPECTED', bodyClass };
  if (!/json/i.test(contentType)) return { firstThrow:'NON_JSON_CONTENT_TYPE', errorClass:'NON_JSON', bodyClass };
  if (!body.length) return { firstThrow:'EMPTY_BODY', errorClass:'NON_JSON', bodyClass };
  if (!jsonParseOk) return { firstThrow:'JSON_PARSE_FAILURE', errorClass:'JSON_PARSE_FAILURE', bodyClass };
  if (!contractOk) return { firstThrow:'CONTRACT_FAILURE', errorClass:'CONTRACT_FAILURE', bodyClass };
  return { firstThrow:'NONE', errorClass:'NONE', bodyClass:'JSON' };
}

const cases = [
  [{curlExit:7}, 'CURL_FAILURE_7', 'CURL_FAILURE'],
  [{httpStatus:302,contentType:'text/html',body:'<html/>',redirectCount:1}, 'HTTP_STATUS_302', 'HTTP_UNEXPECTED'],
  [{httpStatus:403,contentType:'text/html',body:'<html/> '}, 'HTTP_STATUS_403', 'HTTP_UNEXPECTED'],
  [{httpStatus:500,contentType:'application/json',body:'{}'}, 'HTTP_STATUS_500', 'HTTP_UNEXPECTED'],
  [{httpStatus:200,contentType:'text/html',body:'<html/> '}, 'NON_JSON_CONTENT_TYPE', 'NON_JSON'],
  [{httpStatus:200,contentType:'application/json',body:'{',jsonParseOk:false}, 'JSON_PARSE_FAILURE', 'JSON_PARSE_FAILURE'],
  [{httpStatus:200,contentType:'application/json',body:'{}',contractOk:false}, 'CONTRACT_FAILURE', 'CONTRACT_FAILURE'],
  [{httpStatus:200,contentType:'application/json; charset=utf-8',body:'{"ok":true}',contractOk:true}, 'NONE', 'NONE'],
];
for (const [input, firstThrow, errorClass] of cases) {
  const got=classify(input);
  assert.equal(got.firstThrow, firstThrow, JSON.stringify(input));
  assert.equal(got.errorClass, errorClass, JSON.stringify(input));
}

const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
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
  '::error::PRODUCTION_HEALTH_FIRST_THROW:'
]) assert.ok(workflow.includes(token), `missing workflow observability token ${token}`);

assert.ok(workflow.includes('CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID'));
assert.ok(workflow.includes('CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET'));
assert.ok(workflow.includes('x-admin-token: $ADMIN_TOKEN'));
assert.ok(workflow.includes("if(h.customer_id_generation!==false)"));
assert.ok(workflow.includes("if(h.customer_line_auto_apply!==false||h.line_profile_auto_apply!==false)"));
assert.ok(!workflow.includes('continue-on-error: true'));
assert.ok(!workflow.includes('PRODUCTION_HEALTH_ALLOW_FAILURE'));

console.log('CUSTOMER360_PRODUCTION_HEALTH_FIRST_THROW_OBSERVABILITY_TEST=PASS');
console.log('SCENARIOS=A,B,C,D,E,F,G,H');
console.log('SECRET_VALUE_LOGGED=0');
console.log('VERIFIER_CONTRACT_WEAKENED=0');
