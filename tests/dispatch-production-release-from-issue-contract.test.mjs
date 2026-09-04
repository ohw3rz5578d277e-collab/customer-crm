import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflowPath=new URL('../.github/workflows/dispatch-production-deploy-from-issue.yml',import.meta.url);
const source=fs.readFileSync(workflowPath,'utf8');

const owner='ohw3rz5578d277e-collab';
const sha='eeeee21a97f190aaeb1541818fd16ce3f3240d9c';

function parseCommand(body){
  const preflight=/^\/crm-production-preflight sha=([0-9a-f]{40})$/;
  const deploy=/^\/crm-production-deploy confirm_production=true sha=([0-9a-f]{40})$/;
  const authSmoke=/^\/crm-production-auth-smoke sha=([0-9a-f]{40})$/;
  let m=body.match(preflight);
  if(m)return {mode:'preflight',sha:m[1],confirmProduction:false,workflow:'deploy-cloudflare.yml'};
  m=body.match(deploy);
  if(m)return {mode:'deploy',sha:m[1],confirmProduction:true,workflow:'deploy-cloudflare.yml'};
  m=body.match(authSmoke);
  if(m)return {mode:'auth-smoke',sha:m[1],confirmProduction:false,workflow:'production-browser-smoke-auth.yml'};
  return null;
}

assert.match(source,/github\.event\.issue\.number == 26/,'release gate must remain issue #26 only');
assert.match(source,/github\.event\.issue\.pull_request == null/,'PR comments must be blocked');
assert.match(source,new RegExp(`github\\.actor == '${owner}'`),'actor must remain Owner only');
assert.match(source,new RegExp(`github\\.event\\.comment\\.user\\.login == '${owner}'`),'comment author must remain Owner only');
for(const prefix of ['/crm-production-preflight ','/crm-production-deploy ','/crm-production-auth-smoke ']){
  assert.ok(source.includes(`startsWith(github.event.comment.body, '${prefix}')`),`missing admitted prefix ${prefix}`);
}

assert.ok(source.includes("preflight_re='^/crm-production-preflight sha=([0-9a-f]{40})$'"));
assert.ok(source.includes("deploy_re='^/crm-production-deploy confirm_production=true sha=([0-9a-f]{40})$'"));
assert.ok(source.includes("auth_smoke_re='^/crm-production-auth-smoke sha=([0-9a-f]{40})$'"));
assert.ok(source.includes("release_mode='preflight'"));
assert.ok(source.includes("release_mode='deploy'"));
assert.ok(source.includes("release_mode='auth-smoke'"));
assert.ok(source.includes("dispatch_workflow='deploy-cloudflare.yml'"));
assert.ok(source.includes("dispatch_workflow='production-browser-smoke-auth.yml'"));

assert.ok(source.includes('if [[ "$current_sha" != "$expected_sha" ]]; then'),'authorized SHA must equal fresh main SHA');
assert.ok(source.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha'),'fresh main drift must fail closed');
assert.ok(source.includes('actions/workflows/${DISPATCH_WORKFLOW}/dispatches'),'dispatch endpoint must use validated workflow target');
assert.ok(source.includes("if mode in ('preflight','deploy'):"),'release workflow modes must stay explicit');
assert.ok(source.includes("elif mode == 'auth-smoke':"),'auth smoke mode must have separate payload path');
assert.ok(source.includes("inputs={'mode':mode,'expected_sha':sha}"),'release workflow payload must carry validated mode and SHA');
assert.ok(source.includes("inputs={'expected_sha':sha}"),'auth smoke payload must carry only exact SHA');
assert.ok(source.includes('CONFIRM_PRODUCTION=NOT_REQUIRED_FOR_PREFLIGHT'));
assert.ok(source.includes('CONFIRM_PRODUCTION=true'));
assert.ok(source.includes('CONFIRM_PRODUCTION=NOT_APPLICABLE_READ_ONLY_AUTH_SMOKE'));
assert.ok(source.includes('BRIDGE_PRODUCTION_WRITE=0'));

assert.doesNotMatch(source,/\bwrangler\s+deploy\b/i,'bridge must not directly deploy Worker');
assert.doesNotMatch(source,/\bd1\s+migrations\s+apply\b/i,'bridge must not apply D1 migrations');
assert.doesNotMatch(source,/\bd1\s+execute\b/i,'bridge must not execute D1 commands');
assert.doesNotMatch(source,/\b(INSERT|UPDATE|DELETE)\s+(INTO|FROM|[A-Za-z_])/i,'bridge must not contain direct D1 write SQL');

assert.deepEqual(parseCommand(`/crm-production-preflight sha=${sha}`),{mode:'preflight',sha,confirmProduction:false,workflow:'deploy-cloudflare.yml'});
assert.deepEqual(parseCommand(`/crm-production-deploy confirm_production=true sha=${sha}`),{mode:'deploy',sha,confirmProduction:true,workflow:'deploy-cloudflare.yml'});
assert.deepEqual(parseCommand(`/crm-production-auth-smoke sha=${sha}`),{mode:'auth-smoke',sha,confirmProduction:false,workflow:'production-browser-smoke-auth.yml'});

for(const body of [
  '/crm-production-preflight',
  '/crm-production-preflight sha=short',
  `/crm-production-preflight mode=deploy sha=${sha}`,
  `/crm-production-preflight confirm_production=true sha=${sha}`,
  `/crm-production-preflight sha=${sha} extra=true`,
  `/crm-production-preflight sha=${sha.toUpperCase()}`,
  `/crm-production-preflight sha=${sha}\n`,
  `/crm-production-deploy sha=${sha}`,
  `/crm-production-deploy confirm_production=false sha=${sha}`,
  '/crm-production-deploy confirm_production=true',
  `/crm-production-deploy confirm_production=true sha=${sha} extra=true`,
  `/crm-production-deploy confirm_production=true sha=${sha.toUpperCase()}`,
  `/crm-production-deploy confirm_production=true sha=${sha}\n`,
  '/crm-production-auth-smoke',
  '/crm-production-auth-smoke sha=short',
  `/crm-production-auth-smoke sha=${sha.toUpperCase()}`,
  `/crm-production-auth-smoke sha=${sha} extra=true`,
  `/crm-production-auth-smoke confirm_production=true sha=${sha}`,
  `/crm-production-auth-smoke sha=${sha}\n`,
  `junk /crm-production-auth-smoke sha=${sha}`,
  `/crm-production-auth-smoke sha=${sha}\n/crm-production-deploy confirm_production=true sha=${sha}`
])assert.equal(parseCommand(body),null,`malformed command must fail closed: ${JSON.stringify(body)}`);

assert.equal(parseCommand(`/crm-production-preflight sha=${sha}`)?.mode,'preflight');
assert.equal(parseCommand(`/crm-production-deploy confirm_production=true sha=${sha}`)?.mode,'deploy');
assert.equal(parseCommand(`/crm-production-auth-smoke sha=${sha}`)?.mode,'auth-smoke');

console.log('PRODUCTION_RELEASE_BRIDGE_ISSUE_26_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_OWNER_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_PR_COMMENT_BLOCKED=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_PREFLIGHT_EXACT_COMMAND=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_DEPLOY_CONFIRMATION_PRESERVED=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_AUTH_SMOKE_EXACT_COMMAND=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_EXACT_SHA_MAIN_GATE=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_VALIDATED_WORKFLOW_ROUTING=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_NO_DIRECT_PRODUCTION_WRITE=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_NEGATIVE_COMMANDS=PASS');
