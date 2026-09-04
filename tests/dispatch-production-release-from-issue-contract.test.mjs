import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflowPath=new URL('../.github/workflows/dispatch-production-deploy-from-issue.yml',import.meta.url);
const source=fs.readFileSync(workflowPath,'utf8');

const owner='ohw3rz5578d277e-collab';
const sha='7e8587fd7874afb73c79693d9aa133e8ddbb715a';

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
assert.match(source,/startsWith\(github\.event\.comment\.body, '\/crm-production-preflight '\)/,'preflight prefix must be admitted only to strict parser');
assert.match(source,/startsWith\(github\.event\.comment\.body, '\/crm-production-deploy '\)/,'deploy prefix must remain admitted only to strict parser');
assert.match(source,/startsWith\(github\.event\.comment\.body, '\/crm-production-auth-smoke '\)/,'auth smoke prefix must be admitted only to strict parser');

assert.match(source,/preflight_re='\^\/crm-production-preflight sha=\(\[0-9a-f\]\{40\}\)\$'/,'preflight command must be exact lowercase 40-hex syntax');
assert.match(source,/deploy_re='\^\/crm-production-deploy confirm_production=true sha=\(\[0-9a-f\]\{40\}\)\
assert.match(source,/release_mode='preflight'/,'preflight must map to preflight mode');
assert.match(source,/release_mode='deploy'/,'deploy must map to deploy mode');
assert.match(source,/release_mode='auth-smoke'/,'auth smoke must map to auth-smoke mode');
assert.match(source,/dispatch_workflow='production-browser-smoke-auth\.yml'/,'auth smoke must target existing read-only browser smoke workflow');
assert.match(source,/MAIN_DRIFT expected=\$expected_sha current=\$current_sha/,'fresh current-main exact SHA gate must fail closed');
assert.match(source,/if \[\[ "\$current_sha" != "\$expected_sha" \]\]; then/,'authorized SHA must equal fresh main SHA');
assert.match(source,/actions\/workflows\/\$\{DISPATCH_WORKFLOW\}\/dispatches/,'dispatch endpoint must use validated workflow output');
assert.match(source,/dispatch_workflow='deploy-cloudflare\.yml'/,'preflight/deploy must retain canonical release workflow target');
assert.match(source,/dispatch_workflow='production-browser-smoke-auth\.yml'/,'auth smoke must target the dedicated read-only workflow');
assert.match(source,/['"]mode['"]:os\.environ\['RELEASE_MODE'\]/,'canonical mode must come from validated parser output');
assert.match(source,/['"]expected_sha['"]:os\.environ\['EXPECTED_SHA'\]/,'validated exact SHA must be passed to canonical workflow');
assert.match(source,/CONFIRM_PRODUCTION=NOT_REQUIRED_FOR_PREFLIGHT/,'preflight must not require Production confirmation');
assert.match(source,/CONFIRM_PRODUCTION=true/,'deploy must retain explicit Production confirmation');
assert.match(source,/BRIDGE_PRODUCTION_WRITE=0/,'bridge must declare no direct Production writes');

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
  '/crm-production-preflight sha= extra=true',
  `/crm-production-preflight mode=deploy sha=${sha}`,
  `/crm-production-preflight confirm_production=true sha=${sha}`,
  `/crm-production-preflight sha=${sha} extra=true`,
  `junk /crm-production-preflight sha=${sha}`,
  `/crm-production-preflight sha=${sha} junk`,
  `/crm-production-preflight sha=${sha.toUpperCase()}`,
  `/crm-production-preflight sha=${sha}\n/crm-production-deploy confirm_production=true sha=${sha}`,
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
  `/crm-production-auth-smoke sha=${sha}\n`
])assert.equal(parseCommand(body),null,`malformed command must fail closed: ${JSON.stringify(body)}`);

assert.equal(parseCommand(`/crm-production-preflight sha=${sha}`)?.mode,'preflight','preflight must never map to deploy');
assert.equal(parseCommand(`/crm-production-deploy confirm_production=true sha=${sha}`)?.mode,'deploy','confirmed deploy must map to deploy');
assert.equal(parseCommand(`/crm-production-auth-smoke sha=${sha}`)?.mode,'auth-smoke','auth smoke must never map to deploy');

console.log('PRODUCTION_RELEASE_BRIDGE_ISSUE_26_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_OWNER_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_PR_COMMENT_BLOCKED=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_PREFLIGHT_EXACT_COMMAND=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_DEPLOY_CONFIRMATION_PRESERVED=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_EXACT_SHA_MAIN_GATE=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_CANONICAL_WORKFLOW_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_NO_DIRECT_PRODUCTION_WRITE=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_AUTH_SMOKE_EXACT_COMMAND=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_AUTH_SMOKE_READ_ONLY_ROUTING=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_NEGATIVE_COMMANDS=PASS');
/,'deploy confirmation contract must remain exact');
assert.match(source,/auth_smoke_re='\^\/crm-production-auth-smoke sha=\(\[0-9a-f\]\{40\}\)\
assert.match(source,/release_mode='preflight'/,'preflight must map to preflight mode');
assert.match(source,/release_mode='deploy'/,'deploy must map to deploy mode');
assert.match(source,/MAIN_DRIFT expected=\$expected_sha current=\$current_sha/,'fresh current-main exact SHA gate must fail closed');
assert.match(source,/if \[\[ "\$current_sha" != "\$expected_sha" \]\]; then/,'authorized SHA must equal fresh main SHA');
assert.match(source,/actions\/workflows\/deploy-cloudflare\.yml\/dispatches/,'canonical workflow must be the only dispatch target');
assert.match(source,/['"]mode['"]:os\.environ\['RELEASE_MODE'\]/,'canonical mode must come from validated parser output');
assert.match(source,/['"]expected_sha['"]:os\.environ\['EXPECTED_SHA'\]/,'validated exact SHA must be passed to canonical workflow');
assert.match(source,/CONFIRM_PRODUCTION=NOT_REQUIRED_FOR_PREFLIGHT/,'preflight must not require Production confirmation');
assert.match(source,/CONFIRM_PRODUCTION=true/,'deploy must retain explicit Production confirmation');
assert.match(source,/BRIDGE_PRODUCTION_WRITE=0/,'bridge must declare no direct Production writes');

assert.doesNotMatch(source,/\bwrangler\s+deploy\b/i,'bridge must not directly deploy Worker');
assert.doesNotMatch(source,/\bd1\s+migrations\s+apply\b/i,'bridge must not apply D1 migrations');
assert.doesNotMatch(source,/\bd1\s+execute\b/i,'bridge must not execute D1 commands');
assert.doesNotMatch(source,/\b(INSERT|UPDATE|DELETE)\s+(INTO|FROM|[A-Za-z_])/i,'bridge must not contain direct D1 write SQL');

assert.deepEqual(parseCommand(`/crm-production-preflight sha=${sha}`),{mode:'preflight',sha,confirmProduction:false});
assert.deepEqual(parseCommand(`/crm-production-deploy confirm_production=true sha=${sha}`),{mode:'deploy',sha,confirmProduction:true});

for(const body of [
  '/crm-production-preflight',
  '/crm-production-preflight sha=short',
  '/crm-production-preflight sha= extra=true',
  `/crm-production-preflight mode=deploy sha=${sha}`,
  `/crm-production-preflight confirm_production=true sha=${sha}`,
  `/crm-production-preflight sha=${sha} extra=true`,
  `junk /crm-production-preflight sha=${sha}`,
  `/crm-production-preflight sha=${sha} junk`,
  `/crm-production-preflight sha=${sha.toUpperCase()}`,
  `/crm-production-preflight sha=${sha}\n/crm-production-deploy confirm_production=true sha=${sha}`,
  `/crm-production-preflight sha=${sha}\n`,
  `/crm-production-deploy sha=${sha}`,
  `/crm-production-deploy confirm_production=false sha=${sha}`,
  '/crm-production-deploy confirm_production=true',
  `/crm-production-deploy confirm_production=true sha=${sha} extra=true`,
  `/crm-production-deploy confirm_production=true sha=${sha.toUpperCase()}`,
  `/crm-production-deploy confirm_production=true sha=${sha}\n`
])assert.equal(parseCommand(body),null,`malformed command must fail closed: ${JSON.stringify(body)}`);

assert.equal(parseCommand(`/crm-production-preflight sha=${sha}`)?.mode,'preflight','preflight must never map to deploy');
assert.equal(parseCommand(`/crm-production-deploy confirm_production=true sha=${sha}`)?.mode,'deploy','confirmed deploy must map to deploy');

console.log('PRODUCTION_RELEASE_BRIDGE_ISSUE_26_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_OWNER_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_PR_COMMENT_BLOCKED=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_PREFLIGHT_EXACT_COMMAND=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_DEPLOY_CONFIRMATION_PRESERVED=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_EXACT_SHA_MAIN_GATE=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_CANONICAL_WORKFLOW_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_NO_DIRECT_PRODUCTION_WRITE=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_NEGATIVE_COMMANDS=PASS');
/,'auth smoke command must be exact lowercase 40-hex syntax');
assert.match(source,/release_mode='preflight'/,'preflight must map to preflight mode');
assert.match(source,/release_mode='deploy'/,'deploy must map to deploy mode');
assert.match(source,/MAIN_DRIFT expected=\$expected_sha current=\$current_sha/,'fresh current-main exact SHA gate must fail closed');
assert.match(source,/if \[\[ "\$current_sha" != "\$expected_sha" \]\]; then/,'authorized SHA must equal fresh main SHA');
assert.match(source,/actions\/workflows\/deploy-cloudflare\.yml\/dispatches/,'canonical workflow must be the only dispatch target');
assert.match(source,/['"]mode['"]:os\.environ\['RELEASE_MODE'\]/,'canonical mode must come from validated parser output');
assert.match(source,/['"]expected_sha['"]:os\.environ\['EXPECTED_SHA'\]/,'validated exact SHA must be passed to canonical workflow');
assert.match(source,/CONFIRM_PRODUCTION=NOT_REQUIRED_FOR_PREFLIGHT/,'preflight must not require Production confirmation');
assert.match(source,/CONFIRM_PRODUCTION=true/,'deploy must retain explicit Production confirmation');
assert.match(source,/BRIDGE_PRODUCTION_WRITE=0/,'bridge must declare no direct Production writes');

assert.doesNotMatch(source,/\bwrangler\s+deploy\b/i,'bridge must not directly deploy Worker');
assert.doesNotMatch(source,/\bd1\s+migrations\s+apply\b/i,'bridge must not apply D1 migrations');
assert.doesNotMatch(source,/\bd1\s+execute\b/i,'bridge must not execute D1 commands');
assert.doesNotMatch(source,/\b(INSERT|UPDATE|DELETE)\s+(INTO|FROM|[A-Za-z_])/i,'bridge must not contain direct D1 write SQL');

assert.deepEqual(parseCommand(`/crm-production-preflight sha=${sha}`),{mode:'preflight',sha,confirmProduction:false});
assert.deepEqual(parseCommand(`/crm-production-deploy confirm_production=true sha=${sha}`),{mode:'deploy',sha,confirmProduction:true});

for(const body of [
  '/crm-production-preflight',
  '/crm-production-preflight sha=short',
  '/crm-production-preflight sha= extra=true',
  `/crm-production-preflight mode=deploy sha=${sha}`,
  `/crm-production-preflight confirm_production=true sha=${sha}`,
  `/crm-production-preflight sha=${sha} extra=true`,
  `junk /crm-production-preflight sha=${sha}`,
  `/crm-production-preflight sha=${sha} junk`,
  `/crm-production-preflight sha=${sha.toUpperCase()}`,
  `/crm-production-preflight sha=${sha}\n/crm-production-deploy confirm_production=true sha=${sha}`,
  `/crm-production-preflight sha=${sha}\n`,
  `/crm-production-deploy sha=${sha}`,
  `/crm-production-deploy confirm_production=false sha=${sha}`,
  '/crm-production-deploy confirm_production=true',
  `/crm-production-deploy confirm_production=true sha=${sha} extra=true`,
  `/crm-production-deploy confirm_production=true sha=${sha.toUpperCase()}`,
  `/crm-production-deploy confirm_production=true sha=${sha}\n`
])assert.equal(parseCommand(body),null,`malformed command must fail closed: ${JSON.stringify(body)}`);

assert.equal(parseCommand(`/crm-production-preflight sha=${sha}`)?.mode,'preflight','preflight must never map to deploy');
assert.equal(parseCommand(`/crm-production-deploy confirm_production=true sha=${sha}`)?.mode,'deploy','confirmed deploy must map to deploy');

console.log('PRODUCTION_RELEASE_BRIDGE_ISSUE_26_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_OWNER_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_PR_COMMENT_BLOCKED=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_PREFLIGHT_EXACT_COMMAND=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_DEPLOY_CONFIRMATION_PRESERVED=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_EXACT_SHA_MAIN_GATE=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_CANONICAL_WORKFLOW_ONLY=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_NO_DIRECT_PRODUCTION_WRITE=PASS');
console.log('PRODUCTION_RELEASE_BRIDGE_NEGATIVE_COMMANDS=PASS');
