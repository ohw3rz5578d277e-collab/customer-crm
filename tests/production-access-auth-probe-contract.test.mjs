import assert from 'node:assert/strict';
import fs from 'node:fs';
import {handleProductionAccessAuthProbe} from '../src/production-index-crm-customer360-entry.js';

const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const probe=fs.readFileSync('.github/workflows/production-access-auth-probe.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-production-access-auth-probe-from-issue.yml','utf8');
const releaseBridge=fs.readFileSync('.github/workflows/dispatch-production-deploy-from-issue.yml','utf8');
const browserSmoke=fs.readFileSync('.github/workflows/production-browser-smoke-auth.yml','utf8');

const throwingDb=new Proxy({},{
  get(){throw new Error('D1_MUST_NOT_BE_TOUCHED_BY_ACCESS_AUTH_PROBE')}
});
const env={ADMIN_TOKEN:'probe-admin-token',DB:throwingDb};

{
  const req=new Request('https://customer-crm-api.example/__crm/access-auth-probe',{
    method:'GET',
    headers:{'x-admin-token':'probe-admin-token'}
  });
  const res=handleProductionAccessAuthProbe(req,env);
  assert.ok(res instanceof Response);
  assert.equal(res.status,200);
  const body=await res.json();
  assert.deepEqual(body,{
    ok:true,
    service:'customer-crm-api',
    access_auth_probe:true,
    production_write:false,
    customer_id_generation:false,
    line_send:false
  });
  assert.equal(res.headers.get('x-crm-access-auth-probe'),'read-only');
}

{
  const req=new Request('https://customer-crm-api.example/__crm/access-auth-probe',{method:'GET'});
  const res=handleProductionAccessAuthProbe(req,env);
  assert.equal(res.status,401);
  assert.equal((await res.json()).error,'unauthorized');
}

{
  const req=new Request('https://customer-crm-api.example/__crm/access-auth-probe',{
    method:'POST',
    headers:{'x-admin-token':'probe-admin-token'}
  });
  assert.equal(handleProductionAccessAuthProbe(req,env),null);
}

assert.ok(entry.includes("url.pathname!=='/__crm/access-auth-probe'"),'pure probe path missing');
assert.ok(entry.includes('const accessAuthProbe=handleProductionAccessAuthProbe(request,env);'),'probe must be handled in top-level Production entry');
assert.ok(
  entry.indexOf('const accessAuthProbe=handleProductionAccessAuthProbe(request,env);') <
  entry.indexOf('const lineProfileApi=await handleCustomer360LineProfileExtraction(request,env);'),
  'probe must run before downstream handlers'
);
assert.ok(
  entry.indexOf('const accessAuthProbe=handleProductionAccessAuthProbe(request,env);') <
  entry.indexOf('let response=await app.fetch(request,env,ctx);'),
  'probe must run before lower application chain'
);

assert.match(probe,/workflow_dispatch:\s*\n\s*inputs:\s*\n\s*expected_sha:/,'probe workflow must require expected_sha');
assert.ok(probe.includes('required: true'),'expected_sha must be required');
assert.ok(probe.includes('^[-0-9a-f]{40}$')===false,'malformed SHA validator must not exist');
assert.ok(probe.includes('^[0-9a-f]{40}$'),'probe must validate lowercase 40-hex SHA');
assert.ok(probe.includes('Checkout exact authorized SHA'),'probe must checkout exact authorized SHA');
assert.ok(probe.includes('git ls-remote origin refs/heads/main'),'probe must fresh-read main');
assert.ok(probe.includes('MAIN_DRIFT expected=$EXPECTED_SHA current=$remote_main'),'probe must fail closed on main drift');
assert.ok(probe.includes('group: customer-crm-production-deploy'),'probe must share Production release concurrency lock');
assert.ok(probe.includes("probe_path='/__crm/access-auth-probe'"),'workflow must call only the pure probe path');
assert.ok(probe.includes('CF-Access-Client-Id'),'service token client ID header missing');
assert.ok(probe.includes('CF-Access-Client-Secret'),'service token client secret header missing');
assert.ok(probe.includes('x-admin-token: $ADMIN_TOKEN'),'admin token header missing');
assert.ok(probe.includes('control_status="$(curl'),'Access-negative control request missing');
assert.ok(probe.includes('CONTROL_ACCESS_PROBE_WORKER_MARKER'),'control request must inspect Worker probe marker');
assert.ok(probe.includes('ACCESS_CONTROL_REACHED_WORKER_PROBE'),'control request must fail if it reaches Worker');
assert.ok(probe.includes('ACCESS_CONTROL_NOT_BLOCKED'),'control request must fail on 2xx without Access');
assert.ok(probe.includes('ACCESS_CONTROL_BLOCK=PASS'),'control request must require Access blocking evidence');
assert.ok(probe.includes('3??|401|403'),'control request must accept only explicit Access-style block statuses');
assert.ok(probe.includes('ACCESS_SERVICE_TOKEN_REDIRECT'),'3xx Access response must fail closed');
assert.ok(probe.includes('u.origin+u.pathname'),'redirect logging must strip query and fragment');
assert.ok(probe.includes('PRODUCTION_D1_READ=0'),'D1 read zero evidence missing');
assert.ok(probe.includes('PRODUCTION_D1_WRITE=0'),'D1 write zero evidence missing');
assert.ok(probe.includes('WORKER_DEPLOY=0'),'Worker deploy zero evidence missing');
assert.doesNotMatch(probe,/\bwrangler\b/i,'Access probe workflow must not use Wrangler');
assert.doesNotMatch(probe,/\bd1\b[^\n]*(execute|migrations|sql)/i,'Access probe workflow must not run D1 operations');
assert.doesNotMatch(probe,/request\.(post|put|patch|delete)\s*\(/i,'Access probe workflow must not issue mutation API requests');

assert.match(bridge,/github\.event\.issue\.number == 26/,'probe bridge must remain issue #26 only');
assert.match(bridge,/github\.event\.issue\.pull_request == null/,'probe bridge must block PR comments');
assert.match(bridge,/github\.actor == 'ohw3rz5578d277e-collab'/,'probe bridge must be Owner actor only');
assert.match(bridge,/github\.event\.comment\.user\.login == 'ohw3rz5578d277e-collab'/,'probe bridge comment author must be Owner only');
assert.ok(bridge.includes("command_re='^/crm-production-auth-probe sha=([0-9a-f]{40})$'"),'probe command must be exact');
assert.ok(bridge.includes('MAIN_DRIFT expected=$expected_sha current=$current_sha'),'probe bridge must fail closed on main drift');
assert.ok(bridge.includes('deploy-cloudflare.yml production-access-auth-probe.yml'),'probe bridge must inspect release/probe workflows before dispatch');
assert.ok(bridge.includes('for status in queued in_progress'),'probe bridge must inspect queued and running Production lock occupants');
assert.ok(bridge.includes('runs?status=$status&per_page=1'),'probe bridge must query live workflow run occupancy');
assert.ok(bridge.includes('PRODUCTION_RELEASE_LOCK_OCCUPIED'),'occupied Production release lock must fail closed');
assert.ok(bridge.includes('PRODUCTION_RELEASE_LOCK=FREE'),'free Production release lock evidence missing');
assert.ok(
  bridge.indexOf('Refuse while Production release lock is occupied') <
  bridge.indexOf('Dispatch read-only Production Access probe'),
  'lock occupancy check must happen before probe dispatch'
);
assert.ok(bridge.includes('actions/workflows/production-access-auth-probe.yml/dispatches'),'bridge must target only dedicated Access probe workflow');
assert.ok(bridge.includes("inputs':{'expected_sha':os.environ['EXPECTED_SHA']}") || bridge.includes("'inputs':{'expected_sha':os.environ['EXPECTED_SHA']}"),'bridge must forward exact SHA');
assert.ok(bridge.includes('PRODUCTION_WRITE=0'),'bridge write-zero evidence missing');
assert.ok(bridge.includes('deploy-cloudflare.yml production-access-auth-probe.yml'),'bridge may reference deploy workflow only for occupancy inspection');
assert.doesNotMatch(
  bridge,
  /actions\/workflows\/deploy-cloudflare\.yml\/dispatches/,
  'Access probe bridge must not dispatch Production deploy workflow'
);
assert.doesNotMatch(bridge,/confirm_production=true/,'Access probe command must not carry deploy confirmation');

assert.ok(releaseBridge.includes('/crm-production-deploy confirm_production=true sha=([0-9a-f]{40})'),'existing deploy confirmation contract must remain unchanged');
assert.ok(browserSmoke.includes('name: Production Authenticated Browser Smoke'),'existing browser smoke workflow must remain present');

console.log('PRODUCTION_ACCESS_AUTH_PROBE_ROUTE_D1_FREE=PASS');
console.log('PRODUCTION_ACCESS_AUTH_PROBE_EXACT_SHA=PASS');
console.log('PRODUCTION_ACCESS_AUTH_PROBE_READ_ONLY=PASS');
console.log('PRODUCTION_ACCESS_CONTROL_NEGATIVE_PROBE=PASS');
console.log('PRODUCTION_RELEASE_LOCK_OCCUPANCY_GUARD=PASS');
console.log('PRODUCTION_ACCESS_AUTH_PROBE_ISSUE_26_OWNER_ONLY=PASS');
console.log('PRODUCTION_DEPLOY_BRIDGE_UNCHANGED=PASS');
