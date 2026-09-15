import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {
  enforceProductionRequestBoundary,
  hardenProductionResponse
} from '../src/production-index-crm-customer360-entry.js';

test('TRACE and CONNECT are rejected at the Production boundary',()=>{
  for(const method of ['TRACE','CONNECT']){
    const req=new Request('https://crm.example.test/admin',{method});
    const res=enforceProductionRequestBoundary(req);
    assert.equal(res.status,405);
  }
});

test('cross-origin and cross-site CRM API requests are rejected',()=>{
  const crossOrigin=new Request('https://crm.example.test/api/customer360/customers',{
    headers:{origin:'https://evil.example','sec-fetch-site':'cross-site'}
  });
  assert.equal(enforceProductionRequestBoundary(crossOrigin).status,403);

  const crossSite=new Request('https://crm.example.test/api/customer360/customers',{
    headers:{'sec-fetch-site':'cross-site'}
  });
  assert.equal(enforceProductionRequestBoundary(crossSite).status,403);
});

test('same-origin API request is allowed through the boundary',()=>{
  const req=new Request('https://crm.example.test/api/customer360/customers',{
    headers:{origin:'https://crm.example.test','sec-fetch-site':'same-origin'}
  });
  assert.equal(enforceProductionRequestBoundary(req),null);
});

test('Production response hardening strips permissive CORS and adds browser security headers',async()=>{
  const upstream=new Response('<!doctype html><html><body>ok</body></html>',{
    status:200,
    headers:{
      'content-type':'text/html; charset=utf-8',
      'access-control-allow-origin':'*',
      'access-control-allow-methods':'GET,POST,PATCH,DELETE,OPTIONS',
      'access-control-allow-headers':'*'
    }
  });
  const req=new Request('https://crm.example.test/admin');
  const res=hardenProductionResponse(upstream,req);
  assert.equal(res.headers.get('access-control-allow-origin'),null);
  assert.equal(res.headers.get('access-control-allow-methods'),null);
  assert.equal(res.headers.get('access-control-allow-headers'),null);
  assert.match(res.headers.get('strict-transport-security')||'',/max-age=31536000/);
  assert.equal(res.headers.get('x-content-type-options'),'nosniff');
  assert.equal(res.headers.get('x-frame-options'),'DENY');
  assert.equal(res.headers.get('referrer-policy'),'no-referrer');
  assert.match(res.headers.get('permissions-policy')||'',/camera=\(\)/);
  assert.equal(res.headers.get('cross-origin-opener-policy'),'same-origin');
  assert.equal(res.headers.get('cross-origin-resource-policy'),'same-origin');
  assert.match(res.headers.get('content-security-policy')||'',/frame-ancestors 'none'/);
  assert.match(res.headers.get('content-security-policy')||'',/object-src 'none'/);
  assert.match(res.headers.get('cache-control')||'',/no-store/);
  assert.match(await res.text(),/ok/);
});

test('security hardening keeps Production entry wired to verified Access context',()=>{
  const source=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
  assert.match(source,/withOwnerPasswordPrincipal\(request,env,ctx\)/);
  const auth=fs.readFileSync('src/crm-owner-password-auth.mjs','utf8');
  assert.match(auth,/ctx\.access\.getIdentity/);
  assert.match(auth,/headers\.delete\('cf-access-authenticated-user-email'\)/);
});

test('Owner login limiter is tightened to five attempts per minute',()=>{
  const config=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
  const limiter=(config.ratelimits||[]).find(x=>x.name==='CRM_OWNER_LOGIN_RATE_LIMITER');
  assert.ok(limiter);
  assert.equal(limiter.simple.limit,5);
  assert.equal(limiter.simple.period,60);
});


test('legacy sync/admin token paths do not expose token diagnostics and use constant-time comparison',()=>{
  const source=fs.readFileSync('src/index.js','utf8');
  assert.match(source,/function constantTimeEqualText\(/);
  assert.doesNotMatch(source,/requestTokenLength/);
  assert.doesNotMatch(source,/workerTokenLength/);
  assert.doesNotMatch(source,/tokensMatch/);
  assert.match(source,/path === "\/api\/debug-env"\) return json\(\{ ok: false, message: "Not Found" \}, 404\)/);
});
