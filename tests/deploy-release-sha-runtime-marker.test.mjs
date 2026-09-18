import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

test('Production entry exposes a fail-safe release SHA marker',async()=>{
  const source=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
  assert.ok(source.includes("typeof CRM_RELEASE_SHA==='string'"));
  assert.ok(source.includes("'source-untracked'"));
  assert.ok(source.includes("h.set('x-crm-release-sha',RELEASE_SHA)"));
  assert.ok(source.includes('customer360_release_sha:RELEASE_SHA'));

  const {patchHealth}=await import('../src/production-index-crm-customer360-entry.js');
  const response=await patchHealth(new Response('{}',{
    status:200,
    headers:{'content-type':'application/json; charset=utf-8'}
  }),{});
  const health=await response.json();
  assert.equal(health.customer360_release_sha,'source-untracked');
  assert.equal(response.headers.get('x-crm-release-sha'),'source-untracked');
});

test('canonical deploy bakes and verifies the exact authorized SHA',()=>{
  const workflow=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');

  assert.ok(workflow.includes(`--define "CRM_RELEASE_SHA:'\${{ steps.authorized_sha.outputs.sha }}'"`));
  assert.ok(workflow.includes('EXPECTED_SHA: ${{ steps.authorized_sha.outputs.sha }}'));
  assert.ok(workflow.includes('h.customer360_release_sha!==process.env.EXPECTED_SHA'));
  assert.ok(workflow.includes('PRODUCTION_RELEASE_SHA_BODY=PASS'));
  assert.ok(workflow.includes('x-crm-release-sha'));
  assert.ok(workflow.includes('PRODUCTION_RELEASE_SHA_HEADER=PASS'));

  const deployIndex=workflow.indexOf('- name: Deploy customer-crm-api');
  const bodyCheckIndex=workflow.indexOf('PRODUCTION_RELEASE_SHA_BODY=PASS');
  const headerCheckIndex=workflow.indexOf('PRODUCTION_RELEASE_SHA_HEADER=PASS');
  assert.ok(deployIndex>=0 && bodyCheckIndex>deployIndex && headerCheckIndex>bodyCheckIndex);
});
