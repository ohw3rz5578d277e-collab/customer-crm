import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

test('canonical Wrangler config disables Worker preview URLs',()=>{
  const config=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
  assert.equal(config.name,'customer-crm-api');
  assert.equal(config.preview_urls,false,'Worker preview URLs must be explicitly disabled');
  assert.equal(config.vars?.CRM_OWNER_AUTH_MODE,'hybrid');
});

test('Cloudflare security audit workflow is read-only by contract',()=>{
  const workflow=fs.readFileSync('.github/workflows/cloudflare-security-readonly-audit.yml','utf8');
  assert.match(workflow,/workflow_dispatch:/);
  assert.match(workflow,/permissions:\n\s+contents: read/);
  assert.match(workflow,/--request GET|curl -sS/);
  assert.doesNotMatch(workflow,/--request\s+(POST|PUT|PATCH|DELETE)/i);
  assert.doesNotMatch(workflow,/wrangler\s+deploy/i);
  assert.doesNotMatch(workflow,/d1\s+execute/i);
});
