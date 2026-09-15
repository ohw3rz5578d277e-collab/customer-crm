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
  assert.match(workflow,/pages\/projects\/customer-crm/);
  assert.match(workflow,/PAGES_PREVIEW_DEPLOYMENT_SETTING/);
  assert.match(workflow,/access\/apps\/\$app_id\/policies/);
  assert.match(workflow,/decision==\"bypass\"/);
  assert.match(workflow,/has\(\"everyone\"\)/);
  assert.match(workflow,/per_page=100/);
  assert.match(workflow,/total_pages/);
  assert.doesNotMatch(workflow,/--request\s+(POST|PUT|PATCH|DELETE)/i);
  assert.doesNotMatch(workflow,/wrangler\s+deploy/i);
  assert.doesNotMatch(workflow,/d1\s+execute/i);
});
