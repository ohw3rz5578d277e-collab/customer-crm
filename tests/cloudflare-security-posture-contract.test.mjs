import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

test('canonical Wrangler config disables Worker preview URLs',()=>{
  const config=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
  assert.equal(config.name,'customer-crm-api');
  assert.equal(config.main,'src/production-index-crm-customer360-entry.js');
  assert.equal(config.preview_urls,false,'Worker preview URLs must be explicitly disabled');
  assert.equal(config.vars?.CRM_OWNER_AUTH_MODE,'hybrid');
});

test('Cloudflare security audit is read-only and enumerates historical Pages previews',()=>{
  const workflow=fs.readFileSync('.github/workflows/cloudflare-security-readonly-audit.yml','utf8');

  assert.match(workflow,/workflow_dispatch:/);
  assert.match(workflow,/permissions:\n\s+contents: read/);

  assert.match(workflow,/workers\/workers\?page=\$workers_page&per_page=100/);
  assert.match(workflow,/access\/apps\?page=\$access_page&per_page=100/);
  assert.match(workflow,/access\/apps\/\$app_id\/policies\?page=\$policy_page&per_page=100/);

  assert.match(workflow,/pages\/projects\/\$\{PAGES_PROJECT\}\/deployments\?page=\$deployments_page&per_page=100/);
  assert.match(workflow,/deployments_total_pages/);
  assert.match(workflow,/PAGES_NONPRODUCTION_DEPLOYMENTS/);
  assert.match(workflow,/PAGES_NONPRODUCTION_ALIASES/);
  assert.match(workflow,/\.environment \/\/ ""\) != "production"/);
  assert.match(workflow,/Historical or current non-production Pages deployments still exist/);

  assert.match(workflow,/decision=="bypass"/);
  assert.match(workflow,/decision=="non_identity"/);
  assert.match(workflow,/has\("everyone"\)/);

  assert.doesNotMatch(workflow,/--request\s+(POST|PUT|PATCH|DELETE)/i);
  assert.doesNotMatch(workflow,/wrangler\s+deploy/i);
  assert.doesNotMatch(workflow,/d1\s+execute/i);
  assert.doesNotMatch(workflow,/LINE.*send/i);
});

test('security audit does not print deployment URLs or customer data',()=>{
  const workflow=fs.readFileSync('.github/workflows/cloudflare-security-readonly-audit.yml','utf8');
  assert.doesNotMatch(workflow,/\.url\b/);
  assert.doesNotMatch(workflow,/customer_id|line_user_id|phone|email/i);
  assert.match(workflow,/pages_total_deployments/);
  assert.match(workflow,/pages_nonproduction_deployments/);
});
