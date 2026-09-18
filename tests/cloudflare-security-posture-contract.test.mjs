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

  assert.ok(workflow.includes('/workers/workers?page=$workers_page&per_page=100'));
  assert.ok(workflow.includes('/access/apps?page=$access_page&per_page=100'));
  assert.ok(workflow.includes('/access/apps/$app_id/policies?page=$policy_page&per_page=100'));

  assert.ok(workflow.includes('/pages/projects/${PAGES_PROJECT}/deployments?page=$deployments_page&per_page=100'));
  assert.ok(workflow.includes('deployments_total_pages'));
  assert.ok(workflow.includes('PAGES_NONPRODUCTION_DEPLOYMENTS'));
  assert.ok(workflow.includes('PAGES_NONPRODUCTION_ALIASES'));
  assert.ok(workflow.includes('(.environment // "") != "production"'));
  assert.ok(workflow.includes('Historical or current non-production Pages deployments still exist'));

  assert.ok(workflow.includes('decision=="bypass"'));
  assert.ok(workflow.includes('decision=="non_identity"'));
  assert.ok(workflow.includes('has("everyone")'));

  assert.doesNotMatch(workflow,/--request\s+(POST|PUT|PATCH|DELETE)/i);
  assert.doesNotMatch(workflow,/wrangler\s+deploy/i);
  assert.doesNotMatch(workflow,/d1\s+execute/i);
});

test('security audit reports counts only and does not print deployment URLs or customer PII fields',()=>{
  const workflow=fs.readFileSync('.github/workflows/cloudflare-security-readonly-audit.yml','utf8');
  assert.doesNotMatch(workflow,/\.url\b/);
  assert.doesNotMatch(workflow,/line_user_id|phone|customer_email/i);
  assert.ok(workflow.includes('pages_total_deployments'));
  assert.ok(workflow.includes('pages_nonproduction_deployments'));
  assert.ok(workflow.includes('pages_nonproduction_aliases'));
});
