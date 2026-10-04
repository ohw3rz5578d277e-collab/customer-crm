import fs from 'node:fs';

const read = (path) => fs.readFileSync(path, 'utf8');
const requireText = (text, needle, label) => {
  if (!text.includes(needle)) throw new Error(`MISSING_${label}:${needle}`);
};
const rejectText = (text, needle, label) => {
  if (text.includes(needle)) throw new Error(`UNEXPECTED_${label}:${needle}`);
};

const deploy = read('.github/workflows/deploy-cloudflare.yml');
requireText(deploy, 'run-name: >-', 'DEPLOY_RUN_NAME');
requireText(deploy, "format('CRM Production {0} {1}', inputs.mode, inputs.expected_sha)", 'DEPLOY_MODE_RUN_NAME');
requireText(deploy, "inputs.mode == 'deploy' && 'customer-crm-production-deploy'", 'DEPLOY_SHARED_MUTATION_LOCK');
requireText(deploy, "format('customer-crm-production-preflight-{0}', github.run_id)", 'PREFLIGHT_ISOLATED_LOCK');

const bridges = [
  '.github/workflows/dispatch-production-deploy-from-issue.yml',
  '.github/workflows/dispatch-member-schema-apply-from-issue.yml',
  '.github/workflows/dispatch-member-production-runtime-secret-stage-from-issue.yml',
  '.github/workflows/dispatch-member-production-version-promotion-from-issue.yml',
];

for (const path of bridges) {
  const text = read(path);
  requireText(text, 'run-name: >-', 'BRIDGE_RUN_NAME');
  requireText(text, 'Production mutation bridge:', 'MUTATION_BRIDGE_LABEL');
  requireText(text, 'PRODUCTION_MUTATION_BUSY_RETRY_REQUIRED', 'BUSY_FAIL_CLOSED');
  requireText(text, 'PRODUCTION_MUTATION_FILTER=ACTUAL_MUTATIONS_ONLY', 'MUTATION_ONLY_FILTER_MARKER');
  requireText(text, 'event=str(r.get("event", ""))', 'EVENT_FILTER');
  requireText(text, 'title=str(r.get("display_title", ""))', 'RUN_TITLE_FILTER');
  requireText(text, 'if workflow == "deploy-cloudflare.yml":', 'DEPLOY_CLASSIFIER');
  requireText(text, 'if event != "workflow_dispatch":', 'DEPLOY_EVENT_CLASSIFIER');
  requireText(text, 'if title.startswith("CRM Production preflight "):', 'PREFLIGHT_EXCLUSION');
  requireText(text, 'return event == "workflow_dispatch"', 'MUTATION_TARGET_EVENT_FILTER');
  requireText(text, 'if title.startswith("Production read-only bridge:") or title == "Production bridge: ignored":', 'READ_ONLY_BRIDGE_EXCLUSION');
  requireText(text, 'if title.startswith("Production mutation bridge:"):', 'MUTATION_BRIDGE_INCLUSION');
  requireText(text, 'return event == "issue_comment"', 'LEGACY_BRIDGE_FAIL_CLOSED');
}

const canonicalBridge = read('.github/workflows/dispatch-production-deploy-from-issue.yml');
requireText(canonicalBridge, 'Production read-only bridge: preflight', 'CRM_PREFLIGHT_READ_ONLY_LABEL');
requireText(canonicalBridge, 'Production mutation bridge: deploy', 'CRM_DEPLOY_MUTATION_LABEL');
requireText(canonicalBridge, "steps.gate.outputs.release_mode == 'deploy'", 'CRM_BUSY_CHECK_DEPLOY_ONLY');

const promotionBridge = read('.github/workflows/dispatch-member-production-version-promotion-from-issue.yml');
requireText(promotionBridge, 'Production read-only bridge: version-snapshot', 'PROMOTION_SNAPSHOT_READ_ONLY_LABEL');
requireText(promotionBridge, 'Production mutation bridge: version-promotion', 'PROMOTION_MUTATION_LABEL');
rejectText(promotionBridge, 'MEMBER_PROMOTION_DISPATCH_LOCK_OCCUPIED', 'LEGACY_PROMOTION_BROAD_LOCK');

const runtimeBridge = read('.github/workflows/dispatch-member-production-runtime-secret-stage-from-issue.yml');
requireText(runtimeBridge, 'Production mutation bridge: runtime-secret-stage', 'RUNTIME_MUTATION_LABEL');
rejectText(runtimeBridge, 'MEMBER_RUNTIME_SECRET_STAGE_LOCK_OCCUPIED', 'LEGACY_RUNTIME_BROAD_LOCK');

const schemaBridge = read('.github/workflows/dispatch-member-schema-apply-from-issue.yml');
requireText(schemaBridge, 'Production mutation bridge: schema-apply', 'SCHEMA_MUTATION_LABEL');

console.log('PRODUCTION_MUTATION_BUSY_FILTER_CONTRACT=PASS');
console.log('NON_MUTATING_PREFLIGHT_EXCLUDED=PASS');
console.log('PR_CI_RUNS_EXCLUDED=PASS');
console.log('LEGACY_UNKNOWN_MUTATION_RUNS_FAIL_CLOSED=PASS');
