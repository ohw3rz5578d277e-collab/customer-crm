import assert from 'node:assert/strict';
import fs from 'node:fs';

const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-runtime-version-promotion-from-issue.yml','utf8');

assert.ok(bridge.includes('PRODUCTION_MUTATION_FILTER=ACTUAL_MUTATIONS_ONLY'));
assert.ok(bridge.includes("title=='Production bridge: ignored'"));
assert.ok(bridge.includes("title.startswith('Production read-only bridge:')"));
assert.ok(bridge.includes("return title.startswith('Production mutation bridge:') or event=='issue_comment'"));
assert.ok(bridge.includes("if int(run.get('id',0))==me:"));
assert.ok(bridge.includes("return event=='workflow_dispatch'"));
assert.ok(bridge.includes('sleep 2'));
assert.ok(bridge.includes('PRODUCTION_MUTATION_BUSY_RETRY_REQUIRED'));
assert.ok(bridge.includes('PRODUCTION_MUTATION_QUEUE_POLICY=REJECT_AND_RETRY'));
assert.ok(bridge.includes('PRODUCTION_MUTATION_ACTIVE_RUN_PAGE_FULL_FAIL_CLOSED'));
assert.ok(bridge.includes('PRODUCTION_MUTATION_QUERY_DRIFT_FAIL_CLOSED=PASS'));
assert.ok(bridge.includes('PRODUCTION_MUTATION_PAGINATION_FAIL_CLOSED=PASS'));

const busyStart=bridge.indexOf('- name: Reject instead of queueing behind another Production mutation');
const dispatchStart=bridge.indexOf('- name: Dispatch exact-version canonical promotion workflow');
assert.ok(busyStart>=0 && dispatchStart>busyStart);
const busy=bridge.slice(busyStart,dispatchStart);
assert.ok(busy.includes("workflow.startswith('dispatch-')"));
assert.ok(busy.includes("title=='Production bridge: ignored'"));
assert.ok(busy.includes("title.startswith('Production mutation bridge:')"));
assert.ok(busy.includes('curl --fail --silent --show-error'));
assert.ok(!busy.includes('if [[ "$code" == 404 ]]'));
assert.ok(busy.includes('if len(runs) >= 100:'));
assert.ok(busy.includes("raise SystemExit('PRODUCTION_MUTATION_ACTIVE_RUN_PAGE_FULL_FAIL_CLOSED')"));

assert.doesNotMatch(bridge,/wrangler versions deploy/);
assert.doesNotMatch(bridge,/deploy-cloudflare\.yml\/dispatches/);
assert.doesNotMatch(bridge,/member-production-runtime-secret-stage\.yml\/dispatches/);
assert.match(bridge,/member-production-runtime-version-promotion\.yml\/dispatches/);

console.log('MEMBER_RUNTIME_PROMOTION_ACTUAL_MUTATION_FILTER=PASS');
console.log('MEMBER_RUNTIME_PROMOTION_IGNORED_LISTENER_SELF_COLLISION=BLOCKED');
console.log('MEMBER_RUNTIME_PROMOTION_QUERY_DRIFT_FAIL_CLOSED=PASS');
console.log('MEMBER_RUNTIME_PROMOTION_ACTIVE_RUN_PAGE_FULL_FAIL_CLOSED=PASS');
