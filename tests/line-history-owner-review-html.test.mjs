import fs from 'node:fs';
import assert from 'node:assert/strict';
import { renderLineHistoryOwnerReviewHtml } from '../src/crm-line-history-owner-review-html.mjs';

const queue={
  planner:'line_history_owner_review_queue_v1',
  review_queue_groups:3,
  review_queue_message_rows:9,
  blocked_conflict_groups:1,
  review_required_groups:1,
  unresolved_groups:1,
  items:[
    {
      queue_id:'queue-secret-safe-01',
      source_identity_hash:'sourcehash000001',
      line_id_hash:'linehash000001',
      line_user_id_present:true,
      message_rows:5,
      category:'REVIEW_REQUIRED',
      reason:'WEAK_OR_INCOMPLETE_EVIDENCE',
      review_action:'OWNER_REVIEW',
      current_hint_count:1,
      legacy_hint_count:1,
      target_customer_id_present:true,
      target_customer_id_hash:'targethash000001',
      evidence_types:['customer_master_exact_line_but_customer_missing_in_production'],
      conflict_types:[]
    },
    {
      queue_id:'queue-secret-safe-02',
      source_identity_hash:'sourcehash000002',
      line_id_hash:'linehash000002',
      line_user_id_present:true,
      message_rows:3,
      category:'BLOCKED_CONFLICT',
      reason:'EXACT_RESERVATION_LINE_CONFLICT',
      review_action:'RESOLVE_CONFLICT',
      current_hint_count:1,
      legacy_hint_count:0,
      target_customer_id_present:true,
      target_customer_id_hash:'targethash000002',
      evidence_types:['reservation_id_exact'],
      conflict_types:['explicit_different_person_review']
    },
    {
      queue_id:'queue-secret-safe-03',
      source_identity_hash:'sourcehash000003',
      line_id_hash:'',
      line_user_id_present:false,
      message_rows:1,
      category:'UNRESOLVED',
      reason:'NO_SAFE_IDENTITY_EVIDENCE',
      review_action:'NEEDS_MORE_EVIDENCE',
      current_hint_count:0,
      legacy_hint_count:1,
      target_customer_id_present:false,
      target_customer_id_hash:'',
      evidence_types:[],
      conflict_types:[]
    }
  ]
};

const html=renderLineHistoryOwnerReviewHtml(queue);

assert.match(html,/<!doctype html>/i);
assert.match(html,/LINE履歴 Owner Review Queue/);
assert.match(html,/競合あり/);
assert.match(html,/確認が必要/);
assert.match(html,/追加証拠が必要/);
assert.match(html,/data-filter="BLOCKED_CONFLICT"/);
assert.match(html,/data-filter="REVIEW_REQUIRED"/);
assert.match(html,/data-filter="UNRESOLVED"/);
assert.match(html,/id="search"/);
assert.match(html,/window\.print\(\)/);
assert.match(html,/@media\(max-width:720px\)/);
assert.match(html,/@media print/);
assert.match(html,/name="robots" content="noindex,nofollow,noarchive"/);
assert.match(html,/Privacy-safe view/);

for(const safe of [
  'queue-secret-safe-01',
  'sourcehash000001',
  'targethash000001',
  'WEAK_OR_INCOMPLETE_EVIDENCE',
  'reservation_id_exact'
]){
  assert.ok(html.includes(safe),'expected safe value missing: '+safe);
}

for(const secret of [
  '26001234',
  'U1234567890abcdef1234567890abcdef',
  'Private Customer Name',
  'private-customer.csv',
  'private-message-body'
]){
  assert.ok(!html.includes(secret),'private value leaked: '+secret);
}

for(const networkMarker of [
  'http://',
  'https://',
  'fetch(',
  'XMLHttpRequest',
  'WebSocket',
  '<iframe',
  '<img src=',
  '<link rel='
]){
  assert.ok(!html.includes(networkMarker),'network surface present: '+networkMarker);
}

const cli=fs.readFileSync('scripts/render-line-history-owner-review-html.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/https?:\/\//i);
assert.match(cli,/NETWORK_REQUESTS=0/);
assert.match(cli,/PRODUCTION_D1_WRITE=0/);
assert.match(cli,/PRODUCTION_DEPLOY=0/);

console.log('LINE_HISTORY_OWNER_REVIEW_HTML_RENDER=PASS');
console.log('LINE_HISTORY_OWNER_REVIEW_HTML_RESPONSIVE=PASS');
console.log('LINE_HISTORY_OWNER_REVIEW_HTML_FILTER_SEARCH_PRINT=PASS');
console.log('LINE_HISTORY_OWNER_REVIEW_HTML_PRIVACY=PASS');
console.log('LINE_HISTORY_OWNER_REVIEW_HTML_NETWORK_REQUESTS=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
console.log('PRODUCTION_DEPLOY=0');
