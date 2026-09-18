import fs from 'node:fs';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { renderLineHistoryOwnerPrivateReviewHtml } from '../src/crm-line-history-owner-private-review.mjs';

const rawLine='Uabcdefabcdefabcdefabcdefabcdef12';
const lineHash=createHash('sha256').update(rawLine).digest('hex').slice(0,16);

const triage={
  candidate_message_rows:6,
  classifications:[
    {
      line_id_hash:lineHash,
      line_user_id_present:true,
      message_rows:4,
      customer_id_hints:[],
      legacy_customer_id_hints:['C-PRIVATE-001'],
      csv_file_names:['private-file.csv'],
      category:'REVIEW_REQUIRED',
      reason:'WEAK_OR_INCOMPLETE_EVIDENCE',
      target_customer_id:'26001234',
      evidence:['reservation_id_exact'],
      conflicts:[]
    },
    {
      line_id_hash:'',
      line_user_id_present:false,
      message_rows:2,
      customer_id_hints:[],
      legacy_customer_id_hints:['C-PRIVATE-002'],
      category:'UNRESOLVED',
      reason:'NO_SAFE_IDENTITY_EVIDENCE',
      target_customer_id:'',
      evidence:[],
      conflicts:[]
    }
  ]
};

const customerMaster=[
  {
    customer_id:'C-PRIVATE-001',
    line_user_id:rawLine,
    name:'山田 太郎',
    line_name:'LINE太郎'
  },
  {
    customer_id:'C-PRIVATE-002',
    line_user_id:'',
    name:'佐藤 花子',
    line_name:''
  }
];

const customers=[
  {
    customer_id:'26001234',
    name:'山田 太郎 CRM',
    line_display_name:'LINE太郎 CRM',
    line_user_id:'',
    phone:'09000000000',
    email:'private@example.com',
    deleted_at:''
  }
];

const html=renderLineHistoryOwnerPrivateReviewHtml({triage,customerMaster,customers});

assert.match(html,/LINE履歴 Private Owner Review/);
assert.match(html,/山田 太郎/);
assert.match(html,/C-PRIVATE-001/);
assert.match(html,/26001234/);
assert.match(html,/山田 太郎 CRM/);
assert.match(html,/LINE太郎/);
assert.match(html,/電話<\/span><b>登録あり/);
assert.match(html,/メール<\/span><b>登録あり/);
assert.doesNotMatch(html,/09000000000/);
assert.doesNotMatch(html,/private@example\.com/);
assert.doesNotMatch(html,new RegExp(rawLine));

assert.match(html,/data-decision="SAME_PERSON"/);
assert.match(html,/data-decision="DIFFERENT_PERSON"/);
assert.match(html,/data-decision="DEFERRED"/);
assert.match(html,/data-decision="NEEDS_MORE_EVIDENCE"/);
assert.match(html,/line_history_owner_review_decisions_v1/);
assert.match(html,/new Blob/);
assert.match(html,/URL\.createObjectURL/);
assert.match(html,/line-history-owner-review-decisions\.json/);

assert.match(html,/@media\(max-width:760px\)/);
assert.match(html,/name="robots" content="noindex,nofollow,noarchive"/);
assert.match(html,/Private local file/);

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

const cli=fs.readFileSync('scripts/render-line-history-owner-private-review.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/https?:\/\//i);
assert.match(cli,/PRIVATE_DATA_OUTPUT=LOCAL_FILE_ONLY/);
assert.match(cli,/PRIVATE_DATA_PRINTED_TO_TERMINAL=0/);
assert.match(cli,/NETWORK_REQUESTS=0/);
assert.match(cli,/PRODUCTION_D1_WRITE=0/);
assert.match(cli,/PRODUCTION_DEPLOY=0/);

console.log('LINE_HISTORY_OWNER_PRIVATE_REVIEW_IDENTITIES=PASS');
console.log('LINE_HISTORY_OWNER_PRIVATE_REVIEW_PHONE_EMAIL_VALUES_HIDDEN=PASS');
console.log('LINE_HISTORY_OWNER_PRIVATE_REVIEW_DECISION_EXPORT=PASS');
console.log('LINE_HISTORY_OWNER_PRIVATE_REVIEW_RESPONSIVE=PASS');
console.log('LINE_HISTORY_OWNER_PRIVATE_REVIEW_NETWORK_REQUESTS=0');
console.log('PRIVATE_DATA_PRINTED_TO_TERMINAL=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
console.log('PRODUCTION_DEPLOY=0');
