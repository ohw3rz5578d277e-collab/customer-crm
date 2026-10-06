import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-r2-bucket-create.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-r2-bucket-create-from-issue.yml','utf8');

for(const marker of [
  'source_comment_id:',
  'issues: read',
  'Verify exact Issue #26 Owner authorization receipt',
  'OWNER_RECEIPT_NOT_ISSUE_26',
  'OWNER_RECEIPT_AUTHOR_MISMATCH',
  'OWNER_RECEIPT_BODY_MISMATCH',
  'ISSUE_26_OWNER_AUTHORIZATION_RECEIPT=PASS',
  'Reconfirm main and active Worker immediately before mutation',
  'PRE_MUTATION_MAIN_DRIFT',
  'PRE_MUTATION_ACTIVE_WORKER_DRIFT',
  'PRE_MUTATION_MAIN_DRIFT=NO',
  'PRE_MUTATION_ACTIVE_WORKER_DRIFT=NO'
]) assert.ok(workflow.includes(marker),`workflow missing hardening marker: ${marker}`);

for(const marker of [
  'SOURCE_COMMENT_ID: ${{ github.event.comment.id }}',
  'source_comment_id=$SOURCE_COMMENT_ID',
  "'source_comment_id':os.environ['SOURCE_COMMENT_ID']",
  'DISPATCHED_SOURCE_COMMENT_ID=$SOURCE_COMMENT_ID'
]) assert.ok(bridge.includes(marker),`bridge missing source receipt marker: ${marker}`);

const sourceIdValidationIndex=bridge.indexOf('[[ "$SOURCE_COMMENT_ID" =~ ^[0-9]+$ ]]');
const commandMatchIndex=bridge.indexOf('[[ "$COMMAND_BODY" =~ $command_re ]]');
const commandCaptureIndex=bridge.indexOf('expected_sha="${BASH_REMATCH[1]}"');
assert.ok(sourceIdValidationIndex>=0,'source comment id validation missing');
assert.ok(commandMatchIndex>sourceIdValidationIndex,'source comment id must be validated before command regex so BASH_REMATCH captures stay intact');
assert.ok(commandCaptureIndex>commandMatchIndex,'command captures must be read immediately after the command regex');

const receiptIndex=workflow.indexOf('Verify exact Issue #26 Owner authorization receipt');
const preflightIndex=workflow.indexOf('Preflight canonical bucket absence');
const preMutationIndex=workflow.indexOf('Reconfirm main and active Worker immediately before mutation');
const postIndex=workflow.indexOf('--request POST');
assert.ok(receiptIndex>=0 && preflightIndex>receiptIndex,'Issue receipt must be verified before R2 preflight');
assert.ok(preMutationIndex>preflightIndex,'pre-mutation drift gate must follow bucket absence preflight');
assert.ok(postIndex>preMutationIndex,'bucket POST must occur only after pre-mutation drift gate');
assert.equal((workflow.match(/--request POST/g)||[]).length,1,'exactly one bucket-create POST is permitted');

console.log('MEMBER_R2_BUCKET_CREATE_HARDENING=PASS');
console.log('ISSUE_26_RECEIPT_REQUIRED=YES');
console.log('PRE_MUTATION_DRIFT_RECHECK=YES');
console.log('BRIDGE_BASH_REMATCH_CAPTURE_ORDER=PASS');
