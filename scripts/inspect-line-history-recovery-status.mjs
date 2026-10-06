import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { resolveLineHistoryRecoveryNextPhase } from '../src/crm-line-history-recovery-next-phase.mjs';
import { buildLineHistoryRecoveryNextCommand } from '../src/crm-line-history-recovery-next-command.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}
function exists(p){return !!p&&fs.existsSync(p)}
function readJson(p){
  if(!exists(p))return null;
  try{return JSON.parse(fs.readFileSync(p,'utf8'))}catch{return null}
}
function fileSha256(p){
  if(!exists(p))return '';
  return createHash('sha256').update(fs.readFileSync(p)).digest('hex');
}

const candidates=arg('--candidates');
const customerMaster=arg('--customer-master');
const resumeDir=arg('--resume-dir');
const decisions=arg('--decisions');
const preauthDir=arg('--preauth-dir');
const d1PreviewDir=arg('--d1-preview-dir');
const approvalFile=arg('--approval-file');
const completionReceiptArg=arg('--completion-receipt');
const mainSha=arg('--main-sha');

const resumeTriage=resumeDir?path.join(resumeDir,'final-triage.json'):'';
const reviewQueue=resumeDir?path.join(resumeDir,'owner-review-queue.json'):'';
const preauthSummary=preauthDir?path.join(preauthDir,'readonly-preview','owner-backfill-preview-summary.json'):'';
const noWriteReceipt=preauthDir?path.join(preauthDir,'no-write-completion-receipt.json'):'';
const packetPath=d1PreviewDir?path.join(d1PreviewDir,'write-authorization-packet.json'):'';
const approvedWriteDir=d1PreviewDir?path.join(d1PreviewDir,'approved-insert-run'):'';
const approvedWriteReceipt=approvedWriteDir?path.join(approvedWriteDir,'completion-receipt.json'):'';
const approvedInsertManifest=approvedWriteDir?path.join(approvedWriteDir,'approved-insert-manifest.json'):'';
const approvedInsertSql=approvedWriteDir?path.join(approvedWriteDir,'approved-insert.sql'):'';
const writeResultPath=approvedWriteDir?path.join(approvedWriteDir,'write-result.json'):'';
const postPreviewResultPath=approvedWriteDir?path.join(approvedWriteDir,'post-preview-result.json'):'';
const inferredReceipt=exists(approvedWriteReceipt)
  ?approvedWriteReceipt
  :exists(noWriteReceipt)
    ?noWriteReceipt
    :'';
const completionReceiptPath=completionReceiptArg||inferredReceipt;

const queue=readJson(reviewQueue);
const preauthSummaryJson=readJson(preauthSummary);
const packet=readJson(packetPath);
const receipt=readJson(completionReceiptPath);
const completionReceiptArtifactDigests=
  receipt?.planner==='line_history_owner_write_completion_receipt_v1'&&d1PreviewDir
    ?{
      packet_sha256:fileSha256(packetPath),
      insert_manifest_sha256:fileSha256(approvedInsertManifest),
      write_result_sha256:fileSha256(writeResultPath),
      post_preview_result_sha256:fileSha256(postPreviewResultPath),
      approved_insert_sql_sha256:fileSha256(approvedInsertSql)
    }
    :null;

const result=resolveLineHistoryRecoveryNextPhase({
  candidatesPresent:exists(candidates),
  customerMasterPresent:exists(customerMaster),
  resumeReady:exists(resumeTriage),
  reviewQueueGroups:queue?Number(queue.review_queue_groups||0):null,
  decisionsPresent:exists(decisions),
  preauthPreviewReady:!!(preauthSummaryJson&&preauthSummaryJson.preview_ready===true&&Number(preauthSummaryJson.validation_error_count||0)===0),
  d1Packet:packet,
  approvalFilePresent:exists(approvalFile),
  completionReceipt:receipt,
  completionReceiptArtifactDigests,
  currentMainSha:mainSha
});

const nextCommand=buildLineHistoryRecoveryNextCommand({
  stage:result.stage,
  candidates,
  customerMaster,
  resumeDir,
  decisions,
  preauthDir,
  d1PreviewDir,
  approvalFile
});

console.log('RESULT=LINE_HISTORY_RECOVERY_NEXT_PHASE');
console.log('STAGE='+result.stage);
console.log('NEXT_PHASE='+result.next_phase);
console.log('NEXT_ACTION='+result.next_action);
console.log('PRODUCTION_WRITE_POSSIBLE='+(result.production_write_possible?'YES':'NO'));
console.log('NEXT_COMMAND_READY='+(nextCommand.ready?'YES':'NO'));
console.log('NEXT_COMMAND_REASON='+nextCommand.reason);
console.log('NEXT_COMMAND_PRODUCTION_WRITE='+(nextCommand.production_write?'YES':'NO'));
console.log('NEXT_COMMAND_REQUIRES_OWNER_APPROVAL='+(nextCommand.requires_owner_approval?'YES':'NO'));
if(nextCommand.command)console.log('NEXT_COMMAND='+nextCommand.command);
console.log('PRIVATE_VALUES_PRINTED_TO_TERMINAL=0');
