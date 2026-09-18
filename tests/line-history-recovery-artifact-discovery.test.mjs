import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import { discoverLineHistoryRecoveryArtifacts } from '../src/crm-line-history-recovery-discovery.mjs';

const root=fs.mkdtempSync(path.join(os.tmpdir(),'crm-line-discovery-'));
try{
  const oldDir=path.join(root,'customer-crm-old');
  const newDir=path.join(root,'customer-crm-new');
  fs.mkdirSync(oldDir,{recursive:true});
  fs.mkdirSync(newDir,{recursive:true});

  const candidates=[
    {message_key:'m1',line_user_id:'U1234567890abcdef1234567890abcdef',message_text:'private'}
  ];
  fs.writeFileSync(path.join(oldDir,'candidate-snapshot.json'),JSON.stringify(candidates));
  fs.writeFileSync(path.join(newDir,'candidate-snapshot.json'),JSON.stringify(candidates));

  const master=[
    {customer_id:'C-ONE',line_user_id:'U1234567890abcdef1234567890abcdef',name:'Private'}
  ];
  fs.writeFileSync(path.join(newDir,'customer-master-20260918.json'),JSON.stringify(master));

  const resume=path.join(newDir,'resume');
  fs.mkdirSync(path.join(resume,'baseline'),{recursive:true});
  fs.writeFileSync(path.join(resume,'final-triage.json'),JSON.stringify({classifications:[]}));
  fs.writeFileSync(path.join(resume,'owner-review-queue.json'),JSON.stringify({review_queue_groups:1,items:[]}));
  fs.writeFileSync(path.join(resume,'baseline','customers.json'),JSON.stringify([]));

  fs.writeFileSync(
    path.join(resume,'line-history-owner-review-decisions.json'),
    JSON.stringify({decisions:[{queue_id:'q1',decision:'DEFERRED'}]})
  );

  const preauth=path.join(resume,'owner-backfill-preauth');
  fs.mkdirSync(path.join(preauth,'readonly-preview'),{recursive:true});
  fs.writeFileSync(path.join(preauth,'decision-plan-private.json'),JSON.stringify({planner:'line_history_owner_decision_plan_v1'}));
  fs.writeFileSync(path.join(preauth,'readonly-preview','owner-backfill-preview-summary.json'),JSON.stringify({preview_ready:true}));

  const d1=path.join(preauth,'readonly-preview','d1-readonly-result');
  fs.mkdirSync(d1,{recursive:true});
  fs.writeFileSync(
    path.join(d1,'write-authorization-packet.json'),
    JSON.stringify({
      planner:'line_history_owner_write_authorization_packet_v2',
      packet_ready:true,
      authorization_required:true
    })
  );

  fs.writeFileSync(path.join(d1,'owner-exact-approval.txt'),'SHOULD NOT BE DISCOVERED');

  const now=Date.now()/1000;
  fs.utimesSync(path.join(oldDir,'candidate-snapshot.json'),now-100,now-100);
  fs.utimesSync(path.join(newDir,'candidate-snapshot.json'),now,now);

  const found=discoverLineHistoryRecoveryArtifacts({roots:[root]});

  assert.equal(found.planner,'line_history_recovery_artifact_discovery_v1');
  assert.equal(found.candidates.count,2);
  assert.equal(found.candidates.equivalent_count,2);
  assert.equal(found.candidates.ambiguous,false);
  assert.equal(found.candidates.path,path.join(newDir,'candidate-snapshot.json'));

  assert.equal(found.customer_master.path,path.join(newDir,'customer-master-20260918.json'));
  assert.equal(found.resume_dir.path,resume);
  assert.equal(found.decisions.path,path.join(resume,'line-history-owner-review-decisions.json'));
  assert.equal(found.preauth_dir.path,preauth);
  assert.equal(found.d1_preview_dir.path,d1);

  assert.equal(found.approval_file.path,'');
  assert.equal(found.approval_file.intentionally_not_discovered,true);
  assert.equal(found.safety.approval_file_auto_discovery,false);
  assert.equal(found.safety.local_filesystem_read_only,true);
  assert.equal(found.safety.command_execution,false);
  assert.equal(found.safety.production_d1_write,0);

  const different=path.join(root,'other');
  fs.mkdirSync(different,{recursive:true});
  fs.writeFileSync(path.join(different,'candidate-snapshot.json'),JSON.stringify([{message_key:'different'}]));
  fs.utimesSync(path.join(different,'candidate-snapshot.json'),now+50,now+50);

  const ambiguous=discoverLineHistoryRecoveryArtifacts({roots:[root]});
  assert.equal(ambiguous.candidates.count,3);
  assert.equal(ambiguous.candidates.ambiguous,true);
  assert.equal(ambiguous.candidates.path,'');

  const cli=fs.readFileSync('scripts/discover-line-history-recovery-artifacts.mjs','utf8');
  assert.doesNotMatch(cli,/child_process/i);
  assert.doesNotMatch(cli,/\bwrangler\b/i);
  assert.doesNotMatch(cli,/d1\s+execute/i);
  assert.doesNotMatch(cli,/https?:\/\//i);
  assert.match(cli,/APPROVAL_FILE_AUTO_DISCOVERY=NO/);
  assert.match(cli,/PRIVATE_FILE_CONTENT_PRINTED=0/);
  assert.match(cli,/COMMAND_EXECUTION=0/);

  const operator=fs.readFileSync('scripts/run-line-history-recovery-operator.sh','utf8');
  assert.match(operator,/--auto-discover/);
  assert.match(operator,/discover-line-history-recovery-artifacts\.mjs/);
  assert.match(operator,/APPROVAL_FILE_AUTO_DISCOVERY=NO/);
  assert.match(operator,/STOP_AUTO_DISCOVER_STATUS_ONLY/);
  assert.match(operator,/\[ -z "\$CANDIDATES" \] && CANDIDATES=/);
  assert.match(operator,/\[ -z "\$D1_PREVIEW_DIR" \] && D1_PREVIEW_DIR=/);
  assert.doesNotMatch(operator,/\[ -z "\$APPROVAL_FILE" \] && APPROVAL_FILE=/);

  console.log('LINE_HISTORY_DISCOVERY_CANDIDATE_EQUIVALENCE=PASS');
  console.log('LINE_HISTORY_DISCOVERY_RECOVERY_LINEAGE=PASS');
  console.log('LINE_HISTORY_DISCOVERY_APPROVAL_EXCLUDED=PASS');
  console.log('LINE_HISTORY_DISCOVERY_LOCAL_READONLY=PASS');
  console.log('LINE_HISTORY_DISCOVERY_OPERATOR_STATUS_ONLY=PASS');
}finally{
  fs.rmSync(root,{recursive:true,force:true});
}
