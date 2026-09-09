import fs from 'node:fs';
import assert from 'node:assert/strict';

const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const shell=fs.readFileSync('src/crm-owner-app-shell.mjs','utf8');
const proposal=fs.readFileSync('src/crm-analysis-approach-proposal-c.mjs','utf8');
const mediaUi=fs.readFileSync('src/crm-customer360-media-ui.mjs','utf8');

function occurrences(source,needle){return source.split(needle).length-1}

assert.equal(occurrences(entry,"./crm-customer360-media-ui.mjs"),1,'media UI import must be single-owner');
assert.equal(occurrences(entry,'injectCustomer360MediaUi'),2,'media UI must be imported and composed exactly once');
assert.ok(entry.indexOf('injectCustomer360MediaUi(withProfile)')<entry.indexOf('injectOwnerAppShell(withMedia)'),'media must compose before Owner shell so Proposal C can wrap the final surface');
assert.match(shell,/injectProposalCAnalysisApproach/,'Proposal C shell integration missing');
assert.equal(occurrences(shell,"./crm-analysis-approach-proposal-c.mjs"),1,'Proposal C import duplicated');
assert.match(proposal,/automatic_line_send:false/,'Proposal C must keep automatic LINE send disabled');
assert.match(proposal,/owner_review_required:true/,'Proposal C must require Owner review');
assert.doesNotMatch(proposal,/MutationObserver\(/,'Proposal C must not introduce MutationObserver');
assert.match(mediaUi,/window\.__crmCustomerMediaUi20260908/,'media UI single-owner marker missing');
assert.doesNotMatch(mediaUi,/document\.documentElement/,'media UI must not observe document root');

console.log('CUSTOMER360_MEDIA_PROPOSAL_C_INTEGRATION=PASS');
console.log('CUSTOMER360_MEDIA_PROPOSAL_C_AUTO_LINE_SEND=0');
