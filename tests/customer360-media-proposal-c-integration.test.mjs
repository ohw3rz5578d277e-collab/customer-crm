import fs from 'node:fs';
import assert from 'node:assert/strict';

const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const shell=fs.readFileSync('src/crm-owner-app-shell.mjs','utf8');
const proposal=fs.readFileSync('src/crm-analysis-approach-proposal-c.mjs','utf8');
const mediaUi=fs.readFileSync('src/crm-customer360-media-ui.mjs','utf8');
const editHandoff=fs.readFileSync('src/crm-customer360-exact-edit-handoff.mjs','utf8');

function occurrences(source,needle){return source.split(needle).length-1}

assert.equal(occurrences(entry,"./crm-customer360-media-ui.mjs"),1,'media UI import must be single-owner');
assert.equal(occurrences(entry,'injectCustomer360MediaUi'),2,'media UI must be imported and composed exactly once');
assert.equal(occurrences(entry,"./crm-customer360-exact-edit-handoff.mjs"),1,'exact edit handoff import must be single-owner');
assert.equal(occurrences(entry,'injectCustomer360ExactEditHandoff'),2,'exact edit handoff must be imported and composed exactly once');
const mediaPos=entry.indexOf('injectCustomer360MediaUi(withProfile)');
const editPos=entry.indexOf('injectCustomer360ExactEditHandoff(withMedia)');
const shellPos=entry.indexOf('injectOwnerAppShell(withEditHandoff)');
assert.ok(mediaPos>=0&&editPos>mediaPos&&shellPos>editPos,'composition must remain profile -> media -> exact edit handoff -> Owner shell so Proposal C wraps the final surface');
assert.match(shell,/injectProposalCAnalysisApproach/,'Proposal C shell integration missing');
assert.equal(occurrences(shell,"./crm-analysis-approach-proposal-c.mjs"),1,'Proposal C import duplicated');
assert.match(proposal,/automatic_line_send:false/,'Proposal C must keep automatic LINE send disabled');
assert.match(proposal,/owner_review_required:true/,'Proposal C must require Owner review');
assert.doesNotMatch(proposal,/MutationObserver\(/,'Proposal C must not introduce MutationObserver');
assert.match(mediaUi,/window\.__crmCustomerMediaUi20260908/,'media UI single-owner marker missing');
assert.doesNotMatch(mediaUi,/document\.documentElement/,'media UI must not observe document root');
assert.match(editHandoff,/customer360_exact_edit_handoff_direct_write:false/,'exact edit handoff must remain navigation-only');
assert.doesNotMatch(editHandoff,/fetch\s*\(/,'exact edit handoff must not add direct write/read fetches');

console.log('CUSTOMER360_MEDIA_PROPOSAL_C_INTEGRATION=PASS');
console.log('CUSTOMER360_MEDIA_PROPOSAL_C_AUTO_LINE_SEND=0');
console.log('CUSTOMER360_EXACT_EDIT_HANDOFF_DIRECT_WRITE=0');
