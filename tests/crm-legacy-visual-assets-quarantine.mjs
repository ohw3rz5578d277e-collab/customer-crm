import assert from 'node:assert/strict';
import {
  LEGACY_OWNER_VISUAL_ASSET_IDS,
  stripLegacyOwnerVisualAssets,
  stripLegacyBaseAdminUi,
  composeCustomer360AdminHtml
} from '../src/production-index-crm-customer360-entry.js';

const legacyAssets=LEGACY_OWNER_VISUAL_ASSET_IDS.map(id =>
  '<style id="'+id+'">#legacy{display:block}</style><script id="'+id+'">window.__legacyVisual=1</script>'
).join('');

const source='<!doctype html><html><head>'+
  '<style id="crm-production-safe-controls">.safe{display:none}</style>'+
  legacyAssets+
  '</head><body><div class="app"><main>base</main></div>'+
  '<a id="crmReconciliationLink" href="/admin/customer-id-reconciliation">legacy review</a>'+
  '</body></html>';

const stripped=stripLegacyOwnerVisualAssets(source);
for(const id of LEGACY_OWNER_VISUAL_ASSET_IDS){
  assert.equal(stripped.includes('id="'+id+'"'),false,'legacy asset survived: '+id);
}
assert.equal(stripped.includes('crmReconciliationLink'),false,'legacy reconciliation floating link survived');
assert.equal(stripped.includes('crm-production-safe-controls'),true,'production safety controls style must be preserved');
assert.equal(stripped.includes('<div class="app">'),true,'base application host must be preserved');

const legacyBase='<!doctype html><html><head>'+
  '<style>:root{--danger:#dc2626}.tablewrap{overflow:auto}.filter-modal{display:none}.rank-row{display:grid}</style>'+
  '<style id="crm-production-safe-controls">.safe{display:none}</style>'+
  '</head><body><div class="app">'+
  '<div class="header"><button id="deleteTestBtn">テスト顧客削除</button></div>'+
  '<div id="status">loading</div><div id="summary"></div><div class="marketing"></div>'+
  '<div class="card"><table><tbody id="tbody"></tbody></table></div></div>'+
  '<div class="modal-bg" id="modalBg"></div><div class="modal" id="modal"></div><div class="filter-modal" id="filterModal"></div>'+
  '<script>(function(){function deleteTest(){return fetch("/api/customers/delete-test",{method:"POST"})}document.getElementById("deleteTestBtn").onclick=deleteTest})();</script>'+
  '</body></html>';
const baseStripped=stripLegacyBaseAdminUi(legacyBase);
assert.equal(baseStripped.includes('id="deleteTestBtn"'),false,'legacy delete-test control survived');
assert.equal(baseStripped.includes('/api/customers/delete-test'),false,'legacy delete-test POST handler survived');
assert.equal(baseStripped.includes('id="status"'),false,'legacy status DOM survived');
assert.equal(baseStripped.includes('id="summary"'),false,'legacy summary DOM survived');
assert.equal(baseStripped.includes('id="tbody"'),false,'legacy customer table DOM survived');
assert.equal(baseStripped.includes('.tablewrap'),false,'legacy base admin stylesheet survived');
assert.equal(baseStripped.includes('crm-production-safe-controls'),true,'safe controls stylesheet must survive base-admin stripping');
assert.equal(baseStripped.includes('<div class="app"></div>'),true,'empty canonical host must remain');

const baseComposed=composeCustomer360AdminHtml(legacyBase);
assert.equal(baseComposed.includes('id="deleteTestBtn"'),false,'legacy delete-test control reappeared after canonical composition');
assert.equal(baseComposed.includes('/api/customers/delete-test'),false,'legacy delete-test POST handler reappeared after canonical composition');
assert.equal(baseComposed.includes('crm-owner-app-shell-script'),true,'canonical shell missing after base-admin stripping');
assert.equal(baseComposed.includes('crm-customer360-marketing-script'),true,'Customer360 marketing/list UI missing after base-admin stripping');
assert.equal(baseComposed.includes('crm-production-safe-controls'),true,'safe controls lost after canonical composition');

const composed=composeCustomer360AdminHtml(source);
for(const id of LEGACY_OWNER_VISUAL_ASSET_IDS){
  assert.equal(composed.includes('id="'+id+'"'),false,'legacy asset reappeared after canonical composition: '+id);
}
assert.equal(/<a\\b[^>]*\\bid=["']crmReconciliationLink["']/i.test(composed),false,'legacy reconciliation link DOM reappeared');
assert.equal(composed.includes('crm-owner-app-shell-script'),true,'canonical Owner App Shell missing');
assert.equal(composed.includes('crm-owner-view-state-v2-script'),true,'canonical view state missing');
assert.equal(composed.includes('crm-owner-line-chat-script'),true,'canonical LINE chat missing');
assert.equal(composed.includes('crm-customer-csv-import-script'),true,'canonical CSV import missing');
assert.equal(composed.includes('crm-production-safe-controls'),true,'production safety controls style removed by composition');

console.log('CRM_LEGACY_VISUAL_ASSETS_QUARANTINE=PASS');
console.log('LEGACY_VISUAL_ASSET_COUNT='+LEGACY_OWNER_VISUAL_ASSET_IDS.length);
console.log('PRODUCTION_WRITE=0');
console.log('LINE_SEND=0');
