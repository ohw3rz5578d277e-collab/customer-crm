import fs from 'node:fs';
import { injectCustomer360Marketing } from '../src/crm-customer360-ui.mjs';
import { injectMobileSearchDetailDensity, mobileSearchDetailDensityContract } from '../src/crm-mobile-search-detail-density.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(msg,ok){assert(ok,msg);console.log(`PASS ${++n}: ${msg}`)}

const shell='<!doctype html><html><head><meta charset="utf-8"></head><body><main class="app"></main></body></html>';
const marketing=injectCustomer360Marketing(shell);
const dense=injectMobileSearchDetailDensity(marketing);

pass('density adapter applies only after Customer360 marketing',dense!==marketing&&dense.includes('crm-mobile-search-detail-density-style'));
pass('adapter is idempotent',injectMobileSearchDetailDensity(dense)===dense);
pass('mobile search header is compact',dense.includes('.crm-search-head{padding:10px!important')&&dense.includes('margin-bottom:8px!important'));
pass('search input remains a 44px minimum touch target',dense.includes('.crm-search-box input{min-height:44px!important'));
pass('search clear action remains at least 44px',dense.includes('.crm-search-clear{right:8px!important;min-height:44px!important;min-width:44px!important'));
pass('detail sheet uses tighter mobile inset and padding',dense.includes('.crm-detail{inset:6px!important;border-radius:16px!important;padding:10px!important}'));
pass('detail close control remains 44px square minimum',dense.includes('.crm-detail-close{width:44px!important;height:44px!important;min-width:44px!important;min-height:44px!important}'));
pass('Customer360 profile typography is compact but readable',dense.includes('.crm-360-profile-main h2{font-size:22px!important;line-height:1.25!important}'));
pass('Customer360 cards and sections use compact spacing',dense.includes('.crm-360-card,.crm-section{padding:10px!important;border-radius:14px!important}'));
pass('Customer360 action buttons remain 44px minimum',dense.includes('.crm-360-actions .crm-mkt-btn{min-height:44px!important'));
pass('summary rows are denser',dense.includes('grid-template-columns:82px minmax(0,1fr)!important')&&dense.includes('padding:5px 0!important'));
pass('contract forbids production/customer/LINE mutation',mobileSearchDetailDensityContract.production_write===false&&mobileSearchDetailDensityContract.customer_mutation===false&&mobileSearchDetailDensityContract.line_send===false);

const src=fs.readFileSync('src/crm-mobile-search-detail-density.mjs','utf8');
pass('density adapter has no direct network access',!/\bfetch\s*\(|XMLHttpRequest|WebSocket/i.test(src));
pass('density adapter contains no customer mutation SQL',!/\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+customers\b/i.test(src));
pass('density adapter contains no LINE send controls',!/broadcast|multicast|pushMessage|replyMessage/i.test(src));

console.log(`CRM_MOBILE_SEARCH_DETAIL_DENSITY=${n}/${n} PASS`);
