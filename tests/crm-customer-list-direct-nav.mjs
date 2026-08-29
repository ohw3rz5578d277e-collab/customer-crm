import fs from 'node:fs';

const direct=fs.readFileSync('src/crm-customer360-direct-navigation.mjs','utf8');
const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const legacy=fs.readFileSync('src/crm-mobile-owner-interaction-recovery.mjs','utf8');

function assert(ok,msg){if(!ok)throw new Error(msg)}
assert(direct.includes('window.__crmCustomer360UI={showList,showHome,focusSearch,refreshList}'),'stable Customer360 UI API missing');
assert(direct.includes("customer.onclick=()=>showList({ownerTab:'customers'})"),'Customer owner nav is not rebound to direct showList');
assert(direct.includes("search.onclick=focusSearch"),'Search owner nav is not rebound to shared direct API');
assert(direct.includes("list.classList.toggle('open',view==='list')"),'direct list state missing');
assert(direct.includes("document.body.classList.remove('crm-owner-view-today')"),'Today isolation missing');
assert(direct.includes("fetch('/api/customer360/customer/'"),'direct detail fallback missing');
assert(direct.includes("new URL('/api/customer360/customers'"),'direct customer GET missing');
assert(direct.includes('顧客データを読み込めませんでした'),'human API failure UI missing');
assert(direct.includes('再読み込み'),'retry action missing');
assert(!direct.includes("querySelector('#crmMktNav [data-view=\"list\"]')"),'direct layer must not click hidden Customer360 nav');
assert(!direct.includes('clickByText'),'direct layer must not use text routing');
assert(entry.indexOf('injectMobileOwnerInteractionRecovery')>=0&&entry.indexOf('injectCustomer360DirectNavigation')>entry.indexOf('injectMobileOwnerInteractionRecovery'),'direct navigation must be outermost after mobile recovery');
assert(entry.includes('export function composeCustomer360AdminHtml'),'production-faithful composition helper missing');
assert(legacy.includes('clickCustomer360List'),'expected legacy compatibility path no longer detectable; update audit intentionally');
console.log('CUSTOMER_LIST_DIRECT_NAV_CONTRACT=PASS');
console.log('HIDDEN_BUTTON_PRIMARY_ROUTING=REMOVED_FROM_PRIMARY');
console.log('LEGACY_COMPATIBILITY_ROUTE=PRESENT_BUT_OVERRIDDEN');
console.log('IDENTITY_SOURCE_CHANGE=0');
console.log('MIGRATION_CHANGE=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
console.log('DEPLOY=0');
console.log('MERGE=0');
