import fs from 'node:fs';
import { injectCustomer360Marketing } from '../src/crm-customer360-ui.mjs';
import { injectCustomerListDailyOperations, customerListDailyOperationsContract } from '../src/crm-customer-list-daily-operations.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(msg,ok){assert(ok,msg);console.log(`PASS ${++n}: ${msg}`)}

const shell='<!doctype html><html><head><meta charset="utf-8"></head><body><main class="app"></main></body></html>';
const marketing=injectCustomer360Marketing(shell);
const daily=injectCustomerListDailyOperations(marketing);

pass('daily operations adapter applies to Customer360 HTML',daily!==marketing&&daily.includes('crm-customer-list-daily-operations-style'));
pass('customer name remains primary and canonical Customer ID is secondary',daily.includes('data-label="顧客"')&&daily.includes('Customer ID '+"'+esc(id)+'"));
pass('missing Customer ID has explicit human status and autofill action',daily.includes('顧客ID 未発行')&&daily.includes('顧客IDを自動生成'));
pass('relationship is derived only from existing shoot count',daily.includes("n>=2?'リピーター':n===1?'初回利用':'利用前'"));
pass('shoot count is visible in list',daily.includes('data-label="撮影回数"')&&daily.includes("v.shoot_count||0"));
pass('cumulative realized revenue is visible in list',daily.includes('data-label="累計売上"')&&daily.includes('yen(v.realized_ltv)'));
pass('last shoot date is visible in list',daily.includes('data-label="最終撮影"')&&daily.includes("v.last_shoot_date||'—'"));
pass('LINE linked state is visible in list',daily.includes('data-label="LINE"')&&daily.includes("v.line_linked?'連携済':'未連携'"));
pass('next candidate remains available without pretending it is a reservation',daily.includes('data-label="次の候補"')&&daily.includes('opp(v.next_opportunity)'));
pass('detail action remains exact valid Customer ID based',daily.includes('dailyCanonicalId(v)')&&daily.includes('data-open=')&&daily.includes('esc(v.customer_id)'));
pass('missing ID detail is fail-safe instead of broken empty-ID request',daily.includes('顧客IDを先に生成してください'));
pass('search communicates name Customer ID LINE display name and phone',daily.includes('placeholder="名前・顧客ID・LINE表示名・電話で検索"'));
pass('desktop header is daily-operations ordered',daily.includes('<th>顧客</th><th>関係</th><th>撮影回数</th><th>累計売上</th><th>最終撮影</th><th>LINE</th><th>次の候補</th><th>詳細</th>'));
pass('mobile action targets remain at least 44px',daily.includes('min-height:44px'));
pass('daily operations no longer depends on duplicate Marketing HOME table',daily.includes('crm-daily-row')&&!daily.includes("top.map(marketingRow).join('')"));
pass('adapter is idempotent',injectCustomerListDailyOperations(daily)===daily);
pass('controlled write contract is limited to missing Customer ID and never LINE send',customerListDailyOperationsContract.read_only===false&&customerListDailyOperationsContract.identity_mutation==='missing_customer_id_only'&&customerListDailyOperationsContract.production_write==='owner_click_only_after_deploy'&&customerListDailyOperationsContract.line_send===false);

const adapterSrc=fs.readFileSync('src/crm-customer-list-daily-operations.mjs','utf8');
const entrySrc=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
pass('UI adapter contains no direct customer mutation SQL',!/\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+customers\b/i.test(adapterSrc));
pass('adapter contains no LINE send controls',!/broadcast|multicast|pushMessage|replyMessage/i.test(adapterSrc));
pass('adapter does not require legacy top opportunity renderer',!adapterSrc.includes('||!out.includes("top.map(row).join')));
pass('production entry applies adapter immediately after Customer360 marketing UI',entrySrc.includes('const withMarketing=injectCustomer360Marketing(html);\n  const withDailyOperations=injectCustomerListDailyOperations(withMarketing);'));
pass('production entry keeps Owner exclusive view composition',entrySrc.includes('injectOwnerViewState(withDirectNavigation)'));

console.log(`CUSTOMER_LIST_DAILY_OPERATIONS=${n}/${n} PASS`);
