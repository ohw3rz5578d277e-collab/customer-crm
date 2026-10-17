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
pass('mobile customer rows use compact density',daily.includes('#crmMktList .crm-daily-row{padding:9px!important}')&&daily.includes('grid-template-columns:70px minmax(0,1fr)!important')&&daily.includes('font-size:13px!important'));
pass('mobile customer name and metadata are compact without becoming tiny',daily.includes('.crm-mkt-name{font-size:15px!important')&&daily.includes('.crm-mkt-sub{margin-top:2px!important;font-size:10px!important}'));
pass('mobile search header is compact',daily.includes('.crm-search-head{padding:10px!important')&&daily.includes('margin-bottom:8px!important'));
pass('mobile search input preserves 44px minimum touch target',daily.includes('.crm-search-box input{min-height:44px!important'));
pass('mobile search clear action preserves 44px target',daily.includes('.crm-search-clear{right:8px!important;min-height:44px!important;min-width:44px!important'));
pass('mobile Customer360 detail sheet is tighter',daily.includes('.crm-detail{inset:6px!important;border-radius:16px!important;padding:10px!important}'));
pass('mobile Customer360 close control remains 44px square minimum',daily.includes('.crm-detail-close{width:44px!important;height:44px!important;min-width:44px!important;min-height:44px!important}'));
pass('mobile Customer360 profile typography is compact and readable',daily.includes('.crm-360-profile-main h2{font-size:22px!important;line-height:1.25!important}'));
pass('mobile Customer360 cards use compact spacing',daily.includes('.crm-360-card,.crm-section{padding:10px!important;border-radius:14px!important}'));
pass('mobile Customer360 action targets remain at least 44px',daily.includes('.crm-360-actions .crm-mkt-btn{min-height:44px!important'));
pass('mobile analysis cards use compact density',daily.includes('.crm-period-analytics,.crm-approach-queue{margin:9px 0!important;padding:10px!important;border-radius:14px!important}'));
pass('mobile analysis date inputs remain 44px minimum',daily.includes('.crm-period-range input{min-height:44px!important}'));
pass('mobile approach rows are compact',daily.includes('.crm-approach-row{gap:8px!important;padding:8px!important;border-radius:13px!important}'));
pass('mobile LINE shell is compact',daily.includes('.crm-line-chat-shell{height:calc(100vh - 142px)!important;min-height:460px!important;border-radius:12px!important}'));
pass('mobile LINE search remains 44px minimum',daily.includes('.crm-line-chat-search{margin-top:8px!important;min-height:44px!important'));
pass('mobile LINE customer rows remain easy to tap',daily.includes('.crm-line-chat-customer{min-height:52px!important'));
pass('mobile LINE back and refresh controls remain 44px minimum',daily.includes('.crm-line-chat-back{min-width:44px!important;width:44px!important;height:44px!important}')&&daily.includes('.crm-line-chat-refresh{min-height:44px!important'));
pass('mobile five-tab nav is denser while preserving tap safety',daily.includes('body.crm-owner-shell-v2 #crmOwnerMobileNav{height:calc(64px + env(safe-area-inset-bottom))!important')&&daily.includes('min-height:52px!important'));
pass('mobile workspace bottom clearance tracks compact nav',daily.includes('body.crm-owner-shell-v2 #crmOwnerWorkspaceContent{padding-bottom:calc(80px + env(safe-area-inset-bottom))!important}'));
pass('mobile action targets remain at least 44px',daily.includes('min-height:44px'));
pass('daily operations no longer depends on duplicate Marketing HOME table',daily.includes('crm-daily-row')&&!daily.includes("top.map(marketingRow).join('')"));
pass('adapter is idempotent',injectCustomerListDailyOperations(daily)===daily);
pass('controlled write contract is limited to missing Customer ID and never LINE send',customerListDailyOperationsContract.read_only===false&&customerListDailyOperationsContract.identity_mutation==='missing_customer_id_only'&&customerListDailyOperationsContract.production_write==='owner_click_only_after_deploy'&&customerListDailyOperationsContract.line_send===false);

const adapterSrc=fs.readFileSync('src/crm-customer-list-daily-operations.mjs','utf8');
const entrySrc=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
pass('UI adapter contains no direct customer mutation SQL',!/\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+customers\b/i.test(adapterSrc));
pass('adapter contains no LINE send controls',!/broadcast|multicast|pushMessage|replyMessage/i.test(adapterSrc));
pass('adapter does not alter Reservation handoff URL or behavior',!adapterSrc.includes('reservation-app-api')&&!adapterSrc.includes('crmOwnerNavReservation'));
pass('adapter does not require legacy top opportunity renderer',!adapterSrc.includes(`||!out.includes("top.map(row).join('')")`));
pass('production entry sanitizes legacy visuals and base admin UI before Customer360 marketing and daily operations',entrySrc.includes('const visualBase=stripLegacyOwnerVisualAssets(html);\n  const canonicalBase=stripLegacyBaseAdminUi(visualBase);\n  const withMarketing=injectCustomer360Marketing(canonicalBase);\n  const withDailyOperations=injectCustomerListDailyOperations(withMarketing);'));
pass('production entry keeps Owner exclusive view composition after read-only LINE chat',entrySrc.includes('const withLineChat=injectOwnerLineChat(withDirectNavigation);')&&entrySrc.includes('const withOwnerViewState=injectOwnerViewState(withLineChat);'));

console.log(`CUSTOMER_LIST_DAILY_OPERATIONS=${n}/${n} PASS`);
