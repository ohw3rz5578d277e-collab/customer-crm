import assert from 'node:assert/strict';
import fs from 'node:fs';

const cockpit = fs.readFileSync(new URL('../src/crm-marketing-cockpit.mjs', import.meta.url), 'utf8');
const rules = fs.readFileSync(new URL('../src/crm-marketing-rules.mjs', import.meta.url), 'utf8');

assert.ok(cockpit.includes('HOME') && cockpit.includes('顧客') && cockpit.includes('セグメント') && cockpit.includes('キャンペーン') && cockpit.includes('分析'), 'navigation labels must exist');
assert.ok(cockpit.includes('名前・顧客ID・LINEで検索'), 'global search placeholder must exist');
assert.ok(cockpit.includes('配信実行なし'), 'campaign page must be planning only');
assert.equal(/broadcast|multicast|message\/push|replyMessage|pushMessage/.test(cockpit), false, 'marketing UI must not add LINE send controls or endpoints');

assert.ok(cockpit.includes("document.body.insertBefore(root,document.body.firstChild)"), 'Marketing root must mount at body level, not inside legacy narrow main');
assert.ok(cockpit.includes("data-crm-root-mount','body-level"), 'body-level root marker must exist');
assert.equal(cockpit.includes("document.querySelector('main')||document.body"), false, 'Marketing root must not mount inside legacy main');

assert.ok(cockpit.includes('@media (max-width:767px)'), 'mobile breakpoint must exist');
assert.ok(cockpit.includes('@media (min-width:768px) and (max-width:1199px)'), 'tablet breakpoint must exist');
assert.ok(cockpit.includes('@media (min-width:1200px)'), 'desktop breakpoint must exist');
assert.ok(cockpit.includes('@media (min-width:1600px)'), 'wide desktop breakpoint must exist');

assert.ok(cockpit.includes('max-width:1600px'), 'desktop workspace must support 1440-1600px class width');
assert.ok(cockpit.includes('grid-template-columns:224px minmax(0,1fr)'), 'desktop sidebar + main workspace composition must exist');
assert.ok(cockpit.includes('crmMSide') && cockpit.includes('data-device-nav="desktop-tablet"'), 'desktop/tablet sidebar navigation must exist');
assert.ok(cockpit.includes('crmMBottom') && cockpit.includes('data-device-nav="mobile"'), 'mobile bottom navigation must exist');
assert.ok(cockpit.includes('.crmMBottom{display:none!important}'), 'desktop/tablet bottom nav hidden rule must exist');
assert.ok(cockpit.includes('.crmMSide{display:none!important}'), 'mobile sidebar hidden rule must exist');

assert.ok(cockpit.includes('crmMDesktopTable') && cockpit.includes('<table class="crmMTable"'), 'desktop customer table must exist');
assert.ok(cockpit.includes('crmMMobileCustomerCard') && cockpit.includes('Customer ID '), 'semantic mobile customer card must exist');
assert.ok(cockpit.includes('crmMCustomerCards{display:none}'), 'mobile card list should not duplicate desktop view on desktop');
assert.ok(cockpit.includes('.crmMDesktopTable{display:none!important}'), 'desktop table must be hidden on mobile');

assert.ok(cockpit.includes('crmM360Grid'), 'Customer 360 responsive grid must exist');
assert.ok(cockpit.includes('grid-template-areas:"summary action"'), 'Customer 360 desktop/tablet multi-column layout must exist');
assert.ok(cockpit.includes('grid-template-areas:"summary" "action"'), 'Customer 360 mobile single-column layout must exist');

assert.ok(cockpit.includes('min-height:44px'), 'mobile/keyboard tap target minimum must be preserved');
assert.ok(cockpit.includes('overflow-x:hidden'), 'mobile horizontal overflow guard must exist');
assert.ok(cockpit.includes('focus-visible'), 'keyboard focus-visible styling must exist');

assert.ok(rules.includes('repeatOpportunityDays') && rules.includes('dormantDays'), 'segment rules must be centralized');

console.log('crm-marketing-ui-contract tests PASS');
