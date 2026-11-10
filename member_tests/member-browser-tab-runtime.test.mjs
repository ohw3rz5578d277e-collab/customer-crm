import assert from 'node:assert/strict';
import vm from 'node:vm';
import { __test as assetTest } from '../src/member-public-asset-delivery.mjs';
import {
  buildMemberAppShellViewModel,
  renderMemberAppShellMarkup
} from '../src/member-app-shell-ui.mjs';

// This executes the exact client JS served to /member-assets/member-app.js.
// A helper-only shell test cannot verify the browser's real event wiring.
const clientJs=assetTest.CLIENT_JS;
new vm.Script(clientJs,{filename:'member-app.js'});
const markup=renderMemberAppShellMarkup(buildMemberAppShellViewModel());
const tabs=['home','memories','create','shop','my'];
for(const tab of tabs){
  assert.ok(markup.includes('id="member-panel-'+tab+'"'),'missing tabpanel '+tab);
  assert.equal(markup.split('aria-controls="member-panel-'+tab+'"').length-1,2,'tab must be linked in both navs');
}

const documentListeners=new Map();
const windowListeners=new Map();
const historyEntries=[];
const focused=[];
const location={pathname:'/member'};
const panels=tabs.map(tab=>({
  tab,
  hidden:tab!=='home',
  getAttribute(name){return name==='data-member-panel'?this.tab:null},
  removeAttribute(name){if(name==='hidden')this.hidden=false},
  setAttribute(name){if(name==='hidden')this.hidden=true}
}));
const root={
  dataset:{},
  querySelectorAll(selector){
    if(selector==='[data-member-panel]')return panels;
    if(selector==='[data-member-tab]')return [...desktop.buttons,...mobile.buttons];
    return [];
  },
  contains(target){return [...desktop.buttons,...mobile.buttons].includes(target)}
};
function makeNavigation(locationName){
  const nav={};
  const buttons=tabs.map(tab=>({
    tab,
    tabIndex:null,
    attrs:{},
    classList:{toggle(){}},
    getAttribute(name){return name==='data-member-tab'?this.tab:this.attrs[name]??null},
    setAttribute(name,value){this.attrs[name]=value},
    closest(selector){
      if(selector==='[data-member-tab]')return this;
      if(selector==='[role="tablist"]')return nav;
      if(selector==='[data-member-app-shell]')return root;
      return null;
    },
    focus(){focused.push(locationName+':'+this.tab)}
  }));
  nav.querySelectorAll=selector=>selector==='[data-member-tab]'?buttons:[];
  return {nav,buttons};
}
const desktop=makeNavigation('desktop');
const mobile=makeNavigation('mobile');
const document={
  querySelector(selector){return selector==='[data-member-app-shell]'?root:null},
  addEventListener(name,listener){documentListeners.set(name,listener)}
};
const context={
  document,
  location,
  history:{pushState(state,title,url){historyEntries.push({state,title,url});location.pathname=url}},
  window:{scrollTo(){}},
  addEventListener(name,listener){windowListeners.set(name,listener)}
};
vm.runInNewContext(clientJs,context,{timeout:2000,filename:'member-app.js'});
assert.ok(documentListeners.has('keydown'),'deployed client JS must bind keyboard navigation');
assert.ok(documentListeners.has('click'),'deployed client JS must keep click navigation');
assert.ok(windowListeners.has('popstate'),'deployed client JS must keep browser history navigation');

function assertActive(expected){
  assert.equal(root.dataset.activeTab,expected,'shell active tab');
  for(const nav of [desktop,mobile]){
    const selected=nav.buttons.filter(x=>x.tabIndex===0);
    assert.deepEqual(selected.map(x=>x.tab),[expected],'selected-only tab stop');
    for(const button of nav.buttons){
      assert.equal(button.attrs['aria-selected'],button.tab===expected?'true':'false');
      assert.equal(button.tabIndex,button.tab===expected?0:-1);
    }
  }
  for(const panel of panels)assert.equal(panel.hidden,panel.tab!==expected,'panel visibility '+panel.tab);
}
function press(target,key,modifiers={}){
  let prevented=false;
  documentListeners.get('keydown')({
    target,key,...modifiers,
    preventDefault(){prevented=true}
  });
  return prevented;
}
assertActive('home');
assert.equal(historyEntries.length,0,'initial render does not push history');
assert.equal(press(desktop.buttons[0],'ArrowRight'),true);
assertActive('memories');
assert.equal(location.pathname,'/member/memories');
assert.equal(focused.at(-1),'desktop:memories','keyboard focus remains on originating desktop nav');
assert.equal(press(mobile.buttons[1],'ArrowLeft'),true);
assertActive('home');
assert.equal(focused.at(-1),'mobile:home','keyboard focus remains on originating mobile nav');
assert.equal(press(mobile.buttons[0],'ArrowLeft'),true);
assertActive('my');
assert.equal(focused.at(-1),'mobile:my','left arrow wraps to last tab');
assert.equal(press(desktop.buttons[4],'ArrowRight'),true);
assertActive('home');
assert.equal(focused.at(-1),'desktop:home','right arrow wraps to first tab');
assert.equal(press(desktop.buttons[2],'End'),true);
assertActive('my');
assert.equal(focused.at(-1),'desktop:my');
assert.equal(press(mobile.buttons[3],'Home'),true);
assertActive('home');
assert.equal(focused.at(-1),'mobile:home');

const before=historyEntries.length;
assert.equal(press(desktop.buttons[0],'ArrowRight',{ctrlKey:true}),false);
assert.equal(press(desktop.buttons[0],'ArrowRight',{altKey:true}),false);
assert.equal(press(desktop.buttons[0],'ArrowRight',{metaKey:true}),false);
assert.equal(press(desktop.buttons[0],'Escape'),false);
assert.equal(historyEntries.length,before,'unrelated/modified keys have no side effects');
assertActive('home');

let prevented=false;
documentListeners.get('click')({
  target:mobile.buttons[3],
  preventDefault(){prevented=true}
});
assert.equal(prevented,true,'browser click handled');
assertActive('shop');
assert.equal(location.pathname,'/member/shop','click updates history');
assert.equal(mobile.buttons[3].tabIndex,0,'clicked tab enters keyboard tab order');

const beforePop=historyEntries.length;
location.pathname='/member/create';
windowListeners.get('popstate')();
assertActive('create');
assert.equal(historyEntries.length,beforePop,'popstate does not push history');
const detached={
  getAttribute(){return 'my'},
  closest(selector){return selector==='[data-member-tab]'?this:null}
};
assert.equal(press(detached,'ArrowRight'),false,'outside tablist ignored');
assertActive('create');

assert.ok(!clientJs.includes('fetch('),'no network requests');
assert.ok(!clientJs.includes('localStorage'),'no persistent client state');
console.log('MEMBER_BROWSER_SERVED_JS_KEYBOARD_NAVIGATION=PASS');
