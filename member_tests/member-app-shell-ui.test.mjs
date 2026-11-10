import {
  MEMBER_APP_TABS,
  normalizeMemberAppTab,
  buildMemberAppShellViewModel,
  renderMemberAppShellMarkup,
  setMemberAppShellActiveTab,
  bindMemberAppShellNavigation,
  memberAppShellCss,
  memberAppShellUiHealth,
  __test
} from '../src/member-app-shell-ui.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

pass('canonical five tabs are exact',MEMBER_APP_TABS.map(x=>x.id).join(',')==='home,memories,create,shop,my');
pass('unknown tab fails closed to HOME',normalizeMemberAppTab('admin')==='home');
pass('CREATE tab remains selectable',normalizeMemberAppTab('CREATE')==='create');

const standard=buildMemberAppShellViewModel({active_tab:'memories'});
pass('standard shell builds',standard.status==='ok'&&standard.active_tab==='memories'&&standard.theme==='standard');
pass('shell view model contains no identity fields',!JSON.stringify(standard).includes('customer_id')&&!JSON.stringify(standard).includes('family_id'));

const black=buildMemberAppShellViewModel({
  active_tab:'my',
  family_pass:{
    status:'ok',
    family_pass:{effective_black:true}
  }
});
pass('BLACK theme comes from authorized Family Pass result',black.black_member===true&&black.theme==='black');
pass('client-like unverified BLACK object does not activate theme',buildMemberAppShellViewModel({family_pass:{family_pass:{effective_black:true}}}).black_member===false);

const markup=renderMemberAppShellMarkup(black);
pass('markup renders all five nav targets',MEMBER_APP_TABS.every(tab=>markup.includes('data-member-tab="'+tab.id+'"')));
pass('markup renders all five panel mounts',MEMBER_APP_TABS.every(tab=>markup.includes('data-member-mount="'+tab.id+'"')));
pass('active MY panel is visible',markup.includes('data-member-panel="my" role="tabpanel" aria-label="MY">'));
pass('inactive HOME panel is hidden',markup.includes('data-member-panel="home" role="tabpanel" aria-label="HOME" hidden'));
pass('legacy Today tab is absent',!markup.includes('data-member-tab="today"'));
pass('each canonical tab controls its matching panel',MEMBER_APP_TABS.every(tab=>
  markup.includes('id="member-panel-'+tab.id+'" data-member-panel="'+tab.id+'"')
  && markup.split('aria-controls="member-panel-'+tab.id+'"').length===3
));
pass('both tablists use selected-only keyboard tab stops',
  (markup.match(/tabindex="0" role="tab"/g)||[]).length===2
  && (markup.match(/tabindex="-1" role="tab"/g)||[]).length===8
);
pass('BLACK card is shown only for BLACK shell',markup.includes('BLACK</span><strong>Family Pass'));
pass('markup contains no external navigation URL',!markup.includes('http://')&&!markup.includes('https://'));

const buttons=[
  {tab:'home',attrs:{},classList:{toggle(){}},getAttribute(k){return k==='data-member-tab'?this.tab:null},setAttribute(k,v){this.attrs[k]=v}},
  {tab:'create',attrs:{},classList:{toggle(){}},getAttribute(k){return k==='data-member-tab'?this.tab:null},setAttribute(k,v){this.attrs[k]=v}}
];
const panels=[
  {tab:'home',hidden:false,getAttribute(k){return k==='data-member-panel'?this.tab:null},setAttribute(k){if(k==='hidden')this.hidden=true},removeAttribute(k){if(k==='hidden')this.hidden=false}},
  {tab:'create',hidden:true,getAttribute(k){return k==='data-member-panel'?this.tab:null},setAttribute(k){if(k==='hidden')this.hidden=true},removeAttribute(k){if(k==='hidden')this.hidden=false}}
];
const root={
  dataset:{},
  querySelectorAll(selector){return selector==='[data-member-tab]'?buttons:panels}
};
const switched=setMemberAppShellActiveTab(root,'create');
pass('tab switch updates canonical active tab',switched.ok===true&&root.dataset.activeTab==='create');
pass('tab switch hides previous and shows selected panel',panels[0].hidden===true&&panels[1].hidden===false);
pass('invalid tab switch resolves to HOME',setMemberAppShellActiveTab(root,'evil').active_tab==='home');
pass('active tab keyboard stop updates with the selected tab',buttons[0].tabIndex===0&&buttons[1].tabIndex===-1);

// Real key events use the tablist that received the event. The inactive
// desktop/mobile duplicate must not steal keyboard focus.
const tabIds=MEMBER_APP_TABS.map(x=>x.id);
const focused=[];
const changed=[];
const dispatched=[];
const listeners=new Map();
const makeTablist=location=>{
  const nav={};
  const buttons=tabIds.map(tab=>({
    tab,
    nav,
    attrs:{},
    classList:{toggle(){}},
    tabIndex:-1,
    getAttribute(k){return k==='data-member-tab'?this.tab:this.attrs[k]??null},
    setAttribute(k,v){this.attrs[k]=v},
    closest(selector){return selector==='[role="tablist"]'?nav:selector==='[data-member-tab]'?this:null},
    focus(){focused.push(location+':'+this.tab)}
  }));
  nav.querySelectorAll=()=>buttons;
  return {nav,buttons};
};
const desktop=makeTablist('desktop');
const mobile=makeTablist('mobile');
const shellPanels=tabIds.map(tab=>({
  tab,attrs:{},
  getAttribute(k){return k==='data-member-panel'?this.tab:null},
  setAttribute(k,v){this.attrs[k]=v},
  removeAttribute(k){delete this.attrs[k]}
}));
const navRoot={
  dataset:{},
  contains(target){return [...desktop.buttons,...mobile.buttons].includes(target)},
  addEventListener(type,fn){listeners.set(type,fn)},
  dispatchEvent(event){dispatched.push(event?.detail?.tab)},
  querySelectorAll(selector){return selector==='[data-member-tab]'
    ?[...desktop.buttons,...mobile.buttons]
    :selector==='[data-member-panel]'?shellPanels:[]}
};
const bound=bindMemberAppShellNavigation(navRoot,{on_tab_change:tab=>changed.push(tab)});
pass('shell navigation binding installs click and keyboard listeners',
  bound.ok===true&&listeners.has('click')&&listeners.has('keydown')
);
pass('duplicate binding is idempotent',bindMemberAppShellNavigation(navRoot).already_bound===true);
const key=(button,name,extra={})=>{
  let prevented=false;
  listeners.get('keydown')({
    key:name,target:button,...extra,
    preventDefault(){prevented=true}
  });
  return prevented;
};
pass('Right wraps MY to HOME inside desktop tablist',
  key(desktop.buttons[4],'ArrowRight')===true
  &&navRoot.dataset.activeTab==='home'
  &&focused.at(-1)==='desktop:home'
  &&desktop.buttons[0].tabIndex===0
  &&mobile.buttons[0].tabIndex===0
);
pass('Left wraps HOME to MY inside mobile tablist',
  key(mobile.buttons[0],'ArrowLeft')===true
  &&navRoot.dataset.activeTab==='my'
  &&focused.at(-1)==='mobile:my'
  &&desktop.buttons[4].tabIndex===0
  &&mobile.buttons[4].tabIndex===0
);
pass('Home/End select correct tab and keep mobile focus',
  key(mobile.buttons[2],'Home')===true
  &&navRoot.dataset.activeTab==='home'
  &&key(mobile.buttons[0],'End')===true
  &&navRoot.dataset.activeTab==='my'
  &&focused.at(-1)==='mobile:my'
);
const previousEvents=changed.length;
pass('modified and unrelated keys do not change navigation',
  key(desktop.buttons[1],'ArrowRight',{ctrlKey:true})===false
  &&key(desktop.buttons[1],'Escape')===false
  &&changed.length===previousEvents
  &&navRoot.dataset.activeTab==='my'
);
pass('navigation dispatch and callback stay in sync',
  changed.join(',')===dispatched.join(',')
  &&changed.join(',')==='home,my,home,my'
);
const wrongTab={getAttribute(){return 'admin'},closest(){return desktop.nav}};
pass('non-member and invalid targets do not navigate',key(wrongTab,'ArrowRight')===false
  &&changed.length===previousEvents);


const css=memberAppShellCss();
pass('mobile navigation is fixed bottom navigation',css.includes('.mp-shell__mobile-nav{position:fixed'));
pass('desktop navigation is left-side layout',css.includes('@media(min-width:820px)')&&css.includes('grid-template-columns:240px minmax(0,1fr)'));
pass('BLACK theme is distinct but restrained',css.includes('.mp-shell--black')&&css.includes('--mp-green:#d6c6a3'));

pass('HTML escaping is present',__test.escapeHtml('<script>')==='&lt;script&gt;');

const health=memberAppShellUiHealth();
pass('health records source-only shell',health.source_only===true&&health.member_app_shell_ui===true);
pass('health keeps Production and writes disabled',health.production_route_wired===false&&health.production_write===false&&health.auto_write===false&&health.line_send===false);
pass('health forbids client tier override',health.client_tier_override===false&&health.black_theme_source==='authorized_family_pass_effective_black');
pass('health records keyboard and tab panel semantics',
  health.keyboard_tablist_navigation===true
  &&health.linked_tab_panels===true
  &&health.roving_tabindex===true
);

console.log(`MEMBER_APP_SHELL_UI=${n}/${n} PASS`);
