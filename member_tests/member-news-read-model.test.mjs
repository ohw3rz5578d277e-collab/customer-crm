import fs from 'node:fs';
import {
  readMemberNewsForSession,
  handleMemberNewsReadRequest,
  memberNewsHealth,
  __test
} from '../src/member-news-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

function news(overrides={}){
  return {
    news_id:'news_01',
    news_type:'news',
    title:'秋のお知らせ',
    summary:'秋の撮影についてのお知らせです。',
    body_text:'ご予約前にご確認ください。',
    hero_asset_id:'asset:news:autumn',
    local_path:'/news/autumn/',
    starts_on:'2026-09-01',
    ends_on:'2026-11-30',
    published_at:'2026-09-20',
    featured_home:1,
    sort_order:10,
    ...overrides
  };
}

pass('valid date accepted',__test.validDateOnly('2026-09-25')==='2026-09-25');
pass('invalid date rejected',__test.validDateOnly('2026-09-31')==='');
pass('local path accepted',__test.validLocalPath('/news/autumn/')===true);
pass('external URL rejected',__test.validLocalPath('https://evil.example/x')===false);
pass('protocol-relative URL rejected',__test.validLocalPath('//evil.example/x')===false);
pass('control chars rejected from local path',__test.validLocalPath('/news/\n')===false);
pass('plain text accepted',__test.validPlainText('撮影のお知らせ',120)===true);
pass('script markup rejected',__test.validPlainText('<script>alert(1)</script>',120)===false);
pass('iframe markup rejected',__test.validPlainText('<iframe src=x>',120)===false);

pass('active schedule accepted',__test.activeSchedule(news(),'2026-09-25').active===true);
pass('future schedule inactive',__test.activeSchedule(news({starts_on:'2026-10-01'}),'2026-09-25').active===false);
pass('expired schedule inactive',__test.activeSchedule(news({ends_on:'2026-09-01'}),'2026-09-25').active===false);
pass('reversed schedule invalid',__test.activeSchedule(news({starts_on:'2026-11-01',ends_on:'2026-10-01'}),'2026-09-25').valid===false);

const normalized=__test.normalizeNewsItem(news(),{asOf:'2026-09-25'});
pass('valid NEWS item normalizes',normalized?.news_id==='news_01');
pass('NEWS asset is logical ID only',normalized.hero_asset_ref.asset_id==='asset:news:autumn'&&normalized.hero_asset_ref.storage_key_exposed===false);
pass('NEWS navigation remains local only',normalized.navigation.local_path==='/news/autumn/'&&normalized.navigation.local_path_only===true);
pass('NEWS never claims delivery',normalized.delivery.push_sent===false&&normalized.delivery.line_sent===false&&normalized.delivery.automatic_contact===false);
pass('unsupported NEWS type rejected',__test.normalizeNewsItem(news({news_type:'html'}),{asOf:'2026-09-25'})===null);
pass('unsafe hero asset rejected',__test.normalizeNewsItem(news({hero_asset_id:'https://evil.example/x'}),{asOf:'2026-09-25'})===null);
pass('unsafe path rejected',__test.normalizeNewsItem(news({local_path:'https://evil.example/x'}),{asOf:'2026-09-25'})===null);
pass('script-like body rejected',__test.normalizeNewsItem(news({body_text:'<script>x</script>'}),{asOf:'2026-09-25'})===null);

const built=__test.buildMemberNewsCatalog([
  news({news_id:'n2',hero_asset_id:'asset:n2',sort_order:20}),
  news({news_id:'n1',hero_asset_id:'asset:n1',sort_order:10}),
  news({news_id:'n3',hero_asset_id:'asset:n3',sort_order:30}),
  news({news_id:'n4',hero_asset_id:'asset:n4',sort_order:40}),
  news({news_id:'future',hero_asset_id:'asset:future',starts_on:'2027-01-01'})
],{as_of:'2026-09-25'});
pass('NEWS catalog returns active valid items only',built.available_count===4&&built.items.length===4);
pass('NEWS catalog preserves Owner sort order',built.items.map(x=>x.news_id).join(',')==='n1,n2,n3,n4');
pass('HOME NEWS capped at three',built.home_news.length===3&&built.home_news.map(x=>x.news_id).join(',')==='n1,n2,n3');
pass('hidden invalid/inactive count tracked',built.hidden_invalid_or_inactive_count===1);
pass('catalog delivery remains disabled',built.push_delivery_ready===false&&built.line_delivery_ready===false&&built.automatic_contact===false);

function makeDb({newsSchema=true}={}){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    ...(newsSchema?['member_news_items']:[])
  ]);
  const rows=[
    news({news_id:'n1',hero_asset_id:'asset:n1',sort_order:1}),
    news({news_id:'n2',news_type:'campaign',title:'Family Campaign',hero_asset_id:'asset:n2',sort_order:2}),
    news({news_id:'n3',news_type:'service',title:'Service Update',hero_asset_id:'asset:n3',sort_order:3}),
    news({news_id:'n4',news_type:'maintenance',title:'Maintenance',hero_asset_id:'asset:n4',featured_home:0,sort_order:4})
  ];
  const seenSql=[];
  const writes=[];
  return {
    seenSql,writes,
    prepare(sql){
      seenSql.push(sql);
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master'))return tables.has(state.params[0])?{name:state.params[0]}:null;
          if(sql.includes('FROM customer_family_groups')){
            return state.params[0]===familyId
              ?{family_id:familyId,display_name:'TEST FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            return state.params[0]===customerId
              ?{results:[{family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return state.params[0]===familyId
              ?{results:[{customer_id:customerId,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          if(sql.includes('FROM member_news_items'))return {results:rows};
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('NEWS read model must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const db=makeDb();
const result=await readMemberNewsForSession(
  {DB:db},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-25'}
);
pass('authorized Member reads NEWS catalog',result.status==='ok'&&result.family_id===familyId&&result.customer_id===customerId);
pass('NEWS catalog returns all published rows',result.items.length===4);
pass('HOME NEWS returns first three featured items',result.home_news.length===3);
pass('NEWS source is plain-text only',result.source.plain_text_only===true&&result.source.arbitrary_html===false&&result.source.arbitrary_external_url===false);
pass('NEWS read performs zero writes',db.writes.length===0&&result.read_only===true);

const sql=db.seenSql.find(x=>x.includes('FROM member_news_items'))||'';
pass('NEWS query requires published rows',sql.includes('published=1'));
pass('NEWS query hides deleted rows',sql.includes("COALESCE(deleted_at,'')=''"));
pass('NEWS query does not select HTML/JS/external URL/storage key',!/\b(html|javascript|script|external_url|storage_key)\b/i.test(sql));

const missing=await readMemberNewsForSession(
  {DB:makeDb({newsSchema:false})},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-25'}
);
pass('missing NEWS schema explicit',missing.status==='news_catalog_schema_not_applied');

const denied=await readMemberNewsForSession(
  {DB:makeDb()},
  {family_id:'fam_B',customer_id:customerId},
  {as_of:'2026-09-25'}
);
pass('cross-Family NEWS access denied',denied.status==='family_access_denied');

const noSession=await handleMemberNewsReadRequest(
  new Request('https://example.test/api/internal/member/news'),
  {DB:makeDb()},
  null
);
pass('NEWS HTTP requires Member session',noSession.status===401);

const post=await handleMemberNewsReadRequest(
  new Request('https://example.test/api/internal/member/news',{method:'POST'}),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
pass('NEWS HTTP is GET only',post.status===405);

const override=await handleMemberNewsReadRequest(
  new Request('https://example.test/api/internal/member/news?customer_id=26000999&family_id=fam_B&as_of=1999-01-01'),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
const overrideBody=await override.json();
pass('request identity cannot override server session',override.status===200&&overrideBody.family_id===familyId&&overrideBody.customer_id===customerId);
pass('client as_of cannot control NEWS clock',overrideBody.as_of!=='1999-01-01');

const health=memberNewsHealth();
pass('health records plain-text-only NEWS',health.plain_text_only===true&&health.arbitrary_html===false&&health.arbitrary_javascript===false);
pass('health supports expected NEWS types',health.supported_news_types.includes('news')&&health.supported_news_types.includes('campaign')&&health.supported_news_types.includes('maintenance'));
pass('health records logical assets/local navigation only',health.logical_asset_id_only===true&&health.storage_key_exposed===false&&health.local_navigation_only===true&&health.arbitrary_external_url_exposed===false);
pass('health caps HOME NEWS at three',health.home_news_limit===3);
pass('health keeps push/LINE/automatic contact disabled',health.push_delivery_ready===false&&health.line_delivery_ready===false&&health.automatic_contact===false);
pass('health records no Production route/write',health.production_route_wired===false&&health.production_write===false);

const migration=fs.readFileSync(
  'migrations_managed/20260924_member_news_catalog_foundation.sql',
  'utf8'
);
const schemaOnly=migration.replace(/^\s*--.*$/gm,'');
pass('NEWS migration additive only',!/\b(DROP|ALTER|DELETE|UPDATE|INSERT)\b/i.test(schemaOnly));
pass('NEWS schema contains no HTML/JS/external URL/storage key columns',!/\b(html|javascript|script|external_url|storage_key)\b/i.test(schemaOnly));
pass('NEWS schema uses body_text not HTML body',schemaOnly.includes('body_text'));
pass('NEWS schema constrains publication state',/published[\s\S]*CHECK \(published IN \(0,1\)\)/i.test(schemaOnly));

console.log(`MEMBER_NEWS_READ_MODEL=${n}/${n} PASS`);
