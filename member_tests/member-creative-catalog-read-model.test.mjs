import fs from 'node:fs';
import {
  readMemberCreativeCatalogForSession,
  handleMemberCreativeCatalogReadRequest,
  memberCreativeCatalogHealth,
  __test
} from '../src/member-creative-catalog-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

function template(overrides={}){
  return {
    template_id:'tpl_wallpaper_01',
    creative_type:'wallpaper',
    title:'Family Wallpaper',
    description:'Photo-first wallpaper',
    season_tag:'autumn',
    starts_on:'2026-09-01',
    ends_on:'2026-11-30',
    photo_slots:1,
    composition_mode:'single_photo',
    canvas_width:1290,
    canvas_height:2796,
    output_mime:'image/png',
    asset_id:'asset:creative:wallpaper:01',
    preview_asset_id:'asset:creative:wallpaper:01:preview',
    minimum_memory_count:1,
    sort_order:10,
    ...overrides
  };
}

pass('valid date accepts normal date',__test.validDateOnly('2026-09-24')==='2026-09-24');
pass('invalid date is rejected',__test.validDateOnly('2026-09-31')==='');
pass('open-ended schedule can be active',__test.activeSchedule(template({starts_on:null,ends_on:null}),'2026-09-24').active===true);
pass('future template is inactive',__test.activeSchedule(template({starts_on:'2026-10-01'}),'2026-09-24').active===false);
pass('expired template is inactive',__test.activeSchedule(template({ends_on:'2026-09-01'}),'2026-09-24').active===false);
pass('reversed schedule is invalid',__test.activeSchedule(template({starts_on:'2026-11-01',ends_on:'2026-10-01'}),'2026-09-24').valid===false);
pass('malformed schedule is invalid',__test.activeSchedule(template({starts_on:'bad'}),'2026-09-24').valid===false);

const oneMemory=__test.normalizeTemplate(template(),{asOf:'2026-09-24',memoryCount:1});
pass('valid template normalizes',oneMemory?.template_id==='tpl_wallpaper_01');
pass('logical asset IDs are exposed, not storage keys',oneMemory.asset_ref.asset_id==='asset:creative:wallpaper:01'&&oneMemory.asset_ref.storage_key_exposed===false);
pass('arbitrary URL is not exposed',oneMemory.asset_ref.arbitrary_url_exposed===false);
pass('browser-side composition is preferred but execution is off',oneMemory.composition.browser_side_preferred===true&&oneMemory.composition.execution_ready===false);
pass('eligible template is marked eligible',oneMemory.eligibility.eligible===true&&oneMemory.eligibility.memories_needed===0);

const needsTwo=__test.normalizeTemplate(template({
  template_id:'tpl_then_now',
  creative_type:'then_and_now',
  composition_mode:'pair_photo',
  photo_slots:2,
  minimum_memory_count:2,
  asset_id:'asset:creative:then-now',
  preview_asset_id:null
}),{asOf:'2026-09-24',memoryCount:1});
pass('insufficient Family history does not hide template',needsTwo?.eligibility.eligible===false);
pass('insufficient history returns memories_needed',needsTwo?.eligibility.memories_needed===1);

pass('unsafe template ID is rejected',__test.normalizeTemplate(template({template_id:'../bad'}),{asOf:'2026-09-24',memoryCount:2})===null);
pass('unsafe asset ID is rejected',__test.normalizeTemplate(template({asset_id:'https://evil.example/x'}),{asOf:'2026-09-24',memoryCount:2})===null);
pass('unsupported creative type is rejected',__test.normalizeTemplate(template({creative_type:'script'}),{asOf:'2026-09-24',memoryCount:2})===null);
pass('unsupported composition mode is rejected',__test.normalizeTemplate(template({composition_mode:'html'}),{asOf:'2026-09-24',memoryCount:2})===null);
pass('unsupported output MIME is rejected',__test.normalizeTemplate(template({output_mime:'text/html'}),{asOf:'2026-09-24',memoryCount:2})===null);
pass('excessive photo slots are rejected',__test.normalizeTemplate(template({photo_slots:99}),{asOf:'2026-09-24',memoryCount:2})===null);

const catalog=__test.buildCreativeCatalog([
  template({template_id:'tpl_b',sort_order:20,asset_id:'asset:b'}),
  template({template_id:'tpl_a',sort_order:10,asset_id:'asset:a'}),
  template({template_id:'tpl_future',starts_on:'2027-01-01',asset_id:'asset:future'}),
  template({template_id:'tpl_bad_asset',asset_id:'https://bad.example'})
],{
  as_of:'2026-09-24',
  visible_memory_count:2
});
pass('catalog returns only active valid templates',catalog.templates.length===2&&catalog.available_count===2);
pass('catalog sorts by sort_order then ID',catalog.templates[0].template_id==='tpl_a'&&catalog.templates[1].template_id==='tpl_b');
pass('catalog counts hidden invalid/inactive rows',catalog.hidden_invalid_or_inactive_count===2);
pass('catalog remains generation disabled',catalog.generation_ready===false&&catalog.read_only===true);

function makeDb({
  creativeSchema=true,
  memorySchema=true,
  mediaSchema=true,
  rows=[
    template({template_id:'tpl_a',sort_order:1,asset_id:'asset:a'}),
    template({
      template_id:'tpl_two',
      creative_type:'then_and_now',
      composition_mode:'pair_photo',
      photo_slots:2,
      minimum_memory_count:3,
      sort_order:2,
      asset_id:'asset:two'
    })
  ]
}={}){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    ...(memorySchema?['member_memories']:[]),
    ...(mediaSchema?['member_memory_media']:[]),
    ...(creativeSchema?['member_creative_templates']:[])
  ]);
  const seenSql=[];
  const writes=[];
  const memories=[
    {
      memory_id:'mem_2',
      family_id:familyId,
      shoot_date:'2026-08-01',
      genre:'七五三',
      title:'七五三',
      amazon_photos_url:'',
      published:1,
      created_at:'2026-08-01',
      updated_at:'2026-08-01'
    },
    {
      memory_id:'mem_1',
      family_id:familyId,
      shoot_date:'2025-08-01',
      genre:'1歳バースデー',
      title:'1st',
      amazon_photos_url:'',
      published:1,
      created_at:'2025-08-01',
      updated_at:'2025-08-01'
    }
  ];

  return {
    seenSql,
    writes,
    prepare(sql){
      seenSql.push(sql);
      const state={params:[]};
      const stmt={
        bind(...params){state.params=params;return stmt},
        async first(){
          if(sql.includes('sqlite_master')){
            return tables.has(state.params[0])?{name:state.params[0]}:null;
          }
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
          if(sql.includes('FROM member_memory_media'))return {results:[]};
          if(sql.includes('FROM member_memories')){
            return state.params[0]===familyId?{results:memories}:{results:[]};
          }
          if(sql.includes('FROM member_creative_templates')){
            return {results:rows};
          }
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('Creative catalog read model must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const db=makeDb();
const result=await readMemberCreativeCatalogForSession(
  {DB:db},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('authorized Member reads Creative catalog',result.status==='ok'&&result.family_id===familyId&&result.customer_id===customerId);
pass('Family visible MEMORY count drives eligibility',result.templates[0].eligibility.visible_memory_count===2);
pass('one-MEMORY template is eligible',result.templates.find(x=>x.template_id==='tpl_a').eligibility.eligible===true);
pass('three-MEMORY template remains visible but locked',result.templates.find(x=>x.template_id==='tpl_two').eligibility.eligible===false);
pass('read model performs zero writes',db.writes.length===0&&result.read_only===true);

const creativeSql=db.seenSql.find(sql=>sql.includes('FROM member_creative_templates'))||'';
pass('Creative query requires published templates',creativeSql.includes('published=1'));
pass('Creative query hides deleted templates',creativeSql.includes("COALESCE(deleted_at,'')=''"));
pass('Creative query never selects storage_key',!creativeSql.includes('storage_key'));
pass('Creative query never selects arbitrary URL or executable-code columns',!/\b(url|html|javascript|script|script_code|external_url|template_url)\b/i.test(creativeSql));

const missingCreative=await readMemberCreativeCatalogForSession(
  {DB:makeDb({creativeSchema:false})},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('missing Creative schema is explicit',missingCreative.status==='creative_catalog_schema_not_applied');

const missingMemory=await readMemberCreativeCatalogForSession(
  {DB:makeDb({memorySchema:false})},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('Creative eligibility requires safe Member MEMORY foundation',missingMemory.status==='member_memory_schema_not_applied');

const denied=await readMemberCreativeCatalogForSession(
  {DB:makeDb()},
  {family_id:'fam_B',customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('cross-Family Member session is denied',denied.status==='family_access_denied');

const noSession=await handleMemberCreativeCatalogReadRequest(
  new Request('https://example.test/api/internal/member/creative/templates'),
  {DB:makeDb()},
  null
);
pass('Creative HTTP contract requires server Member session',noSession.status===401);

const post=await handleMemberCreativeCatalogReadRequest(
  new Request('https://example.test/api/internal/member/creative/templates',{method:'POST'}),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
pass('Creative HTTP contract is GET only',post.status===405);

const override=await handleMemberCreativeCatalogReadRequest(
  new Request('https://example.test/api/internal/member/creative/templates?customer_id=26000999&family_id=fam_B&as_of=1999-01-01'),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
const overrideBody=await override.json();
pass('request identity cannot override server Member session',override.status===200&&overrideBody.family_id===familyId&&overrideBody.customer_id===customerId);
pass('client as_of does not control Creative catalog clock',overrideBody.as_of!=='1999-01-01');

const health=memberCreativeCatalogHealth();
pass('health records logical asset refs only',health.asset_ref_is_logical_id_only===true&&health.storage_key_exposed===false);
pass('health forbids arbitrary HTML/JS/external URL',health.arbitrary_template_html===false&&health.arbitrary_template_javascript===false&&health.arbitrary_external_url===false);
pass('health keeps generation/upload/customer photo write disabled',health.generation_ready===false&&health.upload_ready===false&&health.customer_photo_write===false);
pass('health records browser-side composition preference',health.browser_side_composition_preferred===true);
pass('health records no route/send/write',health.production_route_wired===false&&health.automatic_contact===false&&health.line_send===false&&health.production_write===false);

const migration=fs.readFileSync(
  'migrations_managed/20260924_member_creative_catalog_foundation.sql',
  'utf8'
);
pass('Creative migration is additive only',!/\b(DROP|ALTER|DELETE|UPDATE|INSERT)\b/i.test(migration));
pass('Creative schema contains no arbitrary HTML/JS/URL fields',!/\b(html|javascript|script|url)\b/i.test(migration));
pass('Creative schema stores logical asset IDs not storage keys',migration.includes('asset_id')&&!migration.includes('storage_key'));
pass('Creative schema constrains published state',/published[\s\S]*CHECK \(published IN \(0,1\)\)/i.test(migration));
pass('Creative schema bounds minimum MEMORY requirement',/minimum_memory_count[\s\S]*BETWEEN 1 AND 12/i.test(migration));

console.log(`MEMBER_CREATIVE_CATALOG_READ_MODEL=${n}/${n} PASS`);
