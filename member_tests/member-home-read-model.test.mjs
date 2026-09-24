import {
  readMemberHomeForSession,
  handleMemberHomeReadRequest,
  memberHomeReadHealth,
  __test
} from '../src/member-home-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const spouseCustomerId='26000456';
const familyId='fam_A';

function memory(id,date,genre,title){
  return {
    memory_id:id,
    shoot_date:date,
    genre,
    title,
    cover:null,
    preview_count:0,
    amazon_photos_available:false,
    favorite:false,
    favorite_mutable:false,
    create_available:false,
    shop_available:false
  };
}

const baseComponents={
  memories:{
    status:'ok',
    family_id:familyId,
    memories:[
      memory('mem_4','2026-09-01','ファミリー','Family 4'),
      memory('mem_3','2026-08-01','七五三','Family 3'),
      memory('mem_2','2026-07-01','1歳バースデー','Family 2'),
      memory('mem_1','2026-06-01','お宮参り','Family 1')
    ],
    read_only:true
  },
  familyPass:{
    status:'ok',
    family_id:familyId,
    customer_id:customerId,
    family_pass:{
      memory_count:4,
      current_tier:'SILVER',
      next_tier:'GOLD',
      memories_to_next:1,
      progress_ratio:0.5
    },
    entitlement:{
      schema_applied:false,
      durable_black:false
    },
    benefit_contract:{
      black_photo_goods_discount_percent:10,
      applies_to_shooting_fee:false,
      enforcement_ready:false
    },
    read_only:true
  },
  passport:{
    status:'ok',
    family_id:familyId,
    customer_id:customerId,
    passport:{
      achieved_count:3,
      total_milestones:4,
      milestones:[
        {code:'OMIYAMAIRI',achieved:true},
        {code:'FIRST_BIRTHDAY',achieved:true},
        {code:'SHICHIGOSAN',achieved:true},
        {code:'SCHOOL_ENTRANCE',achieved:false}
      ],
      child_specific:false
    },
    read_only:true
  },
  todayMemory:{
    status:'ok',
    today_memory:{
      as_of:'2026-09-24',
      mode:'exact_anniversary',
      headline:'この日の思い出',
      primary:memory('mem_today','2025-09-24','ファミリー','去年の今日'),
      memories:[memory('mem_today','2025-09-24','ファミリー','去年の今日')],
      exact_match_count:1,
      seasonal_fallback_used:false
    },
    source:{reader:'member_memories_read_model',family_scoped:true},
    read_only:true
  },
  creative:{
    status:'ok',
    family_id:familyId,
    customer_id:customerId,
    templates:[
      {
        template_id:'tpl_a',creative_type:'wallpaper',title:'Wallpaper',
        composition:{mode:'single_photo',photo_slots:1,canvas_width:1290,canvas_height:2796,output_mime:'image/png',execution_ready:false,browser_side_preferred:true},
        asset_ref:{asset_id:'asset:a',preview_asset_id:null,storage_key_exposed:false,arbitrary_url_exposed:false},
        eligibility:{eligible:true,minimum_memory_count:1,visible_memory_count:4,memories_needed:0},sort_order:1
      },
      {
        template_id:'tpl_b',creative_type:'then_and_now',title:'Then & Now',
        composition:{mode:'pair_photo',photo_slots:2,canvas_width:1600,canvas_height:1200,output_mime:'image/jpeg',execution_ready:false,browser_side_preferred:true},
        asset_ref:{asset_id:'asset:b',preview_asset_id:null,storage_key_exposed:false,arbitrary_url_exposed:false},
        eligibility:{eligible:true,minimum_memory_count:2,visible_memory_count:4,memories_needed:0},sort_order:2
      }
    ],
    available_count:2,
    eligible_count:2,
    generation_ready:false,
    source:{authorized_memory_result_reused:true},
    read_only:true
  },
  shopPickup:{
    status:'ok',
    family_id:familyId,
    customer_id:customerId,
    home_pickup:[
      {
        product_id:'album',product_type:'album',title:'Family Album',
        hero_asset_ref:{asset_id:'asset:album',storage_key_exposed:false,arbitrary_external_url_exposed:false},
        navigation:{shop_path:'/shop/album/',local_path_only:true,cta_label:'商品を見る'},
        pricing:{authoritative:false,amount:null,currency:'JPY',source_connected:false},
        checkout:{ready:false,provider:null},
        family_pass_benefit:{enforcement_ready:false,discount_amount:null},
        featured_home:true,sort_order:1
      },
      {
        product_id:'canvas',product_type:'canvas',title:'Canvas',
        hero_asset_ref:{asset_id:'asset:canvas',storage_key_exposed:false,arbitrary_external_url_exposed:false},
        navigation:{shop_path:'/shop/canvas/',local_path_only:true,cta_label:'商品を見る'},
        pricing:{authoritative:false,amount:null,currency:'JPY',source_connected:false},
        checkout:{ready:false,provider:null},
        family_pass_benefit:{enforcement_ready:false,discount_amount:null},
        featured_home:true,sort_order:2
      }
    ],
    available_count:4,
    pricing_authoritative:false,
    checkout_ready:false,
    discount_enforcement_ready:false,
    source:{presentation_catalog_only:true,price_source_connected:false,checkout_source_connected:false},
    read_only:true
  },
  news:{
    status:'ok',
    family_id:familyId,
    customer_id:customerId,
    home_news:[
      {
        news_id:'news_home_1',
        news_type:'news',
        title:'秋のお知らせ',
        summary:'秋の撮影について',
        body_text:'ご予約前にご確認ください。',
        published_at:'2026-09-20',
        hero_asset_ref:{asset_id:'asset:news:1',storage_key_exposed:false,arbitrary_external_url_exposed:false},
        navigation:{local_path:'/news/autumn/',local_path_only:true},
        featured_home:true,
        delivery:{push_sent:false,line_sent:false,automatic_contact:false},
        sort_order:1
      },
      {
        news_id:'news_home_2',
        news_type:'campaign',
        title:'Family Campaign',
        summary:'',
        body_text:'',
        published_at:'2026-09-18',
        hero_asset_ref:null,
        navigation:{local_path:null,local_path_only:true},
        featured_home:true,
        delivery:{push_sent:false,line_sent:false,automatic_contact:false},
        sort_order:2
      }
    ],
    available_count:4,
    push_delivery_ready:false,
    line_delivery_ready:false,
    automatic_contact:false,
    source:{plain_text_only:true,arbitrary_html:false,arbitrary_external_url:false},
    read_only:true
  },
  nextMemory:{
    status:'ok',
    family_id:familyId,
    customer_id:customerId,
    next_memory:{
      type:'first_birthday',
      label:'A 1歳誕生日',
      target_date:'2026-10-01',
      days_until:7
    },
    candidates:[
      {type:'first_birthday',label:'A 1歳誕生日',automatic_contact:false},
      {type:'shichigosan',label:'3歳七五三候補',automatic_contact:false},
      {type:'school_entry_candidate',label:'小学校入学候補',automatic_contact:false},
      {type:'birthday',label:'誕生日',automatic_contact:false}
    ],
    consultation_cta:{
      channel:'line',
      intent:'consultation',
      automatic_send:false
    },
    family_history_is_child_specific:false,
    child_memory_link_available:false,
    read_only:true
  }
};

function compose(overrides={}){
  return __test.composeMemberHomeModel({
    session:{family_id:familyId,customer_id:customerId},
    memories:overrides.memories??baseComponents.memories,
    familyPass:overrides.familyPass??baseComponents.familyPass,
    passport:overrides.passport??baseComponents.passport,
    nextMemory:overrides.nextMemory??baseComponents.nextMemory,
    todayMemory:overrides.todayMemory??baseComponents.todayMemory,
    creative:overrides.creative??baseComponents.creative,
    shopPickup:overrides.shopPickup??baseComponents.shopPickup,
    news:overrides.news??baseComponents.news
  });
}

const full=compose();
pass('full HOME composition succeeds',full.status==='ok'&&full.family_id===familyId&&full.customer_id===customerId);
pass('HOME keeps only three recent MEMORIES',full.home.recent_memories.length===3&&full.home.recent_memories[0].memory_id==='mem_4'&&full.home.recent_memories[2].memory_id==='mem_2');
pass('HOME reports loaded visible MEMORY count without inventing DB total',full.home.visible_memory_count===4);
pass('HOME composes FAMILY PASS',full.home.family_pass.family_pass.current_tier==='SILVER');
pass('HOME composes FAMILY PASSPORT',full.home.family_passport.achieved_count===3);
pass('HOME composes TODAY\'S MEMORY',full.home.today_memory.today_memory.mode==='exact_anniversary'&&full.home.today_memory.today_memory.headline==='この日の思い出');
pass('HOME composes Creative catalog',full.home.creative.featured_templates.length===2&&full.home.creative.eligible_count===2&&full.home.creative.generation_ready===false);
pass('HOME composes Shop Pickup presentation catalog',full.home.shop_pickup.products.length===2&&full.home.shop_pickup.available_count===4);
pass('HOME Shop Pickup keeps commerce disabled',full.home.shop_pickup.pricing_authoritative===false&&full.home.shop_pickup.checkout_ready===false&&full.home.shop_pickup.discount_enforcement_ready===false);
pass('HOME composes NEWS catalog',full.home.news.items.length===2&&full.home.news.available_count===4);
pass('HOME NEWS keeps all delivery disabled',full.home.news.push_delivery_ready===false&&full.home.news.line_delivery_ready===false&&full.home.news.automatic_contact===false);
pass('HOME composes NEXT MEMORY',full.home.next_memory.next_memory.type==='first_birthday');
pass('HOME caps NEXT MEMORY candidate preview at three',full.home.next_memory.candidates.length===3);
pass('HOME keeps LINE consultation CTA non-automatic',full.home.next_memory.consultation_cta.channel==='line'&&full.home.next_memory.consultation_cta.automatic_send===false);
pass('HOME does not claim Family history is child specific',full.home.next_memory.family_history_is_child_specific===false&&full.home.next_memory.child_memory_link_available===false);
pass('full HOME is not partial',full.home.partial===false&&full.home.unavailable_sections.length===0);
pass('all current HOME content modules are source-active',full.home.future_modules.today_memory===true&&full.home.future_modules.creative===true&&full.home.future_modules.shop_pickup===true&&full.home.future_modules.news===true);
pass('HOME result is read-only',full.read_only===true);

const degradedNext=compose({
  nextMemory:{
    status:'child_profile_schema_not_applied',
    next_memory:null,
    candidates:[],
    read_only:true
  }
});
pass('missing optional child schema degrades NEXT MEMORY only',degradedNext.status==='ok'&&degradedNext.home.partial===true&&degradedNext.home.next_memory===null);
pass('degraded HOME reports unavailable NEXT MEMORY section',degradedNext.home.unavailable_sections.length===1&&degradedNext.home.unavailable_sections[0].section==='next_memory'&&degradedNext.home.unavailable_sections[0].error==='child_profile_schema_not_applied');
pass('degraded NEXT MEMORY does not hide safe recent MEMORIES',degradedNext.home.recent_memories.length===3);

const degradedToday=compose({
  todayMemory:{
    status:'invalid_as_of',
    today_memory:null,
    read_only:true
  }
});
pass('TODAY\'S MEMORY internal clock failure can remain section-local',degradedToday.status==='ok'&&degradedToday.home.partial===true&&degradedToday.home.today_memory===null);
pass('degraded TODAY\'S MEMORY section reports its reason',degradedToday.home.unavailable_sections.some(x=>x.section==='today_memory'&&x.error==='invalid_as_of'));

const degradedCreative=compose({
  creative:{status:'creative_catalog_schema_not_applied',templates:[],read_only:true}
});
pass('missing Creative schema degrades Creative section only',degradedCreative.status==='ok'&&degradedCreative.home.partial===true&&degradedCreative.home.creative===null);
pass('degraded Creative section reports schema reason',degradedCreative.home.unavailable_sections.some(x=>x.section==='creative'&&x.error==='creative_catalog_schema_not_applied'));

const degradedShop=compose({
  shopPickup:{status:'shop_catalog_schema_not_applied',home_pickup:[],products:[],read_only:true}
});
pass('missing Shop schema degrades Shop Pickup section only',degradedShop.status==='ok'&&degradedShop.home.partial===true&&degradedShop.home.shop_pickup===null);
pass('degraded Shop Pickup section reports schema reason',degradedShop.home.unavailable_sections.some(x=>x.section==='shop_pickup'&&x.error==='shop_catalog_schema_not_applied'));

const degradedNews=compose({
  news:{status:'news_catalog_schema_not_applied',home_news:[],items:[],read_only:true}
});
pass('missing NEWS schema degrades NEWS section only',degradedNews.status==='ok'&&degradedNews.home.partial===true&&degradedNews.home.news===null);
pass('degraded NEWS section reports schema reason',degradedNews.home.unavailable_sections.some(x=>x.section==='news'&&x.error==='news_catalog_schema_not_applied'));

const degradedPass=compose({
  familyPass:{
    status:'family_pass_temporarily_unavailable',
    read_only:true
  }
});
pass('non-security FAMILY PASS failure can remain section-local',degradedPass.status==='ok'&&degradedPass.home.partial===true&&degradedPass.home.family_pass===null);

const fatal=compose({
  nextMemory:{
    status:'ambiguous_child_identity',
    review_required:true,
    read_only:true
  }
});
pass('ambiguous child identity fails HOME closed',fatal.status==='ambiguous_child_identity'&&fatal.review_required===true&&!('home' in fatal));

const denied=compose({
  passport:{
    status:'family_access_denied',
    read_only:true
  }
});
pass('cross-Family component status fails HOME closed',denied.status==='family_access_denied'&&!('home' in denied));

const mismatch=compose({
  familyPass:{
    ...baseComponents.familyPass,
    family_id:'fam_B'
  }
});
pass('component Family mismatch fails HOME closed',mismatch.status==='component_identity_mismatch'&&mismatch.review_required===true);

const customerMismatch=compose({
  passport:{
    ...baseComponents.passport,
    customer_id:'26000999'
  }
});
pass('component Customer mismatch fails HOME closed',customerMismatch.status==='component_identity_mismatch');

const noMemoryCore=compose({
  memories:{
    status:'member_memory_schema_not_applied',
    memories:[],
    read_only:true
  }
});
pass('MEMORIES schema is core requirement for HOME',noMemoryCore.status==='member_memory_schema_not_applied'&&!('home' in noMemoryCore));

pass('invalid Member session is rejected by composer',__test.composeMemberHomeModel({
  session:{family_id:familyId,customer_id:'bad'},
  ...baseComponents
}).status==='invalid_member_session');

function makeDb({
  childSchema=true,
  memorySchema=true,
  mediaSchema=true,
  creativeSchema=true,
  shopSchema=true,
  newsSchema=true
}={}){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    ...(childSchema?['customer_family_members']:[]),
    ...(memorySchema?['member_memories']:[]),
    ...(mediaSchema?['member_memory_media']:[]),
    ...(creativeSchema?['member_creative_templates']:[]),
    ...(shopSchema?['member_shop_products']:[]),
    ...(newsSchema?['member_news_items']:[])
  ]);
  const writes=[];
  const seenSql=[];
  const memoryRows=[
    {
      memory_id:'mem_new',
      family_id:familyId,
      shoot_date:'2026-08-15',
      genre:'七五三',
      title:'七五三',
      amazon_photos_url:'https://example.test/a',
      published:1,
      created_at:'2026-08-15',
      updated_at:'2026-08-15'
    },
    {
      memory_id:'mem_today',
      family_id:familyId,
      shoot_date:'2025-09-24',
      genre:'ファミリー',
      title:'去年の今日',
      amazon_photos_url:'',
      published:1,
      created_at:'2025-09-24',
      updated_at:'2025-09-24'
    },
    {
      memory_id:'mem_old',
      family_id:familyId,
      shoot_date:'2025-10-01',
      genre:'1歳バースデー',
      title:'1st Birthday',
      amazon_photos_url:'',
      published:1,
      created_at:'2025-10-01',
      updated_at:'2025-10-01'
    }
  ];

  return {
    writes,
    seenSql,
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
          if(sql.includes('COUNT(*) AS memory_count')){
            return state.params[0]===familyId?{memory_count:memoryRows.length}:{memory_count:0};
          }
          if(sql.includes('FROM member_family_pass_entitlements')){
            return null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            const id=state.params[0];
            if(![customerId,spouseCustomerId].includes(id))return {results:[]};
            return {results:[{
              family_id:familyId,
              customer_id:id,
              relation:id===customerId?'owner':'spouse',
              access_role:id===customerId?'owner':'member'
            }]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return state.params[0]===familyId
              ?{results:[
                  {customer_id:customerId,relation:'owner',access_role:'owner'},
                  {customer_id:spouseCustomerId,relation:'spouse',access_role:'member'}
                ]}
              :{results:[]};
          }
          if(sql.includes('FROM member_memory_media')){
            return {results:[]};
          }
          if(sql.includes('FROM customer_family_members')){
            const id=state.params[0];
            if(id===customerId){
              return {results:[{
                id:'child_1',
                customer_id:customerId,
                relation:'child',
                name:'A',
                birthdate:'2025-10-01',
                school_stage:'',
                deleted_at:''
              }]};
            }
            return {results:[]};
          }
          if(sql.includes('FROM member_memories')){
            return state.params[0]===familyId?{results:memoryRows}:{results:[]};
          }
          if(sql.includes('FROM member_creative_templates')){
            return {results:[
              {
                template_id:'tpl_home_1',
                creative_type:'wallpaper',
                title:'Family Wallpaper',
                description:'',
                season_tag:'evergreen',
                starts_on:null,
                ends_on:null,
                photo_slots:1,
                composition_mode:'single_photo',
                canvas_width:1290,
                canvas_height:2796,
                output_mime:'image/png',
                asset_id:'asset:home:1',
                preview_asset_id:null,
                minimum_memory_count:1,
                sort_order:1
              },
              {
                template_id:'tpl_home_2',
                creative_type:'then_and_now',
                title:'Then & Now',
                description:'',
                season_tag:'evergreen',
                starts_on:null,
                ends_on:null,
                photo_slots:2,
                composition_mode:'pair_photo',
                canvas_width:1600,
                canvas_height:1200,
                output_mime:'image/jpeg',
                asset_id:'asset:home:2',
                preview_asset_id:null,
                minimum_memory_count:2,
                sort_order:2
              }
            ]};
          }
          if(sql.includes('FROM member_shop_products')){
            return {results:[
              {
                product_id:'album',
                product_type:'album',
                title:'Family Album',
                description:'',
                season_tag:'evergreen',
                starts_on:null,
                ends_on:null,
                hero_asset_id:'asset:shop:album',
                shop_path:'/shop/album/',
                cta_label:'商品を見る',
                featured_home:1,
                sort_order:1
              },
              {
                product_id:'canvas',
                product_type:'canvas',
                title:'Canvas',
                description:'',
                season_tag:'evergreen',
                starts_on:null,
                ends_on:null,
                hero_asset_id:'asset:shop:canvas',
                shop_path:'/shop/canvas/',
                cta_label:'商品を見る',
                featured_home:1,
                sort_order:2
              }
            ]};
          }
          if(sql.includes('FROM member_news_items')){
            return {results:[
              {
                news_id:'news_home_1',
                news_type:'news',
                title:'秋のお知らせ',
                summary:'秋の撮影について',
                body_text:'ご予約前にご確認ください。',
                hero_asset_id:'asset:news:1',
                local_path:'/news/autumn/',
                starts_on:'2026-09-01',
                ends_on:'2026-11-30',
                published_at:'2026-09-20',
                featured_home:1,
                sort_order:1
              },
              {
                news_id:'news_home_2',
                news_type:'campaign',
                title:'Family Campaign',
                summary:'',
                body_text:'',
                hero_asset_id:null,
                local_path:null,
                starts_on:null,
                ends_on:null,
                published_at:'2026-09-18',
                featured_home:1,
                sort_order:2
              }
            ]};
          }
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('Member HOME read model must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const db=makeDb();
const integrated=await readMemberHomeForSession(
  {DB:db},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('integrated HOME read succeeds from existing component readers',integrated.status==='ok'&&integrated.family_id===familyId);
pass('integrated HOME reads MEMORIES',integrated.home.recent_memories.length===3&&integrated.home.recent_memories[0].memory_id==='mem_new');
pass('integrated HOME reads FAMILY PASS',integrated.home.family_pass.family_pass.current_tier==='SILVER');
pass('integrated HOME reads FAMILY PASSPORT',integrated.home.family_passport.achieved_count===2);
pass('integrated HOME composes exact TODAY\'S MEMORY from the already-loaded MEMORIES',integrated.home.today_memory.today_memory?.mode==='exact_anniversary'&&integrated.home.today_memory.today_memory?.primary?.memory_id==='mem_today');
pass('integrated HOME reads Creative catalog from authorized MEMORIES',integrated.home.creative.featured_templates.length===2&&integrated.home.creative.source.authorized_memory_result_reused===true);
pass('integrated HOME reads NEXT MEMORY from canonical child',integrated.home.next_memory.next_memory?.type==='first_birthday');
pass('integrated HOME performs zero writes',db.writes.length===0&&integrated.read_only===true);

const childMissingDb=makeDb({childSchema:false});
const integratedPartial=await readMemberHomeForSession(
  {DB:childMissingDb},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('integrated HOME degrades only NEXT MEMORY when child schema is absent',integratedPartial.status==='ok'&&integratedPartial.home.partial===true&&integratedPartial.home.next_memory===null&&integratedPartial.home.recent_memories.length===3&&integratedPartial.home.today_memory.today_memory?.primary?.memory_id==='mem_today'&&integratedPartial.home.creative.featured_templates.length===2&&integratedPartial.home.shop_pickup.products.length===2&&integratedPartial.home.news.items.length===2);

const noCreativeDb=makeDb({creativeSchema:false});
const integratedNoCreative=await readMemberHomeForSession(
  {DB:noCreativeDb},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('integrated HOME degrades Creative only when Creative schema is absent',integratedNoCreative.status==='ok'&&integratedNoCreative.home.partial===true&&integratedNoCreative.home.creative===null&&integratedNoCreative.home.recent_memories.length===3);

const noShopDb=makeDb({shopSchema:false});
const integratedNoShop=await readMemberHomeForSession(
  {DB:noShopDb},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('integrated HOME degrades Shop Pickup only when Shop schema is absent',integratedNoShop.status==='ok'&&integratedNoShop.home.partial===true&&integratedNoShop.home.shop_pickup===null&&integratedNoShop.home.recent_memories.length===3);

const noNewsDb=makeDb({newsSchema:false});
const integratedNoNews=await readMemberHomeForSession(
  {DB:noNewsDb},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('integrated HOME degrades NEWS only when NEWS schema is absent',integratedNoNews.status==='ok'&&integratedNoNews.home.partial===true&&integratedNoNews.home.news===null&&integratedNoNews.home.recent_memories.length===3);

const noMediaDb=makeDb({mediaSchema:false});
const integratedNoMemory=await readMemberHomeForSession(
  {DB:noMediaDb},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('integrated HOME fails when core MEMORIES schema is incomplete',integratedNoMemory.status==='member_memory_schema_not_applied');

const queryOverrideResponse=await handleMemberHomeReadRequest(
  new Request('https://example.test/api/internal/member/home?customer_id=26000999&family_id=fam_B&as_of=1999-01-01'),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
const queryOverrideBody=await queryOverrideResponse.json();
pass('HTTP HOME ignores request-supplied identity controls',queryOverrideResponse.status===200&&queryOverrideBody.family_id===familyId&&queryOverrideBody.customer_id===customerId);
pass('HTTP HOME ignores client-controlled as_of',queryOverrideBody.home.next_memory?.next_memory?.target_date!=='1999-01-01');

const noSession=await handleMemberHomeReadRequest(
  new Request('https://example.test/api/internal/member/home'),
  {DB:makeDb()},
  null
);
pass('HTTP HOME requires server Member session',noSession.status===401);

const post=await handleMemberHomeReadRequest(
  new Request('https://example.test/api/internal/member/home',{method:'POST'}),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
pass('HTTP HOME is GET only',post.status===405);

const health=memberHomeReadHealth();
pass('health records eight composed components',health.components.join(',')==='member_memories,family_pass,family_passport,today_memory,creative,shop_pickup,news,next_memory');
pass('health requires component identity consistency',health.component_identity_consistency_required===true&&health.identity_security_failures_fail_closed===true);
pass('health records MEMORIES as core and NEXT MEMORY optional degradation',health.memories_core_required===true&&health.next_memory_optional_schema_degradation===true);
pass('health records TODAY\'S MEMORY derives from loaded MEMORIES with no extra DB read',health.today_memory_derived_from_loaded_memories===true&&health.today_memory_extra_db_read===false);
pass('health records Creative reuses authorized MEMORIES with no extra MEMORY list read',health.creative_uses_authorized_memories===true&&health.creative_extra_memory_db_read===false&&health.creative_optional_schema_degradation===true);
pass('health records Shop Pickup as optional presentation-only commerce-disabled section',health.shop_pickup_optional_schema_degradation===true&&health.shop_pickup_presentation_only===true&&health.shop_pickup_pricing_authoritative===false&&health.shop_pickup_checkout_ready===false&&health.shop_pickup_discount_enforcement_ready===false);
pass('health records NEWS as optional plain-text delivery-disabled section',health.news_optional_schema_degradation===true&&health.news_plain_text_only===true&&health.news_push_delivery_ready===false&&health.news_line_delivery_ready===false&&health.news_automatic_contact===false);
pass('health records response limits',health.recent_memory_limit===3&&health.next_memory_candidate_limit===3&&health.creative_template_limit===3&&health.shop_pickup_limit===3&&health.news_limit===3);
pass('health source-activates all current HOME content modules',health.today_memory_active===true&&health.creative_active===true&&health.shop_pickup_active===true&&health.news_active===true);
pass('health records no contact/send/reservation/write',health.automatic_contact===false&&health.line_send===false&&health.reservation_creation===false&&health.production_write===false);
pass('health records source-only route state',health.production_route_wired===false&&health.read_only===true&&health.request_customer_id_input===false&&health.request_family_id_input===false&&health.request_as_of_input===false);

console.log(`MEMBER_HOME_READ_MODEL=${n}/${n} PASS`);
