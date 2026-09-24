import fs from 'node:fs';
import {
  readMemberShopCatalogForSession,
  handleMemberShopCatalogReadRequest,
  memberShopPickupHealth,
  __test
} from '../src/member-shop-pickup-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

function product(overrides={}){
  return {
    product_id:'product_album_01',
    product_type:'album',
    title:'Family Album',
    description:'家族の写真を一冊に。',
    season_tag:'evergreen',
    starts_on:null,
    ends_on:null,
    hero_asset_id:'asset:shop:album:01',
    shop_path:'/shop/album/',
    cta_label:'商品を見る',
    featured_home:1,
    sort_order:10,
    ...overrides
  };
}

pass('valid date accepts calendar date',__test.validDateOnly('2026-09-24')==='2026-09-24');
pass('invalid calendar date rejected',__test.validDateOnly('2026-09-31')==='');
pass('local shop path accepted',__test.validShopPath('/寿-kotobuki/')===true);
pass('protocol-relative shop path rejected',__test.validShopPath('//evil.example/x')===false);
pass('absolute external URL rejected',__test.validShopPath('https://evil.example/x')===false);
pass('control chars rejected from shop path',__test.validShopPath('/shop/\n')===false);

pass('open schedule is active',__test.activeSchedule(product(),'2026-09-24').active===true);
pass('future schedule inactive',__test.activeSchedule(product({starts_on:'2026-10-01'}),'2026-09-24').active===false);
pass('expired schedule inactive',__test.activeSchedule(product({ends_on:'2026-09-01'}),'2026-09-24').active===false);
pass('reversed schedule invalid',__test.activeSchedule(product({starts_on:'2026-11-01',ends_on:'2026-10-01'}),'2026-09-24').valid===false);

const normalized=__test.normalizeProduct(product(),{asOf:'2026-09-24'});
pass('valid product normalizes',normalized?.product_id==='product_album_01');
pass('product exposes logical asset ID only',normalized.hero_asset_ref.asset_id==='asset:shop:album:01'&&normalized.hero_asset_ref.storage_key_exposed===false);
pass('product exposes local navigation only',normalized.navigation.shop_path==='/shop/album/'&&normalized.navigation.local_path_only===true);
pass('product makes no authoritative price claim',normalized.pricing.authoritative===false&&normalized.pricing.amount===null&&normalized.pricing.source_connected===false);
pass('product does not claim checkout readiness',normalized.checkout.ready===false&&normalized.checkout.provider===null);
pass('product does not enforce Family Pass discount',normalized.family_pass_benefit.enforcement_ready===false&&normalized.family_pass_benefit.discount_amount===null);

pass('unsupported product type rejected',__test.normalizeProduct(product({product_type:'service'}),{asOf:'2026-09-24'})===null);
pass('unsafe product ID rejected',__test.normalizeProduct(product({product_id:'../bad'}),{asOf:'2026-09-24'})===null);
pass('unsafe asset ID rejected',__test.normalizeProduct(product({hero_asset_id:'https://evil.example/x'}),{asOf:'2026-09-24'})===null);
pass('external shop URL rejected',__test.normalizeProduct(product({shop_path:'https://example.com/shop'}),{asOf:'2026-09-24'})===null);
pass('overlong CTA rejected',__test.normalizeProduct(product({cta_label:'x'.repeat(41)}),{asOf:'2026-09-24'})===null);

const built=__test.buildMemberShopCatalog([
  product({product_id:'p2',hero_asset_id:'asset:p2',sort_order:20,featured_home:1}),
  product({product_id:'p1',hero_asset_id:'asset:p1',sort_order:10,featured_home:1}),
  product({product_id:'p3',hero_asset_id:'asset:p3',sort_order:30,featured_home:1}),
  product({product_id:'p4',hero_asset_id:'asset:p4',sort_order:40,featured_home:1}),
  product({product_id:'future',hero_asset_id:'asset:future',starts_on:'2027-01-01'})
],{as_of:'2026-09-24'});
pass('catalog returns active valid products only',built.available_count===4&&built.products.length===4);
pass('catalog preserves sort order',built.products.map(x=>x.product_id).join(',')==='p1,p2,p3,p4');
pass('HOME pickup is capped at three',built.home_pickup.length===3&&built.home_pickup.map(x=>x.product_id).join(',')==='p1,p2,p3');
pass('catalog counts hidden invalid/inactive rows',built.hidden_invalid_or_inactive_count===1);
pass('catalog declares price/checkout/discount disabled',built.pricing_authoritative===false&&built.checkout_ready===false&&built.discount_enforcement_ready===false);

function makeDb({shopSchema=true}={}){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    ...(shopSchema?['member_shop_products']:[])
  ]);
  const seenSql=[];
  const writes=[];
  const rows=[
    product({product_id:'album',hero_asset_id:'asset:album',sort_order:1,featured_home:1}),
    product({product_id:'canvas',product_type:'canvas',title:'Canvas',hero_asset_id:'asset:canvas',shop_path:'/shop/canvas/',sort_order:2,featured_home:1}),
    product({product_id:'frame',product_type:'frame',title:'Frame',hero_asset_id:'asset:frame',shop_path:'/shop/frame/',sort_order:3,featured_home:1}),
    product({product_id:'kotobuki',product_type:'kotobuki',title:'KOTOBUKI',hero_asset_id:'asset:kotobuki',shop_path:'/寿-kotobuki/',sort_order:4,featured_home:1})
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
          if(sql.includes('FROM member_shop_products'))return {results:rows};
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('Shop Pickup read model must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const db=makeDb();
const result=await readMemberShopCatalogForSession(
  {DB:db},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('authorized Member reads Shop catalog',result.status==='ok'&&result.family_id===familyId&&result.customer_id===customerId);
pass('Shop catalog returns four products',result.products.length===4);
pass('HOME pickup returns only first three featured products',result.home_pickup.length===3&&result.home_pickup[0].product_id==='album');
pass('Shop source explicitly remains presentation-only',result.source.presentation_catalog_only===true&&result.source.price_source_connected===false&&result.source.checkout_source_connected===false);
pass('Shop read performs zero writes',db.writes.length===0&&result.read_only===true);

const sql=db.seenSql.find(x=>x.includes('FROM member_shop_products'))||'';
pass('Shop query requires published rows',sql.includes('published=1'));
pass('Shop query hides deleted rows',sql.includes("COALESCE(deleted_at,'')=''"));
pass('Shop query does not select price fields',!/(price|amount|currency)/i.test(sql));
pass('Shop query does not select checkout/payment fields',!/(checkout|payment|square|woocommerce)/i.test(sql));
pass('Shop query does not select storage_key or arbitrary URL',!/(storage_key|https?:|external_url)/i.test(sql));

const missing=await readMemberShopCatalogForSession(
  {DB:makeDb({shopSchema:false})},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('missing Shop schema is explicit',missing.status==='shop_catalog_schema_not_applied');

const denied=await readMemberShopCatalogForSession(
  {DB:makeDb()},
  {family_id:'fam_B',customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('cross-Family session is denied',denied.status==='family_access_denied');

const noSession=await handleMemberShopCatalogReadRequest(
  new Request('https://example.test/api/internal/member/shop/products'),
  {DB:makeDb()},
  null
);
pass('HTTP Shop catalog requires Member session',noSession.status===401);

const post=await handleMemberShopCatalogReadRequest(
  new Request('https://example.test/api/internal/member/shop/products',{method:'POST'}),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
pass('HTTP Shop catalog is GET only',post.status===405);

const override=await handleMemberShopCatalogReadRequest(
  new Request('https://example.test/api/internal/member/shop/products?customer_id=26000999&family_id=fam_B&as_of=1999-01-01'),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
const overrideBody=await override.json();
pass('request identity cannot override server Member session',override.status===200&&overrideBody.family_id===familyId&&overrideBody.customer_id===customerId);
pass('client as_of cannot control Shop clock',overrideBody.as_of!=='1999-01-01');

const health=memberShopPickupHealth();
pass('health records presentation catalog only',health.presentation_catalog_only===true);
pass('health supports expected product types',health.supported_product_types.includes('album')&&health.supported_product_types.includes('kotobuki'));
pass('health records logical assets/local paths only',health.logical_asset_id_only===true&&health.storage_key_exposed===false&&health.local_shop_path_only===true&&health.arbitrary_external_url_exposed===false);
pass('health makes no price/inventory claim',health.pricing_authoritative===false&&health.price_source_connected===false&&health.inventory_claim===false);
pass('health keeps checkout/order/payment disabled',health.checkout_ready===false&&health.checkout_provider_connected===false&&health.order_creation===false&&health.payment_execution===false);
pass('health keeps Family Pass discount enforcement disabled',health.family_pass_discount_enforcement===false);
pass('health caps HOME pickup at three',health.home_pickup_limit===3);
pass('health records no send/write/Production route',health.automatic_contact===false&&health.line_send===false&&health.production_route_wired===false&&health.production_write===false);

const migration=fs.readFileSync(
  'migrations_managed/20260924_member_shop_catalog_foundation.sql',
  'utf8'
);
pass('Shop migration is additive only',!/\b(DROP|ALTER|DELETE|UPDATE|INSERT)\b/i.test(migration));
pass('Shop schema contains no price/payment/order/inventory columns',!/\b(price|amount|currency|payment|order|inventory|stock|square|woocommerce)\b/i.test(migration));
pass('Shop schema contains no storage_key or arbitrary URL column',!/\b(storage_key|external_url|product_url|checkout_url)\b/i.test(migration));
pass('Shop schema uses local shop_path instead',migration.includes('shop_path'));
pass('Shop schema constrains publication state',/published[\s\S]*CHECK \(published IN \(0,1\)\)/i.test(migration));

console.log(`MEMBER_SHOP_PICKUP_READ_MODEL=${n}/${n} PASS`);
