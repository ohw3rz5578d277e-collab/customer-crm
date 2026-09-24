import {
  readMemberCreativeCatalogFromAuthorizedMemories,
  memberCreativeCatalogHealth
} from '../src/member-creative-catalog-read-model.mjs';
import {
  readMemberShopCatalogForSession,
  memberShopPickupHealth
} from '../src/member-shop-pickup-read-model.mjs';
import {
  readMemberNewsForSession,
  memberNewsHealth
} from '../src/member-news-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const familyId='fam_A';
const customerId='26000123';

function makeDb({assetSchema=true}={}){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    'member_creative_templates',
    'member_shop_products',
    'member_news_items',
    ...(assetSchema?['member_public_assets']:[])
  ]);
  const writes=[];

  const assets=[
    {asset_id:'asset:creative:main',asset_kind:'image',local_path:'/member-assets/creative/main.webp',mime_type:'image/webp',width:1600,height:1200},
    {asset_id:'asset:creative:preview',asset_kind:'image',local_path:'/member-assets/creative/preview.webp',mime_type:'image/webp',width:800,height:600},
    {asset_id:'asset:shop:album',asset_kind:'image',local_path:'/member-assets/shop/album.webp',mime_type:'image/webp',width:1600,height:1200},
    {asset_id:'asset:news:autumn',asset_kind:'image',local_path:'/member-assets/news/autumn.jpg',mime_type:'image/jpeg',width:1800,height:1200}
  ];

  return {
    writes,
    prepare(sql){
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
          if(sql.includes('FROM member_creative_templates')){
            return {results:[{
              template_id:'tpl_1',
              creative_type:'wallpaper',
              title:'Wallpaper',
              description:'',
              season_tag:'evergreen',
              starts_on:null,
              ends_on:null,
              photo_slots:1,
              composition_mode:'single_photo',
              canvas_width:1290,
              canvas_height:2796,
              output_mime:'image/png',
              asset_id:'asset:creative:main',
              preview_asset_id:'asset:creative:preview',
              minimum_memory_count:1,
              sort_order:1
            }]};
          }
          if(sql.includes('FROM member_shop_products')){
            return {results:[{
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
            }]};
          }
          if(sql.includes('FROM member_news_items')){
            return {results:[{
              news_id:'news_1',
              news_type:'news',
              title:'秋のお知らせ',
              summary:'',
              body_text:'',
              hero_asset_id:'asset:news:autumn',
              local_path:'/news/autumn/',
              starts_on:null,
              ends_on:null,
              published_at:'2026-09-20',
              featured_home:1,
              sort_order:1
            }]};
          }
          if(sql.includes('FROM member_public_assets')){
            return {results:assets.filter(asset=>state.params.includes(asset.asset_id))};
          }
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('public asset integration must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const session={family_id:familyId,customer_id:customerId};
const authorizedMemories={
  status:'ok',
  family_id:familyId,
  customer_id:customerId,
  memories:[{memory_id:'mem_1'}]
};

const creativeDb=makeDb();
const creative=await readMemberCreativeCatalogFromAuthorizedMemories(
  {DB:creativeDb},
  session,
  authorizedMemories,
  {as_of:'2026-09-25'}
);
pass('Creative remains available with public asset registry',creative.status==='ok'&&creative.public_assets_available===true);
pass('Creative main asset resolves to trusted local path',creative.templates[0].asset_ref.public_asset?.public_path==='/member-assets/creative/main.webp');
pass('Creative preview asset resolves independently',creative.templates[0].asset_ref.preview_public_asset?.public_path==='/member-assets/creative/preview.webp');
pass('Creative keeps logical IDs alongside resolved presentation assets',creative.templates[0].asset_ref.asset_id==='asset:creative:main'&&creative.templates[0].asset_ref.preview_asset_id==='asset:creative:preview');
pass('Creative integration performs zero writes',creativeDb.writes.length===0);

const shopDb=makeDb();
const shop=await readMemberShopCatalogForSession(
  {DB:shopDb},
  session,
  {as_of:'2026-09-25'}
);
pass('Shop remains available with public asset registry',shop.status==='ok'&&shop.public_assets_available===true);
pass('Shop hero resolves to trusted local path',shop.products[0].hero_asset_ref.public_asset?.public_path==='/member-assets/shop/album.webp');
pass('Shop HOME pickup preserves enriched product',shop.home_pickup[0].hero_asset_ref.public_asset?.public_path==='/member-assets/shop/album.webp');
pass('Shop integration performs zero writes',shopDb.writes.length===0);

const newsDb=makeDb();
const news=await readMemberNewsForSession(
  {DB:newsDb},
  session,
  {as_of:'2026-09-25'}
);
pass('NEWS remains available with public asset registry',news.status==='ok'&&news.public_assets_available===true);
pass('NEWS hero resolves to trusted local path',news.items[0].hero_asset_ref.public_asset?.public_path==='/member-assets/news/autumn.jpg');
pass('HOME NEWS preserves enriched item',news.home_news[0].hero_asset_ref.public_asset?.public_path==='/member-assets/news/autumn.jpg');
pass('NEWS integration performs zero writes',newsDb.writes.length===0);

const creativeNoAsset=await readMemberCreativeCatalogFromAuthorizedMemories(
  {DB:makeDb({assetSchema:false})},
  session,
  authorizedMemories,
  {as_of:'2026-09-25'}
);
pass('missing public asset schema does not hide Creative',creativeNoAsset.status==='ok'&&creativeNoAsset.public_assets_available===false&&creativeNoAsset.templates.length===1);
pass('Creative degrades to logical IDs with null public assets',creativeNoAsset.templates[0].asset_ref.asset_id==='asset:creative:main'&&creativeNoAsset.templates[0].asset_ref.public_asset===null&&creativeNoAsset.templates[0].asset_ref.preview_public_asset===null);

const shopNoAsset=await readMemberShopCatalogForSession(
  {DB:makeDb({assetSchema:false})},
  session,
  {as_of:'2026-09-25'}
);
pass('missing public asset schema does not hide Shop',shopNoAsset.status==='ok'&&shopNoAsset.public_assets_available===false&&shopNoAsset.products.length===1);
pass('Shop degrades to logical hero ID',shopNoAsset.products[0].hero_asset_ref.asset_id==='asset:shop:album'&&shopNoAsset.products[0].hero_asset_ref.public_asset===null);

const newsNoAsset=await readMemberNewsForSession(
  {DB:makeDb({assetSchema:false})},
  session,
  {as_of:'2026-09-25'}
);
pass('missing public asset schema does not hide NEWS',newsNoAsset.status==='ok'&&newsNoAsset.public_assets_available===false&&newsNoAsset.items.length===1);
pass('NEWS degrades to logical hero ID',newsNoAsset.items[0].hero_asset_ref.asset_id==='asset:news:autumn'&&newsNoAsset.items[0].hero_asset_ref.public_asset===null);

for(const result of [creative,shop,news]){
  const serialized=JSON.stringify(result);
  pass('enriched result exposes no actual private location fields',
    !/"storage_key":/.test(serialized)
    && !/"signed_url":/.test(serialized)
    && !/"external_url":/.test(serialized)
  );
}

const creativeHealth=memberCreativeCatalogHealth();
pass('Creative health records optional local-only public asset resolution',creativeHealth.public_asset_resolution_optional===true&&creativeHealth.public_asset_local_path_only===true&&creativeHealth.public_asset_schema_absence_degrades_locally===true);

const shopHealth=memberShopPickupHealth();
pass('Shop health records optional local-only public asset resolution',shopHealth.public_asset_resolution_optional===true&&shopHealth.public_asset_local_path_only===true&&shopHealth.public_asset_schema_absence_degrades_locally===true);

const newsHealth=memberNewsHealth();
pass('NEWS health records optional local-only public asset resolution',newsHealth.public_asset_resolution_optional===true&&newsHealth.public_asset_local_path_only===true&&newsHealth.public_asset_schema_absence_degrades_locally===true);

console.log(`MEMBER_PUBLIC_ASSET_INTEGRATION=${n}/${n} PASS`);
