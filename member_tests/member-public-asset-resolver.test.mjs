import fs from 'node:fs';
import {
  resolveMemberPublicAssets,
  handleMemberPublicAssetResolveRequest,
  memberPublicAssetResolverHealth,
  __test
} from '../src/member-public-asset-resolver.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const image={
  asset_id:'asset:shop:album',
  asset_kind:'image',
  local_path:'/member-assets/shop/album.webp',
  mime_type:'image/webp',
  width:1600,
  height:1200
};

pass('logical asset id accepted',__test.validAssetId('asset:shop:album')===true);
pass('asset id outer whitespace rejected',__test.validAssetId(' asset:shop:album')===false);
pass('asset id control character rejected',__test.validAssetId('asset:shop:album\n')===false);
pass('trusted local asset path accepted',__test.validLocalAssetPath('/member-assets/shop/album.webp')===true);
pass('external URL rejected',__test.validLocalAssetPath('https://cdn.example/album.webp')===false);
pass('protocol-relative URL rejected',__test.validLocalAssetPath('//cdn.example/album.webp')===false);
pass('wrong local prefix rejected',__test.validLocalAssetPath('/assets/album.webp')===false);
pass('literal traversal rejected',__test.validLocalAssetPath('/member-assets/../secret')===false);
pass('percent-encoded traversal surface rejected',__test.validLocalAssetPath('/member-assets/%2e%2e/secret')===false);
pass('backslash path rejected',__test.validLocalAssetPath('/member-assets\\secret')===false);
pass('query string rejected',__test.validLocalAssetPath('/member-assets/a.webp?x=1')===false);
pass('fragment rejected',__test.validLocalAssetPath('/member-assets/a.webp#x')===false);

const normalized=__test.normalizeAssetRow(image);
pass('valid public image asset normalizes',normalized?.asset_id===image.asset_id&&normalized.public_path===image.local_path);
pass('normalized asset explicitly hides storage/signed/external sources',normalized.source.storage_key_exposed===false&&normalized.source.signed_url_exposed===false&&normalized.source.arbitrary_external_url_exposed===false&&normalized.source.private_customer_media===false);
pass('image requires image MIME',__test.normalizeAssetRow({...image,mime_type:'video/mp4'})===null);
pass('image requires valid dimensions',__test.normalizeAssetRow({...image,width:null})===null);
pass('video mp4 accepted',__test.normalizeAssetRow({
  asset_id:'asset:creative:movie',
  asset_kind:'video',
  local_path:'/member-assets/creative/movie.mp4',
  mime_type:'video/mp4',
  width:1920,
  height:1080
})?.asset_kind==='video');
pass('video non-mp4 rejected',__test.normalizeAssetRow({...image,asset_kind:'video',mime_type:'image/webp'})===null);

const ids=__test.normalizeRequestedAssetIds(['asset:a','asset:a','asset:b']);
pass('requested asset IDs dedupe while preserving order',ids.join(',')==='asset:a,asset:b');
pass('empty asset request rejected',__test.normalizeRequestedAssetIds([])===null);
pass('more than twenty asset IDs rejected',__test.normalizeRequestedAssetIds(Array.from({length:21},(_,i)=>`asset:${i}`))===null);
pass('body accepts only asset_ids key',!!__test.exactResolveBody({asset_ids:['asset:a']}));
pass('body rejects extra keys',__test.exactResolveBody({asset_ids:['asset:a'],customer_id:'26000001'})===null);

function makeDb({schema=true}={}){
  const rows=[
    image,
    {
      asset_id:'asset:news:autumn',
      asset_kind:'image',
      local_path:'/member-assets/news/autumn.jpg',
      mime_type:'image/jpeg',
      width:1800,
      height:1200
    },
    {
      asset_id:'asset:bad:path',
      asset_kind:'image',
      local_path:'https://evil.example/bad.jpg',
      mime_type:'image/jpeg',
      width:100,
      height:100
    }
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
          if(sql.includes('sqlite_master')){
            return schema&&state.params[0]==='member_public_assets'
              ?{name:'member_public_assets'}
              :null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM member_public_assets')){
            return {results:rows.filter(row=>state.params.includes(row.asset_id))};
          }
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('public asset resolver must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const db=makeDb();
const resolved=await resolveMemberPublicAssets(
  {DB:db},
  ['asset:news:autumn','asset:missing','asset:shop:album']
);
pass('resolver preserves requested order for resolved assets',resolved.assets.map(x=>x.asset_id).join(',')==='asset:news:autumn,asset:shop:album');
pass('resolver reports unresolved logical IDs',resolved.unresolved_asset_ids.join(',')==='asset:missing');
pass('partial resolution is not marked complete',resolved.complete===false);
pass('resolver performs zero writes',db.writes.length===0&&resolved.read_only===true);
pass('resolver output contains no private location fields',resolved.assets.every(asset=>
  !('storage_key' in asset)
  && !('signed_url' in asset)
  && !('external_url' in asset)
));
pass('resolver query requires published assets',db.seenSql.some(sql=>sql.includes('published=1')));
pass('resolver query hides deleted assets',db.seenSql.some(sql=>sql.includes("COALESCE(deleted_at,'')=''")));

const invalidRowDb=makeDb();
const invalidRow=await resolveMemberPublicAssets({DB:invalidRowDb},['asset:bad:path']);
pass('unsafe registry row is hidden rather than returned',invalidRow.status==='ok'&&invalidRow.assets.length===0&&invalidRow.unresolved_asset_ids[0]==='asset:bad:path'&&invalidRow.hidden_invalid_count===1);

const missingSchema=await resolveMemberPublicAssets(
  {DB:makeDb({schema:false})},
  ['asset:shop:album']
);
pass('missing public asset schema is explicit',missingSchema.status==='public_asset_schema_not_applied');

const noSession=await handleMemberPublicAssetResolveRequest(
  new Request('https://example.test/api/internal/member/assets/resolve',{
    method:'POST',
    headers:{'content-type':'application/json'},
    body:JSON.stringify({asset_ids:['asset:shop:album']})
  }),
  {DB:makeDb()},
  null
);
pass('HTTP resolver requires Member session boundary',noSession.status===401);

const get=await handleMemberPublicAssetResolveRequest(
  new Request('https://example.test/api/internal/member/assets/resolve'),
  {DB:makeDb()},
  {family_id:'fam_A',customer_id:'26000123'}
);
pass('HTTP resolver is POST only',get.status===405);

const badBody=await handleMemberPublicAssetResolveRequest(
  new Request('https://example.test/api/internal/member/assets/resolve',{
    method:'POST',
    headers:{'content-type':'application/json'},
    body:JSON.stringify({asset_ids:['asset:shop:album'],family_id:'fam_B'})
  }),
  {DB:makeDb()},
  {family_id:'fam_A',customer_id:'26000123'}
);
pass('HTTP body cannot inject identity or extra fields',badBody.status===400);

const okHttp=await handleMemberPublicAssetResolveRequest(
  new Request('https://example.test/api/internal/member/assets/resolve',{
    method:'POST',
    headers:{'content-type':'application/json'},
    body:JSON.stringify({asset_ids:['asset:shop:album']})
  }),
  {DB:makeDb()},
  {family_id:'fam_A',customer_id:'26000123'}
);
const okBody=await okHttp.json();
pass('HTTP resolver returns trusted local public path',okHttp.status===200&&okBody.assets[0].public_path==='/member-assets/shop/album.webp');
pass('HTTP response does not echo Member identity',!('family_id' in okBody)&&!('customer_id' in okBody));

const health=memberPublicAssetResolverHealth();
pass('health records trusted local prefix',health.local_path_prefix==='/member-assets/');
pass('health records bounded batch size',health.max_asset_ids_per_request===20);
pass('health records supported image/video formats',health.supported_asset_kinds.includes('image')&&health.supported_asset_kinds.includes('video')&&health.supported_mime_types.includes('image/webp')&&health.supported_mime_types.includes('video/mp4'));
pass('health records public asset only and private media false',health.private_customer_media===false&&health.customer_photo_delivery===false);
pass('health records no storage/signed/external exposure',health.storage_key_exposed===false&&health.signed_url_exposed===false&&health.arbitrary_external_url_exposed===false);
pass('health records source-only no Production route/write',health.production_route_wired===false&&health.production_schema_applied===false&&health.production_write===false);

const migration=fs.readFileSync(
  'migrations_managed/20260924_member_public_asset_registry_foundation.sql',
  'utf8'
);
const schemaOnly=migration.replace(/^\s*--.*$/gm,'');
pass('public asset migration is additive only',!/\b(DROP|ALTER|DELETE|UPDATE|INSERT)\b/i.test(schemaOnly));
pass('public asset schema has no private storage key or URL column',!/\b(storage_key|external_url|signed_url)\b/i.test(schemaOnly));
pass('public asset schema supports only image/video kinds',schemaOnly.includes("'image','video'"));
pass('public asset schema constrains known MIME types',schemaOnly.includes("'image/webp'")&&schemaOnly.includes("'video/mp4'"));

console.log(`MEMBER_PUBLIC_ASSET_RESOLVER=${n}/${n} PASS`);
