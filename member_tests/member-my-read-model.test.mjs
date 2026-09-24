import {
  readMemberMyForSession,
  handleMemberMyReadRequest,
  memberMyReadHealth,
  __test
} from '../src/member-my-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

const session={family_id:familyId,customer_id:customerId};

const family={
  status:'linked',
  customer_id:customerId,
  family:{
    family_id:familyId,
    display_name:'YAMADA FAMILY',
    relation:'owner',
    access_role:'owner',
    member_customer_ids:[customerId,'26000456']
  }
};

const familyPass={
  status:'ok',
  family_id:familyId,
  customer_id:customerId,
  family_pass:{
    memory_count:3,
    current_tier:'SILVER',
    effective_black:false
  },
  entitlement:{
    schema_applied:false,
    durable_black:false
  },
  benefit_contract:{
    black_photo_goods_discount_percent:10,
    applies_to_shooting_fee:false,
    enforcement_ready:false
  }
};

const passport={
  status:'ok',
  family_id:familyId,
  customer_id:customerId,
  passport:{
    achieved_count:2,
    total_milestones:4,
    milestones:[
      {code:'OMIYAMAIRI',label:'お宮参り',achieved:true},
      {code:'FIRST_BIRTHDAY',label:'1st Birthday',achieved:true},
      {code:'SHICHIGOSAN',label:'七五三',achieved:false},
      {code:'SCHOOL_ENTRANCE',label:'入学',achieved:false}
    ]
  }
};

const favorites={
  status:'ok',
  family_id:familyId,
  customer_id:customerId,
  favorite_count:2,
  mutable:false
};

const nextMemory={
  status:'ok',
  family_id:familyId,
  customer_id:customerId,
  canonical_child_count:1,
  next_memory:{
    type:'birthday',
    label:'バースデー',
    target_date:'2026-10-10'
  },
  consultation_cta:{
    channel:'line',
    intent:'consultation',
    automatic_send:false
  }
};

const composed=__test.composeMemberMyModel({
  session,
  family,
  familyPass,
  passport,
  favorites,
  nextMemory
});

pass('MY composition succeeds',composed.status==='ok'&&composed.read_only===true);
pass('MY family summary exposes display metadata only',composed.my.family.display_name==='YAMADA FAMILY'&&composed.my.family.linked_member_count===2);
pass('MY family summary hides linked Customer ID list',!('member_customer_ids' in composed.my.family));
pass('MY uses Family Pass as authoritative MEMORY count',composed.my.memory_count===3&&composed.my.current_tier==='SILVER');
pass('MY composes Family Passport',composed.my.family_passport.achieved_count===2);
pass('MY composes exact Member Favorite count',composed.my.favorite_count===2);
pass('MY composes Next Memory',composed.my.next_memory.next_memory.type==='birthday');
pass('MY keeps consultation CTA non-automatic',composed.my.next_memory.consultation_cta.automatic_send===false);
pass('MY settings remain non-mutable placeholders',Object.values(composed.my.settings).every(value=>value===false));
pass('MY capabilities do not enable edits/send/reservation',Object.values(composed.my.capabilities).every(value=>value===false));
pass('full MY composition is not partial',composed.my.partial===false&&composed.my.unavailable_sections.length===0);

const favoritesMissing=__test.composeMemberMyModel({
  session,
  family,
  familyPass,
  passport,
  favorites:{status:'favorites_schema_not_applied',favorites:[],read_only:true},
  nextMemory
});
pass('missing Favorite schema degrades Favorites section only',favoritesMissing.status==='ok'&&favoritesMissing.my.partial===true&&favoritesMissing.my.favorite_count===null);
pass('Favorite degradation reason is explicit',favoritesMissing.my.unavailable_sections.some(x=>x.section==='favorites'&&x.error==='favorites_schema_not_applied'));

const passportMissing=__test.composeMemberMyModel({
  session,
  family,
  familyPass,
  passport:{status:'passport_unavailable',passport:null,read_only:true},
  favorites,
  nextMemory
});
pass('Passport non-security failure degrades locally',passportMissing.status==='ok'&&passportMissing.my.family_passport===null&&passportMissing.my.partial===true);

const childSchemaMissing=__test.composeMemberMyModel({
  session,
  family,
  familyPass,
  passport,
  favorites,
  nextMemory:{status:'child_profile_schema_not_applied',next_memory:null,candidates:[],read_only:true}
});
pass('missing child schema degrades Next Memory only',childSchemaMissing.status==='ok'&&childSchemaMissing.my.next_memory===null&&childSchemaMissing.my.partial===true);

const ambiguousChild=__test.composeMemberMyModel({
  session,
  family,
  familyPass,
  passport,
  favorites,
  nextMemory:{status:'ambiguous_child_identity',review_required:true,read_only:true}
});
pass('ambiguous child identity fails MY closed',ambiguousChild.status==='ambiguous_child_identity'&&ambiguousChild.review_required===true);

const wrongComponentIdentity=__test.composeMemberMyModel({
  session,
  family,
  familyPass:{...familyPass,family_id:'fam_B'},
  passport,
  favorites,
  nextMemory
});
pass('component Family mismatch fails MY closed',wrongComponentIdentity.status==='component_identity_mismatch'&&wrongComponentIdentity.review_required===true);

const wrongCustomerIdentity=__test.composeMemberMyModel({
  session,
  family,
  familyPass,
  passport,
  favorites:{...favorites,customer_id:'26000999'},
  nextMemory
});
pass('component Customer mismatch fails MY closed',wrongCustomerIdentity.status==='component_identity_mismatch');

const familyPassMissing=__test.composeMemberMyModel({
  session,
  family,
  familyPass:{status:'member_memory_schema_not_applied',read_only:true},
  passport,
  favorites,
  nextMemory
});
pass('Family Pass is core and cannot silently degrade',familyPassMissing.status==='member_memory_schema_not_applied');

const wrongFamily=__test.composeMemberMyModel({
  session,
  family:{...family,family:{...family.family,family_id:'fam_B'}},
  familyPass,
  passport,
  favorites,
  nextMemory
});
pass('Family identity mismatch fails MY closed',wrongFamily.status==='family_access_denied');

pass('family summary ignores malformed linked Customer IDs',__test.safeFamilySummary({
  family:{
    display_name:'A',
    relation:'owner',
    access_role:'owner',
    member_customer_ids:[customerId,'bad','26000002']
  }
}).linked_member_count===2);

function makeDb({
  favoriteSchema=true,
  childSchema=true,
  ambiguousFamily=false
}={}){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    'member_memories',
    ...(favoriteSchema?['member_memory_favorites']:[]),
    ...(childSchema?['customer_family_members']:[])
  ]);

  const memories=[
    {
      memory_id:'mem_1',
      family_id:familyId,
      shoot_date:'2024-11-01',
      genre:'お宮参り',
      published:1,
      deleted_at:''
    },
    {
      memory_id:'mem_2',
      family_id:familyId,
      shoot_date:'2025-10-10',
      genre:'1歳バースデー',
      published:1,
      deleted_at:''
    },
    {
      memory_id:'mem_3',
      family_id:familyId,
      shoot_date:'2026-05-05',
      genre:'ファミリー',
      published:1,
      deleted_at:''
    }
  ];

  const writes=[];

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
              ?{
                  family_id:familyId,
                  display_name:'YAMADA FAMILY',
                  status:'active',
                  created_at:'',
                  updated_at:''
                }
              :null;
          }

          if(sql.includes('COUNT(*) AS memory_count')){
            return {memory_count:3};
          }

          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            if(state.params[0]!==customerId)return {results:[]};
            return {
              results:ambiguousFamily
                ?[
                    {family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'},
                    {family_id:'fam_B',customer_id:customerId,relation:'owner',access_role:'owner'}
                  ]
                :[
                    {family_id:familyId,customer_id:customerId,relation:'owner',access_role:'owner'}
                  ]
            };
          }

          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return state.params[0]===familyId
              ?{results:[
                  {customer_id:customerId,relation:'owner',access_role:'owner'}
                ]}
              :{results:[]};
          }

          if(sql.includes('FROM member_memory_favorites')){
            return favoriteSchema
              ?{results:[{memory_id:'mem_2',created_at:'2026-09-20T00:00:00Z'}]}
              :{results:[]};
          }

          if(sql.includes('FROM customer_family_members')){
            return childSchema
              ?{results:[{
                  id:'child_1',
                  customer_id:customerId,
                  relation:'child',
                  name:'TARO',
                  birthdate:'2023-10-10',
                  school_stage:'',
                  deleted_at:''
                }]}
              :{results:[]};
          }

          if(sql.includes('FROM member_memories')){
            return {results:memories.map(row=>({...row}))};
          }

          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('Member MY read model must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const integrationDb=makeDb();
const integrated=await readMemberMyForSession(
  {DB:integrationDb},
  session,
  {as_of:'2026-09-25'}
);
pass('integrated MY succeeds through existing component readers',integrated.status==='ok'&&integrated.my.memory_count===3);
pass('integrated MY reads Favorite count',integrated.my.favorite_count===1);
pass('integrated MY reads Passport from existing MEMORY genres',integrated.my.family_passport.achieved_count===2);
pass('integrated MY reads canonical child count through Next Memory',integrated.my.next_memory.canonical_child_count===1);
pass('integrated MY performs zero writes',integrationDb.writes.length===0);

const integratedFavoriteMissing=await readMemberMyForSession(
  {DB:makeDb({favoriteSchema:false})},
  session,
  {as_of:'2026-09-25'}
);
pass('integrated MY degrades missing Favorites schema',integratedFavoriteMissing.status==='ok'&&integratedFavoriteMissing.my.favorite_count===null&&integratedFavoriteMissing.my.partial===true);

const integratedChildMissing=await readMemberMyForSession(
  {DB:makeDb({childSchema:false})},
  session,
  {as_of:'2026-09-25'}
);
pass('integrated MY degrades missing child profile schema',integratedChildMissing.status==='ok'&&integratedChildMissing.my.next_memory===null&&integratedChildMissing.my.partial===true);

const integratedAmbiguousFamily=await readMemberMyForSession(
  {DB:makeDb({ambiguousFamily:true})},
  session
);
pass('integrated MY fails closed on ambiguous Family identity',integratedAmbiguousFamily.status==='ambiguous_family_identity'&&integratedAmbiguousFamily.review_required===true);

const noSession=await handleMemberMyReadRequest(
  new Request('https://example.test/api/internal/member/my'),
  {DB:makeDb()},
  null
);
pass('MY HTTP requires server Member session',noSession.status===401);

const post=await handleMemberMyReadRequest(
  new Request('https://example.test/api/internal/member/my',{method:'POST'}),
  {DB:makeDb()},
  session
);
pass('MY HTTP is GET only',post.status===405);

const tamperedQuery=await handleMemberMyReadRequest(
  new Request('https://example.test/api/internal/member/my?customer_id=26000999&family_id=fam_B&as_of=2035-01-01'),
  {DB:makeDb()},
  session
);
const tamperedBody=await tamperedQuery.json();
pass('MY HTTP ignores request identity and clock controls',tamperedQuery.status===200&&tamperedBody.ok===true&&tamperedBody.my.memory_count===3);
pass('MY HTTP does not expose Customer or Family IDs',!('customer_id' in tamperedBody)&&!('family_id' in tamperedBody)&&!('identity' in tamperedBody));
pass('MY HTTP does not expose linked member Customer IDs',!JSON.stringify(tamperedBody).includes('26000456'));
pass('MY HTTP is no-store/no-referrer',tamperedQuery.headers.get('cache-control')==='no-store'&&tamperedQuery.headers.get('referrer-policy')==='no-referrer');

const health=memberMyReadHealth();
pass('health records MY read-only source foundation',health.member_my_read_model===true&&health.read_only===true&&health.production_route_wired===false);
pass('health records Family identity and Family Pass as core',health.family_identity_core_required===true&&health.family_pass_core_required===true);
pass('health records existing Family Pass MEMORY count source',health.memory_count_source==='family_pass_published_member_memories');
pass('health records optional Passport/Favorites/Next degradation',health.family_passport_optional_degradation===true&&health.favorites_optional_degradation===true&&health.next_memory_optional_schema_degradation===true);
pass('health records public identity minimization',health.customer_id_public_response===false&&health.family_id_public_response===false&&health.family_member_customer_ids_public_response===false);
pass('health keeps all MY mutations inactive',health.family_profile_edit_ready===false&&health.member_profile_edit_ready===false&&health.favorites_mutation_ready===false&&health.notification_settings_mutation_ready===false);
pass('health records no reservation/contact/send/write',health.reservation_creation===false&&health.automatic_contact===false&&health.line_send===false&&health.production_write===false);

console.log(`MEMBER_MY_READ_MODEL=${n}/${n} PASS`);
