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
    nextMemory:overrides.nextMemory??baseComponents.nextMemory
  });
}

const full=compose();
pass('full HOME composition succeeds',full.status==='ok'&&full.family_id===familyId&&full.customer_id===customerId);
pass('HOME keeps only three recent MEMORIES',full.home.recent_memories.length===3&&full.home.recent_memories[0].memory_id==='mem_4'&&full.home.recent_memories[2].memory_id==='mem_2');
pass('HOME reports loaded visible MEMORY count without inventing DB total',full.home.visible_memory_count===4);
pass('HOME composes FAMILY PASS',full.home.family_pass.family_pass.current_tier==='SILVER');
pass('HOME composes FAMILY PASSPORT',full.home.family_passport.achieved_count===3);
pass('HOME composes NEXT MEMORY',full.home.next_memory.next_memory.type==='first_birthday');
pass('HOME caps NEXT MEMORY candidate preview at three',full.home.next_memory.candidates.length===3);
pass('HOME keeps LINE consultation CTA non-automatic',full.home.next_memory.consultation_cta.channel==='line'&&full.home.next_memory.consultation_cta.automatic_send===false);
pass('HOME does not claim Family history is child specific',full.home.next_memory.family_history_is_child_specific===false&&full.home.next_memory.child_memory_link_available===false);
pass('full HOME is not partial',full.home.partial===false&&full.home.unavailable_sections.length===0);
pass('future HOME modules stay inactive',full.home.future_modules.today_memory===false&&full.home.future_modules.creative===false&&full.home.future_modules.shop_pickup===false&&full.home.future_modules.news===false);
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
  mediaSchema=true
}={}){
  const tables=new Set([
    'customer_family_groups',
    'customer_family_customer_links',
    ...(childSchema?['customer_family_members']:[]),
    ...(memorySchema?['member_memories']:[]),
    ...(mediaSchema?['member_memory_media']:[])
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
pass('integrated HOME reads MEMORIES',integrated.home.recent_memories.length===2&&integrated.home.recent_memories[0].memory_id==='mem_new');
pass('integrated HOME reads FAMILY PASS',integrated.home.family_pass.family_pass.current_tier==='WELCOME_BACK');
pass('integrated HOME reads FAMILY PASSPORT',integrated.home.family_passport.achieved_count===2);
pass('integrated HOME reads NEXT MEMORY from canonical child',integrated.home.next_memory.next_memory?.type==='first_birthday');
pass('integrated HOME performs zero writes',db.writes.length===0&&integrated.read_only===true);

const childMissingDb=makeDb({childSchema:false});
const integratedPartial=await readMemberHomeForSession(
  {DB:childMissingDb},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('integrated HOME degrades only NEXT MEMORY when child schema is absent',integratedPartial.status==='ok'&&integratedPartial.home.partial===true&&integratedPartial.home.next_memory===null&&integratedPartial.home.recent_memories.length===2);

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
pass('health records four composed components',health.components.join(',')==='member_memories,family_pass,family_passport,next_memory');
pass('health requires component identity consistency',health.component_identity_consistency_required===true&&health.identity_security_failures_fail_closed===true);
pass('health records MEMORIES as core and NEXT MEMORY optional degradation',health.memories_core_required===true&&health.next_memory_optional_schema_degradation===true);
pass('health records response limits',health.recent_memory_limit===3&&health.next_memory_candidate_limit===3);
pass('health keeps future modules inactive',health.today_memory_active===false&&health.creative_active===false&&health.shop_pickup_active===false&&health.news_active===false);
pass('health records no contact/send/reservation/write',health.automatic_contact===false&&health.line_send===false&&health.reservation_creation===false&&health.production_write===false);
pass('health records source-only route state',health.production_route_wired===false&&health.read_only===true&&health.request_customer_id_input===false&&health.request_family_id_input===false&&health.request_as_of_input===false);

console.log(`MEMBER_HOME_READ_MODEL=${n}/${n} PASS`);
