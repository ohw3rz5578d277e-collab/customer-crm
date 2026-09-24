import {
  readMemberNextMemoryForSession,
  handleMemberNextMemoryReadRequest,
  memberNextMemoryReadHealth,
  __test
} from '../src/member-next-memory-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const spouseCustomerId='26000456';
const familyId='fam_A';

function makeDb({
  linkedCustomerIds=[customerId,spouseCustomerId],
  childrenByCustomer={},
  memoryRows=[],
  familySchema=true,
  childSchema=true,
  memorySchema=true
}={}){
  const tables=new Set([
    ...(familySchema?['customer_family_groups','customer_family_customer_links']:[]),
    ...(childSchema?['customer_family_members']:[]),
    ...(memorySchema?['member_memories']:[])
  ]);
  const seenSql=[];
  const writes=[];

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
              ?{family_id:familyId,display_name:'YAMADA FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            return linkedCustomerIds.includes(state.params[0])
              ?{results:[{family_id:familyId,customer_id:state.params[0],relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return state.params[0]===familyId
              ?{results:linkedCustomerIds.map((id,i)=>({
                  customer_id:id,
                  relation:i===0?'owner':'spouse',
                  access_role:i===0?'owner':'member',
                  created_at:`2026-01-0${i+1}`
                }))}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_members')){
            const id=state.params[0];
            return {
              results:(childrenByCustomer[id]||[]).map(x=>({
                ...x,
                customer_id:id,
                relation:'child',
                deleted_at:''
              }))
            };
          }
          if(sql.includes('FROM member_memories')){
            return {
              results:state.params[0]===familyId
                ?memoryRows.filter(x=>x.family_id===familyId)
                :[]
            };
          }
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error('NEXT MEMORY read model must remain read-only');
        }
      };
      return stmt;
    }
  };
}

const asOf='2026-09-24';

const firstBirthday=__test.buildNextMemoryCandidates(
  [{id:'child_1',relation:'child',name:'A',birthdate:'2025-10-01',school_stage:'',source_customer_id:customerId}],
  [],
  asOf
);
pass('upcoming first birthday is primary',firstBirthday.next_memory?.type==='first_birthday');
pass('first birthday target date is exact from managed birthdate',firstBirthday.next_memory?.target_date==='2026-10-01'&&firstBirthday.next_memory?.days_until===7);
pass('generic birthday is deduped when specific first birthday shares date',firstBirthday.candidates.filter(x=>x.target_date==='2026-10-01').length===1);

const shichigosan=__test.buildNextMemoryCandidates(
  [{id:'child_3',relation:'child',name:'B',birthdate:'2023-10-10',school_stage:'',source_customer_id:customerId}],
  ['七五三'],
  asOf
);
pass('upcoming age-three milestone becomes 七五三 candidate',shichigosan.next_memory?.type==='shichigosan'&&shichigosan.next_memory?.target_date==='2026-10-10');
pass('Family 七五三 history is context only',shichigosan.next_memory?.family_level_history_match===true&&shichigosan.next_memory?.history_scope==='family_not_child');
pass('past Family genre does not suppress a new child-age candidate',shichigosan.candidates.some(x=>x.type==='shichigosan'));

const noSchoolEstimate=__test.buildNextMemoryCandidates(
  [{id:'child_6',relation:'child',name:'C',birthdate:'2020-10-01',school_stage:'',source_customer_id:customerId}],
  [],
  asOf
);
pass('birthdate age six does not infer school entrance or graduation timing',!noSchoolEstimate.candidates.some(x=>x.type==='school_entry_candidate'||x.type==='graduation_candidate'));

const noGraduationEstimate=__test.buildNextMemoryCandidates(
  [{id:'child_12',relation:'child',name:'C2',birthdate:'2014-10-01',school_stage:'',source_customer_id:customerId}],
  [],
  asOf
);
pass('birthdate age twelve does not infer graduation timing',!noGraduationEstimate.candidates.some(x=>x.type==='graduation_candidate'));

const explicitEntrance=__test.buildNextMemoryCandidates(
  [{id:'child_stage_1',relation:'child',name:'D',birthdate:'',school_stage:'年長',source_customer_id:customerId}],
  [],
  asOf
);
pass('explicit 年長 school stage creates school entrance candidate without birthdate',explicitEntrance.candidates.some(x=>x.type==='school_entry_candidate'&&x.source==='school_stage'));
pass('school stage candidate has no invented target date',explicitEntrance.candidates.find(x=>x.type==='school_entry_candidate')?.target_date===null);

const explicitGraduation=__test.buildNextMemoryCandidates(
  [{id:'child_stage_2',relation:'child',name:'E',birthdate:'',school_stage:'小学6年',source_customer_id:customerId}],
  [],
  asOf
);
pass('explicit 小学6年 creates graduation candidate',explicitGraduation.candidates.some(x=>x.type==='graduation_candidate'&&x.label==='小学校卒業候補'));
pass('graduation school stage candidate has no invented date',explicitGraduation.candidates.find(x=>x.type==='graduation_candidate')?.days_until===null);

pass('invalid calendar date is rejected',__test.dateOnly('2026-02-31')==='');
pass('valid calendar date is normalized',__test.dateOnly('2026-02-28T12:00:00Z')==='2026-02-28');

const db=makeDb({
  childrenByCustomer:{
    [customerId]:[
      {id:'child_owner',name:'Owner Child',birthdate:'2025-10-01',school_stage:'',created_at:'2025-01-01'}
    ],
    [spouseCustomerId]:[
      {id:'child_spouse',name:'Spouse Child',birthdate:'2023-10-10',school_stage:'年長',created_at:'2025-01-02'}
    ]
  },
  memoryRows:[
    {memory_id:'mem_1',family_id:familyId,shoot_date:'2025-10-01',genre:'1歳バースデー'},
    {memory_id:'mem_2',family_id:familyId,shoot_date:'2026-01-01',genre:'ファミリー'}
  ]
});
const result=await readMemberNextMemoryForSession(
  {DB:db},
  {family_id:familyId,customer_id:customerId},
  {as_of:asOf}
);
pass('authorized Family session reads NEXT MEMORY',result.status==='ok'&&result.family_id===familyId&&result.customer_id===customerId);
pass('all exact linked Customer IDs are used as Family child sources',result.family_customer_count===2&&result.canonical_child_count===2);
pass('published Family MEMORY genres are retained as family-level context',result.family_genre_history.includes('1歳バースデー')&&result.family_genre_history.includes('ファミリー'));
pass('history remains explicitly non-child-specific',result.family_history_is_child_specific===false&&result.child_memory_link_available===false);
pass('LINE CTA is consultation-only and never automatic send',result.consultation_cta.channel==='line'&&result.consultation_cta.intent==='consultation'&&result.consultation_cta.automatic_send===false);
pass('read model performs zero writes',result.read_only===true&&db.writes.length===0);

const childSql=db.seenSql.find(sql=>sql.includes('FROM customer_family_members'))||'';
pass('child query is exact Customer ID scoped',childSql.includes('CAST(customer_id AS TEXT)=?'));
pass('child query accepts only relation child and non-deleted rows',childSql.includes("relation='child'")&&childSql.includes("COALESCE(deleted_at,'')=''"));
const memorySql=db.seenSql.find(sql=>sql.includes('FROM member_memories'))||'';
pass('MEMORY query is exact Family scoped',memorySql.includes('WHERE family_id=?'));
pass('MEMORY query is published-only and deleted-hidden',memorySql.includes('published=1')&&memorySql.includes("COALESCE(deleted_at,'')=''"));

const sameNameDb=makeDb({
  childrenByCustomer:{
    [customerId]:[
      {id:'child_exact_a',name:'同じ名前',birthdate:'2024-01-01',school_stage:'',created_at:''}
    ],
    [spouseCustomerId]:[
      {id:'child_exact_b',name:'同じ名前',birthdate:'2024-01-01',school_stage:'',created_at:''}
    ]
  }
});
const sameName=await readMemberNextMemoryForSession(
  {DB:sameNameDb},
  {family_id:familyId,customer_id:customerId},
  {as_of:asOf}
);
pass('same child name and birthdate are not fuzzy-deduplicated',sameName.status==='ok'&&sameName.canonical_child_count===2);

const ambiguousIdDb=makeDb({
  childrenByCustomer:{
    [customerId]:[
      {id:'child_shared',name:'A',birthdate:'2024-01-01',school_stage:'',created_at:''}
    ],
    [spouseCustomerId]:[
      {id:'child_shared',name:'B',birthdate:'2024-01-01',school_stage:'',created_at:''}
    ]
  }
});
const ambiguous=await readMemberNextMemoryForSession(
  {DB:ambiguousIdDb},
  {family_id:familyId,customer_id:customerId},
  {as_of:asOf}
);
pass('conflicting exact child ID across linked customers fails closed',ambiguous.status==='ambiguous_child_identity'&&ambiguous.review_required===true);

const crossFamily=await readMemberNextMemoryForSession(
  {DB:makeDb()},
  {family_id:'fam_B',customer_id:customerId},
  {as_of:asOf}
);
pass('cross-Family session is denied before child data is returned',crossFamily.status==='family_access_denied');

const missingChildSchema=await readMemberNextMemoryForSession(
  {DB:makeDb({childSchema:false})},
  {family_id:familyId,customer_id:customerId},
  {as_of:asOf}
);
pass('missing managed child schema is explicit',missingChildSchema.status==='child_profile_schema_not_applied');

const missingMemorySchema=await readMemberNextMemoryForSession(
  {DB:makeDb({memorySchema:false})},
  {family_id:familyId,customer_id:customerId},
  {as_of:asOf}
);
pass('missing Member MEMORY schema is explicit',missingMemorySchema.status==='member_memory_schema_not_applied');

const noSession=await handleMemberNextMemoryReadRequest(
  new Request('https://example.test/api/internal/member/next-memory?as_of=1999-01-01'),
  {DB:makeDb()},
  null
);
pass('NEXT MEMORY HTTP contract requires server Member session',noSession.status===401);

const post=await handleMemberNextMemoryReadRequest(
  new Request('https://example.test/api/internal/member/next-memory',{method:'POST'}),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
pass('NEXT MEMORY HTTP contract is GET only',post.status===405);

const health=memberNextMemoryReadHealth();
pass('health records exact Family and child identity boundaries',health.explicit_family_link_required===true&&health.family_linked_customer_ids_only===true&&health.child_identity_by_exact_id_only===true);
pass('health records no child-name identity inference',health.child_name_identity_inference===false);
pass('health records no birthdate-based school timing inference',health.school_timing_birthdate_inference===false&&health.school_stage_explicit_only===true);
pass('health records Family history is not child-specific',health.child_memory_link_available===false&&health.family_history_is_child_specific===false);
pass('health records request cannot supply as_of',health.request_as_of_input===false);
pass('health records shared birthdate logic without marketing priority',health.shared_birthdate_opportunity_logic==='crm-customer360-marketing-engine'&&health.marketing_priority_used===false);
pass('health records no marketing draft/contact/write behavior',health.line_draft_used===false&&health.automatic_contact===false&&health.line_send===false&&health.production_write===false);
pass('health records source-only route state',health.production_route_wired===false&&health.read_only===true);

console.log(`MEMBER_NEXT_MEMORY_READ_MODEL=${n}/${n} PASS`);
