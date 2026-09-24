import {
  readMemberNextMemoryForSession,
  handleMemberNextMemoryReadRequest,
  memberNextMemoryReadHealth,
  __test
} from '../src/member-next-memory-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerA='26000123';
const customerB='26000456';
const familyId='fam_A';

function makeDb({
  childRowsByCustomer={},
  memoryRows=[],
  familyCustomerIds=[customerA,customerB],
  familySchema=true,
  childSchema=true,
  memorySchema=true,
  activeFamily=true
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
            return activeFamily&&state.params[0]===familyId
              ?{family_id:familyId,display_name:'TEST FAMILY',status:'active',created_at:'',updated_at:''}
              :null;
          }
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('customer_id=?')){
            const id=state.params[0];
            return familyCustomerIds.includes(id)
              ?{results:[{family_id:familyId,customer_id:id,relation:'owner',access_role:'owner'}]}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_customer_links')&&sql.includes('family_id=?')){
            return state.params[0]===familyId
              ?{results:familyCustomerIds.map((id,i)=>({
                  customer_id:id,
                  relation:i===0?'owner':'spouse',
                  access_role:i===0?'owner':'member'
                }))}
              :{results:[]};
          }
          if(sql.includes('FROM customer_family_members')){
            const id=state.params[0];
            return {results:(childRowsByCustomer[id]||[]).map(x=>({...x,customer_id:id}))};
          }
          if(sql.includes('FROM member_memories')){
            return state.params[0]===familyId?{results:memoryRows}:{results:[]};
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
const children=[
  {id:'child_first',relation:'child',name:'一歳候補',birthdate:'2025-10-05',school_stage:'',source_customer_id:customerA},
  {id:'child_753',relation:'child',name:'七五三候補',birthdate:'2023-10-15',school_stage:'',source_customer_id:customerA},
  {id:'child_school',relation:'child',name:'入学候補',birthdate:'2020-10-29',school_stage:'年長',source_customer_id:customerB}
];
const pure=__test.buildNextMemoryCandidates(
  children,
  ['お宮参り','1歳バースデー','七五三'],
  asOf
);

pass('NEXT MEMORY builds candidates from canonical child evidence',pure.candidates.length>=3);
pass('nearest candidate becomes next_memory',pure.next_memory?.type==='school_entry_candidate'&&pure.next_memory?.child?.child_id==='child_school');
pass('first birthday candidate is present',pure.candidates.some(x=>x.type==='first_birthday'&&x.child.child_id==='child_first'));
pass('three-year shichigosan candidate is present',pure.candidates.some(x=>x.type==='shichigosan'&&x.child.child_id==='child_753'));
pass('school-stage candidate is present',pure.candidates.some(x=>x.type==='school_entry_candidate'&&x.child.child_id==='child_school'));
pass('generic birthday is suppressed when same child/date has first birthday',!pure.candidates.some(x=>x.type==='birthday'&&x.child.child_id==='child_first'));
pass('generic birthday is suppressed when same child/date has shichigosan',!pure.candidates.some(x=>x.type==='birthday'&&x.child.child_id==='child_753'));
pass('family genre history is not presented as child-specific',pure.family_history_is_child_specific===false&&pure.child_memory_link_available===false);
pass('family history match can annotate candidate without suppressing it',pure.candidates.some(x=>x.type==='first_birthday'&&x.family_level_history_match===true&&x.history_scope==='family_not_child'));
pass('NEXT MEMORY never enables automatic contact',pure.automatic_contact===false&&pure.candidates.every(x=>x.automatic_contact===false));

pass('generic family birthday history exact match is recognized',__test.genreEvidenceForType('birthday',['バースデー'])===true);
pass('generic family birthday history does not imply first birthday',__test.genreEvidenceForType('first_birthday',['バースデー'])===false);
pass('七五三 family evidence uses exact normalized genre',__test.genreEvidenceForType('shichigosan',['七五三'])===true);
pass('七五三 substring is not treated as exact history evidence',__test.genreEvidenceForType('shichigosan',['七五三後のファミリー'])===false);

const childRowsByCustomer={
  [customerA]:[
    {id:'child_first',relation:'child',name:'一歳候補',birthdate:'2025-10-05',school_stage:'',deleted_at:null}
  ],
  [customerB]:[
    {id:'child_school',relation:'child',name:'入学候補',birthdate:'2020-10-29',school_stage:'年長',deleted_at:null}
  ]
};
const memoryRows=[
  {memory_id:'mem_1',shoot_date:'2025-01-01',genre:'お宮参り'},
  {memory_id:'mem_2',shoot_date:'2026-01-01',genre:'1歳バースデー'}
];

const db=makeDb({childRowsByCustomer,memoryRows});
const result=await readMemberNextMemoryForSession(
  {DB:db},
  {family_id:familyId,customer_id:customerA},
  {as_of:asOf}
);
pass('authorized Member session reads NEXT MEMORY',result.status==='ok'&&result.family_id===familyId&&result.customer_id===customerA);
pass('all explicitly Family-linked canonical Customer IDs are read',result.family_customer_count===2);
pass('children from multiple explicitly linked Customer IDs can contribute',result.canonical_child_count===2);
pass('published MEMORY history is Family-scoped evidence',result.published_memory_count===2&&result.family_genre_history.includes('お宮参り'));
pass('result remains read-only',result.read_only===true&&db.writes.length===0);

const childSql=db.seenSql.find(sql=>sql.includes('FROM customer_family_members'))||'';
pass('child query is exact Customer-ID scoped',childSql.includes('CAST(customer_id AS TEXT)=?'));
pass('child query requires relation child and non-deleted row',childSql.includes("relation='child'")&&childSql.includes("COALESCE(deleted_at,'')=''"));
const memorySql=db.seenSql.find(sql=>sql.includes('FROM member_memories'))||'';
pass('MEMORY query is exact Family scoped and published only',memorySql.includes('WHERE family_id=?')&&memorySql.includes('published=1'));
pass('MEMORY query hides deleted rows',memorySql.includes("COALESCE(deleted_at,'')=''"));

const noChildrenDb=makeDb({
  childRowsByCustomer:{},
  memoryRows
});
const noChildren=await readMemberNextMemoryForSession(
  {DB:noChildrenDb},
  {family_id:familyId,customer_id:customerA},
  {as_of:asOf}
);
pass('no canonical child evidence yields no recommendation rather than inference',noChildren.status==='ok'&&noChildren.next_memory===null&&noChildren.candidates.length===0);

const invalidBirthDb=makeDb({
  childRowsByCustomer:{
    [customerA]:[{id:'child_bad',relation:'child',name:'不明',birthdate:'not-a-date',school_stage:'年長',deleted_at:null}]
  },
  memoryRows
});
const invalidBirth=await readMemberNextMemoryForSession(
  {DB:invalidBirthDb},
  {family_id:familyId,customer_id:customerA},
  {as_of:asOf}
);
pass('invalid birthdate does not generate age-based recommendation',invalidBirth.status==='ok'&&!invalidBirth.candidates.some(x=>x.child.child_id==='child_bad'));

const missingChildSchema=await readMemberNextMemoryForSession(
  {DB:makeDb({childSchema:false,memoryRows})},
  {family_id:familyId,customer_id:customerA},
  {as_of:asOf}
);
pass('missing child profile schema is explicit',missingChildSchema.status==='child_profile_schema_not_applied');

const missingMemorySchema=await readMemberNextMemoryForSession(
  {DB:makeDb({memorySchema:false,childRowsByCustomer})},
  {family_id:familyId,customer_id:customerA},
  {as_of:asOf}
);
pass('missing Member MEMORY schema is explicit',missingMemorySchema.status==='member_memory_schema_not_applied');

const crossFamily=await readMemberNextMemoryForSession(
  {DB:makeDb({childRowsByCustomer,memoryRows})},
  {family_id:'fam_B',customer_id:customerA},
  {as_of:asOf}
);
pass('cross-Family session is denied',crossFamily.status==='family_access_denied');

const duplicateChildConflict=await readMemberNextMemoryForSession(
  {DB:makeDb({
    childRowsByCustomer:{
      [customerA]:[{id:'same_child',relation:'child',name:'A',birthdate:'2025-01-01',school_stage:'',deleted_at:null}],
      [customerB]:[{id:'same_child',relation:'child',name:'B',birthdate:'2024-01-01',school_stage:'',deleted_at:null}]
    },
    memoryRows
  })},
  {family_id:familyId,customer_id:customerA},
  {as_of:asOf}
);
pass('conflicting exact child ID across linked Customers fails closed',duplicateChildConflict.status==='ambiguous_child_identity'&&duplicateChildConflict.review_required===true);

const noSession=await handleMemberNextMemoryReadRequest(
  new Request('https://example.test/api/internal/member/next-memory'),
  {DB:makeDb({childRowsByCustomer,memoryRows})},
  null
);
pass('HTTP contract requires server Member session',noSession.status===401);

const post=await handleMemberNextMemoryReadRequest(
  new Request('https://example.test/api/internal/member/next-memory',{method:'POST'}),
  {DB:makeDb({childRowsByCustomer,memoryRows})},
  {family_id:familyId,customer_id:customerA}
);
pass('HTTP contract is GET only',post.status===405);

const health=memberNextMemoryReadHealth();
pass('health records exact Family-linked Customer source only',health.family_linked_customer_ids_only===true&&health.explicit_family_link_required===true);
pass('health forbids child-name identity inference',health.child_identity_by_exact_id_only===true&&health.child_name_identity_inference===false);
pass('health records no child-specific MEMORY link',health.child_memory_link_available===false&&health.family_history_is_child_specific===false);
pass('health reuses shared opportunity logic but not marketing priority',health.shared_opportunity_logic==='crm-customer360-marketing-engine'&&health.marketing_priority_used===false);
pass('health records no LINE draft, automatic contact, send or Production write',health.line_draft_used===false&&health.automatic_contact===false&&health.line_send===false&&health.production_write===false);
pass('health records no Production route wiring',health.production_route_wired===false&&health.request_as_of_input===false);

console.log(`MEMBER_NEXT_MEMORY_READ_MODEL=${n}/${n} PASS`);
