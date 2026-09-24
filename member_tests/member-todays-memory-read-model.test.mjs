import {
  readMemberTodaysMemoryForSession,
  handleMemberTodaysMemoryReadRequest,
  memberTodaysMemoryReadHealth,
  __test
} from '../src/member-todays-memory-read-model.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const customerId='26000123';
const familyId='fam_A';

function memory(id,date,title='MEMORY',genre='ファミリー'){
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

pass('valid date-only accepts leap day in leap year',__test.validDateOnly('2024-02-29')==='2024-02-29');
pass('invalid leap day is rejected',__test.validDateOnly('2025-02-29')==='');
pass('invalid calendar day is rejected',__test.validDateOnly('2026-09-31')==='');
pass('timestamp is not accepted as as_of',__test.validDateOnly('2026-09-24T00:00:00Z')==='');

const exact=__test.buildTodaysMemory([
  memory('mem_2025','2025-09-24','2025'),
  memory('mem_2024','2024-09-24','2024'),
  memory('mem_2023','2023-09-24','2023'),
  memory('mem_2022','2022-09-24','2022'),
  memory('mem_near','2025-09-22','near')
],'2026-09-24');

pass('exact anniversary mode wins over nearby seasonal memories',exact.status==='ok'&&exact.today_memory.mode==='exact_anniversary');
pass('exact anniversary headline is honest',exact.today_memory.headline==='この日の思い出');
pass('exact anniversary returns at most three memories',exact.today_memory.memories.length===3);
pass('exact anniversary chooses most recent prior year as primary',exact.today_memory.primary.memory_id==='mem_2025');
pass('exact anniversary preserves years_ago metadata',exact.today_memory.primary.anniversary.years_ago===1);
pass('exact anniversary reports no seasonal fallback',exact.today_memory.seasonal_fallback_used===false);
pass('fourth exact anniversary is omitted from preview limit',!exact.today_memory.memories.some(x=>x.memory_id==='mem_2022'));

const seasonal=__test.buildTodaysMemory([
  memory('mem_far','2025-09-10','far'),
  memory('mem_3d','2024-09-21','3d older'),
  memory('mem_2d_old','2023-09-22','2d older'),
  memory('mem_2d_new','2025-09-22','2d newer'),
  memory('mem_oct','2025-10-01','other month')
],'2026-09-24');

pass('seasonal fallback is used only when no exact anniversary exists',seasonal.today_memory.mode==='seasonal_nearby');
pass('seasonal fallback headline says この頃 rather than この日',seasonal.today_memory.headline==='この頃の思い出');
pass('seasonal fallback chooses nearest calendar day first',seasonal.today_memory.primary.memory_id==='mem_2d_new');
pass('seasonal fallback breaks equal-distance tie by newest shoot year',seasonal.today_memory.primary.anniversary.years_ago===1);
pass('seasonal fallback exposes calendar distance',seasonal.today_memory.primary.anniversary.calendar_distance_days===2);
pass('seasonal fallback returns one memory only',seasonal.today_memory.memories.length===1);
pass('different month is never seasonal fallback',seasonal.today_memory.primary.memory_id!=='mem_oct');

const none=__test.buildTodaysMemory([
  memory('mem_far','2025-09-10'),
  memory('mem_other','2025-08-24')
],'2026-09-24');
pass('no exact or ±7 same-month memory returns null rather than inventing one',none.status==='ok'&&none.today_memory===null);

const currentYear=__test.buildTodaysMemory([
  memory('mem_same_year','2026-09-24'),
  memory('mem_prior','2025-09-23')
],'2026-09-24');
pass('current-year MEMORY is excluded from anniversary logic',currentYear.today_memory?.primary?.memory_id==='mem_prior');

const leapExact=__test.buildTodaysMemory([
  memory('mem_leap','2024-02-29'),
  memory('mem_feb28','2025-02-28')
],'2028-02-29');
pass('Feb 29 exact anniversary matches past Feb 29',leapExact.today_memory.mode==='exact_anniversary'&&leapExact.today_memory.primary.memory_id==='mem_leap');

const nonLeap=__test.buildTodaysMemory([
  memory('mem_leap','2024-02-29')
],'2027-02-28');
pass('Feb 29 is not mislabeled exact on Feb 28',nonLeap.today_memory.mode==='seasonal_nearby'&&nonLeap.today_memory.primary.memory_id==='mem_leap');
pass('Feb 29 on Feb 28 uses honest seasonal label',nonLeap.today_memory.headline==='この頃の思い出');

const invalidAsOf=__test.buildTodaysMemory([memory('mem','2025-09-24')],'bad');
pass('invalid deterministic as_of fails closed',invalidAsOf.status==='invalid_as_of'&&invalidAsOf.today_memory===null);

const invalidMemoryDate=__test.buildTodaysMemory([
  memory('bad','2025-02-31'),
  memory('good','2025-09-24')
],'2026-09-24');
pass('invalid MEMORY shoot date is ignored',invalidMemoryDate.today_memory.primary.memory_id==='good');

function makeDb({
  memories=[
    {
      memory_id:'mem_exact',
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
      shoot_date:'2024-09-24',
      genre:'お宮参り',
      title:'2年前の今日',
      amazon_photos_url:'',
      published:1,
      created_at:'2024-09-24',
      updated_at:'2024-09-24'
    }
  ],
  familySchema=true,
  memorySchema=true,
  mediaSchema=true
}={}){
  const tables=new Set([
    ...(familySchema?['customer_family_groups','customer_family_customer_links']:[]),
    ...(memorySchema?['member_memories']:[]),
    ...(mediaSchema?['member_memory_media']:[])
  ]);
  const writes=[];
  const seenSql=[];

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
          if(sql.includes('FROM member_memory_media')){
            return {results:[]};
          }
          if(sql.includes('FROM member_memories')){
            return state.params[0]===familyId?{results:memories}:{results:[]};
          }
          return {results:[]};
        },
        async run(){
          writes.push({sql,params:state.params});
          throw new Error("TODAY'S MEMORY read model must remain read-only");
        }
      };
      return stmt;
    }
  };
}

const db=makeDb();
const integrated=await readMemberTodaysMemoryForSession(
  {DB:db},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('authorized Family reads TODAY\'S MEMORY',integrated.status==='ok'&&integrated.family_id===familyId&&integrated.customer_id===customerId);
pass('integrated reader returns exact Family anniversary',integrated.today_memory.mode==='exact_anniversary'&&integrated.today_memory.primary.memory_id==='mem_exact');
pass('integrated reader reports visible bounded source count',integrated.visible_memory_count===2);
pass('integrated reader performs zero writes',integrated.read_only===true&&db.writes.length===0);

const crossFamily=await readMemberTodaysMemoryForSession(
  {DB:makeDb()},
  {family_id:'fam_B',customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('cross-Family Member session fails closed',crossFamily.status==='family_access_denied');

const missingMemory=await readMemberTodaysMemoryForSession(
  {DB:makeDb({memorySchema:false})},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('missing MEMORY schema is explicit',missingMemory.status==='member_memory_schema_not_applied');

const missingMedia=await readMemberTodaysMemoryForSession(
  {DB:makeDb({mediaSchema:false})},
  {family_id:familyId,customer_id:customerId},
  {as_of:'2026-09-24'}
);
pass('missing media schema is also treated as incomplete MEMORY schema',missingMedia.status==='member_memory_schema_not_applied');

const noSession=await handleMemberTodaysMemoryReadRequest(
  new Request('https://example.test/api/internal/member/todays-memory'),
  {DB:makeDb()},
  null
);
pass('HTTP TODAY\'S MEMORY requires server Member session',noSession.status===401);

const post=await handleMemberTodaysMemoryReadRequest(
  new Request('https://example.test/api/internal/member/todays-memory',{method:'POST'}),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
pass('HTTP TODAY\'S MEMORY is GET only',post.status===405);

const override=await handleMemberTodaysMemoryReadRequest(
  new Request('https://example.test/api/internal/member/todays-memory?as_of=2025-01-01&family_id=fam_B&customer_id=26000999'),
  {DB:makeDb()},
  {family_id:familyId,customer_id:customerId}
);
const overrideBody=await override.json();
pass('HTTP request identity cannot override server session',override.status===200&&overrideBody.family_id===familyId&&overrideBody.customer_id===customerId);
pass('HTTP client as_of is ignored',overrideBody.today_memory?.as_of!=='2025-01-01');

const health=memberTodaysMemoryReadHealth();
pass('health prioritizes exact anniversaries',health.exact_anniversary_priority===true&&health.exact_match_limit===3);
pass('health records honest same-month ±7 seasonal fallback',health.seasonal_fallback_same_month_only===true&&health.seasonal_fallback_window_days===7&&health.seasonal_fallback_honest_label===true);
pass('health excludes current year from nostalgia matching',health.current_year_excluded===true);
pass('health does not falsely claim unbounded database total',health.bounded_source_window===true&&health.database_total_claim===false);
pass('health leaves HOME activation off',health.home_activation===false);
pass('health records no automatic contact, LINE send, route or write',health.automatic_contact===false&&health.line_send===false&&health.production_route_wired===false&&health.production_write===false);

console.log(`MEMBER_TODAYS_MEMORY_READ_MODEL=${n}/${n} PASS`);
