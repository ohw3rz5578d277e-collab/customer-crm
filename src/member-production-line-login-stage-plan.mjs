const BUILD='member-production-line-login-stage-plan-20261012-02';
const CANONICAL_ENTRYPOINT='src/production-index-crm-customer360-entry.js';

const REQUIRED_DISABLED_MODES=Object.freeze([
  'MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE',
  'MEMBER_FAVORITES_MUTATION_ROUTE_MODE',
  'MEMBER_FAVORITES_RATE_LIMIT_MODE',
  'MEMBER_FAVORITES_WRITE_MODE',
  'MEMBER_MEMORY_WRITE_MODE',
  'MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE'
]);

const REQUIRED_BINDINGS=Object.freeze([
  'MEMBER_PRIVATE_MEDIA_BUCKET',
  'DB',
  'LINE_SERVICE',
  'RESERVATION_SERVICE'
]);

const text=value=>value==null?'':String(value);
const enabled=value=>String(value||'').trim().toLowerCase()==='enabled';

function clone(value){return JSON.parse(JSON.stringify(value));}

export function buildMemberLineLoginStageCandidate(entrySource,wranglerConfig){
  const source=text(entrySource);
  const cfg=clone(wranglerConfig||{});

  if(text(cfg.main)!==CANONICAL_ENTRYPOINT){
    throw new Error('CANONICAL_ENTRYPOINT_MISMATCH');
  }

  const ownerFalse='const MEMBER_PRODUCTION_OWNER_APPROVED=false;';
  const ownerTrue='const MEMBER_PRODUCTION_OWNER_APPROVED=true;';
  if((source.match(/const MEMBER_PRODUCTION_OWNER_APPROVED=false;/g)||[]).length!==1){
    throw new Error('OWNER_DEFAULT_FALSE_NOT_EXACTLY_ONE');
  }
  if(source.includes(ownerTrue))throw new Error('OWNER_ALREADY_ENABLED_IN_CANONICAL_SOURCE');
  if((source.match(/line_login_approved:false/g)||[]).length!==1){
    throw new Error('LINE_LOGIN_APPROVAL_FALSE_NOT_EXACTLY_ONE');
  }
  if(source.includes('line_login_approved:true'))throw new Error('LINE_LOGIN_APPROVAL_ALREADY_ENABLED');

  const r2=Array.isArray(cfg.r2_buckets)?cfg.r2_buckets:[];
  if(r2.length!==1||r2[0]?.binding!=='MEMBER_PRIVATE_MEDIA_BUCKET'||r2[0]?.bucket_name!=='customer-crm-member-private-media'){
    throw new Error('CANONICAL_PRIVATE_MEDIA_BINDING_MISMATCH');
  }

  const vars={...(cfg.vars||{})};
  for(const key of ['MEMBER_PRODUCTION_ROUTE_MODE','MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE',...REQUIRED_DISABLED_MODES]){
    if(enabled(vars[key]))throw new Error('CANONICAL_MODE_ALREADY_ENABLED:'+key);
  }

  const candidateEntry=source
    .replace(ownerFalse,ownerTrue)
    .replace('line_login_approved:false','line_login_approved:true');

  const candidateConfig=clone(cfg);
  candidateConfig.main=CANONICAL_ENTRYPOINT;
  candidateConfig.vars={
    ...(candidateConfig.vars||{}),
    MEMBER_PRODUCTION_ROUTE_MODE:'enabled',
    MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE:'enabled',
    MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE:'disabled',
    MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'disabled',
    MEMBER_FAVORITES_RATE_LIMIT_MODE:'disabled',
    MEMBER_FAVORITES_WRITE_MODE:'disabled',
    MEMBER_MEMORY_WRITE_MODE:'disabled',
    MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'disabled'
  };

  return {
    entry_source:candidateEntry,
    wrangler_config:candidateConfig,
    plan:{
      build:BUILD,
      source_only:true,
      immutable_candidate_required:true,
      production_traffic_target_percent:0,
      owner_approved:true,
      member_route_enabled:true,
      member_session_secret_required:true,
      line_login_approved:true,
      line_login_external_exchange_enabled:true,
      private_media_content_route_enabled:false,
      favorite_write_enabled:false,
      memory_write_enabled:false,
      family_pass_entitlement_write_enabled:false,
      customer_create:false,
      customer_id_generation:false,
      family_create:false,
      family_auto_link:false,
      crm_write:false,
      line_send:false,
      commerce_activation:false,
      black_automatic_award:false
    }
  };
}

export function validateMemberLineLoginStageCandidate(candidate){
  const entry=text(candidate?.entry_source);
  const cfg=candidate?.wrangler_config||{};
  const vars=cfg.vars||{};

  const checks={
    main:text(cfg.main)===CANONICAL_ENTRYPOINT,
    owner:(entry.match(/const MEMBER_PRODUCTION_OWNER_APPROVED=true;/g)||[]).length===1 && !entry.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=false;'),
    line_approval:(entry.match(/line_login_approved:true/g)||[]).length===1 && !entry.includes('line_login_approved:false'),
    route:enabled(vars.MEMBER_PRODUCTION_ROUTE_MODE),
    exchange:enabled(vars.MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE),
    private_media:!enabled(vars.MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE),
    favorites:!enabled(vars.MEMBER_FAVORITES_MUTATION_ROUTE_MODE)&&!enabled(vars.MEMBER_FAVORITES_RATE_LIMIT_MODE)&&!enabled(vars.MEMBER_FAVORITES_WRITE_MODE),
    memory:!enabled(vars.MEMBER_MEMORY_WRITE_MODE),
    family_pass:!enabled(vars.MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE),
    r2:Array.isArray(cfg.r2_buckets)&&cfg.r2_buckets.length===1&&cfg.r2_buckets[0]?.binding==='MEMBER_PRIVATE_MEDIA_BUCKET'&&cfg.r2_buckets[0]?.bucket_name==='customer-crm-member-private-media',
    bindings:REQUIRED_BINDINGS.every(name=>JSON.stringify(cfg).includes('"'+name+'"'))
  };
  const failed=Object.entries(checks).filter(([,ok])=>!ok).map(([name])=>name);
  if(failed.length)throw new Error('LINE_LOGIN_STAGE_CANDIDATE_INVALID:'+failed.join(','));
  return {ok:true,checks};
}

export function memberProductionLineLoginStagePlanHealth(){
  return {
    member_production_line_login_stage_plan:true,
    build:BUILD,
    source_only:true,
    production_mutation:false,
    production_traffic_change:false,
    production_deploy:false,
    production_d1_read:false,
    production_d1_write:false,
    r2_object_read:false,
    r2_object_write:false,
    secret_change:false,
    line_send:false,
    customer_prospect_mutation:false,
    customer_id_generation:false,
    commerce_activation:false,
    black_automatic_award:false,
    next_boundary:'OWNER_GATED_IMMUTABLE_LINE_LOGIN_CANDIDATE_STAGE'
  };
}

export const __test={BUILD,CANONICAL_ENTRYPOINT,REQUIRED_DISABLED_MODES,REQUIRED_BINDINGS,enabled};
