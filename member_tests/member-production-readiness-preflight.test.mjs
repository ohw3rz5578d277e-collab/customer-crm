import {
  buildMemberProductionReadinessPreflight,
  memberProductionReadinessPreflightHealth,
  __test
} from '../src/member-production-readiness-preflight.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const empty=buildMemberProductionReadinessPreflight();
pass('default preflight is source-only',empty.status==='ok'&&empty.source_only===true&&empty.health.source_only===true);
pass('five-tab UI is source-ready',empty.gates.source_ui.source_ready===true&&empty.gates.source_ui.activation_ready===true);
pass('auth source readiness includes the complete source-only LINE login chain',empty.source_health.auth_session===true&&empty.gates.auth_session.source_ready===true);
pass('read-only app source readiness includes signed-session public read router',empty.source_health.read_only_app===true&&empty.gates.read_only_app.source_ready===true);
pass('Member app HTTP composition is included in source readiness',empty.source_health.composition===true);
pass('Production route wiring plan is source-ready static analysis only',empty.source_health.production_route_wiring_plan===true);
pass('main Production activation defaults blocked',empty.main_activation_ready===false);
pass('Member session secret missing is explicit',empty.gates.auth_session.blockers.includes('MEMBER_SESSION_SECRET_NOT_CONFIGURED'));
pass('LINE login transaction config missing is explicit',empty.gates.auth_session.blockers.includes('LINE_LOGIN_TRANSACTION_NOT_CONFIGURED'));
pass('core Family schema is not inferred',empty.gates.read_only_app.blockers.includes('MEMBER_FAMILY_IDENTITY_SCHEMA_NOT_VERIFIED'));
pass('core MEMORY schema is not inferred',empty.gates.read_only_app.blockers.includes('MEMBER_MEMORY_CORE_SCHEMA_NOT_VERIFIED'));
pass('historical MEMORY write requires fresh Owner gate',empty.gates.historical_bootstrap.blockers.includes('OWNER_PRODUCTION_D1_WRITE_AUTHORIZATION_REQUIRED'));
pass('private media source readiness includes public private-media router',empty.source_health.private_media===true&&empty.gates.private_media.source_ready===true);
pass('private media defaults blocked',empty.gates.private_media.activation_ready===false&&empty.gates.private_media.blockers.length>0);
pass('Favorites write remains independently multi-gated',empty.gates.favorites_write.activation_ready===false&&empty.gates.favorites_write.blockers.includes('MEMBER_FAVORITES_MUTATION_ROUTE_MODE_NOT_ENABLED')&&empty.gates.favorites_write.blockers.includes('MEMBER_FAVORITES_RATE_LIMIT_MODE_NOT_ENABLED')&&empty.gates.favorites_write.blockers.includes('MEMBER_FAVORITES_WRITE_MODE_NOT_ENABLED'));
pass('BLACK entitlement is separate from commerce',empty.gates.black_entitlement.source_ready===true&&empty.gates.black_entitlement.required===false&&empty.gates.commerce.source_ready===false);
pass('Commerce is not source-ready',empty.gates.commerce.activation_ready===false&&empty.gates.commerce.blockers.includes('AUTHORITATIVE_PRICE_SOURCE_NOT_READY'));
pass('no migration is inferred applied',empty.migration_inventory.items.every(item=>item.verified_applied===false));
pass('default preflight executes zero Production action',Object.values(empty.invariant).every(value=>value===false)&&empty.health.production_route_mutation===false&&empty.health.schema_apply===false&&empty.health.write===false&&empty.health.deploy===false);
pass('health exposes no secret value and no Customer ID generation',empty.health.secret_values_exposed===false&&empty.health.customer_id_generation===false&&empty.health.line_send===false);

const env={
  MEMBER_SESSION_SECRET:'member-session-secret-12345678901234567890',
  MEMBER_LINE_LOGIN_TRANSACTION_SECRET:'line-login-transaction-secret-123456789012345',
  MEMBER_LINE_LOGIN_CHANNEL_ID:'1234567890',
  MEMBER_LINE_LOGIN_REDIRECT_URI:'https://example.test/member/line/callback',
  MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET:'private-media-delivery-secret-123456789012345',
  MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE:'enabled',
  MEMBER_MEMORY_WRITE_MODE:'enabled',
  MEMBER_FAVORITES_MUTATION_ROUTE_MODE:'enabled',
  MEMBER_FAVORITES_RATE_LIMIT_MODE:'enabled',
  MEMBER_FAVORITES_WRITE_MODE:'enabled',
  MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE:'enabled'
};

const observed={
  applied_migrations:__test.MIGRATIONS.map(item=>item.name),
  line_external_token_exchange_ready:true,
  line_external_id_token_verification_ready:true,
  login_routes_wired:true,
  member_read_routes_wired:true,
  production_member_route_entry_ready:true,
  historical_memory_write_owner_authorized:true,
  historical_memory_plan_verified:true,
  private_media_storage_adapter_ready:true,
  private_media_storage_binding_ready:true,
  private_media_routes_wired:true,
  private_media_owner_authorized:true,
  favorite_mutation_route_wired:true,
  favorite_write_owner_authorized:true,
  black_exact_family_plan_verified:true,
  black_qualifying_memory_count:10,
  black_entitlement_write_owner_authorized:true,
  commerce_owner_authorized:false
};

const staged=buildMemberProductionReadinessPreflight({env,observed});
pass('auth/session can become activation-ready from explicit evidence',staged.gates.auth_session.activation_ready===true);
pass('read-only app can become activation-ready from explicit evidence',staged.gates.read_only_app.activation_ready===true);
pass('historical bootstrap can become technically activation-ready',staged.gates.historical_bootstrap.activation_ready===true);
pass('private media can become technically activation-ready',staged.gates.private_media.activation_ready===true);
pass('Favorite mutation can become technically activation-ready',staged.gates.favorites_write.activation_ready===true);
pass('BLACK entitlement can become technically activation-ready',staged.gates.black_entitlement.activation_ready===true);
pass('commerce remains false even when other gates are ready',staged.gates.commerce.source_ready===false&&staged.gates.commerce.activation_ready===false);
pass('main activation can become ready without optional Favorite BLACK or commerce dependency',staged.main_activation_ready===true&&staged.gates.favorites_write.required===false&&staged.gates.black_entitlement.required===false&&staged.gates.commerce.required===false);
pass('staged fixture still executes zero Production action',Object.values(staged.invariant).every(value=>value===false)&&staged.health.write===false&&staged.health.deploy===false&&staged.health.schema_apply===false);

const stagedWithoutOptional={...observed};
stagedWithoutOptional.favorite_write_owner_authorized=false;
stagedWithoutOptional.black_entitlement_write_owner_authorized=false;
const mainOnly=buildMemberProductionReadinessPreflight({env,observed:stagedWithoutOptional});
pass('optional Favorite and BLACK write gates do not block main activation',mainOnly.main_activation_ready===true&&mainOnly.gates.favorites_write.activation_ready===false&&mainOnly.gates.black_entitlement.activation_ready===false);

const noPrivateOwner={...observed,private_media_owner_authorized:false};
const blockedMain=buildMemberProductionReadinessPreflight({env,observed:noPrivateOwner});
pass('private media Owner gate blocks main activation',blockedMain.main_activation_ready===false&&blockedMain.gates.private_media.blockers.includes('OWNER_PRIVATE_MEDIA_PRODUCTION_AUTHORIZATION_REQUIRED'));

const health=memberProductionReadinessPreflightHealth();
pass('static health is source-only and mutation-free',health.member_production_readiness_preflight===true&&health.source_only===true&&health.production_observation_explicit_input_only===true&&health.secret_values_exposed===false&&health.production_route_mutation===false&&health.schema_apply===false&&health.write===false&&health.deploy===false&&health.line_send===false&&health.customer_id_generation===false&&health.automatic_contact===false&&health.canonical_crm_write===false);

console.log(`MEMBER_PRODUCTION_READINESS_PREFLIGHT=${n}/${n} PASS`);
