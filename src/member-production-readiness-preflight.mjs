import { memberAppSourceAcceptanceHealth } from './member-app-source-integration.mjs';
import { memberSessionFoundationHealth } from './member-session-foundation.mjs';
import { memberLineTokenVerificationHealth } from './member-line-token-verification.mjs';
import { memberLineLoginTransactionHealth } from './member-line-login-transaction.mjs';
import { memberLineLoginExchangeExecutorHealth } from './member-line-login-exchange-executor.mjs';
import { memberLineLoginHttpContractHealth } from './member-line-login-http-contract.mjs';
import { memberReadOnlyHttpRouterHealth } from './member-readonly-http-router.mjs';
import { memberBootstrapPlanHealth } from './member-bootstrap-plan.mjs';
import { memberMemoryWriteExecutorHealth } from './member-memory-write-executor.mjs';
import { memberPrivateMediaAccessHealth } from './member-private-media-access.mjs';
import { memberPrivateMediaDeliveryGrantHealth } from './member-private-media-delivery-grant.mjs';
import { memberPrivateMediaContentAdapterHealth } from './member-private-media-content-adapter.mjs';
import { memberPrivateMediaHttpRouterHealth } from './member-private-media-http-router.mjs';
import { memberAppHttpCompositionHealth } from './member-app-http-composition.mjs';
import { memberFavoritesHttpContractHealth } from './member-favorites-http-contract.mjs';
import { memberFavoritesRateLimitHealth } from './member-favorites-rate-limit.mjs';
import { memberFavoritesWriteExecutorHealth } from './member-favorites-write-executor.mjs';
import { memberFamilyPassBlackWriteExecutorHealth } from './member-family-pass-black-write-executor.mjs';
import { memberShopPickupHealth } from './member-shop-pickup-read-model.mjs';

const BUILD='member-production-readiness-preflight-20260925-01';

const MIGRATIONS=Object.freeze([
  {name:'20260924_member_family_identity_foundation.sql',role:'core_family_identity'},
  {name:'20260924_member_memory_core_foundation.sql',role:'core_memory'},
  {name:'20260924_member_memory_favorites_foundation.sql',role:'favorites'},
  {name:'20260924_member_favorite_mutation_rate_limit_foundation.sql',role:'favorites_rate_limit'},
  {name:'20260924_member_family_pass_entitlement_foundation.sql',role:'black_entitlement'},
  {name:'20260924_member_public_asset_registry_foundation.sql',role:'optional_public_assets'},
  {name:'20260924_member_creative_catalog_foundation.sql',role:'optional_creative_catalog'},
  {name:'20260924_member_shop_catalog_foundation.sql',role:'optional_shop_catalog'},
  {name:'20260924_member_news_catalog_foundation.sql',role:'optional_news_catalog'}
]);

const CORE_FAMILY='20260924_member_family_identity_foundation.sql';
const CORE_MEMORY='20260924_member_memory_core_foundation.sql';
const FAVORITES='20260924_member_memory_favorites_foundation.sql';
const FAVORITES_RATE_LIMIT='20260924_member_favorite_mutation_rate_limit_foundation.sql';
const BLACK_ENTITLEMENT='20260924_member_family_pass_entitlement_foundation.sql';

const text=value=>value==null?'':String(value).trim();
const observedTrue=(observed,key)=>observed?.[key]===true;
const enabled=(env,key)=>text(env?.[key]).toLowerCase()==='enabled';
const configured=(env,key)=>text(env?.[key])!=='';
const strongSecret=(env,key)=>text(env?.[key]).length>=32;

function appliedMigrationSet(observed){
  const values=Array.isArray(observed?.applied_migrations)?observed.applied_migrations:[];
  return new Set(values.map(text).filter(Boolean));
}

function gate({required,source_ready,conditions,notes=[]}){
  const blockers=conditions.filter(condition=>!condition.ok).map(condition=>condition.blocker);
  return {
    required,
    source_ready,
    activation_ready:source_ready===true&&blockers.length===0,
    blockers,
    notes
  };
}

export function buildMemberProductionReadinessPreflight({env={},observed={}}={}){
  const applied=appliedMigrationSet(observed);
  const hasMigration=name=>applied.has(name);

  // Source readiness is derived from the reviewed module health contracts,
  // not from repository-file presence or a hard-coded ready flag.
  const sourceHealth={
    ui:memberAppSourceAcceptanceHealth(),
    session:memberSessionFoundationHealth(env),
    line_verify:memberLineTokenVerificationHealth(),
    line_transaction:memberLineLoginTransactionHealth(env),
    line_exchange:memberLineLoginExchangeExecutorHealth(env),
    line_http:memberLineLoginHttpContractHealth(env),
    read_router:memberReadOnlyHttpRouterHealth(),
    bootstrap:memberBootstrapPlanHealth(),
    memory_write:memberMemoryWriteExecutorHealth(env),
    private_access:memberPrivateMediaAccessHealth(),
    private_grant:memberPrivateMediaDeliveryGrantHealth(env),
    private_content:memberPrivateMediaContentAdapterHealth(env),
    private_router:memberPrivateMediaHttpRouterHealth(env),
    composition:memberAppHttpCompositionHealth(env),
    favorites_http:memberFavoritesHttpContractHealth(env),
    favorites_rate_limit:memberFavoritesRateLimitHealth(env),
    favorites_write:memberFavoritesWriteExecutorHealth(env),
    black_write:memberFamilyPassBlackWriteExecutorHealth(env),
    shop:memberShopPickupHealth()
  };

  const sourceUiReady=sourceHealth.ui.all_five_ui_foundations_present===true;
  const authSourceReady=
    sourceHealth.session.member_session_foundation===true
    && sourceHealth.line_verify.member_line_token_verification===true
    && sourceHealth.line_transaction.member_line_login_transaction===true
    && sourceHealth.line_exchange.member_line_login_exchange_executor===true
    && sourceHealth.line_http.member_line_login_http_contract===true
    && sourceHealth.composition.line_login_source_ready===true;
  const readOnlySourceReady=
    sourceUiReady
    && authSourceReady
    && sourceHealth.read_router.member_readonly_http_router===true
    && sourceHealth.read_router.read_only===true
    && sourceHealth.composition.read_only_source_ready===true;
  const historicalSourceReady=
    sourceHealth.bootstrap.member_bootstrap_plan===true
    && sourceHealth.memory_write.member_memory_write_executor===true;
  const privateSourceReady=
    sourceHealth.private_access.member_private_media_access===true
    && sourceHealth.private_grant.member_private_media_delivery_grant===true
    && sourceHealth.private_content.member_private_media_content_adapter===true
    && sourceHealth.private_router.member_private_media_http_router===true
    && sourceHealth.composition.private_media_source_ready===true;
  const favoritesSourceReady=
    sourceHealth.favorites_http.member_favorites_http_contract===true
    && sourceHealth.favorites_rate_limit.member_favorites_rate_limit===true
    && sourceHealth.favorites_write.member_favorites_write_executor===true;
  const blackSourceReady=sourceHealth.black_write.member_family_pass_black_write_executor===true;

  const sourceUi=gate({
    required:true,
    source_ready:sourceUiReady,
    conditions:[],
    notes:[
      'Canonical HOME / MEMORIES / CREATE / SHOP / MY source integration is present.',
      'This gate performs no Production action.'
    ]
  });

  const authSession=gate({
    required:true,
    source_ready:authSourceReady,
    conditions:[
      {ok:strongSecret(env,'MEMBER_SESSION_SECRET'),blocker:'MEMBER_SESSION_SECRET_NOT_CONFIGURED'},
      {
        ok:strongSecret(env,'MEMBER_LINE_LOGIN_TRANSACTION_SECRET')
          && configured(env,'MEMBER_LINE_LOGIN_CHANNEL_ID')
          && configured(env,'MEMBER_LINE_LOGIN_REDIRECT_URI'),
        blocker:'LINE_LOGIN_TRANSACTION_NOT_CONFIGURED'
      },
      {ok:observedTrue(observed,'line_external_token_exchange_ready'),blocker:'LINE_TOKEN_EXCHANGE_RUNTIME_NOT_VERIFIED'},
      {ok:observedTrue(observed,'line_external_id_token_verification_ready'),blocker:'LINE_ID_TOKEN_VERIFICATION_RUNTIME_NOT_VERIFIED'},
      {ok:observedTrue(observed,'login_routes_wired'),blocker:'MEMBER_LOGIN_ROUTES_NOT_WIRED'},
      {ok:observedTrue(observed,'member_read_routes_wired'),blocker:'MEMBER_READ_ROUTES_NOT_WIRED'}
    ],
    notes:[
      'Member session, LINE login transaction, token verification, guarded exchange executor, and HTTP login contract are source-ready.',
      'Verified LINE subject remains the only login identity source.'
    ]
  });

  const readOnlyApp=gate({
    required:true,
    source_ready:readOnlySourceReady,
    conditions:[
      {ok:authSession.activation_ready,blocker:'MEMBER_AUTH_SESSION_NOT_ACTIVATION_READY'},
      {ok:hasMigration(CORE_FAMILY),blocker:'MEMBER_FAMILY_IDENTITY_SCHEMA_NOT_VERIFIED'},
      {ok:hasMigration(CORE_MEMORY),blocker:'MEMBER_MEMORY_CORE_SCHEMA_NOT_VERIFIED'},
      {ok:sourceUi.activation_ready,blocker:'MEMBER_SOURCE_UI_NOT_READY'},
      {ok:observedTrue(observed,'production_member_route_entry_ready'),blocker:'MEMBER_PRODUCTION_ROUTE_ENTRY_NOT_VERIFIED'}
    ],
    notes:[
      'Source readiness includes the five-tab UI, complete auth/session chain, and signed-session read-only HTTP router.',
      'Core read-only activation requires explicit Production schema and route evidence.',
      'Optional catalog or Favorite schemas may remain unavailable without blocking the basic read-only app.'
    ]
  });

  const historicalBootstrap=gate({
    required:false,
    source_ready:historicalSourceReady,
    conditions:[
      {ok:hasMigration(CORE_FAMILY)&&hasMigration(CORE_MEMORY),blocker:'CORE_MEMBER_SCHEMAS_NOT_VERIFIED'},
      {ok:enabled(env,'MEMBER_MEMORY_WRITE_MODE'),blocker:'MEMBER_MEMORY_WRITE_MODE_NOT_ENABLED'},
      {ok:observedTrue(observed,'historical_memory_plan_verified'),blocker:'HISTORICAL_MEMORY_PLAN_NOT_VERIFIED'},
      {ok:observedTrue(observed,'historical_memory_write_owner_authorized'),blocker:'OWNER_PRODUCTION_D1_WRITE_AUTHORIZATION_REQUIRED'}
    ],
    notes:[
      'Bootstrap planning is read-only; activation readiness covers the separately gated historical MEMORY write executor.',
      'Owner authorization must be fresh and must not be inferred or reused.'
    ]
  });

  const privateMedia=gate({
    required:true,
    source_ready:privateSourceReady,
    conditions:[
      {ok:strongSecret(env,'MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET'),blocker:'MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET_NOT_CONFIGURED'},
      {ok:enabled(env,'MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE'),blocker:'MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE_NOT_ENABLED'},
      {ok:observedTrue(observed,'private_media_storage_adapter_ready'),blocker:'PRIVATE_MEDIA_TRUSTED_STORAGE_ADAPTER_NOT_VERIFIED'},
      {ok:observedTrue(observed,'private_media_storage_binding_ready'),blocker:'PRIVATE_MEDIA_PRODUCTION_STORAGE_BINDING_NOT_VERIFIED'},
      {ok:observedTrue(observed,'private_media_routes_wired'),blocker:'PRIVATE_MEDIA_ROUTES_NOT_WIRED'},
      {ok:observedTrue(observed,'private_media_owner_authorized'),blocker:'OWNER_PRIVATE_MEDIA_PRODUCTION_AUTHORIZATION_REQUIRED'}
    ],
    notes:[
      'Private media source readiness includes authorization, short-lived grant, binary content adapter, and the public private-media router.',
      'Private media requires signed Member session, short-lived grant, exact Family reauthorization, and trusted storage adapter.',
      'No implicit storage binding or external redirect is accepted.'
    ]
  });

  const favoritesWrite=gate({
    required:false,
    source_ready:favoritesSourceReady,
    conditions:[
      {ok:hasMigration(FAVORITES),blocker:'MEMBER_FAVORITES_SCHEMA_NOT_VERIFIED'},
      {ok:hasMigration(FAVORITES_RATE_LIMIT),blocker:'MEMBER_FAVORITES_RATE_LIMIT_SCHEMA_NOT_VERIFIED'},
      {ok:observedTrue(observed,'favorite_mutation_route_wired'),blocker:'MEMBER_FAVORITES_MUTATION_ROUTE_NOT_WIRED'},
      {ok:enabled(env,'MEMBER_FAVORITES_MUTATION_ROUTE_MODE'),blocker:'MEMBER_FAVORITES_MUTATION_ROUTE_MODE_NOT_ENABLED'},
      {ok:enabled(env,'MEMBER_FAVORITES_RATE_LIMIT_MODE'),blocker:'MEMBER_FAVORITES_RATE_LIMIT_MODE_NOT_ENABLED'},
      {ok:enabled(env,'MEMBER_FAVORITES_WRITE_MODE'),blocker:'MEMBER_FAVORITES_WRITE_MODE_NOT_ENABLED'},
      {ok:observedTrue(observed,'favorite_write_owner_authorized'),blocker:'OWNER_FAVORITES_WRITE_AUTHORIZATION_REQUIRED'}
    ],
    notes:[
      'Favorite mutation is optional for basic Member activation and remains independently gated.',
      'Runtime authorization still requires signed Member session, same-origin POST, exact JSON, and current Family/MEMORY access.'
    ]
  });

  const blackEntitlement=gate({
    required:false,
    source_ready:blackSourceReady,
    conditions:[
      {ok:hasMigration(BLACK_ENTITLEMENT),blocker:'MEMBER_BLACK_ENTITLEMENT_SCHEMA_NOT_VERIFIED'},
      {ok:observedTrue(observed,'black_exact_family_plan_verified'),blocker:'BLACK_EXACT_FAMILY_PLAN_NOT_VERIFIED'},
      {ok:Number.isInteger(observed?.black_qualifying_memory_count)&&observed.black_qualifying_memory_count>=10,blocker:'BLACK_QUALIFYING_MEMORY_THRESHOLD_NOT_VERIFIED'},
      {ok:enabled(env,'MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE'),blocker:'MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE_NOT_ENABLED'},
      {ok:observedTrue(observed,'black_entitlement_write_owner_authorized'),blocker:'OWNER_BLACK_ENTITLEMENT_WRITE_AUTHORIZATION_REQUIRED'}
    ],
    notes:[
      'BLACK entitlement is a durable lifetime status and is separate from commerce discount enforcement.',
      'The qualifying MEMORY threshold must be explicit current-Family evidence; it is not inferred.'
    ]
  });

  const commerce={
    required:false,
    source_ready:false,
    activation_ready:false,
    blockers:[
      'AUTHORITATIVE_PRICE_SOURCE_NOT_READY',
      'CHECKOUT_PAYMENT_PROVIDER_NOT_READY',
      'BLACK_PHOTO_GOODS_DISCOUNT_ENFORCEMENT_NOT_READY',
      'OWNER_COMMERCE_ACTIVATION_AUTHORIZATION_REQUIRED'
    ],
    notes:[
      'Member SHOP remains presentation-only.',
      'BLACK benefit is PHOTO GOODS 10% OFF FOREVER and does not apply to shooting fees.',
      'Owner commerce authorization alone cannot make this gate ready while source/runtime commerce foundations are incomplete.'
    ]
  };

  const gates={
    source_ui:sourceUi,
    auth_session:authSession,
    read_only_app:readOnlyApp,
    historical_bootstrap:historicalBootstrap,
    private_media:privateMedia,
    favorites_write:favoritesWrite,
    black_entitlement:blackEntitlement,
    commerce
  };

  const migrationInventory={
    explicit_observation_only:true,
    applied_migrations:[...applied],
    items:MIGRATIONS.map(item=>({
      ...item,
      verified_applied:hasMigration(item.name)
    }))
  };

  const ownerGates={
    historical_memory_write:observedTrue(observed,'historical_memory_write_owner_authorized'),
    private_media:observedTrue(observed,'private_media_owner_authorized'),
    favorites_write:observedTrue(observed,'favorite_write_owner_authorized'),
    black_entitlement_write:observedTrue(observed,'black_entitlement_write_owner_authorized'),
    commerce_activation:observedTrue(observed,'commerce_owner_authorized'),
    authorization_policy:'fresh_explicit_exact_scope_only_no_reuse'
  };

  const invariant={
    automatic_contact:false,
    line_send:false,
    customer_id_generation:false,
    canonical_crm_write:false,
    production_deploy_executed:false,
    production_schema_apply_executed:false,
    production_write_executed:false
  };

  const health={
    source_only:true,
    production_observation_explicit_input_only:true,
    secret_values_exposed:false,
    production_route_mutation:false,
    schema_apply:false,
    write:false,
    deploy:false,
    line_send:false,
    customer_id_generation:false
  };

  return {
    status:'ok',
    build:BUILD,
    source_only:true,
    main_activation_ready:
      authSession.activation_ready===true
      && readOnlyApp.activation_ready===true
      && privateMedia.activation_ready===true,
    gates,
    source_health:{
      five_tab_ui:sourceUiReady,
      auth_session:authSourceReady,
      read_only_app:readOnlySourceReady,
      historical_bootstrap:historicalSourceReady,
      private_media:privateSourceReady,
      favorites_write:favoritesSourceReady,
      black_entitlement:blackSourceReady,
      shop_presentation:sourceHealth.shop.member_shop_pickup_read_model===true
        && sourceHealth.shop.presentation_catalog_only===true,
      composition:sourceHealth.composition.member_app_http_composition===true
        && sourceHealth.composition.all_source_layers_present===true
    },
    migration_inventory:migrationInventory,
    owner_gates:ownerGates,
    invariant,
    health
  };
}

export function memberProductionReadinessPreflightHealth(){
  return {
    member_production_readiness_preflight:true,
    build:BUILD,
    source_only:true,
    production_observation_explicit_input_only:true,
    secret_values_exposed:false,
    production_route_mutation:false,
    schema_apply:false,
    write:false,
    deploy:false,
    line_send:false,
    customer_id_generation:false,
    automatic_contact:false,
    canonical_crm_write:false
  };
}

export const __test={
  MIGRATIONS,
  appliedMigrationSet,
  enabled,
  strongSecret
};
