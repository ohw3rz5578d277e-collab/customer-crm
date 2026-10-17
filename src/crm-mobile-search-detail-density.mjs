const STYLE_ID='crm-mobile-search-detail-density-style';

const STYLE=`<style id="${STYLE_ID}">
@media(max-width:767px){
  .crm-search-head{padding:10px!important;border-radius:0 0 14px 14px!important;margin-bottom:8px!important}
  .crm-search-row{gap:7px!important}
  .crm-search-box input{min-height:44px!important;border-radius:12px!important;padding:10px 62px 10px 12px!important;font-size:15px!important}
  .crm-search-clear{right:8px!important;min-height:44px!important;min-width:44px!important}
  .crm-result-count{font-size:13px!important}
  .crm-quick{gap:5px!important;margin-top:8px!important}
  .crm-chip{min-height:36px!important;padding:6px 9px!important;font-size:11px!important}
  .crm-active-wrap{gap:5px!important;margin:7px 0!important}
  .crm-active-chip{padding:5px 8px!important;font-size:11px!important}
  .crm-detail{inset:6px!important;border-radius:16px!important;padding:10px!important}
  .crm-detail-close{width:44px!important;height:44px!important;min-width:44px!important;min-height:44px!important}
  .crm-360-profile-hero{gap:8px!important}
  .crm-360-profile-main,.crm-360-next{padding:10px!important;border-radius:14px!important}
  .crm-360-profile-main h2{font-size:22px!important;line-height:1.25!important}
  .crm-360-profile-id{font-size:11px!important}
  .crm-360-status-row{gap:4px!important;margin-top:8px!important}
  .crm-360-status-chip{padding:4px 7px!important;font-size:10px!important}
  .crm-360-next>strong{font-size:18px!important;margin:3px 0!important}
  .crm-360-actions{gap:6px!important;margin:8px 0 9px!important}
  .crm-360-actions .crm-mkt-btn{min-height:44px!important;padding:7px 9px!important}
  .crm-360-stat-grid{gap:6px!important}
  .crm-360-card,.crm-section{padding:10px!important;border-radius:14px!important}
  .crm-360-card strong{font-size:17px!important;margin-top:3px!important}
  .crm-grid2{gap:8px!important;margin-top:8px!important}
  .crm-section h3{margin-bottom:7px!important;font-size:14px!important}
  .crm-360-summary-list{gap:4px!important}
  .crm-360-summary-list p{grid-template-columns:82px minmax(0,1fr)!important;gap:6px!important;padding:5px 0!important;font-size:12px!important}
  .crm-360-history-row{gap:8px!important;padding:6px 0!important;font-size:12px!important}
  .crm-family-item{padding:6px 0!important}
  .crm-opportunity-item{padding:7px!important;margin:4px 0!important}
}
</style>`;

export function injectMobileSearchDetailDensity(html){
  const source=String(html||'');
  if(!source||source.includes(STYLE_ID))return source;
  if(!source.includes('crm-customer360-marketing-style'))return source;
  if(source.includes('</head>'))return source.replace('</head>',STYLE+'</head>');
  return STYLE+source;
}

export const mobileSearchDetailDensityContract=Object.freeze({
  viewport_max_px:767,
  search_input_min_height_px:44,
  detail_close_min_size_px:44,
  action_min_height_px:44,
  production_write:false,
  customer_mutation:false,
  line_send:false
});
