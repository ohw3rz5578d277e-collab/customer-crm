import fs from 'node:fs';
import crypto from 'node:crypto';
import {
  buildMemberProductionEntryDefaultOffManifest,
  memberProductionEntryDefaultOffManifestHealth,
  __test
} from '../src/member-production-entry-default-off-manifest.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const source=fs.readFileSync(__test.ENTRY_PATH,'utf8');

function gitBlobSha(content){
  const bytes=Buffer.from(content,'utf8');
  const header=Buffer.from('blob '+bytes.length+'\0','utf8');
  return crypto.createHash('sha1').update(Buffer.concat([header,bytes])).digest('hex');
}

const actualBlob=gitBlobSha(source);
const freshMain='f6ef8e120af73353b4310cea23921f734aecd2e1';

pass('canonical Production entry blob matches exact manifest baseline',actualBlob===__test.BASE_ENTRY_BLOB_SHA);
pass('SHA normalizer accepts exact lowercase or uppercase SHA',__test.normalizeSha(freshMain)===freshMain&&__test.normalizeSha(freshMain.toUpperCase())===freshMain);
pass('SHA normalizer rejects malformed SHA',__test.normalizeSha('not-a-sha')==='');

const manifest=buildMemberProductionEntryDefaultOffManifest({
  production_entry_source:source,
  observed_current_main_sha:freshMain,
  expected_current_main_sha:freshMain,
  base_entry_blob_sha:actualBlob
});

pass('manifest builds from fresh exact main plus exact entry blob',manifest.status==='ok'&&manifest.baseline_matches===true);
pass('manifest records fresh exact main equality',manifest.exact_baseline.observed_current_main_sha===freshMain&&manifest.exact_baseline.expected_current_main_sha===freshMain&&manifest.exact_baseline.exact_main_match===true);
pass('manifest binds exact Production entry blob SHA',manifest.exact_baseline.entry_blob_sha===__test.BASE_ENTRY_BLOB_SHA);
pass('manifest generates a non-empty exact candidate source',typeof manifest.candidate.source==='string'&&manifest.candidate.source.length>source.length);
pass('candidate defaults Owner activation off',manifest.candidate.default_off===true&&manifest.candidate.inspection.owner_flag_default_false===true);
pass('candidate keeps Member route mode explicit',manifest.candidate.inspection.route_mode_checked===true);
pass('candidate keeps LINE exchange approval false',manifest.candidate.line_login_approved===false&&manifest.candidate.inspection.line_login_default_false===true);
pass('candidate keeps public asset adapter null',manifest.candidate.public_asset_adapter===null&&manifest.candidate.inspection.public_asset_adapter_default_null===true);
pass('candidate keeps private media adapter null',manifest.candidate.private_media_storage_adapter===null&&manifest.candidate.inspection.private_media_adapter_default_null===true);
pass('candidate preserves exact callback GET scope',manifest.candidate.inspection.exact_callback_get_only===true);
pass('candidate double-gates callback boundary exception',manifest.candidate.inspection.callback_exception_owner_and_mode_gated===true);
pass('candidate preserves CRM fallback ordering',manifest.candidate.inspection.dispatch_after_boundary_before_crm===true&&manifest.candidate.inspection.existing_crm_dispatch_preserved===true);

pass('future patch contract requires a fresh exact main gate',
  manifest.future_patch_contract.fresh_current_main_sha_required===true
  && manifest.future_patch_contract.expected_main_sha_must_match_fresh_observation===true
);
pass('future patch contract forbids manual post-generation edits',
  manifest.future_patch_contract.apply_exact_candidate_only===true
  && manifest.future_patch_contract.manual_edit_after_generation===false
);
pass('future patch contract requires Owner authorization before apply and merge',
  manifest.future_patch_contract.owner_authorization_required_before_apply===true
  && manifest.future_patch_contract.owner_authorization_required_before_merge===true
);
pass('future patch contract keeps deploy and route activation separate',
  manifest.future_patch_contract.production_deploy_separate_gate===true
  && manifest.future_patch_contract.route_activation_separate_gate===true
);
pass('future patch contract keeps storage bindings separate',
  manifest.future_patch_contract.public_asset_binding_separate_gate===true
  && manifest.future_patch_contract.private_media_binding_separate_gate===true
);
pass('existing deploy workflow remains full-regression dry-run gate',
  manifest.existing_release_gate.workflow==='.github/workflows/deploy-cloudflare.yml'
  && manifest.existing_release_gate.production_entry_path_triggers_full_regression===true
  && manifest.existing_release_gate.all_local_tests===true
  && manifest.existing_release_gate.production_entry_import===true
  && manifest.existing_release_gate.wrangler_dry_run===true
  && manifest.existing_release_gate.deploy_on_pull_request===false
);

const missingMain=buildMemberProductionEntryDefaultOffManifest({
  production_entry_source:source,
  base_entry_blob_sha:actualBlob
});
pass('missing fresh current main fails closed',missingMain.status==='current_main_sha_required');

const wrongMain=buildMemberProductionEntryDefaultOffManifest({
  production_entry_source:source,
  observed_current_main_sha:freshMain,
  expected_current_main_sha:'0000000000000000000000000000000000000000',
  base_entry_blob_sha:actualBlob
});
pass('fresh observed/expected main mismatch fails closed',wrongMain.status==='current_main_sha_mismatch');

const wrongBlob=buildMemberProductionEntryDefaultOffManifest({
  production_entry_source:source,
  observed_current_main_sha:freshMain,
  expected_current_main_sha:freshMain,
  base_entry_blob_sha:'0000000000000000000000000000000000000000'
});
pass('wrong entry blob fails closed',wrongBlob.status==='entry_blob_mismatch');

pass('manifest performs zero Production mutation',Object.values(manifest.invariant).every(value=>value===false));

const health=memberProductionEntryDefaultOffManifestHealth();
pass('health uses fresh exact main instead of static main pin',health.main_sha_strategy==='fresh_exact_match_required'&&health.static_main_sha_pinning===false&&health.fresh_main_gate_required===true);
pass('health remains source-only exact-candidate manifest',health.member_production_entry_default_off_manifest===true&&health.source_only===true&&health.exact_candidate_only===true);
pass('health records zero Production modification activation deploy and write',health.production_entry_modified===false&&health.production_route_activated===false&&health.production_deploy===false&&health.production_write===false);

console.log('MEMBER_PRODUCTION_ENTRY_DEFAULT_OFF_MANIFEST='+n+'/'+n+' PASS');
