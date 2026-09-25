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
const freshMain='742c947f6f58f7276ab37161816e8af43b5e0554';

pass('canonical Production entry is exact applied default-off blob',actualBlob===__test.APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA);
pass('pre-wiring and applied blobs are distinct',__test.PRE_WIRING_ENTRY_BLOB_SHA!==__test.APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA);
pass('SHA normalizer accepts exact lowercase or uppercase SHA',__test.normalizeSha(freshMain)===freshMain&&__test.normalizeSha(freshMain.toUpperCase())===freshMain);
pass('SHA normalizer rejects malformed SHA',__test.normalizeSha('not-a-sha')==='');

const manifest=buildMemberProductionEntryDefaultOffManifest({
  production_entry_source:source,
  observed_current_main_sha:freshMain,
  expected_current_main_sha:freshMain,
  base_entry_blob_sha:actualBlob
});

pass('manifest recognizes canonical source as already default-off applied',manifest.status==='already_applied_default_off'&&manifest.baseline_matches===true);
pass('manifest records fresh exact main equality',manifest.exact_baseline.observed_current_main_sha===freshMain&&manifest.exact_baseline.expected_current_main_sha===freshMain&&manifest.exact_baseline.exact_main_match===true);
pass('manifest binds exact applied Production entry blob',manifest.exact_baseline.entry_blob_sha===__test.APPLIED_DEFAULT_OFF_ENTRY_BLOB_SHA);
pass('manifest inspects current source instead of regenerating it',manifest.candidate.source===source&&manifest.future_patch_contract.entry_patch_required===false);
pass('current source defaults Owner activation off',manifest.candidate.default_off===true&&manifest.candidate.inspection.owner_flag_default_false===true);
pass('current source keeps Member route mode explicit',manifest.candidate.inspection.route_mode_checked===true);
pass('current source keeps LINE exchange approval false',manifest.candidate.line_login_approved===false&&manifest.candidate.inspection.line_login_default_false===true);
pass('current source keeps public asset adapter null',manifest.candidate.public_asset_adapter===null&&manifest.candidate.inspection.public_asset_adapter_default_null===true);
pass('current source keeps private media adapter null',manifest.candidate.private_media_storage_adapter===null&&manifest.candidate.inspection.private_media_adapter_default_null===true);
pass('current source preserves exact callback GET scope',manifest.candidate.inspection.exact_callback_get_only===true);
pass('current source double-gates callback boundary exception',manifest.candidate.inspection.callback_exception_owner_and_mode_gated===true);
pass('current source preserves CRM fallback ordering',manifest.candidate.inspection.dispatch_after_boundary_before_crm===true&&manifest.candidate.inspection.existing_crm_dispatch_preserved===true);
pass('canonical source state does not claim Production runtime deployment',manifest.canonical_source_state.default_off_wiring_applied===true&&manifest.canonical_source_state.production_runtime_deployed===false&&manifest.canonical_source_state.production_route_activated===false);

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
pass('unknown entry blob fails closed',wrongBlob.status==='entry_blob_mismatch');

pass('manifest operation performs zero Production mutation',Object.values(manifest.invariant).every(value=>value===false));

const health=memberProductionEntryDefaultOffManifestHealth();
pass('health uses fresh exact main instead of static main pin',health.main_sha_strategy==='fresh_exact_match_required'&&health.static_main_sha_pinning===false&&health.fresh_main_gate_required===true);
pass('health records canonical source default-off wiring as applied',health.canonical_source_entry_default_off_applied===true&&health.applied_default_off_entry_blob_sha===actualBlob);
pass('health does not claim Production deploy or route activation',health.production_runtime_deployed===false&&health.production_route_activated===false&&health.production_deploy===false&&health.production_write===false);
pass('health remains source-only exact-candidate manifest',health.member_production_entry_default_off_manifest===true&&health.source_only===true&&health.exact_candidate_only===true);

console.log('MEMBER_PRODUCTION_ENTRY_DEFAULT_OFF_MANIFEST='+n+'/'+n+' PASS');
