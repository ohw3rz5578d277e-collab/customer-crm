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
function gitBlobSha(content){const bytes=Buffer.from(content,'utf8');const header=Buffer.from('blob '+bytes.length+'\0','utf8');return crypto.createHash('sha1').update(Buffer.concat([header,bytes])).digest('hex');}
const actualBlob=gitBlobSha(source);
const freshMain='c2ee3a2d79af72fccea3d38d776d060d516e99f9';

pass('current wired entry advances beyond historical default-off blob',actualBlob!==__test.HISTORICAL_DEFAULT_OFF_ENTRY_BLOB_SHA);
pass('historical pre-wiring and default-off blobs remain distinct provenance',__test.PRE_WIRING_ENTRY_BLOB_SHA!==__test.HISTORICAL_DEFAULT_OFF_ENTRY_BLOB_SHA);
pass('SHA normalizer accepts exact lowercase or uppercase SHA',__test.normalizeSha(freshMain)===freshMain&&__test.normalizeSha(freshMain.toUpperCase())===freshMain);
pass('SHA normalizer rejects malformed SHA',__test.normalizeSha('not-a-sha')==='');
pass('manifest git blob helper matches independent implementation',__test.gitBlobSha(source)===actualBlob);

const manifest=buildMemberProductionEntryDefaultOffManifest({production_entry_source:source,observed_current_main_sha:freshMain,expected_current_main_sha:freshMain,base_entry_blob_sha:actualBlob,expected_entry_blob_sha:actualBlob});
pass('manifest recognizes canonical wired-but-off source',manifest.status==='already_applied_source_only_private_media_wiring'&&manifest.baseline_matches===true);
pass('manifest records fresh exact main equality',manifest.exact_baseline.observed_current_main_sha===freshMain&&manifest.exact_baseline.expected_current_main_sha===freshMain&&manifest.exact_baseline.exact_main_match===true);
pass('manifest requires and records exact entry blob equality',manifest.exact_baseline.entry_blob_sha===actualBlob&&manifest.exact_baseline.expected_entry_blob_sha===actualBlob&&manifest.exact_baseline.exact_entry_blob_match===true);
pass('manifest inspects current source instead of regenerating it',manifest.candidate.source===source&&manifest.future_patch_contract.entry_patch_required===false);
pass('current source defaults Owner activation off',manifest.candidate.default_off===true&&manifest.candidate.inspection.owner_flag_default_false===true);
pass('current source keeps Member route mode explicit',manifest.candidate.inspection.route_mode_checked===true);
pass('current source keeps LINE exchange approval false',manifest.candidate.line_login_approved===false&&manifest.candidate.inspection.line_login_default_false===true);
pass('current source keeps public asset adapter null',manifest.candidate.public_asset_adapter===null&&manifest.candidate.inspection.public_asset_adapter_default_null===true);
pass('current source wires private adapter explicitly',manifest.candidate.private_media_storage_adapter==='explicit_read_only_adapter'&&manifest.candidate.inspection.private_media_adapter_source_wired===true&&manifest.candidate.inspection.private_media_adapter_constructed_explicitly===true);
pass('current source preserves callback and CRM ordering safety',manifest.candidate.inspection.exact_callback_get_only===true&&manifest.candidate.inspection.callback_exception_owner_and_mode_gated===true&&manifest.candidate.inspection.dispatch_after_boundary_before_crm===true&&manifest.candidate.inspection.existing_crm_dispatch_preserved===true);
pass('canonical source state stays default-off with storage fetch zero',manifest.canonical_source_state.default_off_wiring_applied===true&&manifest.canonical_source_state.private_media_source_wiring_applied===true&&manifest.canonical_source_state.production_storage_fetch===false&&manifest.canonical_source_state.production_runtime_deployed===false&&manifest.canonical_source_state.production_route_activated===false);

const generatedCandidateState=__test.commonResult({observedMain:freshMain,entryBlob:__test.PRE_WIRING_ENTRY_BLOB_SHA,inspection:{candidate:true},source:'generated-candidate',status:'candidate_ready',canonicalWiringApplied:false,entryPatchRequired:true});
pass('generated candidate never claims canonical wiring already applied',generatedCandidateState.status==='candidate_ready'&&generatedCandidateState.canonical_source_state.default_off_wiring_applied===false&&generatedCandidateState.canonical_source_state.private_media_source_wiring_applied===false);
pass('generated candidate explicitly requires entry patch before canonical source can advance',generatedCandidateState.future_patch_contract.entry_patch_required===true);

const missingMain=buildMemberProductionEntryDefaultOffManifest({production_entry_source:source,base_entry_blob_sha:actualBlob,expected_entry_blob_sha:actualBlob});
pass('missing fresh current main fails closed',missingMain.status==='current_main_sha_required');
const wrongMain=buildMemberProductionEntryDefaultOffManifest({production_entry_source:source,observed_current_main_sha:freshMain,expected_current_main_sha:'0000000000000000000000000000000000000000',base_entry_blob_sha:actualBlob,expected_entry_blob_sha:actualBlob});
pass('fresh observed/expected main mismatch fails closed',wrongMain.status==='current_main_sha_mismatch');
const missingBlob=buildMemberProductionEntryDefaultOffManifest({production_entry_source:source,observed_current_main_sha:freshMain,expected_current_main_sha:freshMain});
pass('missing entry blob evidence fails closed before structural success',missingBlob.status==='entry_blob_sha_required'&&missingBlob.baseline_matches===false);
const wrongBlob=buildMemberProductionEntryDefaultOffManifest({production_entry_source:source,observed_current_main_sha:freshMain,expected_current_main_sha:freshMain,base_entry_blob_sha:actualBlob,expected_entry_blob_sha:'0000000000000000000000000000000000000000'});
pass('unrelated expected entry blob fails closed before structural success',wrongBlob.status==='entry_blob_mismatch'&&wrongBlob.baseline_matches===false);
const forgedBlob=buildMemberProductionEntryDefaultOffManifest({production_entry_source:source,observed_current_main_sha:freshMain,expected_current_main_sha:freshMain,base_entry_blob_sha:'0000000000000000000000000000000000000000',expected_entry_blob_sha:'0000000000000000000000000000000000000000'});
pass('equal but unrelated blob evidence cannot authorize structurally valid source',forgedBlob.status==='entry_source_blob_mismatch'&&forgedBlob.baseline_matches===false&&forgedBlob.source_blob_sha===actualBlob);
const invalidSource=source.replace('const MEMBER_PRODUCTION_OWNER_APPROVED=false;','const MEMBER_PRODUCTION_OWNER_APPROVED=true;');
const invalidBlob=gitBlobSha(invalidSource);
const invalid=buildMemberProductionEntryDefaultOffManifest({production_entry_source:invalidSource,observed_current_main_sha:freshMain,expected_current_main_sha:freshMain,base_entry_blob_sha:invalidBlob,expected_entry_blob_sha:invalidBlob});
pass('unknown source failing structural inspection fails closed',invalid.status==='entry_source_inspection_failed');
pass('manifest operation performs zero prohibited mutation/fetch',Object.values(manifest.invariant).every(value=>value===false));

const health=memberProductionEntryDefaultOffManifestHealth();
pass('health uses fresh exact main and source-bound entry blob plus structural stage inspection',health.main_sha_strategy==='fresh_exact_match_required'&&health.entry_blob_sha_strategy==='fresh_exact_match_required'&&health.source_blob_sha_verification===true&&health.static_main_sha_pinning===false&&health.static_entry_blob_pinning===false&&health.fresh_main_gate_required===true&&health.fresh_entry_blob_gate_required===true&&health.structural_source_inspection===true);
pass('health retains historical blob only as provenance',health.historical_default_off_entry_blob_sha===__test.HISTORICAL_DEFAULT_OFF_ENTRY_BLOB_SHA);
pass('health expects wired private media but fetch remains off',health.private_media_source_wiring_expected===true&&health.production_storage_fetch===false);
pass('health does not claim Production deploy or route activation',health.production_runtime_deployed===false&&health.production_route_activated===false&&health.production_deploy===false&&health.production_write===false);

console.log('MEMBER_PRODUCTION_ENTRY_DEFAULT_OFF_MANIFEST='+n+'/'+n+' PASS');
