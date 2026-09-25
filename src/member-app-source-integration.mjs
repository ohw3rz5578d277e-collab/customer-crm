import { mountMemberHomeUi } from './member-home-ui.mjs';
import { createMemberMemoriesController } from './member-memories-ui.mjs';
import { createMemberCreativeShellController } from './member-create-shell-integration.mjs';
import { createMemberShopController } from './member-shop-ui.mjs';
import { mountMemberMyUi } from './member-my-ui.mjs';
import {
  memberAppShellUiHealth,
  setMemberAppShellActiveTab
} from './member-app-shell-ui.mjs';
import { memberHomeUiHealth } from './member-home-ui.mjs';
import { memberMemoriesUiHealth } from './member-memories-ui.mjs';
import { memberCreateShellIntegrationHealth } from './member-create-shell-integration.mjs';
import { memberShopUiHealth } from './member-shop-ui.mjs';
import { memberMyUiHealth } from './member-my-ui.mjs';

const BUILD='member-app-source-integration-20260925-01';
const TABS=Object.freeze(['home','memories','create','shop','my']);

function validateCanonicalMounts(shellRoot){
  if(!shellRoot||typeof shellRoot.querySelector!=='function'){
    return {ok:false,error:'member_app_shell_required',mounts:{}};
  }

  const mounts={};
  for(const tab of TABS){
    const mount=shellRoot.querySelector('[data-member-mount="'+tab+'"]');
    if(!mount||typeof mount.addEventListener!=='function'){
      return {
        ok:false,
        error:'member_app_mount_required',
        missing_tab:tab,
        mounts:{}
      };
    }
    mounts[tab]=mount;
  }

  return {ok:true,mounts};
}

function destroyHandles(handles=[]){
  const results=[];
  for(const handle of [...handles].reverse()){
    if(handle&&typeof handle.destroy==='function'){
      try{
        results.push(handle.destroy());
      }catch(error){
        results.push({ok:false,error:'destroy_failed'});
      }
    }
  }
  return results;
}

export function createMemberAppSourceIntegration({
  shell_root,
  home_result,
  memories_result,
  creative_catalog,
  creative_memory_details=[],
  creative_plan_composition,
  creative_render_composition,
  creative_runtime,
  creative_document_ref=globalThis.document,
  creative_url_ref=globalThis.URL,
  memory_detail_loader,
  shop_result,
  my_result,
  initial_tab='home',
  on_tab_change=null,
  on_error=null
}={}){
  const validated=validateCanonicalMounts(shell_root);
  if(!validated.ok)return validated;

  const handles=[];
  const controllers={};

  const fail=(error,detail={})=>{
    destroyHandles(handles);
    return {
      ok:false,
      error,
      ...detail
    };
  };

  const home=mountMemberHomeUi(shell_root,home_result,{
    on_tab_change
  });
  if(!home?.ok)return fail(home?.error||'home_mount_failed',{tab:'home'});
  handles.push(home);
  controllers.home=home;

  const memories=createMemberMemoriesController({
    shell_root,
    list_result:memories_result,
    load_detail:memory_detail_loader,
    on_error
  });
  if(!memories?.ok)return fail(memories?.error||'memories_mount_failed',{tab:'memories'});
  handles.push(memories);
  controllers.memories=memories;

  const create=createMemberCreativeShellController({
    shell_root,
    catalog:creative_catalog,
    memory_details:creative_memory_details,
    plan_composition:creative_plan_composition,
    render_composition:creative_render_composition,
    runtime:creative_runtime,
    document_ref:creative_document_ref,
    url_ref:creative_url_ref,
    on_error
  });
  if(!create?.ok)return fail(create?.error||'create_mount_failed',{tab:'create'});
  handles.push(create);
  controllers.create=create;

  const shop=createMemberShopController({
    shell_root,
    shop_result
  });
  if(!shop?.ok)return fail(shop?.error||'shop_mount_failed',{tab:'shop'});
  handles.push(shop);
  controllers.shop=shop;

  const my=mountMemberMyUi(shell_root,my_result,{
    on_tab_change
  });
  if(!my?.ok)return fail(my?.error||'my_mount_failed',{tab:'my'});
  handles.push(my);
  controllers.my=my;

  const initial=setMemberAppShellActiveTab(shell_root,initial_tab);
  if(!initial.ok)return fail(initial.error||'initial_tab_failed');

  let destroyed=false;

  return {
    ok:true,
    source_only:true,
    tabs:[...TABS],
    controllers,
    active_tab:initial.active_tab,
    set_active_tab(tab){
      if(destroyed)return {ok:false,error:'controller_destroyed'};
      const result=setMemberAppShellActiveTab(shell_root,tab);
      if(result.ok&&typeof on_tab_change==='function')on_tab_change(result.active_tab);
      return result;
    },
    destroy(){
      if(destroyed)return {ok:true,already_destroyed:true};
      destroyed=true;
      const results=destroyHandles(handles);
      return {
        ok:results.every(result=>result?.ok!==false),
        already_destroyed:false,
        results
      };
    }
  };
}

export function memberAppSourceAcceptanceHealth(){
  const shell=memberAppShellUiHealth();
  const home=memberHomeUiHealth();
  const memories=memberMemoriesUiHealth();
  const create=memberCreateShellIntegrationHealth();
  const shop=memberShopUiHealth();
  const my=memberMyUiHealth();

  return {
    member_app_source_integration:true,
    build:BUILD,
    source_only:true,
    canonical_tabs:[...TABS],
    all_five_ui_foundations_present:
      shell.member_app_shell_ui===true
      && home.member_home_ui===true
      && memories.member_memories_ui===true
      && create.member_create_shell_integration===true
      && shop.member_shop_ui===true
      && my.member_my_ui===true,
    canonical_mounts_required:true,
    partial_mount_fail_closed:true,
    initial_tab_local_only:true,
    auto_fetch:false,
    auto_memory_detail_fetch:false,
    auto_creative_preview:false,
    auto_creative_download:false,
    customer_id_exposed:false,
    family_id_exposed:false,
    child_id_exposed:false,
    private_storage_key_exposed:false,
    private_media_binary_rendering:false,
    favorite_mutation_active:false,
    profile_edit_active:false,
    settings_mutation_active:false,
    shop_checkout_active:false,
    shop_payment_active:false,
    black_discount_enforcement_active:false,
    reservation_creation:false,
    automatic_contact:false,
    line_send:false,
    generated_output_server_write:false,
    production_route_wired:false,
    production_private_media_delivery_active:false,
    production_write:false
  };
}

export const __test={
  TABS,
  validateCanonicalMounts,
  destroyHandles
};
