import {
  buildMemberCreativeCreateViewModel,
  toggleMemberCreativeMediaSelection,
  runMemberCreativeCreatePreview,
  saveMemberCreativeOutputFromUserGesture,
  renderMemberCreativeCreateMarkup,
  memberCreativeCreateCss
} from './member-creative-create-ui.mjs';

const BUILD='member-create-shell-integration-20260925-01';
const text=v=>v==null?'':String(v).trim();

function createPreviewReleaser(urlRef){
  return output=>{
    const objectUrl=text(output?.object_url);
    if(!objectUrl.startsWith('blob:'))return false;
    if(!urlRef||typeof urlRef.revokeObjectURL!=='function')return false;
    urlRef.revokeObjectURL(objectUrl);
    return true;
  };
}

export function resolveMemberCreateMount(shellRoot){
  if(!shellRoot||typeof shellRoot.querySelector!=='function')return null;
  return shellRoot.querySelector('[data-member-mount="create"]')||null;
}

export function createMemberCreativeShellController({
  shell_root,
  catalog,
  memory_details=[],
  plan_composition,
  render_composition,
  runtime,
  document_ref=globalThis.document,
  url_ref=globalThis.URL,
  on_state_change=null,
  on_error=null
}={}){
  const mount=resolveMemberCreateMount(shell_root);
  if(!mount||typeof mount.addEventListener!=='function'){
    return {ok:false,error:'member_create_mount_required'};
  }

  const releasePreview=createPreviewReleaser(url_ref);
  const state={
    selected_template_id:'',
    selected_media_ids:[],
    preview:null,
    destroyed:false
  };

  const notify=()=>{
    if(typeof on_state_change==='function'){
      on_state_change({
        selected_template_id:state.selected_template_id,
        selected_media_ids:[...state.selected_media_ids],
        preview_ready:viewModel().preview_ready===true
      });
    }
  };

  const viewModel=()=>buildMemberCreativeCreateViewModel({
    catalog,
    memory_details,
    selected_template_id:state.selected_template_id,
    selected_media_ids:state.selected_media_ids,
    preview:state.preview
  });

  const render=()=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    const vm=viewModel();
    mount.innerHTML='<style data-member-create-style>'+memberCreativeCreateCss()+'</style>'+renderMemberCreativeCreateMarkup(vm);
    return {ok:true,view_model:vm};
  };

  const clearPreview=()=>{
    if(state.preview?.output)releasePreview(state.preview.output);
    state.preview=null;
  };

  const selectTemplate=templateId=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    const id=text(templateId);
    const vm=viewModel();
    const template=(vm.templates||[]).find(item=>item.template_id===id);
    if(!template)return {ok:false,error:'creative_template_not_found'};
    if(template.eligible!==true)return {ok:false,error:'creative_template_locked'};

    clearPreview();
    state.selected_template_id=id;
    state.selected_media_ids=[];
    const result=render();
    notify();
    return {ok:true,view_model:result.view_model};
  };

  const toggleMedia=mediaId=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    const vm=viewModel();
    if(!vm.selected_template)return {ok:false,error:'creative_template_required'};
    const next=toggleMemberCreativeMediaSelection(vm,mediaId);
    const before=state.selected_media_ids.join('\u0000');
    const after=next.join('\u0000');
    if(before===after)return {ok:false,error:'creative_media_not_selectable'};

    clearPreview();
    state.selected_media_ids=next;
    const result=render();
    notify();
    return {ok:true,view_model:result.view_model};
  };

  const preview=async()=>{
    if(state.destroyed)return {status:'controller_destroyed',rendered:false};
    const vm=viewModel();
    const result=await runMemberCreativeCreatePreview({
      view_model:vm,
      plan_composition,
      render_composition,
      runtime
    });

    if(result?.status==='ok'&&result?.rendered===true&&result?.output){
      clearPreview();
      state.preview=result;
    }else{
      clearPreview();
    }

    render();
    notify();
    if(result?.status!=='ok'&&typeof on_error==='function')on_error(result);
    return result;
  };

  const save=()=>{
    if(state.destroyed)return {ok:false,error:'controller_destroyed'};
    const vm=viewModel();
    if(vm.preview_ready!==true||!vm.preview){
      return {ok:false,error:'creative_preview_required'};
    }
    return saveMemberCreativeOutputFromUserGesture(vm.preview,{document_ref});
  };

  const handleClick=event=>{
    if(state.destroyed)return;
    const target=event?.target?.closest?.('[data-creative-template],[data-creative-media],[data-creative-preview],[data-creative-save]');
    if(!target||!mount.contains?.(target))return;

    if(target.hasAttribute?.('data-creative-template')){
      selectTemplate(target.getAttribute('data-creative-template'));
      return;
    }
    if(target.hasAttribute?.('data-creative-media')){
      toggleMedia(target.getAttribute('data-creative-media'));
      return;
    }
    if(target.hasAttribute?.('data-creative-preview')){
      if(target.disabled)return;
      void preview();
      return;
    }
    if(target.hasAttribute?.('data-creative-save')){
      if(target.disabled)return;
      const result=save();
      if(!result.ok&&typeof on_error==='function')on_error(result);
    }
  };

  mount.addEventListener('click',handleClick);
  const initial=render();

  return {
    ok:true,
    source_only:true,
    mount,
    get_view_model:viewModel,
    render,
    select_template:selectTemplate,
    toggle_media:toggleMedia,
    preview,
    save,
    destroy(){
      if(state.destroyed)return {ok:true,already_destroyed:true};
      clearPreview();
      state.destroyed=true;
      if(typeof mount.removeEventListener==='function'){
        mount.removeEventListener('click',handleClick);
      }
      return {ok:true,already_destroyed:false};
    },
    initial_view_model:initial.view_model
  };
}

export function memberCreateShellIntegrationHealth(){
  return {
    member_create_shell_integration:true,
    build:BUILD,
    source_only:true,
    canonical_mount:'create',
    uses_existing_create_ui:true,
    planner_injected:true,
    renderer_injected:true,
    runtime_injected:true,
    auto_fetch:false,
    auto_preview:false,
    auto_download:false,
    generated_output_server_write:false,
    customer_id_exposed:false,
    family_id_exposed:false,
    private_storage_key_exposed:false,
    arbitrary_external_navigation:false,
    production_route_wired:false,
    production_private_media_delivery_active:false,
    production_write:false
  };
}

export const __test={
  createPreviewReleaser
};
