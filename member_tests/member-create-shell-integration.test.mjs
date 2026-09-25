import {
  resolveMemberCreateMount,
  createMemberCreativeShellController,
  memberCreateShellIntegrationHealth
} from '../src/member-create-shell-integration.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

function fixture(){
  const mount={
    innerHTML:'',
    listener:null,
    addEventListener(type,fn){if(type==='click')this.listener=fn},
    removeEventListener(type,fn){if(type==='click'&&this.listener===fn)this.listener=null},
    contains(){return true}
  };
  const shell={
    querySelector(selector){return selector==='[data-member-mount="create"]'?mount:null}
  };
  return {shell,mount};
}

const catalog={
  status:'ok',
  templates:[
    {
      template_id:'wallpaper-1',
      creative_type:'wallpaper',
      title:'Family Wallpaper',
      description:'1枚の写真でつくる',
      composition:{mode:'single_photo',photo_slots:1,output_mime:'image/jpeg'},
      eligibility:{eligible:true,memories_needed:0},
      asset_ref:{preview_public_asset:{public_path:'/member-assets/creative/wallpaper-1.jpg'}}
    },
    {
      template_id:'locked-1',
      creative_type:'wallpaper',
      title:'Locked',
      composition:{mode:'single_photo',photo_slots:1,output_mime:'image/jpeg'},
      eligibility:{eligible:false,memories_needed:2},
      asset_ref:{}
    }
  ]
};

const memoryDetails=[
  {
    memory:{memory_id:'memory-1',title:'七五三',shoot_date:'2026-11-15'},
    media:[{media_id:'media-1',media_type:'image',role:'hero',width:1800,height:1200}]
  }
];

const {shell,mount}=fixture();
pass('CREATE mount resolves only canonical shell destination',resolveMemberCreateMount(shell)===mount);
pass('missing shell fails mount resolution',resolveMemberCreateMount(null)===null);

let planCalls=0;
let renderCalls=0;
let plannedInput=null;
const revoked=[];
let clicked=0;

const controller=createMemberCreativeShellController({
  shell_root:shell,
  catalog,
  memory_details:memoryDetails,
  plan_composition:async input=>{
    planCalls++;
    plannedInput=input;
    return {
      status:'ok',
      plan:{
        browser_execution:{
          readiness:{runtime_ready:true,blockers:[]}
        }
      }
    };
  },
  render_composition:async()=>{
    renderCalls++;
    return {
      status:'ok',
      rendered:true,
      output:{
        object_url:'blob:creative-output-1',
        filename:'family-wallpaper.jpg',
        mime_type:'image/jpeg',
        width:1080,
        height:1920
      }
    };
  },
  runtime:{},
  url_ref:{revokeObjectURL(value){revoked.push(value)}},
  document_ref:{
    body:{appendChild(){}},
    createElement(){
      return {
        style:{},
        click(){clicked++},
        remove(){}
      };
    }
  }
});

pass('controller mounts source UI',controller.ok===true&&mount.innerHTML.includes('data-member-creative-create'));
pass('mount does not auto plan or render',planCalls===0&&renderCalls===0);
pass('initial UI carries no customer/family identity',!mount.innerHTML.includes('customer_id')&&!mount.innerHTML.includes('family_id'));

const locked=controller.select_template('locked-1');
pass('locked template cannot be selected through controller API',locked.ok===false&&locked.error==='creative_template_locked');

const selected=controller.select_template('wallpaper-1');
pass('eligible template can be selected',selected.ok===true&&selected.view_model.selected_template.template_id==='wallpaper-1');

const media=controller.toggle_media('media-1');
pass('authorized visible media can be selected',media.ok===true&&media.view_model.plan_ready===true);

const result=await controller.preview();
pass('preview uses existing plan and renderer chain',result.status==='ok'&&result.rendered===true&&planCalls===1&&renderCalls===1);
pass('planner input is exact template_id + media_ids only',JSON.stringify(plannedInput)==='{"template_id":"wallpaper-1","media_ids":["media-1"]}');
pass('successful preview becomes local Blob view state',controller.get_view_model().preview_ready===true&&controller.get_view_model().preview.object_url==='blob:creative-output-1');

const saved=controller.save();
pass('save remains explicit user gesture and local only',saved.ok===true&&saved.local_only===true&&saved.server_write===false&&clicked===1);

controller.select_template('wallpaper-1');
pass('changing selection revokes previous Blob URL',revoked.includes('blob:creative-output-1'));

const destroyed=controller.destroy();
pass('destroy removes click listener',destroyed.ok===true&&mount.listener===null);
pass('destroyed controller refuses actions',controller.select_template('wallpaper-1').error==='controller_destroyed');

const bad=createMemberCreativeShellController({shell_root:{querySelector(){return null}},catalog});
pass('controller fails closed without canonical CREATE mount',bad.ok===false&&bad.error==='member_create_mount_required');

const health=memberCreateShellIntegrationHealth();
pass('health declares source-only existing-module integration',health.source_only===true&&health.uses_existing_create_ui===true&&health.canonical_mount==='create');
pass('health keeps network/action automation disabled',health.auto_fetch===false&&health.auto_preview===false&&health.auto_download===false);
pass('health keeps Production and identity boundaries closed',health.production_route_wired===false&&health.production_write===false&&health.customer_id_exposed===false&&health.family_id_exposed===false);

console.log(`MEMBER_CREATE_SHELL_INTEGRATION=${n}/${n} PASS`);
