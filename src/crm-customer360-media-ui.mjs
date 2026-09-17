const BUILD='crm-customer360-media-ui-20260917-02';
const MARKER='crm-customer360-media-ui-20260917-02';

const STYLE=`<style id="${MARKER}-style">
#crmCustomerMediaCard{margin:14px 0;border:1px solid #e5e7eb;border-radius:18px;background:#fff;overflow:hidden}
#crmCustomerMediaCard>summary{list-style:none;display:grid;grid-template-columns:52px minmax(0,1fr) auto;gap:11px;align-items:center;padding:12px 14px;cursor:pointer;-webkit-tap-highlight-color:transparent}
#crmCustomerMediaCard>summary::-webkit-details-marker{display:none}
.crm-cm-avatar{width:52px;height:52px;border-radius:50%;background:#f1f5f9;display:grid;place-items:center;overflow:hidden;font-size:22px;color:#64748b;border:1px solid #e2e8f0}
.crm-cm-avatar img{width:100%;height:100%;object-fit:cover;display:block}
.crm-cm-summary b{display:block;font-size:15px}.crm-cm-summary small{display:block;color:#64748b;margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.crm-cm-chevron{font-size:20px;color:#94a3b8}
.crm-cm-body{border-top:1px solid #eef2f7;padding:14px;display:grid;gap:16px}.crm-cm-section{display:grid;gap:9px}.crm-cm-section h4{margin:0;font-size:14px}.crm-cm-help{margin:0;color:#64748b;font-size:12px;line-height:1.55}.crm-cm-preview{display:flex;align-items:center;gap:12px}.crm-cm-preview .crm-cm-avatar{width:68px;height:68px;flex:0 0 68px}.crm-cm-actions{display:flex;gap:8px;flex-wrap:wrap}.crm-cm-btn{min-height:42px;border:1px solid #dbe2ea;border-radius:12px;background:#fff;color:#0969da;font:inherit;font-weight:800;padding:9px 12px}.crm-cm-btn.primary{background:#0969da;color:#fff;border-color:#0969da}.crm-cm-btn:disabled{opacity:.55}.crm-cm-form{display:grid;gap:8px}.crm-cm-form input{width:100%;min-height:44px;border:1px solid #dbe2ea;border-radius:12px;padding:9px 11px;font:inherit;font-size:16px}.crm-cm-links{display:grid;gap:8px}.crm-cm-link{border:1px solid #eef2f7;border-radius:12px;padding:10px;display:grid;gap:4px}.crm-cm-link a{color:#0969da;font-weight:800;overflow-wrap:anywhere}.crm-cm-link small{color:#64748b}.crm-cm-status{min-height:18px;color:#64748b;font-size:12px}.crm-cm-status.bad{color:#b91c1c}
#crmCustomerAvatarHero{margin:0 0 14px;padding:14px;border:1px solid #e2e8f0;border-radius:18px;background:#fff;display:flex;align-items:center;gap:14px}
#crmCustomerAvatarHero .crm-cm-avatar{width:76px;height:76px;flex:0 0 76px;font-size:28px}
.crm-cm-hero-copy{min-width:0;flex:1}.crm-cm-hero-copy b{display:block;font-size:16px}.crm-cm-hero-copy small{display:block;margin-top:4px;color:#64748b;line-height:1.45}
.crm-cm-hero-actions{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}
.crm-cm-btn.danger{color:#b91c1c;border-color:#fecaca;background:#fff}
@media(max-width:767px){#crmCustomerAvatarHero{align-items:flex-start;flex-wrap:wrap}.crm-cm-hero-copy{min-width:calc(100% - 96px)}.crm-cm-hero-actions{width:100%;justify-content:flex-start}.crm-cm-btn{min-height:44px}}
</style>`;

const SCRIPT=`<script id="${MARKER}">(function(){
if(window.__crmCustomerMediaUi20260917)return;window.__crmCustomerMediaUi20260917=1;
var currentId='',selectedAvatar='';
function text(v){return v==null?'':String(v).trim()}
function esc(v){return text(v).replace(/[&<>\"']/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]})}
function body(){return document.getElementById('crmMktDetailBody')}
async function waitForDetail(id,n){n=n||0;var h=body();if(h&&h.textContent.includes(id))return h;if(n>200)return null;await new Promise(function(r){setTimeout(r,50)});return waitForDetail(id,n+1)}
function status(msg,bad){var el=document.getElementById('crmCmStatus');if(!el)return;el.textContent=msg||'';el.classList.toggle('bad',!!bad)}
function friendlyMediaError(err){var code=text(err&&err.message||err);if(code==='customer360_media_write_disabled'||code==='customer360_write_disabled')return 'プロフィール画像の保存はまだ有効化されていません。';if(code==='owner_auth_required'||code==='unauthorized')return 'プロフィール画像を保存する権限を確認してください。';return 'プロフィール画像を保存できませんでした。'}
async function api(path,opts){var r=await fetch(path,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opts||{}));var t=await r.text(),j={};try{j=JSON.parse(t)}catch(_){}if(!r.ok||!j||j.ok===false)throw new Error(j&&j.error||('HTTP '+r.status));return j}
function avatarHtml(media,cls){var src=text(media&&media.avatar_data_url);return '<span class="crm-cm-avatar'+(cls?' '+cls:'')+'">'+(src?'<img src="'+esc(src)+'" alt="顧客の代表写真">':'📷')+'</span>'}
function linkHtml(x){var label=text(x&&x.label)||'Amazon Photos';var date=text(x&&x.delivered_at||x&&x.created_at).slice(0,10);return '<div class="crm-cm-link"><a href="'+esc(x.url)+'" target="_blank" rel="noopener noreferrer">'+esc(label)+'</a><small>'+(date?esc(date)+' / ':'')+'Amazon Photos</small></div>'}
function render(id,media){
 var h=body();if(!h||id!==currentId)return;
 h.querySelector('#crmCustomerMediaCard')?.remove();
 h.querySelector('#crmCustomerAvatarHero')?.remove();
 var links=Array.isArray(media&&media.delivery_links)?media.delivery_links:[],latest=media&&media.latest_delivery_link;
 var hero=document.createElement('section');hero.id='crmCustomerAvatarHero';
 hero.innerHTML=avatarHtml(media)+'<div class="crm-cm-hero-copy"><b>プロフィール画像</b><small>'+(media&&media.avatar_data_url?'設定済み。顧客詳細とLINE画面に表示します。':'未設定です。写真を設定できます。')+'</small></div><div class="crm-cm-hero-actions"><button type="button" class="crm-cm-btn" id="crmCmHeroChoose">画像を変更</button>'+(media&&media.avatar_data_url?'<button type="button" class="crm-cm-btn danger" id="crmCmHeroDelete">削除</button>':'')+'</div>';
 var card=document.createElement('details');card.id='crmCustomerMediaCard';
 card.innerHTML='<summary>'+avatarHtml(media)+'<span class="crm-cm-summary"><b>写真・納品</b><small>'+(latest?'最新のAmazon Photosリンクあり':'プロフィール画像・納品リンクを管理')+'</small></span><span class="crm-cm-chevron">›</span></summary><div class="crm-cm-body"><section class="crm-cm-section"><h4>プロフィール画像</h4><p class="crm-cm-help">顧客一覧・詳細・LINE画面に使う小さい画像だけを保存します。元画像は保存しません。</p><div class="crm-cm-preview">'+avatarHtml(media)+'<div class="crm-cm-actions"><button type="button" class="crm-cm-btn" id="crmCmChoose">ライブラリから選択</button><button type="button" class="crm-cm-btn" id="crmCmCamera">カメラで撮影</button><button type="button" class="crm-cm-btn primary" id="crmCmSaveAvatar" disabled>保存</button>'+(media&&media.avatar_data_url?'<button type="button" class="crm-cm-btn danger" id="crmCmDeleteAvatar">削除</button>':'')+'</div></div><input id="crmCmFile" type="file" accept="image/jpeg,image/png,image/webp" hidden><input id="crmCmCameraFile" type="file" accept="image/jpeg,image/png,image/webp" capture="environment" hidden></section><section class="crm-cm-section"><h4>Amazon Photos 納品リンク</h4><p class="crm-cm-help">撮影ごとに残せるので、過去の納品先も上書きされません。</p><div class="crm-cm-form"><input id="crmCmLinkLabel" type="text" placeholder="例：2026年9月 お宮参り"><input id="crmCmLinkUrl" type="url" inputmode="url" placeholder="https://..."><input id="crmCmDeliveredAt" type="date"><button type="button" class="crm-cm-btn primary" id="crmCmSaveLink">納品リンクを保存</button></div><div class="crm-cm-links">'+(links.length?links.slice(0,5).map(linkHtml).join(''):'<p class="crm-cm-help">まだ納品リンクはありません。</p>')+'</div></section><div class="crm-cm-status" id="crmCmStatus"></div></div>';
 h.prepend(hero);h.appendChild(card);bind(id,card,hero)
}
async function load(id){id=text(id);if(!/^\d{8}$/.test(id))return;currentId=id;selectedAvatar='';var h=await waitForDetail(id,0);if(!h||id!==currentId)return;try{var d=await api('/api/customer360/media/'+id);if(id===currentId)render(id,d.media||{})}catch(e){/* Existing Customer360 detail stays usable if media schema is not ready. */}}
function readAsImage(file){return new Promise(function(resolve,reject){var url=URL.createObjectURL(file),img=new Image();img.onload=function(){try{var size=256,c=document.createElement('canvas');c.width=size;c.height=size;var g=c.getContext('2d'),sw=img.naturalWidth,sh=img.naturalHeight,s=Math.min(sw,sh),sx=(sw-s)/2,sy=(sh-s)/2;g.drawImage(img,sx,sy,s,s,0,0,size,size);var out=c.toDataURL('image/jpeg',0.72);URL.revokeObjectURL(url);if(out.length>95000)reject(new Error('画像が大きすぎます。別の写真を選んでください'));else resolve(out)}catch(e){URL.revokeObjectURL(url);reject(e)}};img.onerror=function(){URL.revokeObjectURL(url);reject(new Error('画像を読み込めませんでした'))};img.src=url})}
function bind(id,card,hero){
 var choose=card.querySelector('#crmCmChoose'),camera=card.querySelector('#crmCmCamera'),file=card.querySelector('#crmCmFile'),cameraFile=card.querySelector('#crmCmCameraFile'),saveAvatar=card.querySelector('#crmCmSaveAvatar'),deleteAvatar=card.querySelector('#crmCmDeleteAvatar'),saveLink=card.querySelector('#crmCmSaveLink'),date=card.querySelector('#crmCmDeliveredAt');
 if(date&&!date.value)date.value=new Date().toISOString().slice(0,10);
 function preview(fileObj){if(!fileObj)return;status('写真を準備中…');readAsImage(fileObj).then(function(out){selectedAvatar=out;var img=card.querySelector('.crm-cm-preview .crm-cm-avatar');if(img)img.innerHTML='<img src="'+esc(selectedAvatar)+'" alt="選択したプロフィール画像">';saveAvatar.disabled=false;card.open=true;status('この写真でよければ「保存」を押してください')}).catch(function(e){selectedAvatar='';saveAvatar.disabled=true;status(e.message,true)})}
 choose.onclick=function(){file.click()};camera.onclick=function(){cameraFile.click()};
 file.onchange=function(){preview(file.files&&file.files[0])};cameraFile.onchange=function(){preview(cameraFile.files&&cameraFile.files[0])};
 var heroChoose=hero&&hero.querySelector('#crmCmHeroChoose');if(heroChoose)heroChoose.onclick=function(){card.open=true;file.click()};
 async function removeAvatar(){status('削除中…');try{var d=await api('/api/customer360/media/'+id,{method:'PATCH',body:JSON.stringify({avatar_data_url:''})});selectedAvatar='';status('プロフィール画像を削除しました');render(id,d.media||{})}catch(e){status(friendlyMediaError(e),true)}}
 var heroDelete=hero&&hero.querySelector('#crmCmHeroDelete');if(heroDelete)heroDelete.onclick=removeAvatar;if(deleteAvatar)deleteAvatar.onclick=removeAvatar;
 saveAvatar.onclick=async function(){if(!selectedAvatar)return;saveAvatar.disabled=true;status('保存中…');try{var d=await api('/api/customer360/media/'+id,{method:'PATCH',body:JSON.stringify({avatar_data_url:selectedAvatar})});selectedAvatar='';status('プロフィール画像を保存しました');render(id,d.media||{})}catch(e){saveAvatar.disabled=false;status(friendlyMediaError(e),true)}};
 saveLink.onclick=async function(){var url=text(card.querySelector('#crmCmLinkUrl').value),label=text(card.querySelector('#crmCmLinkLabel').value),delivered_at=text(date&&date.value);if(!url){status('Amazon PhotosのURLを入力してください',true);return}saveLink.disabled=true;status('保存中…');try{var d=await api('/api/customer360/media/'+id+'/delivery-links',{method:'POST',body:JSON.stringify({provider:'amazon_photos',url:url,label:label,delivered_at:delivered_at})});status('納品リンクを保存しました');render(id,d.media||{})}catch(e){saveLink.disabled=false;status('納品リンクを保存できませんでした。',true)}}
}
document.addEventListener('crm:customer-detail-opened',function(e){var id=text(e&&e.detail&&e.detail.customer_id);if(/^\d{8}$/.test(id))load(id)});document.addEventListener('click',function(e){var x=e.target&&e.target.closest?e.target.closest('[data-open],[data-direct-customer]'):null;var id=text(x&&((x.dataset&&x.dataset.open)||(x.dataset&&x.dataset.directCustomer)));if(/^\d{8}$/.test(id))setTimeout(function(){load(id)},0)},true);
})();</script>`;

export function injectCustomer360MediaUi(html){let out=String(html||'');if(!out||out.includes(MARKER))return out;out=out.includes('</head>')?out.replace('</head>',STYLE+'</head>'):STYLE+out;out=out.includes('</body>')?out.replace('</body>',SCRIPT+'</body>'):out+SCRIPT;return out}
export function customer360MediaUiHealth(){return{customer360_media_ui:true,customer360_media_ui_build:BUILD,customer360_media_ui_compact_details:true,customer360_media_ui_single_owner:true,customer360_avatar_client_compression:true}}
