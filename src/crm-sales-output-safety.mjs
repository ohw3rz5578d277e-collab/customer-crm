export function safeCsvCell(value){
  const raw=Array.isArray(value)?value.join(' | '):String(value??'');
  const neutralized=/^\s*[=+\-@]/u.test(raw)?"'"+raw:raw;
  return '"'+neutralized.replaceAll('"','""')+'"';
}

export function escapeHtml(value){
  return String(value??'')
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'",'&#39;');
}
