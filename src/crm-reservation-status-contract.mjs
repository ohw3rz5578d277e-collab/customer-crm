const text=v=>v==null?'':String(v).trim();
const COMPLETED=new Set(['撮影完了','撮影済み','先行データ待ち','本納品まち','納品済み','completed','complete','done']);
const CANCELLED=new Set(['キャンセル','cancelled','canceled']);

export function normalizeReservationStatus(value){
  return text(value).toLowerCase();
}

export function isCompletedReservationStatus(value){
  const raw=text(value);
  return COMPLETED.has(raw)||COMPLETED.has(normalizeReservationStatus(raw));
}

export function isCancelledReservationStatus(value){
  const raw=text(value);
  return CANCELLED.has(raw)||CANCELLED.has(normalizeReservationStatus(raw));
}

export function reservationStatusContract(){
  return {
    completed:[...COMPLETED],
    cancelled:[...CANCELLED]
  };
}
