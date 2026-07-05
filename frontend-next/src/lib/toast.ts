export type ToastType = 'error' | 'success' | 'info';

export interface ToastItem {
  id: number;
  message: string;
  type: ToastType;
}

let _nextId = 1;
// Dedup: zelfde message binnen dit venster niet opnieuw tonen. Voorkomt
// toast-spam wanneer een pollende query (Leads refetcht elke 8s) tegen een
// down-API aan blijft lopen.
const DEDUP_WINDOW_MS = 30_000;
const _recent = new Map<string, number>();

/** Toon een toast. Dependency-vrij — dispatcht een window-event dat de
 *  <Toaster/> in App.tsx oppakt. */
export function toast(message: string, type: ToastType = 'error') {
  const now = Date.now();
  const last = _recent.get(message);
  if (last && now - last < DEDUP_WINDOW_MS) return;
  _recent.set(message, now);
  window.dispatchEvent(
    new CustomEvent('heatr:toast', { detail: { id: _nextId++, message, type } })
  );
}
