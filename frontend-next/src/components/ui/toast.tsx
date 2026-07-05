import { useEffect, useState } from 'react';
import type { ToastItem, ToastType } from '@/lib/toast';

const TYPE_STYLES: Record<ToastType, string> = {
  error: 'border-[var(--color-danger,#c0392b)] bg-[var(--color-danger-bg,#fde8e8)] text-[var(--color-danger,#c0392b)]',
  success: 'border-[var(--color-success,#2e7d4f)] bg-[var(--color-success-bg,#e6f4ec)] text-[var(--color-success,#2e7d4f)]',
  info: 'border-[var(--color-info,#2b6cb0)] bg-[var(--color-info-bg,#e8f0fa)] text-[var(--color-info,#2b6cb0)]',
};

/** Rendert toasts die via lib/toast.toast() zijn gedispatcht. */
export function Toaster() {
  const [items, setItems] = useState<ToastItem[]>([]);

  useEffect(() => {
    const handler = (e: Event) => {
      const item = (e as CustomEvent<ToastItem>).detail;
      setItems((prev) => [...prev.slice(-4), item]); // max 5 tegelijk
      setTimeout(() => {
        setItems((prev) => prev.filter((t) => t.id !== item.id));
      }, 6000);
    };
    window.addEventListener('heatr:toast', handler);
    return () => window.removeEventListener('heatr:toast', handler);
  }, []);

  if (items.length === 0) return null;

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2">
      {items.map((t) => (
        <div
          key={t.id}
          className={`rounded-lg border px-4 py-3 text-sm shadow-md max-w-md ${TYPE_STYLES[t.type]}`}
          role="alert"
        >
          {t.message}
          <button
            onClick={() => setItems((prev) => prev.filter((x) => x.id !== t.id))}
            className="ml-3 underline hover:no-underline"
          >
            sluiten
          </button>
        </div>
      ))}
    </div>
  );
}
