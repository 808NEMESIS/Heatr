import type { ReactNode } from 'react';

interface QueryStateGateProps {
  isLoading: boolean;
  isError: boolean;
  error?: unknown;
  onRetry?: () => void;
  /** True wanneer de query slaagde maar 0 items terugkwam. */
  isEmpty?: boolean;
  emptyLabel?: string;
  emptyHint?: string;
  /** Aantal skeleton-rijen tijdens laden. */
  skeletonRows?: number;
  children: ReactNode;
}

/**
 * Uniforme loading / error / empty / content gate voor pagina-queries.
 * Maakt het onderscheid zichtbaar dat voorheen wegviel achter silent
 * .catch(() => ({}))-patronen: een lege lijst door een API-fout ziet er
 * nu anders uit dan een echt lege lijst.
 */
export function QueryStateGate({
  isLoading,
  isError,
  error,
  onRetry,
  isEmpty = false,
  emptyLabel = 'Geen resultaten',
  emptyHint,
  skeletonRows = 6,
  children,
}: QueryStateGateProps) {
  if (isLoading) {
    return (
      <div className="space-y-2 p-6" aria-busy="true" aria-label="Laden">
        {Array.from({ length: skeletonRows }).map((_, i) => (
          <div key={i} className="skeleton h-12 rounded-lg bg-[var(--color-border,#e5e0da)]" />
        ))}
      </div>
    );
  }

  if (isError) {
    const message =
      error instanceof Error ? error.message : 'Onbekende fout bij het laden';
    return (
      <div className="m-6 rounded-lg border border-[var(--color-danger,#c0392b)] bg-[var(--color-danger-bg,#fde8e8)] p-6 text-sm">
        <p className="font-medium text-[var(--color-danger,#c0392b)] mb-1">
          Kon data niet laden
        </p>
        <p className="text-[var(--color-stone-500,#6b6560)] mb-3">{message}</p>
        {onRetry && (
          <button
            onClick={onRetry}
            className="rounded bg-[var(--color-blush-500,#c45d34)] px-3 py-1.5 text-sm text-white hover:opacity-90"
          >
            Opnieuw proberen
          </button>
        )}
      </div>
    );
  }

  if (isEmpty) {
    return (
      <div className="m-6 rounded-lg border border-dashed border-[var(--color-border,#e5e0da)] p-10 text-center">
        <p className="text-sm font-medium mb-1">{emptyLabel}</p>
        {emptyHint && (
          <p className="text-xs text-[var(--color-stone-500,#6b6560)]">{emptyHint}</p>
        )}
      </div>
    );
  }

  return <>{children}</>;
}
