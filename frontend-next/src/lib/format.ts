export function fmtInt(n: number | null | undefined): string {
  if (n == null) return '—';
  return n.toLocaleString('nl-NL');
}

export function fmtEuro(n: number | null | undefined): string {
  if (n == null) return '—';
  return `€ ${Math.round(n).toLocaleString('nl-NL')}`;
}

export function fmtRelative(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  const diffMs = Date.now() - d.getTime();
  const mins = Math.floor(diffMs / 60_000);
  if (mins < 1) return 'zojuist';
  if (mins < 60) return `${mins}m geleden`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}u geleden`;
  const days = Math.floor(hrs / 24);
  if (days < 7) return `${days}d geleden`;
  const weeks = Math.floor(days / 7);
  if (weeks < 5) return `${weeks}w geleden`;
  return d.toLocaleDateString('nl-NL', { day: 'numeric', month: 'short', year: 'numeric' });
}

export function priorityFromScore(score: number | null | undefined): {
  key: 'urgent' | 'hoog' | 'medium' | 'laag';
  label: string;
  color: string;
  bg: string;
  border: string;
} {
  // Drempels: config/scoring_thresholds.py (herijking 2026-07-21, p25/p50/p75).
  // Houd deze getallen gelijk aan die bron.
  const s = score ?? 100;
  if (s < 41) return { key: 'urgent', label: 'urgent', color: '#991b1b', bg: '#fee2e2', border: '#dc2626' };
  if (s < 49) return { key: 'hoog', label: 'hoog', color: '#9a3412', bg: '#fed7aa', border: '#ea580c' };
  if (s < 56) return { key: 'medium', label: 'medium', color: '#854d0e', bg: '#fef3c7', border: '#ca8a04' };
  return { key: 'laag', label: 'laag', color: '#14532d', bg: '#dcfce7', border: '#16a34a' };
}

export function scoreColor(score: number | null | undefined): string {
  const s = score ?? 0;
  if (s < 30) return '#dc2626';
  if (s < 45) return '#ea580c';
  if (s < 70) return '#ca8a04';
  return '#16a34a';
}
