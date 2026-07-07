/**
 * SystemToggle — operator-shell switch Heatr ⇄ Warmr (A0, Sprint 5).
 *
 * Heatr-kant is de huidige app; Warmr-kant is een LINK-OUT (volledige
 * navigatie, geen iframe) naar Warmr's bestaande app onder dezelfde origin
 * (`/warmr/` — Vite-proxy in dev, reverse-proxy in prod). Same-origin ⇒ de
 * Supabase-sessie in localStorage is gedeeld, dus één login geldt voor beide.
 *
 * Bewust een plain <a> (geen react-router <Link>): `/warmr/` is geen
 * react-route maar een aparte app op dezelfde origin, dus we willen een echte
 * page-navigatie.
 */
const WARMR_PATH = import.meta.env.VITE_WARMR_PATH || '/warmr/';

export function SystemToggle() {
  return (
    <div
      className="flex items-center rounded-lg border border-[var(--color-border)] bg-[var(--color-ivory-100)] p-0.5 text-xs font-semibold"
      role="group"
      aria-label="Systeem"
    >
      <span
        aria-current="page"
        className="rounded-md px-2.5 py-1 bg-white text-[var(--color-blush-700)] shadow-sm"
      >
        Heatr
      </span>
      <a
        href={WARMR_PATH}
        title="Wissel naar Warmr — zelfde login (same-origin sessie)"
        className="rounded-md px-2.5 py-1 text-[var(--color-stone-500)] transition-colors hover:text-[var(--color-stone-800)]"
      >
        Warmr
      </a>
    </div>
  );
}
