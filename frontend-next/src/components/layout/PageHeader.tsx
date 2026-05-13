import type { ReactNode } from 'react';
import { cn } from '@/lib/cn';

interface PageHeaderProps {
  eyebrow?: string;
  title: string;
  titleAccent?: string;
  subtitle?: string;
  actions?: ReactNode;
  className?: string;
}

export function PageHeader({ eyebrow, title, titleAccent, subtitle, actions, className }: PageHeaderProps) {
  return (
    <header className={cn('flex items-start justify-between gap-6 mb-8', className)}>
      <div>
        {eyebrow && (
          <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-[var(--color-blush-500)] mb-2">
            — {eyebrow}
          </div>
        )}
        <h1 className="font-display text-[44px] font-semibold leading-[1.05] tracking-tight text-[var(--color-stone-800)]">
          {title}
          {titleAccent && <em className="not-italic font-display italic font-normal text-[var(--color-blush-500)]"> {titleAccent}</em>}
        </h1>
        {subtitle && (
          <p className="mt-2 text-[15px] text-[var(--color-stone-500)] max-w-xl leading-relaxed">
            {subtitle}
          </p>
        )}
      </div>
      {actions && <div className="flex items-center gap-2 shrink-0">{actions}</div>}
    </header>
  );
}
