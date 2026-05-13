import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';

export function PlaceholderPage({ title, subtitle }: { title: string; subtitle?: string }) {
  return (
    <div className="max-w-6xl mx-auto px-10 py-8">
      <PageHeader title={title} subtitle={subtitle} />
      <Card className="p-16 text-center">
        <div className="font-display text-5xl text-[var(--color-stone-200)] mb-3">⬡</div>
        <p className="text-sm text-[var(--color-stone-500)]">
          Deze pagina wordt binnenkort gemigreerd naar de nieuwe UI.
        </p>
      </Card>
    </div>
  );
}
