import { Component, type ErrorInfo, type ReactNode } from 'react';

interface Props {
  children: ReactNode;
}

interface State {
  error: Error | null;
}

/**
 * Root-level error boundary — vangt render-crashes zodat de operator een
 * herstelbare fallback ziet in plaats van een witte pagina. Reset-knop
 * probeert opnieuw te renderen zonder harde page-reload; als dezelfde
 * crash terugkomt is er ook een reload-knop.
 */
export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Console is hier bewust: geen error-tracking-service geconfigureerd.
    console.error('[Heatr] Render-crash:', error, info.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        <div className="flex min-h-screen items-center justify-center bg-[var(--color-background)] p-8">
          <div className="max-w-lg rounded-lg border border-[var(--color-danger,#c0392b)] bg-white p-6 shadow-sm">
            <h1 className="font-display text-xl mb-2">Er ging iets mis</h1>
            <p className="text-sm text-[var(--color-stone-500,#6b6560)] mb-4">
              De pagina crashte tijdens het renderen. Je data is niet
              aangetast — dit is een weergavefout.
            </p>
            <pre className="text-xs bg-[var(--color-danger-bg,#fde8e8)] rounded p-3 mb-4 overflow-x-auto">
              {this.state.error.message}
            </pre>
            <div className="flex gap-3">
              <button
                onClick={() => this.setState({ error: null })}
                className="rounded bg-[var(--color-blush-500,#c45d34)] px-4 py-2 text-sm text-white hover:opacity-90"
              >
                Probeer opnieuw
              </button>
              <button
                onClick={() => window.location.reload()}
                className="rounded border border-[var(--color-border)] px-4 py-2 text-sm hover:bg-[var(--color-background)]"
              >
                Pagina herladen
              </button>
            </div>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
