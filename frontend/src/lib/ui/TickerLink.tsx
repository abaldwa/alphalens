import { useTickerStore } from '@/app/tickerStore'

/**
 * Framework-level convention: every ticker rendered anywhere in the app
 * goes through this one component. Clicking it opens /charts?ticker=... in
 * a new tab — the user should never have to re-select the symbol once the
 * chart window is open. SymbolOverviewPage seeds the global ticker store
 * from the URL param on mount so the new tab lands directly on the right
 * chart, and it stays reactive to store updates for the case where a chart
 * tab is already open and the user wants to re-point it.
 */
export function TickerLink({ ticker }: { ticker: string | null | undefined }) {
  const setTicker = useTickerStore((s) => s.setTicker)

  if (!ticker) return <>{'—'}</>

  return (
    <a
      href={`/charts?ticker=${encodeURIComponent(ticker)}`}
      target="_blank"
      rel="noopener noreferrer"
      onClick={() => setTicker(ticker)}
      className="font-medium text-primary underline-offset-2 hover:underline"
    >
      {ticker}
    </a>
  )
}
