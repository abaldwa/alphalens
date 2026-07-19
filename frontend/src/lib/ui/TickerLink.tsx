// Framework-level convention: every ticker rendered anywhere in the app
// links to the Technical Chart page (`technical-chart.html?ticker=XYZ`),
// which reads the `ticker` query param on load (see TechnicalChartPage).
export function TickerLink({ ticker }: { ticker: string | null | undefined }) {
  if (!ticker) return <>{'—'}</>
  return (
    <a
      href={`/technical-chart.html?ticker=${encodeURIComponent(ticker)}`}
      className="font-medium text-primary underline-offset-2 hover:underline"
    >
      {ticker}
    </a>
  )
}
