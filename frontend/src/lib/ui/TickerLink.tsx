import { useNavigate } from 'react-router-dom'

import { useTickerStore } from '@/app/tickerStore'

/**
 * Framework-level convention: every ticker rendered anywhere in the app
 * goes through this one component. Clicking it writes the symbol into the
 * global ticker store and routes to /charts — the persistent TradingView
 * widget mounted there re-symbolizes reactively off the store rather than
 * remounting, so clicking a second ticker while already on /charts updates
 * the chart in place instead of navigating again.
 */
export function TickerLink({ ticker }: { ticker: string | null | undefined }) {
  const navigate = useNavigate()
  const setTicker = useTickerStore((s) => s.setTicker)

  if (!ticker) return <>{'—'}</>

  return (
    <button
      type="button"
      onClick={() => {
        setTicker(ticker)
        navigate('/charts')
      }}
      className="font-medium text-primary underline-offset-2 hover:underline"
    >
      {ticker}
    </button>
  )
}
