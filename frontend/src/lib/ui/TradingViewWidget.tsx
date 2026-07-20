import { useEffect, useRef } from 'react'

declare global {
  interface Window {
    TradingView?: { widget: new (config: Record<string, unknown>) => unknown }
  }
}

const SCRIPT_SRC = 'https://s3.tradingview.com/tv.js'
let scriptPromise: Promise<void> | null = null

function loadTradingViewScript(): Promise<void> {
  if (window.TradingView) return Promise.resolve()
  if (!scriptPromise) {
    scriptPromise = new Promise((resolve, reject) => {
      const script = document.createElement('script')
      script.src = SCRIPT_SRC
      script.async = true
      script.onload = () => resolve()
      script.onerror = () => reject(new Error('Failed to load TradingView widget script'))
      document.head.appendChild(script)
    })
  }
  return scriptPromise
}

/**
 * Persistent TradingView widget for the Symbol Overview page. Mounted
 * once at the /charts route (see SymbolOverviewPage) — re-symbolizes via
 * the widget's own `symbol` option on ticker change rather than
 * unmounting/remounting, matching the "instantly load" requirement for
 * clicking a ticker while already on /charts.
 */
export function TradingViewWidget({ ticker, exchangePrefix = 'NSE' }: { ticker: string; exchangePrefix?: string }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const widgetIdRef = useRef(`tradingview-widget-${Math.random().toString(36).slice(2)}`)

  useEffect(() => {
    let cancelled = false
    loadTradingViewScript().then(() => {
      if (cancelled || !window.TradingView || !containerRef.current) return
      containerRef.current.innerHTML = ''
      new window.TradingView.widget({
        container_id: widgetIdRef.current,
        symbol: `${exchangePrefix}:${ticker}`,
        autosize: true,
        interval: 'D',
        timezone: 'Asia/Kolkata',
        theme: document.documentElement.classList.contains('dark') ? 'dark' : 'light',
        style: '1',
        locale: 'en',
        hide_side_toolbar: false,
      })
    })
    return () => {
      cancelled = true
    }
  }, [ticker, exchangePrefix])

  return <div ref={containerRef} id={widgetIdRef.current} className="h-[480px] w-full" />
}
