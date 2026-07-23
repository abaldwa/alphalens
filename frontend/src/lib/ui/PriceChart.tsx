import { useEffect, useRef } from 'react'
import {
  CandlestickSeries,
  ColorType,
  createChart,
  createSeriesMarkers,
  HistogramSeries,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  type SeriesMarker,
  type Time,
  type UTCTimestamp,
} from 'lightweight-charts'

export interface PriceChartCandle {
  time: string // YYYY-MM-DD
  open: number
  high: number
  low: number
  close: number
  volume?: number
}

export interface PriceChartMarker {
  time: string // YYYY-MM-DD
  position: 'aboveBar' | 'belowBar' | 'inBar'
  color: string
  shape: 'circle' | 'square' | 'arrowUp' | 'arrowDown'
  text?: string
}

export interface PriceChartProps {
  data: PriceChartCandle[]
  markers?: PriceChartMarker[]
  height?: number
  className?: string
}

// Reads the app's CSS custom properties (set per light/dark theme in
// src/index.css) so the chart matches the rest of the UI instead of
// hard-coding colors.
function readThemeColors() {
  const style = getComputedStyle(document.documentElement)
  const get = (name: string, fallback: string) => style.getPropertyValue(name).trim() || fallback
  return {
    background: get('--background', '#ffffff'),
    textColor: get('--foreground', '#12141c'),
    border: get('--border', '#e2e4eb'),
    green: get('--green', '#1a9338'),
    red: get('--red', '#d13438'),
    mutedForeground: get('--muted-foreground', '#6b7280'),
  }
}

/**
 * Reusable OHLC candlestick + volume histogram chart built on
 * lightweight-charts. Wrapped here (rather than left inline in a page) so
 * any other price-chart need in the app can reuse it, per the @/lib/ui
 * library-boundary pattern.
 */
export function PriceChart({ data, markers, height = 420, className }: PriceChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null)
  const markersPluginRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null)

  // Create the chart once.
  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    const colors = readThemeColors()

    const chart = createChart(container, {
      width: container.clientWidth,
      height,
      layout: {
        background: { type: ColorType.Solid, color: colors.background },
        textColor: colors.textColor,
      },
      grid: {
        vertLines: { color: colors.border },
        horzLines: { color: colors.border },
      },
      rightPriceScale: { borderColor: colors.border },
      timeScale: { borderColor: colors.border, timeVisible: false },
      crosshair: { mode: 0 },
    })

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: colors.green,
      downColor: colors.red,
      borderUpColor: colors.green,
      borderDownColor: colors.red,
      wickUpColor: colors.green,
      wickDownColor: colors.red,
    })

    const volumeSeries = chart.addSeries(HistogramSeries, {
      color: colors.mutedForeground,
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    })
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.82, bottom: 0 },
    })
    candleSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.05, bottom: 0.22 },
    })

    chartRef.current = chart
    candleSeriesRef.current = candleSeries
    volumeSeriesRef.current = volumeSeries
    markersPluginRef.current = createSeriesMarkers(candleSeries, [])

    // lightweight-charts does not auto-resize with its container — a
    // ResizeObserver + explicit applyOptions/resize call is required.
    const resizeObserver = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (!entry) return
      const width = entry.contentRect.width
      chart.applyOptions({ width })
    })
    resizeObserver.observe(container)

    return () => {
      resizeObserver.disconnect()
      chart.remove()
      chartRef.current = null
      candleSeriesRef.current = null
      volumeSeriesRef.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [height])

  // Push data whenever it changes.
  useEffect(() => {
    const candleSeries = candleSeriesRef.current
    const volumeSeries = volumeSeriesRef.current
    if (!candleSeries || !volumeSeries) return

    const candleData: CandlestickData<Time>[] = data.map((d) => ({
      time: d.time as Time,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    }))
    const colors = readThemeColors()
    const volumeData: HistogramData<Time>[] = data.map((d) => ({
      time: d.time as Time,
      value: d.volume ?? 0,
      color: d.close >= d.open ? `${colors.green}66` : `${colors.red}66`,
    }))

    candleSeries.setData(candleData)
    volumeSeries.setData(volumeData)

    if (markers && markers.length) {
      const seriesMarkers: SeriesMarker<Time>[] = markers
        .slice()
        .sort((a, b) => (a.time < b.time ? -1 : 1))
        .map((m) => ({
          time: m.time as Time,
          position: m.position,
          color: m.color,
          shape: m.shape,
          text: m.text,
        }))
      markersPluginRef.current?.setMarkers(seriesMarkers)
    } else {
      markersPluginRef.current?.setMarkers([])
    }

    chartRef.current?.timeScale().fitContent()
  }, [data, markers])

  return <div ref={containerRef} className={className} style={{ width: '100%', height }} />
}

export type { UTCTimestamp }
