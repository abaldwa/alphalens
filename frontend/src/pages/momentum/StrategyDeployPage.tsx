// Momentum Strategy Deployment Page -- Live Strategy Configuration & Deployment
// Allows selecting per-band strategies with any filter combination, configuring
// capital deployment (one-time + SIP), start date, rebalance schedule, and
// viewing historical YoY returns with Red/Green P&L coloring.
import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'

import {
  AppShell,
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  DataTable,
  Input,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Label,
} from '@/lib/ui'
import { toConfigForm } from '@/features/backtest-report/core/toConfigForm'
import { parsePrefillParam } from '@/features/backtest-report/core/prefill'
import { strategyDetailUrl } from '@/features/backtest-report/core/strategyKey'
import { rate, pct } from '@/features/backtest-report/core/format'
import { useReportData } from '@/features/backtest-report/data/useReportData'
import { apiGet, apiPost, apiPut, apiDelete } from '@/shared/api/client'
import type {
  MomentumStrategyConfigResponse,
  MomentumStrategyConfigCreate,
  MomentumStrategyConfigUpdate,
  MomentumYoyReturnRow,
  MomentumStrategyCategory,
} from './types'

const CATEGORIES = ['all_risk', 'balanced', 'risk_managed', 'max_defensive'] as const
const CATEGORY_LABELS: Record<(typeof CATEGORIES)[number], string> = {
  all_risk: 'All Risk',
  balanced: 'Balanced',
  risk_managed: 'Risk-Managed',
  max_defensive: 'Max-Defensive',
}

const REBALANCE_FREQUENCIES = ['monthly', 'biweekly'] as const
const HMM_REGIME_FILTERS = ['none', 'bearish', 'bearish_sideways'] as const

const BAND_LABELS: Record<number, string> = {
  1: 'Large Cap (1-50)',
  2: 'Mid-Large (51-100)',
  3: 'Mid Cap (101-150)',
  4: 'Small-Mid (151-200)',
  5: 'Small Cap (201-250)',
  6: 'Micro-Small (251-500)',
  7: 'Micro (501-800)',
}

function fmtPct(v: number | null | undefined) {
  return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : '—'
}
function fmtNum(v: number | null | undefined, digits = 2) {
  return typeof v === 'number' ? v.toFixed(digits) : '—'
}
function fmtInr(v: number | null | undefined) {
  return typeof v === 'number' ? `₹${Math.round(v).toLocaleString('en-IN')}` : '—'
}
function fmtDate(v: string | null | undefined) {
  return v ? new Date(v).toLocaleDateString('en-IN', { year: 'numeric', month: 'short', day: 'numeric' }) : '—'
}

interface ConfigFormData {
  band_id: number
  categories: MomentumStrategyCategory[]  // multi-select via checkboxes
  lookback_months: number
  top_n: number
  rebalance_frequency: 'monthly' | 'biweekly'
  // [2026-08-18] grace_period, exit_rank and trailing_stop_pct removed.
  // Momentum is a plain list swap: held while in the top N on raw momentum,
  // sold the moment it is not. downtrend_filter_pct stays -- it is a BUY-side
  // filter, and buy-side filters are retained.
  downtrend_filter_pct: number | null
  hmm_regime_filter: 'none' | 'bearish' | 'bearish_sideways'
  initial_capital: number
  sip_amount: number
  start_date: string
  rebalance_day_of_month: number | null
  portfolio_id: number | null
}

const initialFormState: ConfigFormData = {
  band_id: 1,
  categories: ['balanced'],
  lookback_months: 6,
  top_n: 15,
  rebalance_frequency: 'monthly',
  downtrend_filter_pct: null,
  hmm_regime_filter: 'none',
  initial_capital: 0,
  sip_amount: 0,
  start_date: new Date().toISOString().split('T')[0],
  rebalance_day_of_month: 1,
  portfolio_id: null,
}

export function StrategyDeployPage() {
  const [formData, setFormData] = useState<ConfigFormData>(initialFormState)
  const [editingId, setEditingId] = useState<number | null>(null)
  // Deploy hand-off from /backtest-report: ?prefill=<StrategyKey>[,<key>...].
  // The queue is stepped through one config at a time — the form creates one
  // config per submit, and batching them silently would deploy strategies the
  // user never looked at.
  const [searchParams, setSearchParams] = useSearchParams()
  const prefillKeys = useMemo(
    () => parsePrefillParam(searchParams.get('prefill')),
    [searchParams],
  )
  const [prefillIndex, setPrefillIndex] = useState(0)
  const [unmappedFields, setUnmappedFields] = useState<string[]>([])
  const { strategies: reportStrategies } = useReportData({ channel: 'momentum' })
  const prefillSource = useMemo(() => {
    const key = prefillKeys[prefillIndex]
    return key ? reportStrategies.find((s) => s.key === key) ?? null : null
  }, [prefillKeys, prefillIndex, reportStrategies])
  const [selectedConfigForReturns, setSelectedConfigForReturns] = useState<number | null>(null)
  const [showReturns, setShowReturns] = useState(false)
  const queryClient = useQueryClient()

  // Fetch existing configs
  const configsQuery = useQuery({
    queryKey: ['momentum-configs'],
    queryFn: () => apiGet<MomentumStrategyConfigResponse[]>('/api/v1/momentum/configs'),
  })

  // Fetch bands for selector -- reuses the same /strategies endpoint the
  // rest of the Momentum dashboard uses for its band dropdown (there is no
  // separate /universe/bands route).
  const bandsQuery = useQuery({
    queryKey: ['momentum-strategies'],
    queryFn: () => apiGet<Array<{ band_id: number; rank_start: number; rank_end: number; label: string }>>('/api/v1/momentum/strategies'),
  })

  // Fetch YoY returns when a config is selected
  const returnsQuery = useQuery({
    queryKey: ['momentum-config-returns', selectedConfigForReturns],
    queryFn: () => apiGet<MomentumYoyReturnRow[]>(`/api/v1/momentum/configs/${selectedConfigForReturns}/returns`),
    enabled: !!selectedConfigForReturns,
  })

  // Mutations
  const createMutation = useMutation({
    mutationFn: (data: MomentumStrategyConfigCreate) => apiPost<MomentumStrategyConfigResponse>('/api/v1/momentum/configs', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['momentum-configs'] })
      setFormData(initialFormState)
      setEditingId(null)
      // Advance the prefill queue so a multi-select hand-off walks the user
      // through one strategy per submit, rather than deploying the rest
      // behind their back.
      setPrefillIndex((i) => i + 1)
    },
  })

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: number; data: MomentumStrategyConfigUpdate }) =>
      apiPut<MomentumStrategyConfigResponse>(`/api/v1/momentum/configs/${id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['momentum-configs'] })
      setEditingId(null)
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: number) => apiDelete(`/api/v1/momentum/configs/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['momentum-configs'] })
      if (selectedConfigForReturns === editingId) {
        setSelectedConfigForReturns(null)
        setShowReturns(false)
      }
      setEditingId(null)
    },
  })

  // Seed the form from the backtested definition. Only the fields the report
  // can actually supply are written; the rest keep their defaults and are
  // listed in unmappedFields so the form can require them. A prefilled zero
  // the user does not notice is worse than an empty field that blocks submit.
  useEffect(() => {
    if (!prefillSource) return
    const { values, unmapped } = toConfigForm(prefillSource)
    setFormData((prev) => ({ ...prev, ...(values as Partial<ConfigFormData>) }))
    setUnmappedFields(unmapped)
  }, [prefillSource])

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    // Deployment choices the backtest cannot supply must be filled in
    // explicitly — capital and start date change the outcome, and inheriting
    // a placeholder for them would deploy something nobody chose.
    if (prefillSource) {
      const missing: string[] = []
      if (unmappedFields.includes('initial_capital') && !formData.initial_capital) {
        missing.push('Initial capital')
      }
      if (unmappedFields.includes('start_date') && !formData.start_date) {
        missing.push('Start date')
      }
      if (missing.length > 0) {
        window.alert(`Fill in before deploying: ${missing.join(', ')}`)
        return
      }
    }
    // Each selected category creates a separate config
    const baseConfig = {
      band_id: formData.band_id,
      lookback_months: formData.lookback_months,
      top_n: formData.top_n,
      rebalance_frequency: formData.rebalance_frequency,
      downtrend_filter_pct: formData.downtrend_filter_pct,
      hmm_regime_filter: formData.hmm_regime_filter,
      initial_capital: formData.initial_capital,
      sip_amount: formData.sip_amount,
      start_date: formData.start_date,
      rebalance_day_of_month: formData.rebalance_day_of_month,
      portfolio_id: formData.portfolio_id,
    }

    formData.categories.forEach((category) => {
      const config = { ...baseConfig, category }
      if (editingId) {
        // For editing, we only update the single config
        if (category === formData.categories[0]) {
          updateMutation.mutate({ id: editingId, data: config })
        }
      } else {
        createMutation.mutate(config)
      }
    })
  }

  const handleEdit = (config: MomentumStrategyConfigResponse) => {
    setEditingId(config.config_id)
    setFormData({
      band_id: config.band_id,
      categories: [config.category],
      lookback_months: config.lookback_months,
      top_n: config.top_n,
      rebalance_frequency: config.rebalance_frequency,
      // The API response type marks this optional; ConfigFormData requires it
      // (nullable). Coerce here rather than loosening the form type -- a
      // controlled input needs a defined value, and `undefined` would flip it
      // to uncontrolled mid-edit.
      downtrend_filter_pct: config.downtrend_filter_pct ?? null,
      hmm_regime_filter: config.hmm_regime_filter ?? 'none',
      initial_capital: config.initial_capital,
      sip_amount: config.sip_amount,
      start_date: config.start_date,
      rebalance_day_of_month: config.rebalance_day_of_month ?? null,
      portfolio_id: config.portfolio_id ?? null,
    })
  }

  const handleViewReturns = (config: MomentumStrategyConfigResponse) => {
    setSelectedConfigForReturns(config.config_id)
    setShowReturns(true)
  }

  const handleCancelEdit = () => {
    setEditingId(null)
    setFormData(initialFormState)
  }

  // Config table columns
  const configColumns = useMemo<ColumnDef<MomentumStrategyConfigResponse, unknown>[]>(() => [
    {
      accessorKey: 'band_id',
      header: 'Band',
      cell: ({ getValue }) => {
        const band = getValue() as number
        return BAND_LABELS[band] ?? `Band ${band}`
      },
      meta: { align: 'center' as const },
    },
    {
      accessorKey: 'category',
      header: 'Category',
      cell: ({ getValue }) => {
        const cat = getValue() as string
        return (
          <Badge variant={cat === 'all_risk' ? 'destructive' : cat === 'max_defensive' ? 'outline' : 'secondary'}>
            {CATEGORY_LABELS[cat as keyof typeof CATEGORY_LABELS] ?? cat}
          </Badge>
        )
      },
      meta: { align: 'center' as const },
    },
    {
      accessorKey: 'lookback_months',
      header: 'Lookback (M)',
      meta: { align: 'center' as const },
    },
    {
      accessorKey: 'top_n',
      header: 'Top N',
      meta: { align: 'center' as const },
    },
    {
      accessorKey: 'rebalance_frequency',
      header: 'Rebalance',
      meta: { align: 'center' as const },
    },
    {
      accessorKey: 'downtrend_filter_pct',
      header: 'Downtrend %',
      cell: ({ getValue }) => {
        const v = getValue<number | null>()
        return v ? fmtPct(v / 100) : '—'
      },
      meta: { align: 'center' as const },
    },
    {
      accessorKey: 'hmm_regime_filter',
      header: 'HMM Filter',
      meta: { align: 'center' as const },
    },
    {
      accessorKey: 'initial_capital',
      header: 'Initial Cap',
      cell: ({ getValue }) => fmtInr(getValue<number | null>()),
      meta: { align: 'right' as const },
    },
    {
      accessorKey: 'sip_amount',
      header: 'SIP',
      cell: ({ getValue }) => fmtInr(getValue<number | null>()),
      meta: { align: 'right' as const },
    },
    {
      accessorKey: 'start_date',
      header: 'Start Date',
      cell: ({ getValue }) => fmtDate(getValue<string | null>()),
      meta: { align: 'center' as const },
    },
    {
      accessorKey: 'portfolio_id',
      header: 'Portfolio ID',
      cell: ({ getValue }) => getValue() ?? '—',
      meta: { align: 'left' as const },
    },
    {
      accessorKey: 'is_active',
      header: 'Active',
      cell: ({ getValue }) => (
        <Badge variant={getValue() ? 'success' : 'secondary'}>{getValue() ? 'Yes' : 'No'}</Badge>
      ),
      meta: { align: 'center' as const },
    },
    {
      id: 'actions',
      header: 'Actions',
      cell: ({ row }) => (
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="sm" onClick={() => handleEdit(row.original)}>
            Edit
          </Button>
          <Button variant="ghost" size="sm" onClick={() => handleViewReturns(row.original)}>
            Returns
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="text-red-600 hover:text-red-700"
            onClick={() => deleteMutation.mutate(row.original.config_id)}
            disabled={deleteMutation.isPending}
          >
            Delete
          </Button>
        </div>
      ),
      meta: { align: 'center' as const },
    },
  ], [])

  // YoY Returns table columns with Red/Green P&L
  const yoyColumns = useMemo<ColumnDef<MomentumYoyReturnRow, unknown>[]>(() => [
    {
      accessorKey: 'fiscal_year',
      header: 'Fiscal Year',
      meta: { align: 'center' as const },
    },
    {
      accessorKey: 'cagr_pct',
      header: 'CAGR %',
      cell: ({ getValue }) => fmtPct((getValue() as number) / 100),
      meta: { align: 'right' as const },
    },
    {
      accessorKey: 'pnl',
      header: 'P&L (₹)',
      cell: ({ getValue }) => {
        const val = getValue() as number
        return (
          <span className={val >= 0 ? 'text-green-600 font-medium' : 'text-red-600 font-medium'}>
            {fmtInr(val)}
          </span>
        )
      },
      meta: { align: 'right' as const },
    },
    {
      accessorKey: 'max_drawdown_pct',
      header: 'Max DD %',
      cell: ({ getValue }) => fmtPct((getValue() as number) / 100),
      meta: { align: 'right' as const },
    },
    {
      accessorKey: 'sharpe',
      header: 'Sharpe',
      cell: ({ getValue }) => fmtNum(getValue<number | null>()),
      meta: { align: 'right' as const },
    },
    {
      accessorKey: 'sortino',
      header: 'Sortino',
      cell: ({ getValue }) => fmtNum(getValue<number | null>()),
      meta: { align: 'right' as const },
    },
    {
      accessorKey: 'num_trades',
      header: 'Trades',
      meta: { align: 'center' as const },
    },
  ], [])

  const bands = bandsQuery.data ?? []

  return (
    <AppShell title="Momentum Strategy Deployment" description="Configure and deploy momentum strategies per band with historical YoY returns">
      {prefillSource ? (
        <Card className="mb-4 border-primary">
          <CardHeader>
            <CardTitle>
              Prefilled from backtest — {prefillSource.label}
              {prefillKeys.length > 1
                ? ` (${prefillIndex + 1} of ${prefillKeys.length})`
                : ''}
            </CardTitle>
            <CardDescription>
              Every field below came from this strategy's backtested definition
              and is fully editable. Deployment choices the backtest cannot
              supply are listed as required.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-2 text-sm">
            <div className="flex flex-wrap gap-x-6 gap-y-1 tabular-nums">
              <span>
                CAGR (post-tax){' '}
                <strong>{rate(prefillSource.returns.cagrPostTax)}</strong>
              </span>
              <span>
                Max drawdown <strong>{pct(prefillSource.risk.maxDrawdown)}</strong>
              </span>
              <span>
                3y rolling median{' '}
                <strong>
                  {rate(
                    prefillSource.consistency.rolling.find((w) => w.window === 3)
                      ?.medianCagr ?? null,
                  )}
                </strong>
              </span>
            </div>
            {unmappedFields.length > 0 ? (
              <p className="text-amber">
                Requires your input: {unmappedFields.join(', ')}
              </p>
            ) : null}
            <div className="flex flex-wrap items-center gap-3">
              <a
                href={strategyDetailUrl(prefillSource.key)}
                className="text-primary hover:underline"
              >
                Back to the full backtest report
              </a>
              {prefillIndex + 1 < prefillKeys.length ? (
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={() => setPrefillIndex((i) => i + 1)}
                >
                  Skip to next selected strategy
                </Button>
              ) : null}
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => {
                  const next = new URLSearchParams(searchParams)
                  next.delete('prefill')
                  setSearchParams(next, { replace: true })
                }}
              >
                Clear prefill
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : null}

      {/* Section 1: Strategy Configuration Form */}
      <Card>
        <CardHeader>
          <CardTitle>{editingId ? 'Edit Strategy Config' : 'Create New Strategy Config(s)'}</CardTitle>
          <CardDescription>
            Select one or more categories to create multiple configs at once. Each category creates a separate config with the same parameters.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-6">
            {/* Band Selector */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <div>
                <Label htmlFor="band_id">Market Cap Band</Label>
                <Select value={String(formData.band_id)} onValueChange={(v) => setFormData({ ...formData, band_id: Number(v) })}>
                  <SelectTrigger id="band_id">
                    <SelectValue placeholder="Select band" />
                  </SelectTrigger>
                  <SelectContent>
                    {bands.map((band) => (
                      <SelectItem key={band.band_id} value={String(band.band_id)}>
                        {BAND_LABELS[band.band_id] ?? `Band ${band.band_id} (${band.rank_start}-${band.rank_end})`}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Category Multi-select (checkboxes) */}
              <div className="md:col-span-2 lg:col-span-3">
                <Label>Strategy Categories (multi-select)</Label>
                <div className="flex flex-wrap gap-4 mt-1">
                  {CATEGORIES.map((cat) => (
                    <label key={cat} className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        className="h-4 w-4"
                        checked={formData.categories.includes(cat)}
                        onChange={(e) =>
                          setFormData({
                            ...formData,
                            categories: e.target.checked
                              ? [...formData.categories, cat]
                              : formData.categories.filter((c) => c !== cat),
                          })
                        }
                      />
                      <span>{CATEGORY_LABELS[cat]}</span>
                    </label>
                  ))}
                </div>
                {formData.categories.length === 0 && (
                  <p className="text-red-500 text-sm mt-1">Select at least one category</p>
                )}
              </div>

              {/* Lookback Months */}
              <div>
                <Label htmlFor="lookback_months">Lookback (months)</Label>
                <Input
                  id="lookback_months"
                  type="number"
                  value={formData.lookback_months}
                  onChange={(e) => setFormData({ ...formData, lookback_months: Number(e.target.value) })}
                  min={1}
                  max={24}
                  step={1}
                />
              </div>

              {/* Top N */}
              <div>
                <Label htmlFor="top_n">Top N</Label>
                <Input
                  id="top_n"
                  type="number"
                  value={formData.top_n}
                  onChange={(e) => setFormData({ ...formData, top_n: Number(e.target.value) })}
                  min={1}
                  max={50}
                  step={1}
                />
              </div>

              {/* Rebalance Frequency */}
              <div>
                <Label htmlFor="rebalance_frequency">Rebalance Frequency</Label>
                <Select value={formData.rebalance_frequency} onValueChange={(v) => setFormData({ ...formData, rebalance_frequency: v as 'monthly' | 'biweekly' })}>
                  <SelectTrigger id="rebalance_frequency">
                    <SelectValue placeholder="Select frequency" />
                  </SelectTrigger>
                  <SelectContent>
                    {REBALANCE_FREQUENCIES.map((freq) => (
                      <SelectItem key={freq} value={freq}>
                        {freq === 'monthly' ? 'Monthly' : 'Biweekly'}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>

            {/* Buy-side filters (the former "Tier 2" -- Tier 1 was deprecated 2026-08-18) */}
            <div className="border-t pt-6">
              <h3 className="text-lg font-semibold mb-4">Buy-Side Filters: Downtrend &amp; HMM Regime</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <div>
                  <Label htmlFor="downtrend_filter_pct">Downtrend Filter %</Label>
                  <Input
                    id="downtrend_filter_pct"
                    type="number"
                    value={formData.downtrend_filter_pct ?? ''}
                    onChange={(e) => setFormData({ ...formData, downtrend_filter_pct: e.target.value ? Number(e.target.value) : null })}
                    min={0}
                    max={50}
                    step={0.5}
                    placeholder="e.g., 10, 15"
                  />
                </div>
                <div>
                  <Label htmlFor="hmm_regime_filter">HMM Regime Filter</Label>
                  <Select value={formData.hmm_regime_filter} onValueChange={(v) => setFormData({ ...formData, hmm_regime_filter: v as 'none' | 'bearish' | 'bearish_sideways' })}>
                    <SelectTrigger id="hmm_regime_filter">
                      <SelectValue placeholder="Select filter" />
                    </SelectTrigger>
                    <SelectContent>
                      {HMM_REGIME_FILTERS.map((filter) => (
                        <SelectItem key={filter} value={filter}>
                          {filter === 'none' ? 'None' : filter === 'bearish' ? 'Bearish' : 'Bearish + Sideways'}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>

            {/* Capital Deployment */}
            <div className="border-t pt-6">
              <h3 className="text-lg font-semibold mb-4">Capital Deployment</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <div>
                  <Label htmlFor="initial_capital">Initial Capital (₹)</Label>
                  <Input
                    id="initial_capital"
                    type="number"
                    value={formData.initial_capital}
                    onChange={(e) => setFormData({ ...formData, initial_capital: Number(e.target.value) || 0 })}
                    min={0}
                    step={10000}
                    placeholder="e.g., 1000000"
                  />
                </div>
                <div>
                  <Label htmlFor="sip_amount">SIP Amount (₹)</Label>
                  <Input
                    id="sip_amount"
                    type="number"
                    value={formData.sip_amount}
                    onChange={(e) => setFormData({ ...formData, sip_amount: Number(e.target.value) || 0 })}
                    min={0}
                    step={5000}
                    placeholder="e.g., 50000"
                  />
                </div>
                <div>
                  <Label htmlFor="start_date">Start Date</Label>
                  <Input
                    id="start_date"
                    type="date"
                    value={formData.start_date}
                    onChange={(e) => setFormData({ ...formData, start_date: e.target.value })}
                  />
                </div>
                <div>
                  <Label htmlFor="rebalance_day_of_month">Rebalance Day (1-28)</Label>
                  <Input
                    id="rebalance_day_of_month"
                    type="number"
                    value={formData.rebalance_day_of_month ?? ''}
                    onChange={(e) => setFormData({ ...formData, rebalance_day_of_month: e.target.value ? Number(e.target.value) : null })}
                    min={1}
                    max={28}
                    step={1}
                  />
                </div>
              </div>
            </div>

            {/* Portfolio Selector */}
            <div className="border-t pt-6">
              <h3 className="text-lg font-semibold mb-4">Portfolio Assignment</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="portfolio_id">Portfolio ID (optional)</Label>
                  <Input
                    id="portfolio_id"
                    type="number"
                    placeholder="No portfolio system wired up yet -- leave blank"
                    value={formData.portfolio_id ?? ''}
                    onChange={(e) => setFormData({ ...formData, portfolio_id: e.target.value ? Number(e.target.value) : null })}
                  />
                </div>
              </div>
            </div>

            {/* Submit Buttons */}
            <div className="border-t pt-6 flex flex-wrap gap-3">
              <Button type="submit" disabled={formData.categories.length === 0 || createMutation.isPending || updateMutation.isPending}>
                {editingId ? 'Update Config' : 'Create Config(s)'}
                {(createMutation.isPending || updateMutation.isPending) && '...'}
              </Button>
              {editingId && (
                <Button type="button" variant="outline" onClick={handleCancelEdit}>
                  Cancel
                </Button>
              )}
            </div>
          </form>
        </CardContent>
      </Card>

      {/* Section 2: Deployed Configs Table */}
      <Card>
        <CardHeader>
          <CardTitle>Deployed Strategy Configurations</CardTitle>
          <CardDescription>
            Click "Edit" to modify, "Returns" to view historical YoY returns, "Delete" to deactivate.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <DataTable
            columns={configColumns}
            data={configsQuery.data ?? []}
            isLoading={configsQuery.isLoading}
            emptyMessage="No strategy configs deployed yet. Create your first config above."
          />
        </CardContent>
      </Card>

      {/* Section 3: Historical YoY Returns for Selected Config */}
      {showReturns && selectedConfigForReturns && (
        <Card>
          <CardHeader className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
            <div>
              <CardTitle>Historical YoY Returns (Fiscal Year Apr-Mar)</CardTitle>
              <CardDescription>
                Returns matched from the latest dynamic report for the selected config's parameters.
                Green = Positive P&L, Red = Negative P&L.
              </CardDescription>
            </div>
            <Button variant="outline" size="sm" onClick={() => { setShowReturns(false); setSelectedConfigForReturns(null); }}>
              Close
            </Button>
          </CardHeader>
          <CardContent>
            {returnsQuery.isLoading ? (
              <div className="text-center py-8 text-muted-foreground">Loading returns...</div>
            ) : returnsQuery.data && returnsQuery.data.length > 0 ? (
              <DataTable
                columns={yoyColumns}
                data={returnsQuery.data}
                isLoading={false}
                emptyMessage="No YoY returns found for this config in the dynamic report."
              />
            ) : (
              <div className="text-center py-8 text-muted-foreground">
                No historical returns available for this configuration in the latest dynamic report.
                Run the dynamic report sweep to populate returns for more variants.
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </AppShell>
  )
}