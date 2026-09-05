import { Navigate, createBrowserRouter } from 'react-router-dom'

import { HomePage } from '@/pages/home/HomePage'

/**
 * Single route tree for the SPA (replaces the former Vite multi-HTML-entry
 * MPA -- one route per former `<section>-<sub>.html` page, same path
 * strings so NAV_SECTIONS hrefs keep working). `/charts` is new: the
 * Symbol Overview route driven by the global ticker store rather than a
 * `?ticker=` query param.
 *
 * [2026-08-13, FE2] Every page was statically imported here, which put all 65
 * of them plus recharts, lightweight-charts and TanStack into ONE 1.5 MB
 * chunk -- every visitor downloaded the DCF page, the TradingView charts and
 * the forensic screeners to look at the home page.
 *
 * Routes now use React Router's `lazy`, so each page is its own chunk and a
 * heavy dependency travels only with the pages that actually use it. HomePage
 * stays eager: it is the landing route, and deferring it would show a blank
 * frame on first paint for no benefit.
 */
export const router = createBrowserRouter([
  { path: '/', element: <HomePage /> },

  { path: '/technical-overview', lazy: async () => ({ Component: (await import('@/pages/technical/overview')).TechnicalOverviewPage }) },
  { path: '/technical-watchlist', lazy: async () => ({ Component: (await import('@/pages/technical/watchlist')).TechnicalWatchlistPage }) },
  { path: '/technical-screener', lazy: async () => ({ Component: (await import('@/pages/technical/screener')).TechnicalScreenerPage }) },
  { path: '/technical-alerts', lazy: async () => ({ Component: (await import('@/pages/technical/alerts')).TechnicalAlertsPage }) },
  { path: '/technical-compare', lazy: async () => ({ Component: (await import('@/pages/technical/compare')).TechnicalComparePage }) },
  { path: '/technical-chart', lazy: async () => ({ Component: (await import('@/pages/technical/chart')).TechnicalChartPage }) },
  { path: '/technical-deep_dive', lazy: async () => ({ Component: (await import('@/pages/technical/deep_dive')).TechnicalDeepDivePage }) },
  { path: '/technical-portfolio', lazy: async () => ({ Component: (await import('@/pages/technical/portfolio')).TechnicalPortfolioPage }) },
  { path: '/technical-experimentation', lazy: async () => ({ Component: (await import('@/pages/technical/experimentation')).TechnicalExperimentationPage }) },
  { path: '/technical-recommended-strategies', lazy: async () => ({ Component: (await import('@/pages/technical/recommended-strategies')).TechnicalRecommendedStrategiesPage }) },
  { path: '/technical-comparison', lazy: async () => ({ Component: (await import('@/pages/technical/comparison')).TechnicalComparisonPage }) },
  { path: '/technical-batch-backtest', lazy: async () => ({ Component: (await import('@/pages/technical/batch-backtest')).TechnicalBatchBacktestPage }) },

  { path: '/fundamental', lazy: async () => ({ Component: (await import('@/pages/fundamental/FundamentalPage')).FundamentalPage }) },
  { path: '/fundamental-screener', lazy: async () => ({ Component: (await import('@/pages/fundamental/screener')).FundamentalScreenerPage }) },
  { path: '/fundamental-peers', lazy: async () => ({ Component: (await import('@/pages/fundamental/peers')).FundamentalPeersPage }) },
  { path: '/fundamental-sector', lazy: async () => ({ Component: (await import('@/pages/fundamental/sector')).FundamentalSectorPage }) },
  { path: '/fundamental-management', lazy: async () => ({ Component: (await import('@/pages/fundamental/management')).FundamentalManagementPage }) },
  { path: '/fundamental-thesis', lazy: async () => ({ Component: (await import('@/pages/fundamental/thesis')).FundamentalThesisPage }) },
  { path: '/fundamental-deep_dive', lazy: async () => ({ Component: (await import('@/pages/fundamental/deep_dive')).FundamentalDeepDivePage }) },
  { path: '/fundamental-strategies', lazy: async () => ({ Component: (await import('@/pages/fundamental/strategies')).FundamentalStrategiesPage }) },

  { path: '/valuation', lazy: async () => ({ Component: (await import('@/pages/valuation/ValuationPage')).ValuationPage }) },
  { path: '/valuation-dcf', lazy: async () => ({ Component: (await import('@/pages/valuation/dcf')).DcfPage }) },
  { path: '/valuation-relative', lazy: async () => ({ Component: (await import('@/pages/valuation/relative')).RelativePage }) },
  { path: '/valuation-batch', lazy: async () => ({ Component: (await import('@/pages/valuation/batch')).BatchPage }) },
  { path: '/valuation-accuracy', lazy: async () => ({ Component: (await import('@/pages/valuation/accuracy')).AccuracyPage }) },

  { path: '/forensic', lazy: async () => ({ Component: (await import('@/pages/forensic/ForensicPage')).ForensicPage }) },
  { path: '/forensic-benford', lazy: async () => ({ Component: (await import('@/pages/forensic/benford')).BenfordPage }) },
  { path: '/forensic-cashflow', lazy: async () => ({ Component: (await import('@/pages/forensic/cashflow')).CashflowPage }) },
  { path: '/forensic-heatmap', lazy: async () => ({ Component: (await import('@/pages/forensic/heatmap')).HeatmapPage }) },
  { path: '/forensic-redflag', lazy: async () => ({ Component: (await import('@/pages/forensic/redflag')).RedflagPage }) },
  { path: '/forensic-report', lazy: async () => ({ Component: (await import('@/pages/forensic/report')).ReportPage }) },
  { path: '/forensic-universe', lazy: async () => ({ Component: (await import('@/pages/forensic/universe')).UniversePage }) },

  { path: '/ml', lazy: async () => ({ Component: (await import('@/pages/ml/MlPage')).MlPage }) },
  { path: '/ml-signal', lazy: async () => ({ Component: (await import('@/pages/ml/signal')).MlSignalPage }) },
  { path: '/ml-backtest', lazy: async () => ({ Component: (await import('@/pages/ml/backtest')).MlBacktestPage }) },
  { path: '/ml-holdings', lazy: async () => ({ Component: (await import('@/pages/ml/holdings')).MlHoldingsPage }) },
  { path: '/ml-positions', lazy: async () => ({ Component: (await import('@/pages/ml/positions')).MlPositionsPage }) },
  { path: '/ml-multibagger', lazy: async () => ({ Component: (await import('@/pages/ml/multibagger')).MlMultibaggerPage }) },
  { path: '/ml-sector_rotation', lazy: async () => ({ Component: (await import('@/pages/ml/sector_rotation')).MlSectorRotationPage }) },
  { path: '/ml-exit_urgency', lazy: async () => ({ Component: (await import('@/pages/ml/exit_urgency')).MlExitUrgencyPage }) },
  { path: '/ml-universe', lazy: async () => ({ Component: (await import('@/pages/ml/universe')).MlUniversePage }) },
  { path: '/ml-tools', lazy: async () => ({ Component: (await import('@/pages/ml/tools')).MlToolsPage }) },
  { path: '/ml-regime', lazy: async () => ({ Component: (await import('@/pages/ml/regime')).MlRegimePage }) },

  { path: '/momentum', lazy: async () => ({ Component: (await import('@/pages/momentum/universe')).MomentumUniversePage }) },
  { path: '/momentum-portfolio', lazy: async () => ({ Component: (await import('@/pages/momentum/portfolio')).MomentumPortfolioPage }) },
  { path: '/momentum-rebalance', lazy: async () => ({ Component: (await import('@/pages/momentum/rebalance')).MomentumRebalancePage }) },
  { path: '/momentum-universe', lazy: async () => ({ Component: (await import('@/pages/momentum/universe')).MomentumUniversePage }) },
  { path: '/momentum-experimentation', lazy: async () => ({ Component: (await import('@/pages/momentum/experimentation')).MomentumExperimentationPage }) },
  { path: '/momentum-dynamic-report', lazy: async () => ({ Component: (await import('@/pages/momentum/dynamic-report')).MomentumDynamicReportPage }) },
  { path: '/momentum-dynamic-report/rolling-returns', lazy: async () => ({ Component: (await import('@/pages/momentum/dynamic-report/rolling-returns')).MomentumRollingReturnsPage }) },
  { path: '/momentum-dynamic-report/strategy-sweep', lazy: async () => ({ Component: (await import('@/pages/momentum/dynamic-report/strategy-sweep')).MomentumStrategySweepPage }) },
  { path: '/momentum-dynamic-report/yoy', lazy: async () => ({ Component: (await import('@/pages/momentum/dynamic-report/yoy')).MomentumYoyPage }) },
  { path: '/momentum-dynamic-report/income-mode', lazy: async () => ({ Component: (await import('@/pages/momentum/dynamic-report/income-mode')).MomentumIncomeModePage }) },
  // Superseded by /backtest-report/consistency, which renders the same pivot
  // through the shared MatrixTable across all four channels rather than
  // momentum alone. Redirected rather than removed so existing links and
  // bookmarks keep working.
  { path: '/momentum-dynamic-report/yoy-matrix', element: <Navigate to="/backtest-report/consistency?channel=momentum" replace /> },
  { path: '/momentum-deploy', lazy: async () => ({ Component: (await import('@/pages/momentum/StrategyDeployPage')).StrategyDeployPage }) },

  { path: '/big_investors', lazy: async () => ({ Component: (await import('@/pages/big_investors/BigInvestorsPage')).BigInvestorsPage }) },
  { path: '/big_investors-announcements', lazy: async () => ({ Component: (await import('@/pages/big_investors/announcements')).BigInvestorsAnnouncementsPage }) },
  { path: '/big_investors-mf_holdings', lazy: async () => ({ Component: (await import('@/pages/big_investors/mf_holdings')).BigInvestorsMfHoldingsPage }) },

  // The unified cross-channel decision report. Every section shares one
  // contract, one set of tables and one strategy identity, so a strategy
  // reads the same on all of them (see features/backtest-report).
  { path: '/backtest-report', lazy: async () => ({ Component: (await import('@/pages/backtest-report/hub')).BacktestReportHubPage }) },
  { path: '/backtest-report/recommendations', lazy: async () => ({ Component: (await import('@/pages/backtest-report/recommendations')).BacktestRecommendationsPage }) },
  // The four metric groups are tabs over one workspace (see MetricTabs); each
  // stays a real route so links, Back and the shared query string keep working.
  { path: '/backtest-report/metrics', lazy: async () => ({ Component: (await import('@/pages/backtest-report/sections')).BacktestAllMetricsPage }) },
  { path: '/backtest-report/returns', lazy: async () => ({ Component: (await import('@/pages/backtest-report/sections')).BacktestReturnsPage }) },
  { path: '/backtest-report/pivot', lazy: async () => ({ Component: (await import('@/pages/backtest-report/pivot')).BacktestPivotPage }) },
  { path: '/backtest-report/consistency', lazy: async () => ({ Component: (await import('@/pages/backtest-report/sections')).BacktestConsistencyPage }) },
  { path: '/backtest-report/risk', lazy: async () => ({ Component: (await import('@/pages/backtest-report/sections')).BacktestRiskPage }) },
  { path: '/backtest-report/trade-quality', lazy: async () => ({ Component: (await import('@/pages/backtest-report/sections')).BacktestTradeQualityPage }) },
  { path: '/backtest-report/strategy/:key', lazy: async () => ({ Component: (await import('@/pages/backtest-report/strategy-detail')).BacktestStrategyDetailPage }) },

  { path: '/backtest', lazy: async () => ({ Component: (await import('@/pages/backtest/BacktestPage')).BacktestPage }) },
  { path: '/backlog', lazy: async () => ({ Component: (await import('@/pages/backlog')).BacklogPage }) },
  { path: '/backtest-experiments', lazy: async () => ({ Component: (await import('@/pages/backtest/ExperimentsPage')).ExperimentsPage }) },
  { path: '/backtest-regimes', lazy: async () => ({ Component: (await import('@/pages/backtest/RegimesPage')).RegimesPage }) },
  // Renamed 2026-09-04: R0 was retired and split into R14-R17, so the old
  // static-report-embed page no longer reflects the current strategy set.
  // Redirected (not removed) so bookmarks/links keep working.
  { path: '/backtest-r0-band-analysis', element: <Navigate to="/momentum-band-strategy-ranking" replace /> },
  { path: '/momentum-band-strategy-ranking', lazy: async () => ({ Component: (await import('@/pages/momentum/band-strategy-ranking')).MomentumBandStrategyRankingPage }) },
  { path: '/momentum-campaign-results', lazy: async () => ({ Component: (await import('@/pages/momentum/campaign-results')).MomentumCampaignResultsPage }) },

  { path: '/ops', lazy: async () => ({ Component: (await import('@/pages/ops/OpsPage')).OpsPage }) },
  { path: '/macro', lazy: async () => ({ Component: (await import('@/pages/macro/MacroPage')).MacroPage }) },

  { path: '/code-graph', lazy: async () => ({ Component: (await import('@/pages/code-graph/index')).default }) },

  { path: '/charts', lazy: async () => ({ Component: (await import('@/pages/symbol/SymbolOverviewPage')).SymbolOverviewPage }) },
])
