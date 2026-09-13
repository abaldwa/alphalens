import { lazy, Suspense, type ReactNode } from 'react'
import { createBrowserRouter } from 'react-router-dom'

// Every page is loaded via a dynamic import() so Vite can code-split each
// route (and the heavy per-page libraries it pulls in, e.g. `recharts` on
// chart-heavy pages and `lightweight-charts` on the OHLC pages) into its
// own chunk instead of all landing in one multi-MB `ui` bundle downloaded
// on first paint. See FeatureBacklog.md FE2.
const HomePage = lazy(() =>
  import('@/pages/home/HomePage').then((m) => ({ default: m.HomePage })),
)

const TechnicalOverviewPage = lazy(() =>
  import('@/pages/technical/overview').then((m) => ({ default: m.TechnicalOverviewPage })),
)
const TechnicalWatchlistPage = lazy(() =>
  import('@/pages/technical/watchlist').then((m) => ({ default: m.TechnicalWatchlistPage })),
)
const TechnicalScreenerPage = lazy(() =>
  import('@/pages/technical/screener').then((m) => ({ default: m.TechnicalScreenerPage })),
)
const TechnicalAlertsPage = lazy(() =>
  import('@/pages/technical/alerts').then((m) => ({ default: m.TechnicalAlertsPage })),
)
const TechnicalComparePage = lazy(() =>
  import('@/pages/technical/compare').then((m) => ({ default: m.TechnicalComparePage })),
)
const TechnicalChartPage = lazy(() =>
  import('@/pages/technical/chart').then((m) => ({ default: m.TechnicalChartPage })),
)
const TechnicalDeepDivePage = lazy(() =>
  import('@/pages/technical/deep_dive').then((m) => ({ default: m.TechnicalDeepDivePage })),
)
const TechnicalPortfolioPage = lazy(() =>
  import('@/pages/technical/portfolio').then((m) => ({ default: m.TechnicalPortfolioPage })),
)

const FundamentalPage = lazy(() =>
  import('@/pages/fundamental/FundamentalPage').then((m) => ({ default: m.FundamentalPage })),
)
const FundamentalScreenerPage = lazy(() =>
  import('@/pages/fundamental/screener').then((m) => ({ default: m.FundamentalScreenerPage })),
)
const FundamentalPeersPage = lazy(() =>
  import('@/pages/fundamental/peers').then((m) => ({ default: m.FundamentalPeersPage })),
)
const FundamentalSectorPage = lazy(() =>
  import('@/pages/fundamental/sector').then((m) => ({ default: m.FundamentalSectorPage })),
)
const FundamentalManagementPage = lazy(() =>
  import('@/pages/fundamental/management').then((m) => ({ default: m.FundamentalManagementPage })),
)
const FundamentalThesisPage = lazy(() =>
  import('@/pages/fundamental/thesis').then((m) => ({ default: m.FundamentalThesisPage })),
)
const FundamentalDeepDivePage = lazy(() =>
  import('@/pages/fundamental/deep_dive').then((m) => ({ default: m.FundamentalDeepDivePage })),
)

const ValuationPage = lazy(() =>
  import('@/pages/valuation/ValuationPage').then((m) => ({ default: m.ValuationPage })),
)
const DcfPage = lazy(() =>
  import('@/pages/valuation/dcf').then((m) => ({ default: m.DcfPage })),
)
const RelativePage = lazy(() =>
  import('@/pages/valuation/relative').then((m) => ({ default: m.RelativePage })),
)
const BatchPage = lazy(() =>
  import('@/pages/valuation/batch').then((m) => ({ default: m.BatchPage })),
)
const AccuracyPage = lazy(() =>
  import('@/pages/valuation/accuracy').then((m) => ({ default: m.AccuracyPage })),
)

const ForensicPage = lazy(() =>
  import('@/pages/forensic/ForensicPage').then((m) => ({ default: m.ForensicPage })),
)
const BenfordPage = lazy(() =>
  import('@/pages/forensic/benford').then((m) => ({ default: m.BenfordPage })),
)
const CashflowPage = lazy(() =>
  import('@/pages/forensic/cashflow').then((m) => ({ default: m.CashflowPage })),
)
const HeatmapPage = lazy(() =>
  import('@/pages/forensic/heatmap').then((m) => ({ default: m.HeatmapPage })),
)
const RedflagPage = lazy(() =>
  import('@/pages/forensic/redflag').then((m) => ({ default: m.RedflagPage })),
)
const ReportPage = lazy(() =>
  import('@/pages/forensic/report').then((m) => ({ default: m.ReportPage })),
)
const ForensicUniversePage = lazy(() =>
  import('@/pages/forensic/universe').then((m) => ({ default: m.UniversePage })),
)

const MlPage = lazy(() => import('@/pages/ml/MlPage').then((m) => ({ default: m.MlPage })))
const MlSignalPage = lazy(() =>
  import('@/pages/ml/signal').then((m) => ({ default: m.MlSignalPage })),
)
const MlBacktestPage = lazy(() =>
  import('@/pages/ml/backtest').then((m) => ({ default: m.MlBacktestPage })),
)
const MlHoldingsPage = lazy(() =>
  import('@/pages/ml/holdings').then((m) => ({ default: m.MlHoldingsPage })),
)
const MlPositionsPage = lazy(() =>
  import('@/pages/ml/positions').then((m) => ({ default: m.MlPositionsPage })),
)
const MlMultibaggerPage = lazy(() =>
  import('@/pages/ml/multibagger').then((m) => ({ default: m.MlMultibaggerPage })),
)
const MlSectorRotationPage = lazy(() =>
  import('@/pages/ml/sector_rotation').then((m) => ({ default: m.MlSectorRotationPage })),
)
const MlExitUrgencyPage = lazy(() =>
  import('@/pages/ml/exit_urgency').then((m) => ({ default: m.MlExitUrgencyPage })),
)
const MlUniversePage = lazy(() =>
  import('@/pages/ml/universe').then((m) => ({ default: m.MlUniversePage })),
)
const MlToolsPage = lazy(() =>
  import('@/pages/ml/tools').then((m) => ({ default: m.MlToolsPage })),
)

const MomentumUniversePage = lazy(() =>
  import('@/pages/momentum/universe').then((m) => ({ default: m.MomentumUniversePage })),
)
const MomentumPortfolioPage = lazy(() =>
  import('@/pages/momentum/portfolio').then((m) => ({ default: m.MomentumPortfolioPage })),
)
const MomentumRebalancePage = lazy(() =>
  import('@/pages/momentum/rebalance').then((m) => ({ default: m.MomentumRebalancePage })),
)

const BigInvestorsPage = lazy(() =>
  import('@/pages/big_investors/BigInvestorsPage').then((m) => ({ default: m.BigInvestorsPage })),
)
const BigInvestorsAnnouncementsPage = lazy(() =>
  import('@/pages/big_investors/announcements').then((m) => ({
    default: m.BigInvestorsAnnouncementsPage,
  })),
)
const BigInvestorsMfHoldingsPage = lazy(() =>
  import('@/pages/big_investors/mf_holdings').then((m) => ({
    default: m.BigInvestorsMfHoldingsPage,
  })),
)

const OpsPage = lazy(() => import('@/pages/ops/OpsPage').then((m) => ({ default: m.OpsPage })))
const MacroPage = lazy(() =>
  import('@/pages/macro/MacroPage').then((m) => ({ default: m.MacroPage })),
)

const SymbolOverviewPage = lazy(() =>
  import('@/pages/symbol/SymbolOverviewPage').then((m) => ({ default: m.SymbolOverviewPage })),
)

const BacktestPage = lazy(() =>
  import('@/pages/backtest/BacktestPage').then((m) => ({ default: m.BacktestPage })),
)

/** Minimal full-page fallback shown while a route's chunk downloads. */
function RouteFallback() {
  return <div className="p-6 text-sm text-muted-foreground">Loading…</div>
}

function withSuspense(children: ReactNode) {
  return <Suspense fallback={<RouteFallback />}>{children}</Suspense>
}

/**
 * Single route tree for the SPA (replaces the former Vite multi-HTML-entry
 * MPA — one route per former `<section>-<sub>.html` page, same path
 * strings so NAV_SECTIONS hrefs keep working). `/charts` is new: the
 * Symbol Overview route driven by the global ticker store rather than a
 * `?ticker=` query param.
 */
export const router = createBrowserRouter([
  { path: '/', element: withSuspense(<HomePage />) },

  { path: '/technical-overview', element: withSuspense(<TechnicalOverviewPage />) },
  { path: '/technical-watchlist', element: withSuspense(<TechnicalWatchlistPage />) },
  { path: '/technical-screener', element: withSuspense(<TechnicalScreenerPage />) },
  { path: '/technical-alerts', element: withSuspense(<TechnicalAlertsPage />) },
  { path: '/technical-compare', element: withSuspense(<TechnicalComparePage />) },
  { path: '/technical-chart', element: withSuspense(<TechnicalChartPage />) },
  { path: '/technical-deep_dive', element: withSuspense(<TechnicalDeepDivePage />) },
  { path: '/technical-portfolio', element: withSuspense(<TechnicalPortfolioPage />) },

  { path: '/fundamental', element: withSuspense(<FundamentalPage />) },
  { path: '/fundamental-screener', element: withSuspense(<FundamentalScreenerPage />) },
  { path: '/fundamental-peers', element: withSuspense(<FundamentalPeersPage />) },
  { path: '/fundamental-sector', element: withSuspense(<FundamentalSectorPage />) },
  { path: '/fundamental-management', element: withSuspense(<FundamentalManagementPage />) },
  { path: '/fundamental-thesis', element: withSuspense(<FundamentalThesisPage />) },
  { path: '/fundamental-deep_dive', element: withSuspense(<FundamentalDeepDivePage />) },

  { path: '/valuation', element: withSuspense(<ValuationPage />) },
  { path: '/valuation-dcf', element: withSuspense(<DcfPage />) },
  { path: '/valuation-relative', element: withSuspense(<RelativePage />) },
  { path: '/valuation-batch', element: withSuspense(<BatchPage />) },
  { path: '/valuation-accuracy', element: withSuspense(<AccuracyPage />) },

  { path: '/forensic', element: withSuspense(<ForensicPage />) },
  { path: '/forensic-benford', element: withSuspense(<BenfordPage />) },
  { path: '/forensic-cashflow', element: withSuspense(<CashflowPage />) },
  { path: '/forensic-heatmap', element: withSuspense(<HeatmapPage />) },
  { path: '/forensic-redflag', element: withSuspense(<RedflagPage />) },
  { path: '/forensic-report', element: withSuspense(<ReportPage />) },
  { path: '/forensic-universe', element: withSuspense(<ForensicUniversePage />) },

  { path: '/ml', element: withSuspense(<MlPage />) },
  { path: '/ml-signal', element: withSuspense(<MlSignalPage />) },
  { path: '/ml-backtest', element: withSuspense(<MlBacktestPage />) },
  { path: '/ml-holdings', element: withSuspense(<MlHoldingsPage />) },
  { path: '/ml-positions', element: withSuspense(<MlPositionsPage />) },
  { path: '/ml-multibagger', element: withSuspense(<MlMultibaggerPage />) },
  { path: '/ml-sector_rotation', element: withSuspense(<MlSectorRotationPage />) },
  { path: '/ml-exit_urgency', element: withSuspense(<MlExitUrgencyPage />) },
  { path: '/ml-universe', element: withSuspense(<MlUniversePage />) },
  { path: '/ml-tools', element: withSuspense(<MlToolsPage />) },

  { path: '/momentum', element: withSuspense(<MomentumUniversePage />) },
  { path: '/momentum-portfolio', element: withSuspense(<MomentumPortfolioPage />) },
  { path: '/momentum-rebalance', element: withSuspense(<MomentumRebalancePage />) },
  { path: '/momentum-universe', element: withSuspense(<MomentumUniversePage />) },

  { path: '/big_investors', element: withSuspense(<BigInvestorsPage />) },
  { path: '/big_investors-announcements', element: withSuspense(<BigInvestorsAnnouncementsPage />) },
  { path: '/big_investors-mf_holdings', element: withSuspense(<BigInvestorsMfHoldingsPage />) },

  { path: '/backtest', element: withSuspense(<BacktestPage />) },

  { path: '/ops', element: withSuspense(<OpsPage />) },
  { path: '/macro', element: withSuspense(<MacroPage />) },

  { path: '/charts', element: withSuspense(<SymbolOverviewPage />) },
])
