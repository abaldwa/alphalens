import { createBrowserRouter } from 'react-router-dom'

import { HomePage } from '@/pages/home/HomePage'

import { TechnicalOverviewPage } from '@/pages/technical/overview'
import { TechnicalWatchlistPage } from '@/pages/technical/watchlist'
import { TechnicalScreenerPage } from '@/pages/technical/screener'
import { TechnicalAlertsPage } from '@/pages/technical/alerts'
import { TechnicalComparePage } from '@/pages/technical/compare'
import { TechnicalChartPage } from '@/pages/technical/chart'
import { TechnicalDeepDivePage } from '@/pages/technical/deep_dive'
import { TechnicalPortfolioPage } from '@/pages/technical/portfolio'
import { TechnicalExperimentationPage } from '@/pages/technical/experimentation'
import { TechnicalRecommendedStrategiesPage } from '@/pages/technical/recommended-strategies'
import { TechnicalBatchBacktestPage } from '@/pages/technical/batch-backtest'

import { FundamentalPage } from '@/pages/fundamental/FundamentalPage'
import { FundamentalScreenerPage } from '@/pages/fundamental/screener'
import { FundamentalPeersPage } from '@/pages/fundamental/peers'
import { FundamentalSectorPage } from '@/pages/fundamental/sector'
import { FundamentalManagementPage } from '@/pages/fundamental/management'
import { FundamentalThesisPage } from '@/pages/fundamental/thesis'
import { FundamentalDeepDivePage } from '@/pages/fundamental/deep_dive'
import { FundamentalStrategiesPage } from '@/pages/fundamental/strategies'

import { ValuationPage } from '@/pages/valuation/ValuationPage'
import { DcfPage } from '@/pages/valuation/dcf'
import { RelativePage } from '@/pages/valuation/relative'
import { BatchPage } from '@/pages/valuation/batch'
import { AccuracyPage } from '@/pages/valuation/accuracy'

import { ForensicPage } from '@/pages/forensic/ForensicPage'
import { BenfordPage } from '@/pages/forensic/benford'
import { CashflowPage } from '@/pages/forensic/cashflow'
import { HeatmapPage } from '@/pages/forensic/heatmap'
import { RedflagPage } from '@/pages/forensic/redflag'
import { ReportPage } from '@/pages/forensic/report'
import { UniversePage as ForensicUniversePage } from '@/pages/forensic/universe'

import { MlPage } from '@/pages/ml/MlPage'
import { MlSignalPage } from '@/pages/ml/signal'
import { MlBacktestPage } from '@/pages/ml/backtest'
import { MlHoldingsPage } from '@/pages/ml/holdings'
import { MlPositionsPage } from '@/pages/ml/positions'
import { MlMultibaggerPage } from '@/pages/ml/multibagger'
import { MlSectorRotationPage } from '@/pages/ml/sector_rotation'
import { MlExitUrgencyPage } from '@/pages/ml/exit_urgency'
import { MlUniversePage } from '@/pages/ml/universe'
import { MlToolsPage } from '@/pages/ml/tools'

import { MomentumUniversePage } from '@/pages/momentum/universe'
import { MomentumPortfolioPage } from '@/pages/momentum/portfolio'
import { MomentumRebalancePage } from '@/pages/momentum/rebalance'
import { MomentumExperimentationPage } from '@/pages/momentum/experimentation'
import { MomentumDynamicReportPage } from '@/pages/momentum/dynamic-report'

import { BigInvestorsPage } from '@/pages/big_investors/BigInvestorsPage'
import { BigInvestorsAnnouncementsPage } from '@/pages/big_investors/announcements'
import { BigInvestorsMfHoldingsPage } from '@/pages/big_investors/mf_holdings'

import { OpsPage } from '@/pages/ops/OpsPage'
import { MacroPage } from '@/pages/macro/MacroPage'

import { SymbolOverviewPage } from '@/pages/symbol/SymbolOverviewPage'

import { BacktestPage } from '@/pages/backtest/BacktestPage'
import { ExperimentsPage } from '@/pages/backtest/ExperimentsPage'
import { RegimesPage } from '@/pages/backtest/RegimesPage'

/**
 * Single route tree for the SPA (replaces the former Vite multi-HTML-entry
 * MPA — one route per former `<section>-<sub>.html` page, same path
 * strings so NAV_SECTIONS hrefs keep working). `/charts` is new: the
 * Symbol Overview route driven by the global ticker store rather than a
 * `?ticker=` query param.
 */
export const router = createBrowserRouter([
  { path: '/', element: <HomePage /> },

  { path: '/technical-overview', element: <TechnicalOverviewPage /> },
  { path: '/technical-watchlist', element: <TechnicalWatchlistPage /> },
  { path: '/technical-screener', element: <TechnicalScreenerPage /> },
  { path: '/technical-alerts', element: <TechnicalAlertsPage /> },
  { path: '/technical-compare', element: <TechnicalComparePage /> },
  { path: '/technical-chart', element: <TechnicalChartPage /> },
  { path: '/technical-deep_dive', element: <TechnicalDeepDivePage /> },
  { path: '/technical-portfolio', element: <TechnicalPortfolioPage /> },
  { path: '/technical-experimentation', element: <TechnicalExperimentationPage /> },
  { path: '/technical-recommended-strategies', element: <TechnicalRecommendedStrategiesPage /> },
  { path: '/technical-batch-backtest', element: <TechnicalBatchBacktestPage /> },

  { path: '/fundamental', element: <FundamentalPage /> },
  { path: '/fundamental-screener', element: <FundamentalScreenerPage /> },
  { path: '/fundamental-peers', element: <FundamentalPeersPage /> },
  { path: '/fundamental-sector', element: <FundamentalSectorPage /> },
  { path: '/fundamental-management', element: <FundamentalManagementPage /> },
  { path: '/fundamental-thesis', element: <FundamentalThesisPage /> },
  { path: '/fundamental-deep_dive', element: <FundamentalDeepDivePage /> },
  { path: '/fundamental-strategies', element: <FundamentalStrategiesPage /> },

  { path: '/valuation', element: <ValuationPage /> },
  { path: '/valuation-dcf', element: <DcfPage /> },
  { path: '/valuation-relative', element: <RelativePage /> },
  { path: '/valuation-batch', element: <BatchPage /> },
  { path: '/valuation-accuracy', element: <AccuracyPage /> },

  { path: '/forensic', element: <ForensicPage /> },
  { path: '/forensic-benford', element: <BenfordPage /> },
  { path: '/forensic-cashflow', element: <CashflowPage /> },
  { path: '/forensic-heatmap', element: <HeatmapPage /> },
  { path: '/forensic-redflag', element: <RedflagPage /> },
  { path: '/forensic-report', element: <ReportPage /> },
  { path: '/forensic-universe', element: <ForensicUniversePage /> },

  { path: '/ml', element: <MlPage /> },
  { path: '/ml-signal', element: <MlSignalPage /> },
  { path: '/ml-backtest', element: <MlBacktestPage /> },
  { path: '/ml-holdings', element: <MlHoldingsPage /> },
  { path: '/ml-positions', element: <MlPositionsPage /> },
  { path: '/ml-multibagger', element: <MlMultibaggerPage /> },
  { path: '/ml-sector_rotation', element: <MlSectorRotationPage /> },
  { path: '/ml-exit_urgency', element: <MlExitUrgencyPage /> },
  { path: '/ml-universe', element: <MlUniversePage /> },
  { path: '/ml-tools', element: <MlToolsPage /> },

  { path: '/momentum', element: <MomentumUniversePage /> },
  { path: '/momentum-portfolio', element: <MomentumPortfolioPage /> },
  { path: '/momentum-rebalance', element: <MomentumRebalancePage /> },
  { path: '/momentum-universe', element: <MomentumUniversePage /> },
  { path: '/momentum-experimentation', element: <MomentumExperimentationPage /> },
  { path: '/momentum-dynamic-report', element: <MomentumDynamicReportPage /> },

  { path: '/big_investors', element: <BigInvestorsPage /> },
  { path: '/big_investors-announcements', element: <BigInvestorsAnnouncementsPage /> },
  { path: '/big_investors-mf_holdings', element: <BigInvestorsMfHoldingsPage /> },

  { path: '/backtest', element: <BacktestPage /> },
  { path: '/backtest-experiments', element: <ExperimentsPage /> },
  { path: '/backtest-regimes', element: <RegimesPage /> },

  { path: '/ops', element: <OpsPage /> },
  { path: '/macro', element: <MacroPage /> },

  { path: '/charts', element: <SymbolOverviewPage /> },
])
