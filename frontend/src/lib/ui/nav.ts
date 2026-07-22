// Sidebar nav model shared by AppShell across the SPA route tree (see
// src/app/router.tsx). Each section links to its first (index) route; most
// sections also have sub-routes, rendered as an expandable sub-menu under
// the section in AppShell's sidebar.
export interface NavSubItem {
  id: string
  label: string
  href: string
  /** External link (e.g. a published research artifact) — opened in a new tab
   * instead of being treated as an internal Vite-entry route. */
  external?: boolean
}

export interface NavSection {
  id: string
  label: string
  href: string
  subItems?: NavSubItem[]
  /** External link (e.g. a published research artifact) — opened in a new tab
   * instead of being treated as an internal Vite-entry route. */
  external?: boolean
}

/** The three sidebar tiers, in display order — every `NavSection.id` must
 * appear in exactly one tier's list. 'pillars' are the five signal-
 * generating strategy pillars (Technical/Fundamental/Valuation/Momentum/ML)
 * the Home page summarizes; 'execution' is portfolio/trade-adjacent
 * sections that consume pillar output rather than generating a signal
 * themselves; 'reference' is diagnostic/supporting material. Purely a
 * sidebar grouping — doesn't change routes or NAV_SECTIONS entries. */
export const NAV_TIERS: { id: string; label: string; sectionIds: string[] }[] = [
  { id: 'pillars', label: 'Strategy Pillars', sectionIds: ['technical', 'fundamental', 'valuation', 'momentum', 'ml'] },
  { id: 'execution', label: 'Portfolio & Execution', sectionIds: ['backtest', 'big_investors'] },
  { id: 'reference', label: 'Reference & Ops', sectionIds: ['explain', 'forensic', 'ops', 'macro'] },
]

export const NAV_SECTIONS: NavSection[] = [
  { id: 'home', label: 'Home', href: '/' },
  {
    id: 'technical',
    label: 'Technical',
    href: '/technical-watchlist',
    subItems: [
      { id: 'overview', label: 'Market Overview', href: '/technical-overview' },
      { id: 'watchlist', label: 'Daily WatchList', href: '/technical-watchlist' },
      { id: 'screener', label: 'Screener', href: '/technical-screener' },
      { id: 'alerts', label: 'Alerts', href: '/technical-alerts' },
      { id: 'compare', label: 'Compare', href: '/technical-compare' },
      { id: 'chart', label: 'Chart', href: '/technical-chart' },
      { id: 'deep_dive', label: 'Deep Dive', href: '/technical-deep_dive' },
    ],
  },
  {
    id: 'fundamental',
    label: 'Fundamental',
    href: '/fundamental',
    subItems: [
      { id: 'dashboard', label: 'Dashboard', href: '/fundamental' },
      { id: 'screener', label: 'Screener', href: '/fundamental-screener' },
      { id: 'peers', label: 'Peers', href: '/fundamental-peers' },
      { id: 'sector', label: 'Sector', href: '/fundamental-sector' },
      { id: 'management', label: 'Management', href: '/fundamental-management' },
      { id: 'thesis', label: 'Thesis', href: '/fundamental-thesis' },
    ],
  },
  {
    id: 'valuation',
    label: 'Valuation',
    href: '/valuation',
    subItems: [
      { id: 'dcf', label: 'DCF', href: '/valuation-dcf' },
      { id: 'relative', label: 'Relative', href: '/valuation-relative' },
      { id: 'batch', label: 'Batch', href: '/valuation-batch' },
      { id: 'accuracy', label: 'Accuracy', href: '/valuation-accuracy' },
    ],
  },
  {
    id: 'forensic',
    label: 'Forensic',
    href: '/forensic',
    subItems: [
      { id: 'dashboard', label: 'Dashboard', href: '/forensic' },
      { id: 'benford', label: 'Benford', href: '/forensic-benford' },
      { id: 'cashflow', label: 'Cash Flow', href: '/forensic-cashflow' },
      { id: 'heatmap', label: 'Heatmap', href: '/forensic-heatmap' },
      { id: 'redflag', label: 'Red Flags', href: '/forensic-redflag' },
      { id: 'report', label: 'Report', href: '/forensic-report' },
      { id: 'universe', label: 'Universe', href: '/forensic-universe' },
    ],
  },
  {
    id: 'ml',
    label: 'ML Signals',
    href: '/ml',
    subItems: [
      { id: 'index', label: 'Hub', href: '/ml' },
      { id: 'signal', label: 'Signal', href: '/ml-signal' },
      { id: 'backtest', label: 'Backtest', href: '/ml-backtest' },
      { id: 'holdings', label: 'Holdings', href: '/ml-holdings' },
      { id: 'positions', label: 'Positions', href: '/ml-positions' },
      { id: 'multibagger', label: 'Multibagger', href: '/ml-multibagger' },
      { id: 'sector_rotation', label: 'Sector Rotation', href: '/ml-sector_rotation' },
      { id: 'exit_urgency', label: 'Exit Urgency', href: '/ml-exit_urgency' },
      { id: 'universe', label: 'Universe', href: '/ml-universe' },
      { id: 'tools', label: 'Tools', href: '/ml-tools' },
    ],
  },
  {
    id: 'momentum',
    label: 'Momentum',
    href: '/momentum',
    subItems: [
      { id: 'index', label: 'Overview', href: '/momentum' },
      { id: 'portfolio', label: 'Portfolio', href: '/momentum-portfolio' },
      { id: 'rebalance', label: 'Rebalance', href: '/momentum-rebalance' },
      { id: 'universe', label: 'Universe', href: '/momentum-universe' },
      // Research artifacts (732-variant grid backtest ledger, year-on-year
      // report) live outside this app as published Claude artifacts, not
      // local screens — mirrors dashboard/static/js/shell.js's momentum
      // app entry.
      {
        id: 'backtest_ledger',
        label: 'Backtest Ledger ↗',
        href: 'https://claude.ai/code/artifact/def4eadd-f11c-40d9-b491-ccc7f213c990',
        external: true,
      },
      {
        id: 'yoy_report',
        label: 'Year-on-Year Report ↗',
        href: 'https://claude.ai/code/artifact/3950a32a-23dc-4600-8d7f-6a4c48520858',
        external: true,
      },
    ],
  },
  {
    // Unified Backtest & Paper Trading Umbrella (BacktestUmbrellaPlan.md) —
    // a deliberate top-level sibling section, NOT nested under 'ml' (which
    // still keeps its own legacy /ml-backtest page, backed by the older
    // backtest_reports.py passthrough — the two coexist, see that router's
    // docstring). This section is the new cross-channel run history for
    // Technical/Fundamental/ML/Momentum backtest, walk-forward, and
    // (eventually) paper-trading runs.
    id: 'backtest',
    label: 'Backtest',
    href: '/backtest',
  },
  {
    id: 'big_investors',
    label: 'Big Investors',
    href: '/big_investors',
    subItems: [
      { id: 'index', label: 'Overview', href: '/big_investors' },
      { id: 'announcements', label: 'Announcements', href: '/big_investors-announcements' },
      { id: 'mf_holdings', label: 'MF Holdings', href: '/big_investors-mf_holdings' },
    ],
  },
  {
    // Reference docs, one static HTML page per app/module, shipped with the
    // app itself (frontend/public/explain/*.html — plain files, not SPA
    // routes, so they're linked as external hrefs the same way Momentum
    // links its research artifacts, just same-origin instead of claude.ai).
    // Pages cross-link to each other via relative hrefs inside the HTML.
    id: 'explain',
    label: 'Explain',
    href: '/explain/backtest-paper-trading.html',
    external: true,
    subItems: [
      {
        id: 'backtest_paper_trading',
        label: 'Backtest, Paper Trading & Forward Testing',
        href: '/explain/backtest-paper-trading.html',
        external: true,
      },
      {
        id: 'backtest_features',
        label: 'Features Captured in Backtest',
        href: '/explain/backtest-features.html',
        external: true,
      },
      {
        id: 'backtest_guide',
        label: 'Backtest Module & Strategy Reference',
        href: '/explain/backtest-guide.html',
        external: true,
      },
      {
        id: 'technical',
        label: 'Technical Analysis',
        href: '/explain/technical.html',
        external: true,
      },
      {
        id: 'fundamental',
        label: 'Fundamental Analysis',
        href: '/explain/fundamental.html',
        external: true,
      },
      {
        id: 'forensic',
        label: 'Forensic Accounting',
        href: '/explain/forensic.html',
        external: true,
      },
      {
        id: 'valuation',
        label: 'Valuation',
        href: '/explain/valuation.html',
        external: true,
      },
      {
        id: 'big_investors',
        label: 'Big Investors',
        href: '/explain/big-investors.html',
        external: true,
      },
      {
        id: 'ml_models',
        label: 'Machine Learning Models',
        href: '/explain/ml-models.html',
        external: true,
      },
      {
        id: 'momentum',
        label: 'Momentum',
        href: '/explain/momentum.html',
        external: true,
      },
    ],
  },
  {
    id: 'ops',
    label: 'Ops',
    href: '/ops',
    subItems: [{ id: 'index', label: 'Overview', href: '/ops' }],
  },
  {
    // Split out from Ops into its own top-level section — was a sub-tab
    // under Ops (A27), now promoted since it has nothing to do with
    // pipeline/job monitoring.
    id: 'macro',
    label: 'Macro',
    href: '/macro',
  },
]
