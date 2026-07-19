// Sidebar nav model shared by AppShell across every Vite entry. Each
// section links to its first (index) page, built as its own .html entry
// under frontend/ (see vite.config.ts `build.rollupOptions.input`). Most
// sections also have sub-pages, each its own Vite HTML entry
// (`<section>-<sub>.html`) — these render as an expandable sub-menu under
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
}

export const NAV_SECTIONS: NavSection[] = [
  { id: 'home', label: 'Home', href: '/index.html' },
  {
    id: 'technical',
    label: 'Technical',
    href: '/technical-watchlist.html',
    subItems: [
      { id: 'overview', label: 'Market Overview', href: '/technical-overview.html' },
      { id: 'watchlist', label: 'Daily WatchList', href: '/technical-watchlist.html' },
      { id: 'screener', label: 'Screener', href: '/technical-screener.html' },
      { id: 'alerts', label: 'Alerts', href: '/technical-alerts.html' },
      { id: 'compare', label: 'Compare', href: '/technical-compare.html' },
      { id: 'chart', label: 'Chart', href: '/technical-chart.html' },
      { id: 'deep_dive', label: 'Deep Dive', href: '/technical-deep_dive.html' },
    ],
  },
  {
    id: 'fundamental',
    label: 'Fundamental',
    href: '/fundamental.html',
    subItems: [
      { id: 'dashboard', label: 'Dashboard', href: '/fundamental.html' },
      { id: 'screener', label: 'Screener', href: '/fundamental-screener.html' },
      { id: 'peers', label: 'Peers', href: '/fundamental-peers.html' },
      { id: 'sector', label: 'Sector', href: '/fundamental-sector.html' },
      { id: 'management', label: 'Management', href: '/fundamental-management.html' },
      { id: 'thesis', label: 'Thesis', href: '/fundamental-thesis.html' },
    ],
  },
  {
    id: 'valuation',
    label: 'Valuation',
    href: '/valuation.html',
    subItems: [
      { id: 'dcf', label: 'DCF', href: '/valuation-dcf.html' },
      { id: 'relative', label: 'Relative', href: '/valuation-relative.html' },
      { id: 'batch', label: 'Batch', href: '/valuation-batch.html' },
      { id: 'accuracy', label: 'Accuracy', href: '/valuation-accuracy.html' },
    ],
  },
  {
    id: 'forensic',
    label: 'Forensic',
    href: '/forensic.html',
    subItems: [
      { id: 'dashboard', label: 'Dashboard', href: '/forensic.html' },
      { id: 'benford', label: 'Benford', href: '/forensic-benford.html' },
      { id: 'cashflow', label: 'Cash Flow', href: '/forensic-cashflow.html' },
      { id: 'heatmap', label: 'Heatmap', href: '/forensic-heatmap.html' },
      { id: 'redflag', label: 'Red Flags', href: '/forensic-redflag.html' },
      { id: 'report', label: 'Report', href: '/forensic-report.html' },
      { id: 'universe', label: 'Universe', href: '/forensic-universe.html' },
    ],
  },
  {
    id: 'ml',
    label: 'ML Signals',
    href: '/ml.html',
    subItems: [
      { id: 'index', label: 'Hub', href: '/ml.html' },
      { id: 'signal', label: 'Signal', href: '/ml-signal.html' },
      { id: 'backtest', label: 'Backtest', href: '/ml-backtest.html' },
      { id: 'holdings', label: 'Holdings', href: '/ml-holdings.html' },
      { id: 'positions', label: 'Positions', href: '/ml-positions.html' },
      { id: 'multibagger', label: 'Multibagger', href: '/ml-multibagger.html' },
      { id: 'sector_rotation', label: 'Sector Rotation', href: '/ml-sector_rotation.html' },
      { id: 'exit_urgency', label: 'Exit Urgency', href: '/ml-exit_urgency.html' },
      { id: 'universe', label: 'Universe', href: '/ml-universe.html' },
      { id: 'tools', label: 'Tools', href: '/ml-tools.html' },
    ],
  },
  {
    id: 'momentum',
    label: 'Momentum',
    href: '/momentum.html',
    subItems: [
      { id: 'index', label: 'Overview', href: '/momentum.html' },
      { id: 'portfolio', label: 'Portfolio', href: '/momentum-portfolio.html' },
      { id: 'rebalance', label: 'Rebalance', href: '/momentum-rebalance.html' },
      { id: 'universe', label: 'Universe', href: '/momentum-universe.html' },
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
    id: 'big_investors',
    label: 'Big Investors',
    href: '/big_investors.html',
    subItems: [
      { id: 'index', label: 'Overview', href: '/big_investors.html' },
      { id: 'announcements', label: 'Announcements', href: '/big_investors-announcements.html' },
      { id: 'mf_holdings', label: 'MF Holdings', href: '/big_investors-mf_holdings.html' },
    ],
  },
  {
    id: 'ops',
    label: 'Ops',
    href: '/ops.html',
    subItems: [{ id: 'index', label: 'Overview', href: '/ops.html' }],
  },
  {
    // Split out from Ops into its own top-level section — was a sub-tab
    // under Ops (A27), now promoted since it has nothing to do with
    // pipeline/job monitoring.
    id: 'macro',
    label: 'Macro',
    href: '/macro.html',
  },
]
