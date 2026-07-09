// dashboard/static/js/shell.js — 5-app shell: top app-bar (app switcher) +
// sub-tabs (per-app screen nav). Replaces the old single 6-page nav now
// that the dashboard is 5 apps x N screens each.

const APPS = [
  {
    id: "ml", name: "AlphaLens.ML", color: "var(--teal)", base: "/ui/ml/",
    screens: [
      { id: "hub", label: "Daily Insights", href: "index.html" },
      { id: "watchlist", label: "Daily WatchList", href: "watchlist.html" },
      { id: "signal", label: "Signal Deep Dive", href: "signal.html" },
      { id: "multibagger", label: "Multibagger", href: "multibagger.html" },
      { id: "positions", label: "Paper Trading", href: "positions.html" },
      { id: "exit_urgency", label: "Exit Urgency", href: "exit_urgency.html" },
      { id: "holdings", label: "My Holdings", href: "holdings.html" },
      { id: "backtest", label: "Backtest", href: "backtest.html" },
      { id: "tools", label: "Tools", href: "tools.html" },
    ],
  },
  {
    id: "technical", name: "AlphaLens.Technical", color: "var(--blue)", base: "/ui/technical/",
    screens: [
      { id: "screener", label: "Screener", href: "screener.html" },
      { id: "chart", label: "Chart", href: "chart.html" },
      { id: "compare", label: "Compare", href: "compare.html" },
      { id: "alerts", label: "Alert Manager", href: "alerts.html" },
      { id: "watchlist", label: "Daily WatchList", href: "watchlist.html" },
      { id: "overview", label: "Market Overview", href: "overview.html" },
    ],
  },
  {
    id: "fundamental", name: "AlphaLens.Fundamental", color: "var(--purple)", base: "/ui/fundamental/",
    screens: [
      { id: "dashboard", label: "Financial Dashboard", href: "dashboard.html" },
      { id: "peers", label: "Peer Comparison", href: "peers.html" },
      { id: "sector", label: "Sector Deep-Dive", href: "sector.html" },
      { id: "screener", label: "Screener", href: "screener.html" },
      { id: "thesis", label: "Thesis Builder", href: "thesis.html" },
      { id: "management", label: "Management Quality", href: "management.html" },
    ],
  },
  {
    id: "valuation", name: "AlphaLens.Valuation", color: "var(--amber)", base: "/ui/valuation/",
    screens: [
      { id: "dcf", label: "Valuation Dashboard", href: "dcf.html" },
      { id: "relative", label: "Relative Valuation", href: "relative.html" },
      { id: "batch", label: "Batch Valuation", href: "batch.html" },
      { id: "accuracy", label: "Valuation Accuracy", href: "accuracy.html" },
    ],
  },
  {
    id: "forensic", name: "AlphaLens.Forensic", color: "var(--red)", base: "/ui/forensic/",
    screens: [
      { id: "dashboard", label: "Forensic Dashboard", href: "dashboard.html" },
      { id: "redflag", label: "Red Flags", href: "redflag.html" },
      { id: "benford", label: "Benford", href: "benford.html" },
      { id: "cashflow", label: "Cash Flow", href: "cashflow.html" },
      { id: "heatmap", label: "Peer Heatmap", href: "heatmap.html" },
      { id: "report", label: "Investigation", href: "report.html" },
      { id: "universe", label: "Universe Scan", href: "universe.html" },
    ],
  },
  {
    // Not part of the 27-screen prototype spec — an operational page the
    // user asked for directly. No per-ticker meaning, so excluded from
    // crosslink.js's CROSSLINK_APP_IDS (never appears in "View in X" links).
    id: "ops", name: "AlphaLens.Ops", color: "var(--tx3)", base: "/ui/ops/",
    screens: [
      { id: "index", label: "Job Autoruns", href: "index.html" },
    ],
  },
  {
    // Phase A/B/C (plan: gentle-wobbling-swing.md) — bulk/block deals
    // (family-attributed + netted) and MF holdings movers/entries-exits.
    // Phase D (quarterly reconciliation) not yet built.
    id: "big_investors", name: "AlphaLens.BigInvestors", color: "var(--amber)", base: "/ui/big_investors/",
    screens: [
      { id: "index", label: "Bulk/Block Deals", href: "index.html" },
      { id: "mf_holdings", label: "MF Holdings", href: "mf_holdings.html" },
      { id: "announcements", label: "Corporate Announcements", href: "announcements.html" },
    ],
  },
];

function renderAppShell(appId, screenId) {
  const app = APPS.find((a) => a.id === appId);
  if (!app) return;

  const bar = document.getElementById("app-bar");
  if (bar) {
    bar.innerHTML = "";
    bar.appendChild(
      el("div", { class: "app-logo" }, [
        el("div", { class: "dot", style: `background:${app.color}` }, []),
        app.name,
      ])
    );
    const tabs = el("div", { class: "app-tabs" }, []);
    APPS.forEach((a) => {
      tabs.appendChild(
        el("a", { href: a.base + a.screens[0].href }, [
          el("span", { class: "app-tab" + (a.id === appId ? " active" : "") }, [a.name]),
        ])
      );
    });
    bar.appendChild(tabs);
    const meta = el("span", { class: "app-meta", id: "app-meta" }, [""]);
    bar.appendChild(meta);
    apiGet("/health")
      .then((h) => {
        meta.textContent = `${h.status} · v${h.version} · ${fmtInt(h.stock_count)} stocks`;
      })
      .catch(() => {
        meta.textContent = "API unreachable";
      });
  }

  const sub = document.getElementById("sub-tabs");
  if (sub) {
    sub.innerHTML = "";
    app.screens.forEach((s) => {
      sub.appendChild(
        el("a", { href: s.href }, [
          el("span", { class: "sub-tab" + (s.id === screenId ? " active" : "") }, [s.label]),
        ])
      );
    });
  }
}
