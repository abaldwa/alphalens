// dashboard/static/js/shell.js — 5-app shell: top app-bar (app switcher) +
// sub-tabs (per-app screen nav). Replaces the old single 6-page nav now
// that the dashboard is 5 apps x N screens each.

const APPS = [
  {
    id: "ml", name: "AlphaLens.ML", short: "ML", color: "var(--teal)", base: "/ui/ml/",
    screens: [
      // ML22 (2026-07-13): Daily Insights and Daily WatchList merged into
      // one screen (index.html) — a single "hub" sub-tab now covers both;
      // watchlist.html still exists only as a redirect for old links.
      { id: "hub", label: "Daily Insights & WatchList", href: "index.html" },
      { id: "signal", label: "Signal Deep Dive", href: "signal.html" },
      { id: "universe", label: "Full Universe", href: "universe.html" },
      { id: "multibagger", label: "Multibagger", href: "multibagger.html" },
      { id: "sector_rotation", label: "Sector Rotation", href: "sector_rotation.html" },
      { id: "positions", label: "Paper Trading", href: "positions.html" },
      { id: "exit_urgency", label: "Exit Urgency", href: "exit_urgency.html" },
      { id: "holdings", label: "My Holdings", href: "holdings.html" },
      { id: "backtest", label: "Backtest", href: "backtest.html" },
      { id: "tools", label: "Tools", href: "tools.html" },
    ],
  },
  {
    id: "technical", name: "AlphaLens.Technical", short: "Technical", color: "var(--blue)", base: "/ui/technical/",
    screens: [
      { id: "screener", label: "Screener", href: "screener.html" },
      { id: "chart", label: "Chart", href: "chart.html" },
      { id: "compare", label: "Compare", href: "compare.html" },
      { id: "alerts", label: "Alert Manager", href: "alerts.html" },
      { id: "watchlist", label: "Daily WatchList", href: "watchlist.html" },
      { id: "deep_dive", label: "Technical Deep Dive", href: "deep_dive.html" },
      { id: "overview", label: "Market Overview", href: "overview.html" },
    ],
  },
  {
    id: "fundamental", name: "AlphaLens.Fundamental", short: "Fundamental", color: "var(--purple)", base: "/ui/fundamental/",
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
    id: "valuation", name: "AlphaLens.Valuation", short: "Valuation", color: "var(--amber)", base: "/ui/valuation/",
    screens: [
      { id: "dcf", label: "Valuation Dashboard", href: "dcf.html" },
      { id: "relative", label: "Relative Valuation", href: "relative.html" },
      { id: "batch", label: "Batch Valuation", href: "batch.html" },
      { id: "accuracy", label: "Valuation Accuracy", href: "accuracy.html" },
    ],
  },
  {
    id: "forensic", name: "AlphaLens.Forensic", short: "Forensic", color: "var(--red)", base: "/ui/forensic/",
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
    id: "ops", name: "AlphaLens.Ops", short: "Ops", color: "var(--tx3)", base: "/ui/ops/",
    screens: [
      { id: "index", label: "Job Autoruns", href: "index.html" },
      { id: "macro", label: "Macro Data Entry", href: "macro.html" },
    ],
  },
  {
    // Phase A/B/C (plan: gentle-wobbling-swing.md) — bulk/block deals
    // (family-attributed + netted) and MF holdings movers/entries-exits.
    // Phase D (quarterly reconciliation) not yet built.
    id: "big_investors", name: "AlphaLens.BigInvestors", short: "BigInvestors", color: "var(--amber)", base: "/ui/big_investors/",
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
          el("span", { class: "app-tab" + (a.id === appId ? " active" : "") }, [a.short || a.name]),
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

// ===== A66/A68/A73 — framework-wide table conventions =====
// Applied generically to every <table> on every screen (via renderAppShell's
// callers, which run on every page load, plus a MutationObserver for
// screens that re-render their tbody after an async fetch) rather than a
// per-page audit/edit — this is the "apply to every table, not just the
// ones that already opted in" gap those items describe. Deliberately DOM-
// level (reads/sorts rendered <td> text, resizes rendered <th> width) so it
// never needs to know about each page's own row-data shape or re-render
// function, and never conflicts with a screen's *own* data-driven sort
// (api.js's sortableHeader/sortRows, used by #21/#23 and others) because
// those headers already set inline `cursor:pointer` and are skipped here.
(function () {
  function cellIsPercentOrRange(text) {
    const t = (text || "").trim();
    if (!t) return false;
    if (t.endsWith("%")) return true;
    return /^-?\d+(\.\d+)?\s*(-|–|to)\s*-?\d+(\.\d+)?%?$/.test(t);
  }

  function cellIsNumeric(text) {
    const t = (text || "").trim();
    if (!t || t === "—") return false;
    return /^[₹\-+]?[\d,]+(\.\d+)?%?$/.test(t);
  }

  // A68 — amount fields right-aligned, percentage/range fields center-aligned.
  // Column intent is sniffed from rendered cell content (>=60% of non-empty
  // cells in that column matching), so it applies uniformly without each
  // page declaring a column type.
  function applyAlignment(table) {
    const headRow = table.querySelector("thead tr");
    const bodyRows = Array.from(table.querySelectorAll("tbody tr"));
    if (!headRow || !bodyRows.length) return;
    const ths = Array.from(headRow.children);
    ths.forEach((th, idx) => {
      let total = 0, numeric = 0, pctOrRange = 0;
      bodyRows.forEach((r) => {
        const cell = r.children[idx];
        if (!cell) return;
        const text = cell.textContent;
        if (!text || !text.trim()) return;
        total++;
        if (cellIsPercentOrRange(text)) pctOrRange++;
        else if (cellIsNumeric(text)) numeric++;
      });
      if (total === 0) return;
      if (pctOrRange / total >= 0.6) {
        th.style.textAlign = "center";
        bodyRows.forEach((r) => { if (r.children[idx]) r.children[idx].style.textAlign = "center"; });
      } else if (numeric / total >= 0.6) {
        th.style.textAlign = "right";
        bodyRows.forEach((r) => { if (r.children[idx]) r.children[idx].style.textAlign = "right"; });
      }
    });
  }

  // A66 — sortable columns everywhere: click a header to sort the rendered
  // rows in place, click again to reverse. Skipped for headers a screen
  // already made sortable itself via api.js's sortableHeader (identified by
  // the inline cursor:pointer style that helper always sets), so this never
  // double-handles a click already wired to a JS-side re-render.
  function applySortable(table) {
    if (table.dataset.a66Sortable === "1") return;
    const headRow = table.querySelector("thead tr");
    const tbody = table.querySelector("tbody");
    if (!headRow || !tbody || !tbody.children.length) return;
    table.dataset.a66Sortable = "1";
    const ths = Array.from(headRow.children);
    ths.forEach((th, idx) => {
      if (th.style.cursor === "pointer") return; // already screen-managed
      th.style.cursor = "pointer";
      th.style.userSelect = "none";
      th.title = "Click to sort";
      let dir = null;
      th.addEventListener("click", () => {
        const rows = Array.from(tbody.querySelectorAll("tr"));
        dir = dir === "asc" ? "desc" : "asc";
        const factor = dir === "asc" ? 1 : -1;
        const parse = (cell) => {
          const text = cell ? cell.textContent.trim() : "";
          if (!text || text === "—") return null;
          const num = Number(text.replace(/[₹,%]/g, ""));
          return Number.isNaN(num) ? text.toLowerCase() : num;
        };
        rows.sort((a, b) => {
          const va = parse(a.children[idx]);
          const vb = parse(b.children[idx]);
          if (va === null && vb === null) return 0;
          if (va === null) return 1;
          if (vb === null) return -1;
          if (va < vb) return -1 * factor;
          if (va > vb) return 1 * factor;
          return 0;
        });
        rows.forEach((r) => tbody.appendChild(r));
      });
    });
  }

  // A73 — resizable columns: a drag handle on each header's right border,
  // width persisted per (page path, header label) in localStorage so it
  // survives reloads, framework-wide convention alongside A66/A68/A69.
  function applyResizable(table) {
    if (table.dataset.a73Resizable === "1") return;
    const ths = table.querySelectorAll("thead th");
    if (!ths.length) return;
    table.dataset.a73Resizable = "1";
    ths.forEach((th) => {
      const label = (th.textContent || "").trim();
      const storeKey = `a73-colwidth:${location.pathname}:${label}`;
      try {
        const saved = localStorage.getItem(storeKey);
        if (saved) th.style.width = saved;
      } catch (e) { /* localStorage unavailable — skip persistence */ }

      if (getComputedStyle(th).position === "static") th.style.position = "relative";
      const handle = document.createElement("span");
      handle.className = "col-resize-handle";
      th.appendChild(handle);

      handle.addEventListener("mousedown", (evt) => {
        evt.preventDefault();
        evt.stopPropagation();
        const startX = evt.pageX;
        const startWidth = th.offsetWidth;
        function onMove(e2) {
          const w = Math.max(48, startWidth + (e2.pageX - startX));
          th.style.width = w + "px";
        }
        function onUp() {
          document.removeEventListener("mousemove", onMove);
          document.removeEventListener("mouseup", onUp);
          try { localStorage.setItem(storeKey, th.style.width); } catch (e) { /* ignore */ }
        }
        document.addEventListener("mousemove", onMove);
        document.addEventListener("mouseup", onUp);
      });
    });
  }

  function enhanceTable(table) {
    if (table.classList.contains("no-enhance")) return;
    applySortable(table);
    applyResizable(table);
    applyAlignment(table);
  }

  function enhanceAllTables() {
    document.querySelectorAll("table").forEach(enhanceTable);
  }

  let scheduled = false;
  function scheduleEnhance() {
    if (scheduled) return;
    scheduled = true;
    setTimeout(() => {
      scheduled = false;
      enhanceAllTables();
    }, 50);
  }

  document.addEventListener("DOMContentLoaded", scheduleEnhance);
  if (document.readyState !== "loading") scheduleEnhance();
  const tableEnhanceObserver = new MutationObserver((mutations) => {
    for (const m of mutations) {
      if (m.addedNodes && m.addedNodes.length) { scheduleEnhance(); return; }
    }
  });
  tableEnhanceObserver.observe(document.documentElement, { childList: true, subtree: true });
})();
