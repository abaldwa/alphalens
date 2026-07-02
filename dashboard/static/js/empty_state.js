// dashboard/static/js/empty_state.js — single reusable "not yet available"
// component for screens (or sub-panels) whose backend doesn't exist yet.
// Per the project's no-synthetic-data rule, these screens never render
// fabricated numbers from the UI prototype mockups — only this honest,
// static, non-spinning terminal state.

// Backend status copy, keyed by app id — reused by every No-Backend screen
// in that app so the explanation isn't hand-written N times.
const BACKEND_STATUS = {
  technical: "Charting, Compare, and Market Overview are now live (real indicators/patterns/breadth — see /api/v1/ta/*). This screen specifically still has no backend: systems/technical_analysis/ has no strategy-screener engine and no alert storage/checker. That's real new logic, not API scaffolding, so it stays empty for now.",
  fundamental: "All 6 AlphaLens.Fundamental screens are now wired to real data (sector-relative ratios, governance, quality/growth/management composite scores, peer ranking, screener presets, thesis synthesis) — see /api/v1/fundamentals/*. Remaining gaps (sector-unique metrics like GNPA/ANDA, related-party-transaction detail) are flagged per-panel, not screen-wide.",
  valuation: "AlphaLens.Valuation has no backend yet — systems/damodaran_valuation/ is an empty stub (no DCF or relative-valuation engine exists). This screen will activate once that system exists.",
  forensic: "This panel needs data the forensic API doesn't expose yet.",
};

function renderEmptyState(containerId, { icon = "—", title = "Not yet available", detail }) {
  const c = document.getElementById(containerId);
  if (!c) return;
  c.innerHTML = "";
  c.appendChild(
    el("div", { class: "empty-state" }, [
      el("div", { class: "es-icon" }, [icon]),
      el("div", { class: "es-title" }, [title]),
      el("div", { class: "es-detail" }, [detail || ""]),
    ])
  );
}
