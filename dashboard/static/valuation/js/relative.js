// dashboard/static/valuation/js/relative.js — Relative (sector-peer PE) Valuation
// GET /api/v1/valuation/{ticker}/relative
renderAppShell("valuation", "relative");
TickerPicker.attach("ticker-input");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function relativeBadge(gapPct) {
  if (gapPct == null) return el("span", { class: "badge b-gray" }, ["N/A"]);
  if (gapPct < -0.10) return el("span", { class: "badge b-green" }, ["Cheap vs Peers"]);
  if (gapPct > 0.10) return el("span", { class: "badge b-red" }, ["Expensive vs Peers"]);
  return el("span", { class: "badge b-amber" }, ["In-line vs Peers"]);
}

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("content");
  apiGet(`/api/v1/valuation/${ticker}/relative`)
    .then((r) => {
      const c = document.getElementById("content");
      c.innerHTML = "";
      c.appendChild(
        el("div", { class: "grid grid-4" }, [
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["vs Peers"]), el("div", { class: "stat-value" }, [relativeBadge(r.gap_pct)])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Actual P/E"]), el("div", { class: "stat-value" }, [fmtNum(r.actual_pe, 1)])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Peer-Implied Fair P/E"]), el("div", { class: "stat-value" }, [fmtNum(r.predicted_pe, 1)])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Gap"]), el("div", { class: "stat-value " + pnlClass(r.gap_pct != null ? -r.gap_pct : null) }, [r.gap_pct != null ? `${(r.gap_pct * 100).toFixed(1)}%` : "—"])]),
        ])
      );
      c.appendChild(
        el("div", { class: "grid grid-4", style: "margin-top:12px" }, [
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["CMP"]), el("div", {}, [fmtMoney(r.current_price)])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Peer-Implied Price"]), el("div", {}, [r.implied_price != null ? fmtMoney(r.implied_price) : "—"])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Sector"]), el("div", {}, [r.sector || "—"])]),
          el("div", { class: "card" }, [el("div", { class: "stat-label" }, ["Peers Used / R²"]), el("div", {}, [`${r.n_peers} / ${fmtNum(r.r_squared, 2)}`])]),
        ])
      );
    })
    .catch((e) => showError("content", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
