// dashboard/static/ml/js/hub.js — ML-A Daily Insight Hub
renderAppShell("ml", "hub");

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

function horizonLabel(modelName) {
  const m = /(\d+)d$/.exec(modelName || "");
  return m ? `${m[1]} days` : "—";
}

function loadRegime() {
  apiGet("/api/v1/macro/regime")
    .then((r) => {
      const c = document.getElementById("regime-strip");
      if (!r.available) {
        c.innerHTML = `<div class="empty">No regime data yet</div>`;
        return;
      }
      const cls = r.hmm_regime === "bull" ? "regime-bull" : r.hmm_regime === "bear" ? "regime-bear" : "regime-side";
      c.innerHTML = "";
      c.appendChild(
        el("div", { style: "display:flex;align-items:center;gap:16px" }, [
          el("span", { class: "regime-badge " + cls }, [`● ${r.hmm_regime || "—"}`]),
          el("span", { style: "font-size:12px;color:var(--tx3)" }, [
            `confidence ${fmtPct(r.hmm_regime_prob)} · stability ${fmtNum(r.hmm_stability)} · as of ${r.date ? r.date.slice(0, 10) : "—"}`,
          ]),
        ])
      );
    })
    .catch((e) => showError("regime-strip", e));
}

// Higher-signal alert types surface first — drift/exit are rare and need
// immediate attention; pnd_block/pnd_flag are common (often dozens/day)
// and shown as an expandable tail so they don't bury everything else.
const ALERT_TYPE_PRIORITY = { drift_halt: 0, exit_urgent: 1, drift_warning: 2, pnd_block: 3, pnd_flag: 4 };
const ALERTS_VISIBLE_CAP = 5;

function alertBanner(a) {
  const cls = a.severity === "high" ? "alert-critical" : a.severity === "medium" ? "alert-warning" : "alert-info";
  return el("div", { class: "alert-banner " + cls }, [`[${a.alert_type}] ${a.message}`]);
}

function loadAlerts() {
  apiGet("/api/v1/alerts/today")
    .then((r) => {
      const c = document.getElementById("alerts");
      c.innerHTML = "";
      if (!r.alerts.length) return;

      const sorted = [...r.alerts].sort(
        (a, b) => (ALERT_TYPE_PRIORITY[a.alert_type] ?? 9) - (ALERT_TYPE_PRIORITY[b.alert_type] ?? 9)
      );
      const visible = sorted.slice(0, ALERTS_VISIBLE_CAP);
      const rest = sorted.slice(ALERTS_VISIBLE_CAP);

      visible.forEach((a) => c.appendChild(alertBanner(a)));

      if (rest.length) {
        const details = el("details", { class: "alert-more" }, [
          el("summary", { style: "cursor:pointer;font-size:13px;color:var(--tx3);margin-bottom:8px" }, [
            `+${rest.length} more alert${rest.length === 1 ? "" : "s"}`,
          ]),
        ]);
        rest.forEach((a) => details.appendChild(alertBanner(a)));
        c.appendChild(details);
      }
    })
    .catch((e) => showError("alerts", e));
}

function insightCard(r, price) {
  return el("div", { class: "card", style: "margin-bottom:12px" }, [
    el("div", { style: "display:flex;align-items:center;gap:12px;margin-bottom:12px" }, [
      el("span", { style: "font-weight:700;font-size:16px" }, [el("a", { href: `signal.html?ticker=${r.ticker}` }, [r.ticker])]),
      el("span", { class: "badge " + (r.signal_direction === "sell" ? "b-red" : "b-green") }, [`${(r.signal_direction || "—").toUpperCase()} ${horizonLabel(r.model_name)}`]),
      r.meta_label ? el("span", { class: "badge b-teal" }, [`Meta: ${r.meta_label}`]) : null,
      buildCrossLinks(r.ticker, "ml"),
    ]),
    el("div", { class: "insight-card" }, [
      el("div", { class: "insight-metric i-entry" }, [
        el("div", { class: "insight-label" }, ["Entry Point"]),
        el("div", { class: "insight-val" }, [price !== null && price !== undefined ? fmtMoney(price) : "—"]),
        el("div", { class: "insight-sub" }, ["Latest close"]),
      ]),
      el("div", { class: "insight-metric i-returns" }, [
        el("div", { class: "insight-label" }, ["Expected Return"]),
        el("div", { class: "insight-val" }, [fmtPct(r.q50_return)]),
        el("div", { class: "insight-sub" }, [`Range: ${fmtPct(r.conformal_lower)} to ${fmtPct(r.conformal_upper)}`]),
      ]),
      el("div", { class: "insight-metric i-duration" }, [
        el("div", { class: "insight-label" }, ["Duration"]),
        el("div", { class: "insight-val" }, [horizonLabel(r.model_name)]),
        el("div", { class: "insight-sub" }, ["Signal horizon"]),
      ]),
      el("div", { class: "insight-metric i-confidence" }, [
        el("div", { class: "insight-label" }, ["Confidence"]),
        el("div", { class: "insight-val" }, [fmtPct(r.buy_prob)]),
        el("div", { class: "insight-sub" }, ["Buy probability"]),
      ]),
    ]),
  ]);
}

function loadTopBuys() {
  const today = todayStr();
  apiGet(`/api/v1/signals/ml/top_buys/${today}`, { n: 5, carry_forward: true })
    .then((rows) => {
      const c = document.getElementById("top-buys");
      if (!rows.length) {
        c.innerHTML = `<div class="empty">No buy signals generated yet</div>`;
        return;
      }
      c.innerHTML = "";
      const signalDate = rows[0].date ? rows[0].date.slice(0, 10) : null;
      if (signalDate && signalDate !== today) {
        c.appendChild(el("div", { class: "empty", style: "margin-bottom:8px" }, [
          `Showing the last generated signals, from ${signalDate} — today's run hasn't produced signals yet`,
        ]));
      }
      const top2 = rows.slice(0, 2);
      Promise.all(top2.map((r) => apiGet(`/api/v1/ohlcv/${r.ticker}/latest`).then((row) => row && row.close).catch(() => null)))
        .then((prices) => {
          top2.forEach((r, i) => c.appendChild(insightCard(r, prices[i])));
          const table = el("table", {}, [
            el("thead", {}, [el("tr", {}, [
              el("th", {}, ["Stock"]), el("th", {}, ["Signal"]), el("th", {}, ["Prob"]), el("th", {}, ["Meta"]),
              el("th", {}, ["Interval"]), el("th", {}, ["P&D"]), el("th", {}, ["Regime"]),
            ])]),
            el("tbody", {}, rows.map((r) => el("tr", {}, [
              el("td", { style: "font-weight:600" }, [el("a", { href: `signal.html?ticker=${r.ticker}` }, [r.ticker])]),
              el("td", {}, [el("span", { class: "badge " + (r.signal_direction === "sell" ? "b-red" : "b-green") }, [`${(r.signal_direction || "—").toUpperCase()} ${horizonLabel(r.model_name)}`])]),
              el("td", { class: "mono " + pnlClass(r.buy_prob - 0.5) }, [fmtPct(r.buy_prob)]),
              el("td", { class: "mono" }, [r.meta_label || "—"]),
              el("td", { class: "mono" }, [`${fmtPct(r.conformal_lower)} to ${fmtPct(r.conformal_upper)}`]),
              el("td", { class: "mono" }, [fmtNum(r.pnd_score, 0)]),
              el("td", {}, [r.hmm_regime ? el("span", { class: "badge b-gray" }, [r.hmm_regime]) : "—"]),
            ]))),
          ]);
          c.appendChild(el("div", { class: "card" }, [table]));
        });
    })
    .catch((e) => showError("top-buys", e));
}

function loadWatchlistMini() {
  apiGet("/api/v1/watchlist/current")
    .then((r) => {
      const c = document.getElementById("watchlist-mini");
      if (!r.implemented || !r.tickers.length) {
        c.innerHTML = `<div class="empty">${r.notes || "No watchlist data yet"}</div>`;
        return;
      }
      const top3 = r.tickers.slice(0, 3);
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", {}, ["Stock"]), el("th", {}, ["Prob"]), el("th", {}, ["Tier"]), el("th", {}, ["Archetype"])])]),
        el("tbody", {}, top3.map((t) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [el("a", { href: `multibagger.html?ticker=${t.ticker}` }, [t.ticker])]),
          el("td", { class: "mono up" }, [fmtPct(t.mb_probability)]),
          el("td", {}, [el("span", { class: "badge b-purple" }, [t.mb_tier || "—"])]),
          el("td", {}, [t.mb_archetype || "—"]),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("watchlist-mini", e));
}

function loadPositionsMini() {
  apiGet("/api/v1/paper_trading/state")
    .then((r) => {
      const c = document.getElementById("positions-mini");
      const real = (r.positions || []).filter((p) => p.ticker !== "_HEARTBEAT_");
      if (!r.available || !real.length) {
        c.innerHTML = `<div class="empty">No open positions</div>`;
        return;
      }
      const top3 = real.slice(0, 3);
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", {}, ["Stock"]), el("th", {}, ["P&L"]), el("th", {}, ["Entry Date"])])]),
        el("tbody", {}, top3.map((p) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [el("a", { href: `positions.html` }, [p.ticker])]),
          el("td", { class: "mono " + pnlClass(p.unrealised_pnl_pct) }, [fmtPct(p.unrealised_pnl_pct)]),
          el("td", { class: "mono" }, [p.entry_date]),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("positions-mini", e));
}

loadRegime();
loadAlerts();
loadTopBuys();
loadWatchlistMini();
loadPositionsMini();
