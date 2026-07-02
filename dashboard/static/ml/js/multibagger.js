// dashboard/static/ml/js/multibagger.js — ML-C Multibagger Watchlist
renderAppShell("ml", "multibagger");

apiGet("/api/v1/watchlist/current")
  .then((r) => {
    document.getElementById("watchlist-notes").textContent = r.notes || "";
    const c = document.getElementById("watchlist-table");
    if (!r.implemented || !r.tickers.length) {
      c.innerHTML = `<div class="empty">${r.notes || "No watchlist data yet"}</div>`;
      return;
    }
    const cols = ["ticker", "mb_probability", "mb_tier", "mb_archetype", "survival_6m", "survival_12m", "survival_18m", "survival_24m", "survival_36m"];
    const labels = ["Ticker", "MB Prob", "Tier", "Archetype", "6m", "12m", "18m", "24m", "36m"];
    const table = el("table", {}, [
      el("thead", {}, [el("tr", {}, labels.map((l) => el("th", {}, [l])))]),
      el("tbody", {}, r.tickers.map((t) => el("tr", {}, cols.map((cc) => {
        if (cc === "ticker") return el("td", { style: "font-weight:600" }, [el("a", { href: `signal.html?ticker=${t.ticker}` }, [t.ticker])]);
        if (cc === "mb_probability") return el("td", { class: "mono" }, [fmtPct(t[cc])]);
        if (cc === "mb_tier") return el("td", {}, [el("span", { class: "badge b-purple" }, [t[cc] || "—"])]);
        if (cc.startsWith("survival")) return el("td", { class: "mono" }, [fmtPct(t[cc])]);
        return el("td", {}, [t[cc] || "—"]);
      })))),
    ]);
    c.innerHTML = "";
    c.appendChild(el("div", { class: "card" }, [table]));
  })
  .catch((e) => showError("watchlist-table", e));
