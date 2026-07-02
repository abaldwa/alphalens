// dashboard/static/fundamental/js/peers.js — FA-B Peer Comparison
renderAppShell("fundamental", "peers");
TickerPicker.attach("ticker-input");

const params = new URLSearchParams(window.location.search);
if (params.get("ticker")) document.getElementById("ticker-input").value = params.get("ticker");

function load() {
  const ticker = document.getElementById("ticker-input").value.trim().toUpperCase();
  if (!ticker) return;
  showLoading("peers-table");

  apiGet(`/api/v1/fundamentals/${ticker}/peers`, { k: 8 })
    .then((r) => {
      document.getElementById("peers-sector").textContent = r.sector ? `Sector: ${r.sector}` : "";
      const c = document.getElementById("peers-table");
      if (!r.peers.length) {
        c.innerHTML = `<div class="empty">No peers found — either this ticker's sector is unknown, or no other ticker in its sector has a computed feature row for today</div>`;
        return;
      }
      const table = el("table", {}, [
        el("thead", {}, [el("tr", {}, [el("th", {}, ["Ticker"]), el("th", {}, ["ROE (z)"]), el("th", {}, ["ROCE (z)"]), el("th", {}, ["D/E (z)"]), el("th", {}, ["PE (z)"])])]),
        el("tbody", {}, r.peers.map((p) => el("tr", {}, [
          el("td", { style: "font-weight:600" }, [el("a", { href: `dashboard.html?ticker=${p.ticker}` }, [p.ticker])]),
          el("td", { class: "mono" }, [p.roe !== null ? fmtNum(p.roe, 2) : "—"]),
          el("td", { class: "mono" }, [p.roce !== null ? fmtNum(p.roce, 2) : "—"]),
          el("td", { class: "mono" }, [p.debt_to_equity !== null ? fmtNum(p.debt_to_equity, 2) : "—"]),
          el("td", { class: "mono" }, [p.pe_ratio !== null ? fmtNum(p.pe_ratio, 2) : "—"]),
        ]))),
      ]);
      c.innerHTML = "";
      c.appendChild(el("div", { class: "card" }, [table]));
    })
    .catch((e) => showError("peers-table", e));
}

document.getElementById("load-btn").addEventListener("click", load);
if (params.get("ticker")) load();
