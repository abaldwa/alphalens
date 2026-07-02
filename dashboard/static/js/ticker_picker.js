// dashboard/static/js/ticker_picker.js — site-wide ticker autocomplete.
// Fetches the ticker universe once (cached module-level), builds a
// <datalist id="ticker-list"> and wires it to input(s) via the `list`
// attribute. Usage: TickerPicker.attach("ticker-input") (or any input id) —
// safe to call from every screen; the fetch only happens once per page load.

const TickerPicker = (() => {
  let cachedTickers = null;
  let fetchPromise = null;
  const DATALIST_ID = "ticker-list";

  function ensureDatalist() {
    let dl = document.getElementById(DATALIST_ID);
    if (!dl) {
      dl = document.createElement("datalist");
      dl.id = DATALIST_ID;
      document.body.appendChild(dl);
    }
    return dl;
  }

  function populate(tickers) {
    const dl = ensureDatalist();
    if (dl.childElementCount === tickers.length) return; // already populated
    dl.innerHTML = "";
    tickers.forEach((t) => {
      const opt = document.createElement("option");
      opt.value = t;
      dl.appendChild(opt);
    });
  }

  function load() {
    if (cachedTickers) return Promise.resolve(cachedTickers);
    if (fetchPromise) return fetchPromise;
    fetchPromise = apiGet("/api/v1/ohlcv/_meta/tickers")
      .then((r) => {
        cachedTickers = (r && r.tickers) || [];
        populate(cachedTickers);
        return cachedTickers;
      })
      .catch(() => {
        cachedTickers = [];
        return cachedTickers;
      });
    return fetchPromise;
  }

  function attach(inputId) {
    const input = document.getElementById(inputId);
    if (!input) return;
    input.setAttribute("list", DATALIST_ID);
    load();
  }

  return { attach, load };
})();
