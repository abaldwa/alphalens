// dashboard/static/big_investors/js/announcements.js — Corporate Announcements
// (real NSE feed, material-event categories only — see
// ingestion/scrapers/nse_corporate_announcements.py's module docstring)
renderAppShell("big_investors", "announcements");

const CATEGORY_LABEL = {
  buyback: "Buyback",
  qip: "QIP",
  board_change: "Board Change",
  investigation: "Investigation",
  insider: "Insider / SAST",
  credit_rating: "Credit Rating",
  auditor_change: "Auditor Change",
  ma: "M&A",
};

const CATEGORY_BADGE = {
  buyback: "b-blue",
  qip: "b-blue",
  board_change: "b-amber",
  investigation: "b-red",
  insider: "b-red",
  credit_rating: "b-teal",
  auditor_change: "b-amber",
  ma: "b-green",
};

function categoryOptions(selected) {
  const opts = [el("option", { value: "" }, ["All categories"])];
  for (const [value, label] of Object.entries(CATEGORY_LABEL)) {
    const attrs = { value };
    if (value === selected) attrs.selected = "selected";
    opts.push(el("option", attrs, [label]));
  }
  return opts;
}

function renderAnnouncementsTable(containerId, rows) {
  const c = document.getElementById(containerId);
  if (!rows.length) {
    c.innerHTML = `<div class="empty">No material announcements in range.</div>`;
    return;
  }
  const table = el("table", {}, [
    el("thead", {}, [el("tr", {}, [
      el("th", {}, ["Date"]), el("th", {}, ["Ticker"]), el("th", {}, ["Company"]),
      el("th", {}, ["Category"]), el("th", {}, ["Subject"]), el("th", {}, ["Filing"]),
    ])]),
    el("tbody", {}, rows.map((r) => el("tr", {}, [
      el("td", { class: "mono" }, [r.announced_at ? r.announced_at.replace("T", " ").slice(0, 16) : "—"]),
      tickerCell(r.ticker),
      el("td", {}, [r.company_name || "—"]),
      el("td", {}, [el("span", { class: "badge " + (CATEGORY_BADGE[r.category] || "b-gray") }, [CATEGORY_LABEL[r.category] || r.category])]),
      el("td", {}, [r.subject || "—"]),
      el("td", {}, [
        r.attachment_url
          ? el("a", { href: r.attachment_url, target: "_blank", rel: "noopener" }, ["View"])
          : "—",
      ]),
    ]))),
  ]);
  c.innerHTML = "";
  c.appendChild(el("div", { class: "card" }, [table]));
}

function loadRecent() {
  const days = document.getElementById("recent-days-select")?.value || "5";
  const category = document.getElementById("recent-category-select")?.value || "";
  showLoading("recent-table");
  const params = { days };
  if (category) params.category = category;
  apiGet("/api/v1/corporate-announcements/recent", params)
    .then((r) => renderAnnouncementsTable("recent-table", r.data))
    .catch((e) => showError("recent-table", e));
}

function renderRecentFilters() {
  const c = document.getElementById("recent-filters");
  const daysSelect = el("select", { id: "recent-days-select" }, [
    el("option", { value: "5", selected: "selected" }, ["Last 5 days"]),
    el("option", { value: "1" }, ["Last 1 day"]),
    el("option", { value: "10" }, ["Last 10 days"]),
    el("option", { value: "30" }, ["Last 30 days"]),
  ]);
  const categorySelect = el("select", { id: "recent-category-select" }, categoryOptions(""));
  daysSelect.addEventListener("change", loadRecent);
  categorySelect.addEventListener("change", loadRecent);
  c.innerHTML = "";
  c.appendChild(daysSelect);
  c.appendChild(categorySelect);
}

function runSearch() {
  const company = document.getElementById("search-company-input").value.trim();
  const category = document.getElementById("search-category-select").value;
  const container = document.getElementById("search-table");
  if (!company) {
    container.innerHTML = `<div class="empty">Enter a company name to search.</div>`;
    return;
  }
  showLoading("search-table");
  const params = { company };
  if (category) params.category = category;
  apiGet("/api/v1/corporate-announcements/search", params)
    .then((r) => renderAnnouncementsTable("search-table", r.data))
    .catch((e) => showError("search-table", e));
}

function renderSearchFilters() {
  const c = document.getElementById("search-filters");
  const companyInput = el("input", { type: "text", id: "search-company-input", placeholder: "Company name (e.g. Reliance)" });
  const categorySelect = el("select", { id: "search-category-select" }, categoryOptions(""));
  const searchBtn = el("button", {}, ["Search"]);
  searchBtn.addEventListener("click", runSearch);
  companyInput.addEventListener("keydown", (e) => { if (e.key === "Enter") runSearch(); });
  c.innerHTML = "";
  c.appendChild(companyInput);
  c.appendChild(categorySelect);
  c.appendChild(searchBtn);
}

renderSearchFilters();
renderRecentFilters();
loadRecent();
document.getElementById("search-table").innerHTML = `<div class="empty">Enter a company name to search.</div>`;
