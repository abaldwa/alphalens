"""
One-off utility: convert every backtest/reports/momentum/*.csv into a
sortable, filterable standalone HTML table for manual analysis, written to
backtest/reports/momentum/html/<same-basename>.html. Self-contained (no
CDN) — inline CSS + a small vanilla-JS click-to-sort/search script.
"""
from __future__ import annotations

import html
from pathlib import Path

import pandas as pd

SRC_DIR = Path("backtest/reports/momentum")
OUT_DIR = SRC_DIR / "html"

_TEMPLATE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; padding: 1.5rem;
          background: #fff; color: #1a1a1a; }}
  @media (prefers-color-scheme: dark) {{ body {{ background: #16181d; color: #e6e6e6; }} }}
  h1 {{ font-size: 1.1rem; margin: 0 0 0.25rem; }}
  .meta {{ font-size: 0.8rem; opacity: 0.65; margin-bottom: 1rem; }}
  input#search {{ padding: 0.4rem 0.6rem; font-size: 0.85rem; width: 320px; max-width: 100%;
                  margin-bottom: 0.75rem; border: 1px solid #8884; border-radius: 6px;
                  background: transparent; color: inherit; }}
  .wrap {{ overflow-x: auto; border: 1px solid #8883; border-radius: 8px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.82rem; white-space: nowrap; }}
  th, td {{ padding: 0.35rem 0.6rem; text-align: right; border-bottom: 1px solid #8882; }}
  th:first-child, td:first-child {{ text-align: left; }}
  th {{ position: sticky; top: 0; background: #8881; cursor: pointer; user-select: none; }}
  th:hover {{ background: #8882; }}
  th.sorted-asc::after {{ content: " \\2191"; }}
  th.sorted-desc::after {{ content: " \\2193"; }}
  tr:nth-child(even) {{ background: #8880a; }}
  tr.hidden {{ display: none; }}
  .neg {{ color: #d9534f; }}
  .pos {{ color: #2e9e5b; }}
</style>
</head>
<body>
<h1>{title}</h1>
<div class="meta">{nrows} rows &middot; converted from {csv_name}</div>
<input id="search" type="text" placeholder="Filter rows...">
<div class="wrap">
{table}
</div>
<script>
(function () {{
  const table = document.querySelector('table');
  const tbody = table.tBodies[0];
  const headers = table.tHead.rows[0].cells;
  let sortState = {{ col: -1, asc: true }};

  Array.from(headers).forEach((th, idx) => {{
    th.addEventListener('click', () => {{
      const rows = Array.from(tbody.rows);
      const asc = sortState.col === idx ? !sortState.asc : true;
      rows.sort((a, b) => {{
        const av = a.cells[idx].dataset.v ?? a.cells[idx].textContent;
        const bv = b.cells[idx].dataset.v ?? b.cells[idx].textContent;
        const an = parseFloat(av), bn = parseFloat(bv);
        const bothNum = !isNaN(an) && !isNaN(bn);
        const cmp = bothNum ? an - bn : String(av).localeCompare(String(bv));
        return asc ? cmp : -cmp;
      }});
      rows.forEach((r) => tbody.appendChild(r));
      Array.from(headers).forEach((h) => h.classList.remove('sorted-asc', 'sorted-desc'));
      th.classList.add(asc ? 'sorted-asc' : 'sorted-desc');
      sortState = {{ col: idx, asc }};
    }});
  }});

  document.getElementById('search').addEventListener('input', (e) => {{
    const q = e.target.value.toLowerCase();
    Array.from(tbody.rows).forEach((r) => {{
      r.classList.toggle('hidden', q && !r.textContent.toLowerCase().includes(q));
    }});
  }});
}})();
</script>
</body>
</html>
"""


def _build_table_html(df: pd.DataFrame) -> str:
    thead = "<tr>" + "".join(f"<th>{html.escape(str(c))}</th>" for c in df.columns) + "</tr>"
    rows = []
    for _, row in df.iterrows():
        cells = []
        for v in row:
            if isinstance(v, float):
                cls = " class=\"neg\"" if v < 0 else (" class=\"pos\"" if v > 0 else "")
                cells.append(f'<td data-v="{v}"{cls}>{v:,.4f}</td>')
            else:
                cells.append(f"<td>{html.escape(str(v))}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead>{thead}</thead><tbody>{''.join(rows)}</tbody></table>"


def convert(csv_path: Path, out_dir: Path) -> Path:
    df = pd.read_csv(csv_path)
    out_path = out_dir / (csv_path.stem + ".html")
    html_doc = _TEMPLATE.format(
        title=html.escape(csv_path.stem),
        nrows=len(df),
        csv_name=html.escape(csv_path.name),
        table=_build_table_html(df),
    )
    out_path.write_text(html_doc)
    return out_path


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_files = sorted(SRC_DIR.glob("*.csv"))
    print(f"Converting {len(csv_files)} CSV files -> {OUT_DIR}")
    index_rows = []
    for csv_path in csv_files:
        try:
            out_path = convert(csv_path, OUT_DIR)
        except Exception as exc:  # noqa: BLE001
            print(f"  FAILED {csv_path.name}: {exc}")
            continue
        index_rows.append((csv_path.stem, out_path.name))

    index_html = ["<!doctype html><html><head><meta charset='utf-8'><title>Momentum CSV Reports</title>",
                  "<style>body{font-family:sans-serif;padding:1.5rem;color-scheme:light dark}",
                  "li{margin:0.2rem 0}a{text-decoration:none}a:hover{text-decoration:underline}</style>",
                  "</head><body><h1>Momentum CSV Reports (HTML)</h1><ul>"]
    for stem, fname in index_rows:
        index_html.append(f"<li><a href='{fname}'>{html.escape(stem)}</a></li>")
    index_html.append("</ul></body></html>")
    (OUT_DIR / "index.html").write_text("\n".join(index_html))
    print(f"Done: {len(index_rows)} converted, index at {OUT_DIR / 'index.html'}")


if __name__ == "__main__":
    main()
