"""
backtest/trade_book_html.py

Phase: Technical Analysis Momentum-parity backtest reporting (2026-08-01)
Owner: Platform / Backtest
Consumers: backtest/export_trade_book.py, scripts/run_technical_*.py,
    scripts/convert_momentum_csv_to_html.py (could migrate to this, not
    required — left as-is since it already works)

Self-contained, sortable/filterable standalone HTML table renderer —
generalized from scripts/convert_momentum_csv_to_html.py's one-off CSV
converter so a trade-book writer can call it directly at write time
(2026-08-01 user request: "write to .html pages also in a specific
directory", for Technical trade books specifically) instead of requiring
a separate manual conversion pass after the fact. No CDN dependency —
inline CSS + a small vanilla-JS click-to-sort/search script, so pages open
fine straight from disk in any browser.
"""

from __future__ import annotations

import html
from pathlib import Path
from typing import Any, Dict, List, Sequence

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
<div class="meta">{nrows} rows &middot; {subtitle}</div>
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


def _build_table_html(columns: Sequence[str], rows: List[Dict[str, Any]]) -> str:
    thead = "<tr>" + "".join(f"<th>{html.escape(str(c))}</th>" for c in columns) + "</tr>"
    body_rows = []
    for row in rows:
        cells = []
        for col in columns:
            v = row.get(col)
            if isinstance(v, float):
                cls = " class=\"neg\"" if v < 0 else (" class=\"pos\"" if v > 0 else "")
                cells.append(f'<td data-v="{v}"{cls}>{v:,.4f}</td>')
            elif v is None:
                cells.append("<td>&mdash;</td>")
            else:
                cells.append(f"<td>{html.escape(str(v))}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead>{thead}</thead><tbody>{''.join(body_rows)}</tbody></table>"


def render_trade_book_html(
    rows: List[Dict[str, Any]], columns: Sequence[str], title: str, out_path: Path,
    subtitle: str = "",
) -> Path:
    """Write one sortable/filterable HTML page for `rows` (a plain list of
    dicts, e.g. dataclasses.asdict(trade) per row) to `out_path`. `columns`
    fixes column order (row dicts may carry extra keys not rendered, e.g.
    entry_feature_vector — pass an explicit column list rather than
    dumping every key). Returns out_path for convenient chaining."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    doc = _TEMPLATE.format(
        title=html.escape(title),
        nrows=len(rows),
        subtitle=html.escape(subtitle) or "trade book",
        table=_build_table_html(columns, rows),
    )
    out_path.write_text(doc)
    return out_path


def write_html_index(entries: List[Dict[str, str]], out_path: Path, title: str = "Trade Book Reports") -> Path:
    """entries: [{"label": ..., "href": ...}, ...]. Writes a simple linked
    index page (same convention as backtest/reports/momentum/html/index.html,
    manually built once for Momentum) into out_path."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        f"<title>{html.escape(title)}</title>",
        "<style>body{font-family:sans-serif;padding:1.5rem;color-scheme:light dark}",
        "li{margin:0.2rem 0}a{text-decoration:none}a:hover{text-decoration:underline}</style>",
        f"</head><body><h1>{html.escape(title)}</h1><ul>",
    ]
    for e in entries:
        lines.append(f"<li><a href='{html.escape(e['href'])}'>{html.escape(e['label'])}</a></li>")
    lines.append("</ul></body></html>")
    out_path.write_text("\n".join(lines))
    return out_path
