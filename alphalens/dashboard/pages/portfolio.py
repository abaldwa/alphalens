"""
alphalens/dashboard/pages/portfolio.py — Page 2: Portfolio Dashboard
"""
import dash, dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html
dash.register_page(__name__, path="/portfolio", name="Portfolio", order=2)

from alphalens.dashboard.components.shared import *


layout = page_layout([
    section_header("Portfolio", "All open positions across 4 timeframes"),
    html.Div(id="portfolio-all-tabs"),
    dcc.Interval(id="portfolio-refresh", interval=60_000, n_intervals=0),
])


@callback(Output("portfolio-all-tabs", "children"), Input("portfolio-refresh", "n_intervals"))
def render_portfolio(_):
    from alphalens.core.portfolio.manager import PortfolioManager
    from alphalens.core.portfolio.pnl import PnlTracker
    from alphalens.core.database import get_duck
    pm  = PortfolioManager()
    pnl = PnlTracker()
    con = get_duck()
    summary = pnl.get_summary()

    # Summary metrics
    metrics = dbc.Row([
        dbc.Col(metric_card("Total P&L", format_inr(summary["total_pnl"]),
                            color=pnl_color(summary["total_pnl"]),
                            subtext=format_pct(summary["total_pnl_pct"])), width=2),
        dbc.Col(metric_card("Booked P&L", format_inr(summary["total_booked_pnl"]),
                            color=pnl_color(summary["total_booked_pnl"])), width=2),
        dbc.Col(metric_card("Notional P&L", format_inr(summary["total_notional_pnl"]),
                            color=pnl_color(summary["total_notional_pnl"])), width=2),
        dbc.Col(metric_card("Invested", format_inr(summary["invested_capital"]),
                            subtext=f"{summary['capital_utilisation']:.0f}% deployed"), width=2),
        dbc.Col(metric_card("Cash Available", format_inr(summary["cash_available"]),
                            color=BULL_COLOR), width=2),
        dbc.Col(metric_card("STCG Tax Est.", format_inr(summary["tax_breakdown"]["estimated_stcg_tax"]),
                            color=NEUTRAL_COLOR,
                            subtext="LTCG: " + format_inr(summary["tax_breakdown"]["estimated_ltcg_tax"])), width=2),
    ], className="g-2 mb-4")

    # Per-timeframe tabs
    tf_sections = []
    for tf in ["intraday", "swing", "medium", "long_term"]:
        cap      = pm.get_capacity(tf)
        holdings = pm.get_holdings(tf)
        prices   = _get_prices(con, [h["symbol"] for h in holdings])

        rows = []
        for h in holdings:
            current  = prices.get(h["symbol"], h["avg_cost"] or 0)
            cost     = h["avg_cost"] or 0
            notional = (current - cost) * (h["qty"] or 0)
            pnl_p    = (current / cost - 1) * 100 if cost > 0 else 0
            hold_days = 0
            import datetime
            if h.get("entry_date"):
                try:
                    hold_days = (datetime.date.today() - h["entry_date"]).days
                except Exception:
                    pass
            tax = "LTCG" if hold_days > 365 else "STCG"
            rows.append(html.Tr([
                html.Td(html.A(h["symbol"],
                               href=f"/chart?symbol={h['symbol']}",
                               style={"color": GOLD_COLOR, "textDecoration": "none",
                                      "fontFamily": "JetBrains Mono", "fontWeight": "600"})),
                html.Td(str(h["qty"] or "–")),
                html.Td(f"₹{cost:,.2f}" if cost else "–"),
                html.Td(f"₹{current:,.2f}"),
                html.Td(html.Span(format_pct(pnl_p), style={"color": pnl_color(pnl_p),
                                                              "fontFamily": "JetBrains Mono",
                                                              "fontWeight": "600"})),
                html.Td(format_inr(notional), style={"color": pnl_color(notional),
                                                       "fontFamily": "JetBrains Mono"}),
                html.Td(f"₹{h.get('target', 0) or 0:,.2f}" if h.get("target") else "–",
                        style={"color": BULL_COLOR, "fontFamily": "JetBrains Mono"}),
                html.Td(f"₹{h.get('stop_loss', 0) or 0:,.2f}" if h.get("stop_loss") else "–",
                        style={"color": BEAR_COLOR, "fontFamily": "JetBrains Mono"}),
                html.Td(str(hold_days) + "d"),
                html.Td(html.Span(tax,
                                   style={"color": GOLD_COLOR if tax == "LTCG" else NEUTRAL_COLOR,
                                          "fontSize": "0.72rem", "fontFamily": "JetBrains Mono"})),
                html.Td(h.get("strategy_id") or "–",
                        style={"color": "#6b7280", "fontSize": "0.72rem"}),
            ]))

        tf_label = {"intraday": "Intraday", "swing": "Swing", "medium": "Medium-term", "long_term": "Long-term"}[tf]
        slots_text = f"{cap['used']}/{cap['max']} slots"
        bg = "#1a4d2e20" if cap["used"] < cap["max"] else "#4d1a1a20"

        tf_sections.append(dbc.Card([
            dbc.CardHeader([
                html.Span(tf_label, style={"fontWeight": "700", "color": "#f9fafb",
                                           "marginRight": "8px"}),
                html.Span(slots_text, style={"color": GOLD_COLOR, "fontFamily": "JetBrains Mono",
                                              "fontSize": "0.78rem"}),
                html.Span(f" · {format_inr(summary['by_timeframe'].get(tf, {}).get('total_pnl', 0))}",
                          style={"color": pnl_color(summary['by_timeframe'].get(tf, {}).get('total_pnl', 0)),
                                 "fontSize": "0.78rem", "marginLeft": "8px"}),
            ], style={"background": bg, "border": "none"}),
            dbc.CardBody([
                dbc.Table([
                    html.Thead(html.Tr([
                        html.Th(h, style={"color": "#6b7280", "fontSize": "0.7rem",
                                          "fontFamily": "JetBrains Mono",
                                          "textTransform": "uppercase", "letterSpacing": "0.08em"})
                        for h in ["Symbol", "Qty", "Avg Cost", "Current", "P&L%", "P&L ₹",
                                  "Target", "SL", "Days", "Tax", "Strategy"]
                    ])),
                    html.Tbody(rows if rows else [
                        html.Tr([html.Td("No open positions", colSpan=11,
                                         style={"color": "#6b7280", "textAlign": "center",
                                                "padding": "16px"})])
                    ]),
                ], bordered=False, hover=True, responsive=True,
                   style={"fontSize": "0.82rem", "marginBottom": "0"}),
            ], style={"padding": "0"}),
        ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                  "marginBottom": "12px"}))

    return html.Div([metrics] + tf_sections)


def _get_prices(con, symbols):
    if not symbols:
        return {}
    ph  = ", ".join(["?"] * len(symbols))
    rows = con.execute(f"""
        SELECT symbol, close FROM daily_prices
        WHERE (symbol, date) IN (
            SELECT symbol, MAX(date) FROM daily_prices WHERE symbol IN ({ph}) GROUP BY symbol
        )
    """, symbols).fetchall()
    return {r[0]: float(r[1]) for r in rows}
