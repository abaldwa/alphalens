"""alphalens/dashboard/pages/watchlist.py — Page 3: Watchlist"""
import dash, dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html
dash.register_page(__name__, path="/watchlist", name="Watchlist", order=3)
from alphalens.dashboard.components.shared import *

layout = page_layout([
    section_header("Watchlist", "Active buy/sell signals · updated at 6:30 PM, 9:30 AM, 3:00 PM"),
    dbc.Row([
        dbc.Col(dcc.Dropdown(id="wl-tf-filter",
            options=[{"label": l, "value": v} for l, v in [
                ("All Timeframes", "all"), ("Intraday", "intraday"),
                ("Swing", "swing"), ("Medium", "medium"), ("Long-term", "long_term")]],
            value="all", clearable=False,
            style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                   "fontSize": "0.83rem"}), width=3),
        dbc.Col(dcc.Dropdown(id="wl-signal-filter",
            options=[{"label": l, "value": v} for l, v in [
                ("BUY + SELL", "all"), ("BUY only", "buy"), ("SELL only", "sell")]],
            value="all", clearable=False,
            style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                   "fontSize": "0.83rem"}), width=2),
    ], className="mb-3 g-2"),
    html.Div(id="watchlist-table"),
    dcc.Interval(id="wl-refresh", interval=60_000, n_intervals=0),
])

@callback(Output("watchlist-table", "children"),
          Input("wl-tf-filter", "value"), Input("wl-signal-filter", "value"),
          Input("wl-refresh", "n_intervals"))
def render_watchlist(tf, signal_f, _):
    from alphalens.core.database import get_sqlite, Watchlist as WL
    with get_sqlite() as session:
        q = session.query(WL).filter(WL.is_active == True)
        if tf != "all":       q = q.filter(WL.timeframe == tf)
        if signal_f != "all": q = q.filter(WL.signal_type == signal_f)
        items = q.order_by(WL.confidence.desc()).all()

    if not items:
        return dbc.Alert("No active signals. Signals are generated at 6:30 PM EOD.",
                         color="secondary", style={"marginTop": "20px"})

    rows = []
    for w in items:
        conf = w.confidence or 0
        rows.append(html.Tr([
            html.Td(html.A(w.symbol, href=f"/chart?symbol={w.symbol}",
                           style={"color": GOLD_COLOR, "textDecoration": "none",
                                  "fontFamily": "JetBrains Mono", "fontWeight": "600"})),
            html.Td(signal_badge(w.signal_type)),
            html.Td(w.timeframe.upper().replace("_","-") if w.timeframe else "–",
                    style={"fontFamily": "JetBrains Mono", "fontSize": "0.75rem",
                           "color": NEUTRAL_COLOR}),
            html.Td(f"₹{w.suggested_entry:,.2f}" if w.suggested_entry else "–",
                    style={"fontFamily": "JetBrains Mono"}),
            html.Td(f"₹{w.target_price:,.2f}" if w.target_price else "–",
                    style={"color": BULL_COLOR, "fontFamily": "JetBrains Mono"}),
            html.Td(f"₹{w.stop_loss:,.2f}" if w.stop_loss else "–",
                    style={"color": BEAR_COLOR, "fontFamily": "JetBrains Mono"}),
            html.Td(f"{w.risk_reward:.1f}x" if w.risk_reward else "–",
                    style={"color": GOLD_COLOR, "fontFamily": "JetBrains Mono"}),
            html.Td(html.Div([
                html.Div(style={"height": "4px", "width": f"{conf*100:.0f}%",
                                "background": BULL_COLOR, "borderRadius": "2px"}),
                html.Span(f"{conf*100:.0f}%", style={"fontSize": "0.7rem",
                                                       "color": NEUTRAL_COLOR, "marginTop": "2px"}),
            ])),
            html.Td(cycle_badge(w.cycle_context or "neutral"), style={}),
            html.Td(w.strategy_id or "–",
                    style={"color": "#6b7280", "fontSize": "0.72rem"}),
            html.Td(w.reasoning[:60] + "…" if w.reasoning and len(w.reasoning) > 60 else (w.reasoning or ""),
                    style={"color": "#6b7280", "fontSize": "0.72rem", "maxWidth": "200px"}),
        ]))

    return dbc.Table([
        html.Thead(html.Tr([html.Th(h, style={"color": "#6b7280", "fontSize": "0.7rem",
                                               "fontFamily": "JetBrains Mono",
                                               "textTransform": "uppercase"})
                            for h in ["Symbol", "Signal", "TF", "Entry", "Target", "SL",
                                      "R:R", "Confidence", "Cycle", "Strategy", "Reasoning"]])),
        html.Tbody(rows),
    ], bordered=False, hover=True, responsive=True,
       style={"fontSize": "0.82rem", "background": CARD_BG,
              "border": f"1px solid {BORDER_COLOR}", "borderRadius": "6px"})
