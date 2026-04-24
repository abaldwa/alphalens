"""alphalens/dashboard/pages/pnl_report.py — Page 6: P&L Reports"""
import dash, dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html
dash.register_page(__name__, path="/pnl", name="P&L", order=6)
from alphalens.dashboard.components.shared import *

layout = page_layout([
    section_header("P&L Reports", "Booked + Notional P&L · STCG/LTCG breakdown · equity curve"),
    html.Div(id="pnl-summary-cards", className="mb-4"),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Equity Curve",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody(dcc.Graph(id="equity-curve", config={"displayModeBar": False},
                                       style={"height": "280px"}),
                             style={"padding": "8px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=8),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Tax Breakdown",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody(html.Div(id="tax-panel"), style={"padding": "12px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=4),
    ], className="mb-3"),
    dbc.Card([
        dbc.CardHeader("Closed Trades History",
                       style={"background": "#111827", "color": "#9ca3af",
                              "fontSize": "0.75rem", "textTransform": "uppercase"}),
        dbc.CardBody(html.Div(id="trades-history-table", style={"maxHeight": "320px", "overflowY": "auto"}),
                     style={"padding": "8px"}),
    ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
    dcc.Interval(id="pnl-refresh", interval=120_000, n_intervals=0),
])

@callback(Output("pnl-summary-cards", "children"),
          Output("equity-curve", "figure"),
          Output("tax-panel", "children"),
          Output("trades-history-table", "children"),
          Input("pnl-refresh", "n_intervals"))
def render_pnl(_):
    from alphalens.core.portfolio.pnl import PnlTracker
    pnl = PnlTracker()
    summary = pnl.get_summary()
    history = pnl.get_history(days=180)
    trades  = pnl.get_closed_trades_summary()
    tax     = summary.get("tax_breakdown", {})

    cards = dbc.Row([
        dbc.Col(metric_card("Total P&L", format_inr(summary["total_pnl"]),
                            pnl_color(summary["total_pnl"]), format_pct(summary["total_pnl_pct"])), width=2),
        dbc.Col(metric_card("Booked P&L", format_inr(summary["total_booked_pnl"]),
                            pnl_color(summary["total_booked_pnl"])), width=2),
        dbc.Col(metric_card("Notional P&L", format_inr(summary["total_notional_pnl"]),
                            pnl_color(summary["total_notional_pnl"])), width=2),
        dbc.Col(metric_card("Win Rate",
                            f"{trades.get('win_rate', 0)*100:.1f}%" if trades.get("win_rate") else "–",
                            BULL_COLOR, f"{trades.get('winners', 0)}W / {trades.get('losers', 0)}L"), width=2),
        dbc.Col(metric_card("STCG Tax Est.", format_inr(tax.get("estimated_stcg_tax", 0)), NEUTRAL_COLOR), width=2),
        dbc.Col(metric_card("LTCG Tax Est.", format_inr(tax.get("estimated_ltcg_tax", 0)), NEUTRAL_COLOR,
                            "10% on gains > ₹1L"), width=2),
    ], className="g-2")

    # Equity curve
    fig = go.Figure()
    if not history.empty:
        fig.add_trace(go.Scatter(x=history["date"], y=history["total_pnl"],
                                  mode="lines+markers", name="Total P&L",
                                  line={"color": GOLD_COLOR, "width": 2},
                                  fill="tozeroy",
                                  fillcolor="rgba(184,134,11,0.1)",
                                  marker={"size": 3}))
        fig.add_trace(go.Scatter(x=history["date"], y=history["booked_pnl"],
                                  mode="lines", name="Booked",
                                  line={"color": BULL_COLOR, "width": 1, "dash": "dot"}))
    fig.update_layout(paper_bgcolor="#1f2937", plot_bgcolor="#1f2937",
                      font={"color": "#9ca3af", "family": "JetBrains Mono", "size": 10},
                      margin={"l": 60, "r": 10, "t": 10, "b": 30},
                      yaxis={"tickformat": ",.0f", "gridcolor": "#374151"},
                      xaxis={"gridcolor": "#374151"},
                      legend={"orientation": "h", "y": 1.05, "bgcolor": "rgba(0,0,0,0)"})

    # Tax panel
    tax_rows = [
        ("STCG Gains",  format_inr(tax.get("stcg_gains", 0)),  BULL_COLOR),
        ("STCG Losses", format_inr(tax.get("stcg_losses", 0)), BEAR_COLOR),
        ("Net STCG",    format_inr(tax.get("net_stcg", 0)),    pnl_color(tax.get("net_stcg", 0))),
        ("LTCG Gains",  format_inr(tax.get("ltcg_gains", 0)),  BULL_COLOR),
        ("LTCG Losses", format_inr(tax.get("ltcg_losses", 0)), BEAR_COLOR),
        ("Net LTCG",    format_inr(tax.get("net_ltcg", 0)),    pnl_color(tax.get("net_ltcg", 0))),
        ("Est. STCG Tax (15%)", format_inr(tax.get("estimated_stcg_tax", 0)), NEUTRAL_COLOR),
        ("Est. LTCG Tax (10%)", format_inr(tax.get("estimated_ltcg_tax", 0)), NEUTRAL_COLOR),
    ]
    tax_panel = html.Div([
        html.Div([
            html.Span(lbl, style={"fontSize": "0.75rem", "color": "#9ca3af", "flex": "1"}),
            html.Span(val, style={"fontFamily": "JetBrains Mono", "fontSize": "0.82rem",
                                   "color": col, "fontWeight": "600"}),
        ], style={"display": "flex", "padding": "5px 0",
                  "borderBottom": f"1px solid {BORDER_COLOR}"})
        for lbl, val, col in tax_rows
    ])

    # Trade history
    trade_list = trades.get("trades_list", [])
    if not trade_list:
        trade_table = html.Div("No closed trades yet.", style={"color": "#6b7280",
                                                                 "padding": "16px", "textAlign": "center"})
    else:
        rows = [html.Tr([
            html.Td(t["symbol"], style={"fontFamily": "JetBrains Mono", "fontWeight": "600",
                                         "color": GOLD_COLOR}),
            html.Td(t.get("timeframe", "–").replace("_","-").upper(),
                    style={"color": NEUTRAL_COLOR, "fontSize": "0.72rem", "fontFamily": "JetBrains Mono"}),
            html.Td(str(t.get("entry_date", "–"))[:10]),
            html.Td(str(t.get("exit_date", "–"))[:10]),
            html.Td(f"₹{t.get('entry_price', 0):,.2f}", style={"fontFamily": "JetBrains Mono"}),
            html.Td(f"₹{t.get('exit_price', 0):,.2f}", style={"fontFamily": "JetBrains Mono"}),
            html.Td(html.Span(format_pct(t.get("pnl_pct", 0)),
                              style={"color": pnl_color(t.get("pnl_pct", 0)),
                                     "fontFamily": "JetBrains Mono", "fontWeight": "600"})),
            html.Td(format_inr(t.get("booked_pnl", 0)),
                    style={"color": pnl_color(t.get("booked_pnl", 0)), "fontFamily": "JetBrains Mono"}),
            html.Td(str(t.get("holding_days", "–")) + "d"),
            html.Td(html.Span(t.get("tax_type", "–"),
                              style={"color": GOLD_COLOR if t.get("tax_type") == "LTCG" else NEUTRAL_COLOR,
                                     "fontSize": "0.72rem", "fontFamily": "JetBrains Mono"})),
            html.Td(t.get("exit_reason", "–"), style={"fontSize": "0.72rem", "color": "#6b7280"}),
        ]) for t in trade_list[:50]]
        trade_table = dbc.Table([
            html.Thead(html.Tr([html.Th(h, style={"color": "#6b7280", "fontSize": "0.7rem",
                                                    "fontFamily": "JetBrains Mono", "textTransform": "uppercase"})
                                for h in ["Symbol", "TF", "Entry Date", "Exit Date", "Buy",
                                          "Sell", "P&L%", "P&L ₹", "Days", "Tax", "Reason"]])),
            html.Tbody(rows),
        ], bordered=False, hover=True, responsive=True, style={"fontSize": "0.82rem"})

    return cards, fig, tax_panel, trade_table
