"""
alphalens/dashboard/pages/capital_config.py — Capital Allocation UI

Features:
  - Total capital + reserve cash %
  - Strategy allocation ratios (slider per strategy)
  - Timeframe allocation ratios
  - Real-time share qty calculator
  - Capital deployed summary
  - Sector exposure tracker
"""

import dash, dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html
dash.register_page(__name__, path="/capital-config", name="Capital Config", order=11)

from alphalens.dashboard.components.shared import *


layout = page_layout([
    section_header("Capital Allocation", "Configure ratios · position sizing · sector exposure limits"),

    dbc.Row([
        # ── Left: Configuration ───────────────────────────────────────────
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Portfolio Capital Settings",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col(html.Div("Total Capital (₹)", style={"fontSize": "0.78rem",
                                                                       "color": "#9ca3af"}), width=5),
                        dbc.Col(dbc.Input(id="cap-total", type="number", value=2_500_000,
                                          style={"background": "#0d0d0d", "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb"}), width=7),
                    ], className="mb-2"),

                    dbc.Row([
                        dbc.Col(html.Div("Reserve Cash %", style={"fontSize": "0.78rem",
                                                                   "color": "#9ca3af"}), width=5),
                        dbc.Col(dbc.Input(id="cap-reserve-pct", type="number", value=10,
                                          min=0, max=50, step=1,
                                          style={"background": "#0d0d0d", "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb"}), width=7),
                    ], className="mb-2"),

                    html.Hr(style={"borderColor": BORDER_COLOR, "margin": "12px 0"}),

                    html.Div("Timeframe Allocation Ratios", style={"fontWeight": "700",
                                                                     "color": GOLD_COLOR,
                                                                     "fontSize": "0.78rem",
                                                                     "marginBottom": "8px"}),
                    *[_ratio_slider(label, key, default)
                      for label, key, default in [
                          ("Intraday",  "tf-intraday",  10),
                          ("Swing",     "tf-swing",     20),
                          ("Medium",    "tf-medium",    30),
                          ("Long-term", "tf-longterm",  40),
                      ]],

                    html.Div(id="tf-ratio-sum", style={"fontSize": "0.72rem", "color": "#6b7280",
                                                         "marginTop": "6px"}),

                    html.Hr(style={"borderColor": BORDER_COLOR, "margin": "12px 0"}),

                    html.Div("Position Limits", style={"fontWeight": "700",
                                                         "color": GOLD_COLOR,
                                                         "fontSize": "0.78rem",
                                                         "marginBottom": "8px"}),
                    dbc.Row([
                        dbc.Col(html.Div("Max ₹ per stock", style={"fontSize": "0.78rem",
                                                                     "color": "#9ca3af"}), width=6),
                        dbc.Col(dbc.Input(id="cap-max-per-stock", type="number", value=200_000,
                                          style={"background": "#0d0d0d", "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb"}), width=6),
                    ], className="mb-2"),

                    dbc.Row([
                        dbc.Col(html.Div("Max sector %", style={"fontSize": "0.78rem",
                                                                  "color": "#9ca3af"}), width=6),
                        dbc.Col(dbc.Input(id="cap-max-sector-pct", type="number", value=25,
                                          min=5, max=50, step=5,
                                          style={"background": "#0d0d0d", "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb"}), width=6),
                    ], className="mb-3"),

                    dbc.Button("Save Configuration", id="cap-save", n_clicks=0,
                               color="warning", style={"width": "100%", "fontWeight": "700"}),
                    html.Div(id="cap-save-result", style={"marginTop": "8px"}),
                ], style={"padding": "16px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=5),

        # ── Right: Live Calculator ────────────────────────────────────────
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Position Size Calculator",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col(dcc.Dropdown(id="calc-symbol", placeholder="Select symbol…",
                                             style={"background": CARD_BG, "fontSize": "0.83rem"}), width=6),
                        dbc.Col(dcc.Dropdown(id="calc-tf",
                                             options=[{"label": l, "value": v} for l, v in [
                                                 ("Intraday", "intraday"), ("Swing", "swing"),
                                                 ("Medium", "medium"), ("Long-term", "long_term")]],
                                             value="swing",
                                             style={"background": CARD_BG, "fontSize": "0.83rem"}), width=6),
                    ], className="mb-2"),

                    dbc.Row([
                        dbc.Col(dbc.Input(id="calc-price", type="number", placeholder="Entry price ₹",
                                          style={"background": "#0d0d0d", "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb"}), width=6),
                        dbc.Col(dbc.Input(id="calc-confidence", type="number", value=1.0, min=0.1, max=1.0, step=0.1,
                                          placeholder="Confidence (0-1)",
                                          style={"background": "#0d0d0d", "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb"}), width=6),
                    ], className="mb-2"),

                    dbc.Button("Calculate", id="calc-button", n_clicks=0,
                               color="secondary", style={"width": "100%"}),

                    html.Div(id="calc-result", style={"marginTop": "16px"}),
                ], style={"padding": "16px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                      "marginBottom": "12px"}),

            # Capital Summary Card
            dbc.Card([
                dbc.CardHeader("Capital Deployment Summary",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody([html.Div(id="cap-summary")],
                             style={"padding": "12px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=7),
    ]),

    dcc.Interval(id="cap-refresh", interval=120_000, n_intervals=0),
])


def _ratio_slider(label: str, key: str, default: int):
    return html.Div([
        html.Div(f"{label}: {default}%", id=f"{key}-label",
                 style={"fontSize": "0.75rem", "color": "#9ca3af", "marginBottom": "2px"}),
        dcc.Slider(id=key, min=0, max=100, step=5, value=default,
                   marks={0: "0%", 25: "", 50: "50%", 75: "", 100: "100%"},
                   tooltip={"always_visible": False, "placement": "bottom"}),
    ], style={"marginBottom": "10px"})


@callback(
    [Output(f"tf-{tf}-label", "children") for tf in ["intraday", "swing", "medium", "longterm"]],
    Output("tf-ratio-sum", "children"),
    [Input(f"tf-{tf}", "value") for tf in ["intraday", "swing", "medium", "longterm"]],
)
def update_tf_labels(intraday, swing, medium, longterm):
    labels = [
        f"Intraday: {intraday}%",
        f"Swing: {swing}%",
        f"Medium: {medium}%",
        f"Long-term: {longterm}%",
    ]
    total = intraday + swing + medium + longterm
    color = BULL_COLOR if total == 100 else BEAR_COLOR
    sum_msg = html.Span(f"Total: {total}% ", style={"color": color, "fontWeight": "600"})
    if total != 100:
        sum_msg = html.Div([sum_msg, html.Span("(must equal 100%)", style={"color": BEAR_COLOR})])
    return labels + [sum_msg]


@callback(
    Output("cap-save-result", "children"),
    Input("cap-save", "n_clicks"),
    State("cap-total", "value"),
    State("cap-reserve-pct", "value"),
    State("cap-max-per-stock", "value"),
    State("cap-max-sector-pct", "value"),
    [State(f"tf-{tf}", "value") for tf in ["intraday", "swing", "medium", "longterm"]],
    prevent_initial_call=True,
)
def save_capital_config(n, total, reserve, max_stock, max_sector, *tf_values):
    intraday, swing, medium, longterm = tf_values
    if intraday + swing + medium + longterm != 100:
        return dbc.Alert("Timeframe ratios must sum to 100%", color="danger", style={"fontSize": "0.82rem"})

    from alphalens.core.capital.allocator import CapitalAllocator
    from alphalens.core.database import set_config
    import json

    set_config("total_capital", total)
    set_config("reserve_cash_pct", reserve / 100)
    set_config("max_capital_per_stock", max_stock)
    set_config("max_sector_exposure_pct", max_sector / 100)
    set_config("timeframe_allocation_ratios", json.dumps({
        "intraday":  intraday / 100,
        "swing":     swing / 100,
        "medium":    medium / 100,
        "long_term": longterm / 100,
    }))

    allocator = CapitalAllocator()
    allocator.reload()

    return dbc.Alert("✓ Capital configuration saved", color="success", style={"fontSize": "0.82rem"})


@callback(
    Output("calc-symbol", "options"),
    Input("cap-refresh", "n_intervals"),
)
def load_symbols(_):
    from alphalens.core.ingestion.universe import get_all_symbols
    symbols = get_all_symbols()
    return [{"label": s, "value": s} for s in symbols]


@callback(
    Output("calc-result", "children"),
    Input("calc-button", "n_clicks"),
    State("calc-symbol", "value"),
    State("calc-tf", "value"),
    State("calc-price", "value"),
    State("calc-confidence", "value"),
    prevent_initial_call=True,
)
def calculate_position(n, symbol, tf, price, conf):
    if not all([symbol, tf, price]):
        return dbc.Alert("Please fill all fields", color="warning", style={"fontSize": "0.82rem"})

    from alphalens.core.capital.allocator import CapitalAllocator
    allocator = CapitalAllocator()
    result = allocator.calculate_position_size("UNKNOWN", tf, float(price), float(conf or 1.0))

    if result.get("qty", 0) == 0:
        return dbc.Alert(f"Error: {result.get('error', 'Unable to calculate')}", color="danger",
                         style={"fontSize": "0.82rem"})

    return html.Div([
        html.Div([
            html.Span("Shares: ", style={"color": "#6b7280"}),
            html.Span(f"{result['qty']:,}", style={"color": BULL_COLOR, "fontWeight": "700",
                                                     "fontSize": "1.1rem", "fontFamily": "JetBrains Mono"}),
        ], style={"marginBottom": "8px"}),
        html.Div([
            html.Span("Value: ", style={"color": "#6b7280"}),
            html.Span(format_inr(result['value_inr']), style={"fontFamily": "JetBrains Mono", "fontWeight": "600"}),
        ]),
        html.Div([
            html.Span("Capital Used: ", style={"color": "#6b7280"}),
            html.Span(f"{result['capital_used_pct']:.1f}%", style={"fontFamily": "JetBrains Mono"}),
        ]),
        html.Div([
            html.Span("Fees + Slippage: ", style={"color": "#6b7280"}),
            html.Span(format_inr(result['fees_total']), style={"fontFamily": "JetBrains Mono", "color": NEUTRAL_COLOR}),
        ]),
        html.Div([
            html.Span("Effective Entry: ", style={"color": "#6b7280"}),
            html.Span(f"₹{result['effective_entry']:,.2f}", style={"fontFamily": "JetBrains Mono"}),
        ]),
    ], style={"fontSize": "0.82rem"})


@callback(
    Output("cap-summary", "children"),
    Input("cap-refresh", "n_intervals"),
)
def render_summary(_):
    from alphalens.core.capital.allocator import CapitalAllocator
    allocator = CapitalAllocator()
    summary = allocator.get_capital_summary()

    return html.Div([
        _summary_row("Total Capital", format_inr(summary['total_capital']), NEUTRAL_COLOR),
        _summary_row("Reserve Cash", format_inr(summary['reserve_cash']), NEUTRAL_COLOR),
        _summary_row("Deployed", format_inr(summary['deployed_capital']), GOLD_COLOR),
        _summary_row("Available", format_inr(summary['cash_available']), BULL_COLOR),
        _summary_row("Utilization", f"{summary['utilization_pct']:.1f}%",
                     BULL_COLOR if summary['utilization_pct'] < 90 else BEAR_COLOR),
    ])


def _summary_row(label: str, value: str, color: str):
    return html.Div([
        html.Span(label, style={"fontSize": "0.75rem", "color": "#6b7280", "flex": "1"}),
        html.Span(value, style={"fontFamily": "JetBrains Mono", "fontSize": "0.82rem",
                                 "color": color, "fontWeight": "600"}),
    ], style={"display": "flex", "padding": "5px 0", "borderBottom": f"1px solid {BORDER_COLOR}"})
