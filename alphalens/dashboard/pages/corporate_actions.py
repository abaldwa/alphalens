"""
alphalens/dashboard/pages/corporate_actions.py — Corporate Actions UI

Features:
  - List all corp actions (splits, bonus, dividends)
  - Manual entry form
  - Impact summary (positions, triggers affected)
  - Apply action button
  - Processed vs pending status
"""

import dash, dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html
from datetime import date
dash.register_page(__name__, path="/corporate-actions", name="Corp Actions", order=13)

from alphalens.dashboard.components.shared import *


layout = page_layout([
    section_header("Corporate Actions", "Splits · Bonus · Dividends · Price adjustments"),

    dbc.Row([
        # ── Manual Entry Form ─────────────────────────────────────────────
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Record Corporate Action",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col(dcc.Dropdown(id="ca-symbol", placeholder="Symbol…",
                                             style={"background": CARD_BG, "fontSize": "0.83rem"}), width=6),
                        dbc.Col(dcc.Dropdown(id="ca-type",
                                             options=[{"label": l, "value": v} for l, v in [
                                                 ("Stock Split", "split"), ("Bonus Issue", "bonus"),
                                                 ("Dividend", "dividend")]],
                                             placeholder="Type…",
                                             style={"background": CARD_BG, "fontSize": "0.83rem"}), width=6),
                    ], className="mb-2"),

                    dbc.Row([
                        dbc.Col(dbc.Input(id="ca-ex-date", type="date",
                                          style={"background": "#0d0d0d", "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb"}), width=6),
                        dbc.Col(dbc.Input(id="ca-ratio", type="number", placeholder="Ratio (e.g. 1.0 for 1:1)",
                                          style={"background": "#0d0d0d", "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb"}), width=6),
                    ], className="mb-2"),

                    dbc.Row([
                        dbc.Col(dbc.Input(id="ca-cash", type="number", placeholder="Cash amount (for dividend)",
                                          style={"background": "#0d0d0d", "border": f"1px solid {BORDER_COLOR}",
                                                 "color": "#e5e7eb"}), width=12),
                    ], className="mb-3"),

                    dbc.Button("Record Action", id="ca-submit", n_clicks=0,
                               color="warning", style={"width": "100%", "fontWeight": "700"}),
                    html.Div(id="ca-result", style={"marginTop": "8px"}),
                ], style={"padding": "16px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=5),

        # ── Actions List ──────────────────────────────────────────────────
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.Span("Corporate Actions Registry", style={"flex": "1"}),
                    dbc.Switch(id="ca-show-processed", label="Show processed", value=False,
                               labelStyle={"fontSize": "0.75rem", "color": "#9ca3af"}),
                ], style={"background": "#111827", "color": "#9ca3af",
                          "fontSize": "0.75rem", "textTransform": "uppercase",
                          "display": "flex", "alignItems": "center"}),
                dbc.CardBody([html.Div(id="ca-list", style={"maxHeight": "400px", "overflowY": "auto"})],
                             style={"padding": "8px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=7),
    ]),

    dcc.Interval(id="ca-refresh", interval=120_000, n_intervals=0),

    # Modal for impact summary
    dbc.Modal([
        dbc.ModalHeader(html.Div(id="ca-modal-title")),
        dbc.ModalBody(html.Div(id="ca-modal-body")),
        dbc.ModalFooter([
            dbc.Button("Close", id="ca-modal-close", color="secondary", size="sm"),
            dbc.Button("Apply Action", id="ca-modal-apply", color="danger", size="sm",
                       style={"fontWeight": "700"}),
        ]),
    ], id="ca-modal", size="lg", scrollable=True),

    dcc.Store(id="ca-selected-id"),
])


@callback(
    Output("ca-symbol", "options"),
    Input("ca-refresh", "n_intervals"),
)
def load_symbols(_):
    from alphalens.core.ingestion.universe import get_all_symbols
    symbols = get_all_symbols()
    return [{"label": s, "value": s} for s in symbols]


@callback(
    Output("ca-result", "children"),
    Input("ca-submit", "n_clicks"),
    State("ca-symbol", "value"),
    State("ca-type", "value"),
    State("ca-ex-date", "value"),
    State("ca-ratio", "value"),
    State("ca-cash", "value"),
    prevent_initial_call=True,
)
def record_action(n, symbol, action_type, ex_date, ratio, cash):
    if not all([symbol, action_type, ex_date]):
        return dbc.Alert("Please fill Symbol, Type, and Ex-Date", color="danger",
                         style={"fontSize": "0.82rem"})

    if action_type in ("split", "bonus") and not ratio:
        return dbc.Alert(f"{action_type.capitalize()} requires ratio", color="danger",
                         style={"fontSize": "0.82rem"})

    from alphalens.core.corporate_actions.adjuster import CorporateActionAdjuster
    from datetime import datetime
    adjuster = CorporateActionAdjuster()

    ex_date_obj = datetime.strptime(ex_date, "%Y-%m-%d").date()

    action_id = adjuster.record_action(
        symbol       = symbol,
        action_type  = action_type,
        ex_date      = ex_date_obj,
        ratio        = float(ratio) if ratio else None,
        cash_amount  = float(cash) if cash else None,
        source       = "manual",
    )

    return dbc.Alert(f"✓ Corporate action recorded: {action_id}", color="success",
                     style={"fontSize": "0.82rem"})


@callback(
    Output("ca-list", "children"),
    Input("ca-show-processed", "value"),
    Input("ca-refresh", "n_intervals"),
)
def render_actions(show_processed, _):
    from alphalens.core.corporate_actions.adjuster import CorporateActionAdjuster
    adjuster = CorporateActionAdjuster()
    actions = adjuster.get_actions(processed=None if show_processed else False)

    if not actions:
        return html.Div("No corporate actions recorded yet.",
                        style={"color": "#6b7280", "padding": "12px", "textAlign": "center"})

    rows = []
    for a in actions:
        proc_color = GOLD_COLOR if a["processed"] else NEUTRAL_COLOR
        rows.append(html.Div([
            html.Div([
                html.Span(a["symbol"], style={"fontFamily": "JetBrains Mono", "fontWeight": "600",
                                               "color": GOLD_COLOR, "marginRight": "8px"}),
                html.Span(a["action_type"].upper(), style={"fontSize": "0.72rem",
                                                             "color": "#9ca3af", "marginRight": "8px"}),
                html.Span(f"ex: {str(a['ex_date'])[:10]}", style={"fontSize": "0.72rem",
                                                                    "color": "#6b7280"}),
            ]),
            html.Div([
                html.Span(f"Ratio: {a['ratio']}" if a['ratio'] else "",
                          style={"fontSize": "0.72rem", "color": "#9ca3af", "marginRight": "8px"}),
                html.Span(f"Cash: ₹{a['cash_amount']}" if a['cash_amount'] else "",
                          style={"fontSize": "0.72rem", "color": "#9ca3af", "marginRight": "8px"}),
                html.Span("✓ PROCESSED" if a["processed"] else "PENDING",
                          style={"fontSize": "0.7rem", "color": proc_color,
                                 "fontFamily": "JetBrains Mono", "fontWeight": "600"}),
            ], style={"fontSize": "0.75rem", "color": "#6b7280"}),
            html.Div([
                dbc.Button("View Impact", id={"type": "ca-impact", "index": a["action_id"]},
                           size="sm", color="secondary", outline=True, n_clicks=0,
                           style={"fontSize": "0.7rem", "marginRight": "4px"}),
                dbc.Button("Apply" if not a["processed"] else "Re-Apply",
                           id={"type": "ca-apply-inline", "index": a["action_id"]},
                           size="sm", color="danger" if not a["processed"] else "secondary",
                           outline=True, n_clicks=0, disabled=a["processed"],
                           style={"fontSize": "0.7rem"}),
            ], style={"marginTop": "4px"}),
        ], style={"padding": "10px", "borderBottom": f"1px solid {BORDER_COLOR}"}))

    return html.Div(rows)


@callback(
    Output("ca-modal", "is_open"),
    Output("ca-modal-title", "children"),
    Output("ca-modal-body", "children"),
    Output("ca-selected-id", "data"),
    Input({"type": "ca-impact", "index": dash.ALL}, "n_clicks"),
    Input({"type": "ca-apply-inline", "index": dash.ALL}, "n_clicks"),
    Input("ca-modal-close", "n_clicks"),
    Input("ca-modal-apply", "n_clicks"),
    State({"type": "ca-impact", "index": dash.ALL}, "id"),
    State({"type": "ca-apply-inline", "index": dash.ALL}, "id"),
    State("ca-selected-id", "data"),
    prevent_initial_call=True,
)
def handle_ca_modal(impact_clicks, apply_clicks, close_clicks, modal_apply, impact_ids, apply_ids, selected_id):
    trigger_btn = ctx.triggered_id

    if trigger_btn == "ca-modal-close":
        return False, "", html.Div(), None

    if trigger_btn == "ca-modal-apply":
        if selected_id:
            from alphalens.core.corporate_actions.adjuster import CorporateActionAdjuster
            adjuster = CorporateActionAdjuster()
            result = adjuster.apply_action(selected_id)
            if result.get("success"):
                return False, "", dbc.Alert(f"✓ Action applied: {selected_id}", color="success"), None
            else:
                return True, "Error", dbc.Alert(f"Error: {result.get('error')}", color="danger"), selected_id
        return False, "", html.Div(), None

    if isinstance(trigger_btn, dict):
        action_id = trigger_btn["index"]

        if trigger_btn.get("type") == "ca-apply-inline":
            # Direct apply without modal
            from alphalens.core.corporate_actions.adjuster import CorporateActionAdjuster
            adjuster = CorporateActionAdjuster()
            result = adjuster.apply_action(action_id)
            return False, "", html.Div(), None

        if trigger_btn.get("type") == "ca-impact":
            # Show impact modal
            from alphalens.core.corporate_actions.adjuster import CorporateActionAdjuster
            adjuster = CorporateActionAdjuster()
            impact = adjuster.get_impact_summary(action_id)

            if "error" in impact:
                return True, "Error", dbc.Alert(impact["error"], color="danger"), None

            body = _build_impact_body(impact)
            return True, f"Impact Summary: {impact['action']['symbol']}", body, action_id

    return False, "", html.Div(), None


def _build_impact_body(impact: dict):
    """Build impact summary display."""
    action = impact.get("action", {})
    positions = impact.get("affected_positions", [])
    triggers = impact.get("affected_triggers", [])
    price_rows = impact.get("price_row_count", 0)

    summary = html.Div([
        html.Div(f"{action['action_type'].upper()}: {action['symbol']}",
                 style={"fontWeight": "700", "fontSize": "1.1rem", "marginBottom": "8px"}),
        html.Div(f"Ex-Date: {action['ex_date']}", style={"fontSize": "0.82rem", "color": "#9ca3af"}),
        html.Div(f"Adjustment Factor: {action['adj_factor']:.6f}",
                 style={"fontSize": "0.82rem", "color": "#9ca3af", "marginBottom": "12px"}),
        html.Div(f"Historical price rows to adjust: {price_rows:,}",
                 style={"fontSize": "0.82rem", "color": GOLD_COLOR}),
    ])

    pos_section = html.Div([
        html.Div(f"Affected Positions: {len(positions)}", style={"fontWeight": "700",
                                                                   "marginTop": "12px", "marginBottom": "6px"}),
        *[html.Div([
            html.Span(f"Holding #{p['holding_id']}: ", style={"fontFamily": "JetBrains Mono"}),
            html.Span(f"{p['qty']} → {p['new_qty']} shares | ", style={"color": BULL_COLOR}),
            html.Span(f"₹{p['avg_cost']:.2f} → ₹{p['new_cost']:.2f}", style={"color": NEUTRAL_COLOR}),
        ], style={"fontSize": "0.78rem", "padding": "3px 0"}) for p in positions]
    ]) if positions else html.Div()

    trig_section = html.Div([
        html.Div(f"Affected Triggers: {len(triggers)}", style={"fontWeight": "700",
                                                                 "marginTop": "12px", "marginBottom": "6px"}),
        *[html.Div([
            html.Span(f"Trigger #{t['trigger_id']}: ", style={"fontFamily": "JetBrains Mono"}),
            html.Span(f"₹{t['trigger_price']:.2f} → ₹{t['new_trigger_price']:.2f}",
                      style={"color": BULL_COLOR}),
        ], style={"fontSize": "0.78rem", "padding": "3px 0"}) for t in triggers]
    ]) if triggers else html.Div()

    return html.Div([summary, pos_section, trig_section])
