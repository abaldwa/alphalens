"""
alphalens/dashboard/pages/trigger_validation.py — Trigger Price Validation UI

Shows all eligible trigger candidates.
User can validate each one (re-check strategy conditions).
Displays pass/fail per rule.
User confirms buy after validation.
"""

import dash, dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html, ctx
dash.register_page(__name__, path="/trigger-validation", name="Trigger Validation", order=12)

from alphalens.dashboard.components.shared import *


layout = page_layout([
    section_header("Trigger Price Validation", "Review candidates · validate strategy · confirm buy"),

    dbc.Row([
        dbc.Col(dcc.Dropdown(id="trig-status-filter",
                             options=[{"label": l, "value": v} for l, v in [
                                 ("All", "all"), ("Pending", "pending"), ("Eligible", "eligible"),
                                 ("Bought", "bought"), ("Expired", "expired"), ("Cancelled", "cancelled")]],
                             value="eligible", clearable=False,
                             style={"background": CARD_BG, "fontSize": "0.83rem"}), width=3),
        dbc.Col(dcc.Dropdown(id="trig-tf-filter",
                             options=[{"label": l, "value": v} for l, v in [
                                 ("All TF", "all"), ("Intraday", "intraday"), ("Swing", "swing"),
                                 ("Medium", "medium"), ("Long-term", "long_term")]],
                             value="all", clearable=False,
                             style={"background": CARD_BG, "fontSize": "0.83rem"}), width=2),
    ], className="mb-3 g-2"),

    html.Div(id="trigger-table"),

    # Modal for validation detail
    dbc.Modal([
        dbc.ModalHeader(html.Div(id="trig-modal-title")),
        dbc.ModalBody(html.Div(id="trig-modal-body")),
        dbc.ModalFooter([
            dbc.Button("Close", id="trig-modal-close", color="secondary", size="sm"),
            dbc.Button("Confirm Buy", id="trig-modal-buy", color="warning", size="sm",
                       style={"fontWeight": "700"}),
        ]),
    ], id="trig-modal", size="xl", scrollable=True,
       style={"background": "#111827"}),

    dcc.Store(id="trig-selected-id"),
    dcc.Interval(id="trig-refresh", interval=60_000, n_intervals=0),
])


@callback(
    Output("trigger-table", "children"),
    Input("trig-status-filter", "value"),
    Input("trig-tf-filter", "value"),
    Input("trig-refresh", "n_intervals"),
)
def render_triggers(status, tf, _):
    from alphalens.core.signals.trigger_manager import TriggerManager
    tm = TriggerManager()
    triggers = tm.get_triggers(status=status, timeframe=tf if tf != "all" else None, limit=100)

    if not triggers:
        return dbc.Alert(f"No {status} triggers found.", color="secondary",
                         style={"marginTop": "12px", "fontSize": "0.82rem"})

    rows = []
    for t in triggers:
        status_color = {
            "pending": NEUTRAL_COLOR, "eligible": BULL_COLOR,
            "bought": GOLD_COLOR, "expired": "#6b7280", "cancelled": BEAR_COLOR
        }.get(t["status"], NEUTRAL_COLOR)

        rows.append(html.Tr([
            html.Td(html.A(t["symbol"], href=f"/chart?symbol={t['symbol']}",
                           style={"color": GOLD_COLOR, "textDecoration": "none",
                                  "fontFamily": "JetBrains Mono", "fontWeight": "600"})),
            html.Td(t["strategy_id"], style={"fontSize": "0.72rem", "color": "#6b7280"}),
            html.Td(t["timeframe"].upper().replace("_","-"), style={"fontFamily": "JetBrains Mono",
                                                                      "fontSize": "0.72rem", "color": NEUTRAL_COLOR}),
            html.Td(str(t["trigger_date"])[:10], style={"fontSize": "0.75rem", "color": "#9ca3af"}),
            html.Td(f"₹{t['trigger_price']:,.2f}" if t['trigger_price'] else "–",
                    style={"fontFamily": "JetBrains Mono"}),
            html.Td(f"₹{t['buy_below_price']:,.2f}" if t['buy_below_price'] else "–",
                    style={"fontFamily": "JetBrains Mono", "color": BULL_COLOR}),
            html.Td(f"₹{t['current_price']:,.2f}" if t['current_price'] else "–",
                    style={"fontFamily": "JetBrains Mono"}),
            html.Td(f"{t['distance_pct']:+.1f}%" if t['distance_pct'] else "–",
                    style={"fontFamily": "JetBrains Mono",
                           "color": pnl_color(t['distance_pct']) if t.get('distance_pct') else NEUTRAL_COLOR}),
            html.Td(str(t["days_old"]) + "d", style={"fontSize": "0.75rem", "color": "#9ca3af"}),
            html.Td(html.Span(t["status"].upper(),
                              style={"color": status_color, "fontSize": "0.72rem",
                                     "fontFamily": "JetBrains Mono", "fontWeight": "600"})),
            html.Td(dbc.Button("Validate", id={"type": "trig-validate", "index": t["trigger_id"]},
                               n_clicks=0, size="sm", color="secondary", outline=True,
                               disabled=(t["status"] not in ["pending", "eligible"]),
                               style={"fontSize": "0.7rem"})),
        ]))

    return dbc.Table([
        html.Thead(html.Tr([html.Th(h, style={"color": "#6b7280", "fontSize": "0.7rem",
                                               "fontFamily": "JetBrains Mono", "textTransform": "uppercase"})
                            for h in ["Symbol", "Strategy", "TF", "Trig Date", "Trig ₹", "Buy-Below ₹",
                                      "Current ₹", "Distance", "Age", "Status", "Action"]])),
        html.Tbody(rows),
    ], bordered=False, hover=True, responsive=True,
       style={"fontSize": "0.82rem", "background": CARD_BG,
              "border": f"1px solid {BORDER_COLOR}", "borderRadius": "6px"})


@callback(
    Output("trig-modal", "is_open"),
    Output("trig-modal-title", "children"),
    Output("trig-modal-body", "children"),
    Output("trig-selected-id", "data"),
    Input({"type": "trig-validate", "index": dash.ALL}, "n_clicks"),
    Input("trig-modal-close", "n_clicks"),
    Input("trig-modal-buy", "n_clicks"),
    State({"type": "trig-validate", "index": dash.ALL}, "id"),
    State("trig-selected-id", "data"),
    prevent_initial_call=True,
)
def handle_trigger_modal(validate_clicks, close_clicks, buy_clicks, ids, selected_id):
    trigger_btn = ctx.triggered_id

    # Close modal
    if trigger_btn == "trig-modal-close":
        return False, "", html.Div(), None

    # Confirm buy
    if trigger_btn == "trig-modal-buy":
        if selected_id:
            from alphalens.core.signals.trigger_manager import TriggerManager
            tm = TriggerManager()
            result = tm.confirm_buy(selected_id)
            if result.get("success"):
                return False, "", dbc.Alert(f"✓ Position created: Holding #{result['holding_id']}",
                                            color="success"), None
            else:
                return True, "Error", dbc.Alert(f"Error: {result.get('error')}", color="danger"), selected_id
        return False, "", html.Div(), None

    # Open validation modal
    if isinstance(trigger_btn, dict) and trigger_btn.get("type") == "trig-validate":
        trigger_id = trigger_btn["index"]
        from alphalens.core.signals.trigger_manager import TriggerManager
        tm = TriggerManager()
        validation = tm.validate_trigger(trigger_id)

        if not validation.get("valid"):
            body = _build_validation_body(validation)
            return True, f"Validation: {validation.get('symbol')} [{validation.get('timeframe')}]", body, trigger_id

        body = _build_validation_body(validation)
        return True, f"Validation: {validation.get('symbol')} [{validation.get('timeframe')}]", body, trigger_id

    return False, "", html.Div(), None


def _build_validation_body(validation: dict):
    """Build validation detail panel."""
    symbol      = validation.get("symbol", "–")
    strategy_id = validation.get("strategy_id", "–")
    valid       = validation.get("valid", False)
    rule_checks = validation.get("rule_checks", [])
    capital_fit = validation.get("capital_fit", False)
    sector_fit  = validation.get("sector_fit", False)
    pos_size    = validation.get("position_size", {})
    sector_check = validation.get("sector_check", {})

    # Overall status badge
    overall_badge = html.Div([
        html.Span("Overall: ", style={"color": "#9ca3af"}),
        html.Span("✓ VALID" if valid else "✗ INVALID",
                  style={"color": BULL_COLOR if valid else BEAR_COLOR,
                         "fontWeight": "700", "fontSize": "1.1rem"}),
    ], style={"marginBottom": "12px"})

    # Rule-by-rule checks
    rule_rows = []
    for r in rule_checks:
        icon = "✓" if r.get("passed") else "✗"
        color = BULL_COLOR if r.get("passed") else BEAR_COLOR
        rule_rows.append(html.Div([
            html.Span(f"Rule {r.get('rule_num', '?')}: ", style={"fontFamily": "JetBrains Mono",
                                                                   "fontSize": "0.75rem", "color": "#6b7280"}),
            html.Span(icon, style={"color": color, "fontWeight": "700", "marginRight": "6px"}),
            html.Span(r.get("reason", ""), style={"fontSize": "0.78rem", "color": "#e5e7eb"}),
        ], style={"padding": "4px 0", "borderBottom": f"1px solid {BORDER_COLOR}"}))

    rule_panel = html.Div([
        html.Div("Strategy Rule Checks", style={"fontWeight": "700", "marginBottom": "6px",
                                                  "color": GOLD_COLOR}),
        *rule_rows,
    ], style={"marginBottom": "16px"})

    # Capital check
    capital_panel = html.Div([
        html.Div("Capital & Position Size", style={"fontWeight": "700", "marginBottom": "6px",
                                                     "color": GOLD_COLOR}),
        html.Div([
            html.Span("Shares: ", style={"color": "#6b7280"}),
            html.Span(f"{pos_size.get('qty', 0):,}", style={"fontFamily": "JetBrains Mono",
                                                              "color": BULL_COLOR, "fontWeight": "600"}),
        ]),
        html.Div([
            html.Span("Value: ", style={"color": "#6b7280"}),
            html.Span(format_inr(pos_size.get('value_inr', 0)), style={"fontFamily": "JetBrains Mono"}),
        ]),
        html.Div([
            html.Span("Capital Fit: ", style={"color": "#6b7280"}),
            html.Span("✓ OK" if capital_fit else "✗ Insufficient",
                      style={"color": BULL_COLOR if capital_fit else BEAR_COLOR, "fontWeight": "600"}),
        ]),
    ], style={"marginBottom": "16px"})

    # Sector check
    sector_panel = html.Div([
        html.Div("Sector Exposure", style={"fontWeight": "700", "marginBottom": "6px",
                                            "color": GOLD_COLOR}),
        html.Div([
            html.Span(f"Sector: {sector_check.get('sector', '–')} ", style={"color": "#9ca3af"}),
        ]),
        html.Div([
            html.Span("Current: ", style={"color": "#6b7280"}),
            html.Span(f"{sector_check.get('current_exposure_pct', 0):.1f}%", style={"fontFamily": "JetBrains Mono"}),
        ]),
        html.Div([
            html.Span("After Buy: ", style={"color": "#6b7280"}),
            html.Span(f"{sector_check.get('new_exposure_pct', 0):.1f}%", style={"fontFamily": "JetBrains Mono"}),
        ]),
        html.Div([
            html.Span("Limit: ", style={"color": "#6b7280"}),
            html.Span(f"{sector_check.get('limit_pct', 0):.1f}%", style={"fontFamily": "JetBrains Mono"}),
        ]),
        html.Div([
            html.Span("Sector Fit: ", style={"color": "#6b7280"}),
            html.Span("✓ OK" if sector_fit else "✗ Exceeds limit",
                      style={"color": BULL_COLOR if sector_fit else BEAR_COLOR, "fontWeight": "600"}),
        ]),
    ])

    return html.Div([
        overall_badge,
        rule_panel,
        capital_panel,
        sector_panel,
    ])
