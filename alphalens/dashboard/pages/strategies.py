"""alphalens/dashboard/pages/strategies.py — Page 5: Strategy Library"""
import dash, dash_bootstrap_components as dbc, json
from dash import Input, Output, State, callback, dcc, html
dash.register_page(__name__, path="/strategies", name="Strategies", order=5)
from alphalens.dashboard.components.shared import *

layout = page_layout([
    section_header("Strategy Library", "Seeded + ML-discovered strategies · full rule definitions"),
    dbc.Row([
        dbc.Col(dbc.Switch(id="strat-active-only", label="Active only", value=True,
                           labelStyle={"color": "#9ca3af", "fontSize": "0.82rem"}), width=2),
        dbc.Col(dcc.Dropdown(id="strat-type-filter",
            options=[{"label": t, "value": t} for t in
                     ["all", "trend_following", "mean_reversion", "breakout",
                      "momentum", "volatility_breakout", "value_momentum", "macro_rotation"]],
            value="all", clearable=False,
            style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}", "fontSize": "0.83rem"}
        ), width=3),
    ], className="mb-3 g-2"),
    html.Div(id="strategy-grid"),
    dbc.Modal([
        dbc.ModalHeader(html.Span(id="strat-modal-title",
                                   style={"fontFamily": "'Playfair Display',serif", "fontSize": "1.1rem"})),
        dbc.ModalBody(html.Div(id="strat-modal-body")),
        dbc.ModalFooter(dbc.Button("Close", id="strat-modal-close", n_clicks=0, color="secondary", size="sm")),
    ], id="strat-modal", size="xl", scrollable=True,
       style={"background": "#111827"}),
    dcc.Store(id="strat-selected-id"),
])

@callback(Output("strategy-grid", "children"),
          Input("strat-active-only", "value"), Input("strat-type-filter", "value"))
def render_strategies(active_only, type_filter):
    from alphalens.core.strategy.library import get_all_strategies
    strats = get_all_strategies(active_only=active_only)
    if type_filter != "all":
        strats = [s for s in strats if s.get("type") == type_filter]

    cards = []
    for s in strats:
        cycles_raw = s.get("best_cycles", [])
        cycles = json.loads(cycles_raw) if isinstance(cycles_raw, str) else (cycles_raw or [])
        tfs_raw = s.get("timeframes", [])
        tfs = json.loads(tfs_raw) if isinstance(tfs_raw, str) else (tfs_raw or [])
        sharpe   = s.get("sharpe_ratio")
        win_rate = s.get("win_rate")
        discovered = s.get("discovered_by", "seeded")
        is_genetic = discovered == "genetic"

        cards.append(dbc.Col(dbc.Card([
            dbc.CardBody([
                html.Div([
                    html.Span(s.get("name", ""), style={"fontWeight": "700", "fontSize": "0.88rem",
                                                          "color": "#f9fafb", "flex": "1"}),
                    html.Span("🧬" if is_genetic else "📖",
                              title="Genetic" if is_genetic else "Seeded",
                              style={"fontSize": "0.85rem"}),
                ], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}),
                html.Div(s.get("description", "")[:120] + "…",
                         style={"fontSize": "0.75rem", "color": "#9ca3af", "marginBottom": "8px",
                                "lineHeight": "1.5"}),
                html.Div([
                    html.Span(c.upper(), style={"color": BULL_COLOR if c == "bull" else (BEAR_COLOR if c == "bear" else NEUTRAL_COLOR),
                                                 "fontSize": "0.65rem", "fontFamily": "JetBrains Mono",
                                                 "background": f"{BULL_COLOR if c == 'bull' else (BEAR_COLOR if c == 'bear' else NEUTRAL_COLOR)}18",
                                                 "padding": "1px 6px", "borderRadius": "2px"})
                    for c in cycles
                ] + [html.Span(" | ", style={"color": "#374151"})] + [
                    html.Span(t.replace("_","-").upper(),
                              style={"color": GOLD_COLOR, "fontSize": "0.65rem",
                                     "fontFamily": "JetBrains Mono",
                                     "background": f"{GOLD_COLOR}18",
                                     "padding": "1px 6px", "borderRadius": "2px"})
                    for t in tfs
                ], style={"display": "flex", "gap": "4px", "flexWrap": "wrap", "marginBottom": "8px"}),
                html.Div([
                    html.Span(f"Sharpe: {sharpe:.2f}" if sharpe else "Not backtested",
                              style={"fontFamily": "JetBrains Mono", "fontSize": "0.72rem",
                                     "color": BULL_COLOR if sharpe and sharpe >= 1.0 else (BEAR_COLOR if sharpe else "#6b7280")}),
                    html.Span(f"  Win: {win_rate*100:.1f}%" if win_rate else "",
                              style={"fontFamily": "JetBrains Mono", "fontSize": "0.72rem", "color": "#9ca3af"}),
                ], style={"marginBottom": "8px"}),
                dbc.Button("View Details", id={"type": "strat-view", "index": s["strategy_id"]},
                           n_clicks=0, size="sm", color="secondary", outline=True,
                           style={"fontSize": "0.72rem", "width": "100%"}),
            ], style={"padding": "12px"}),
        ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                  "height": "100%"}), width=4, className="mb-3"))

    return dbc.Row(cards) if cards else html.Div("No strategies found.",
                                                  style={"color": "#6b7280", "padding": "20px"})

@callback(Output("strat-modal", "is_open"), Output("strat-modal-title", "children"),
          Output("strat-modal-body", "children"),
          Input({"type": "strat-view", "index": dash.ALL}, "n_clicks"),
          Input("strat-modal-close", "n_clicks"),
          State({"type": "strat-view", "index": dash.ALL}, "id"),
          prevent_initial_call=True)
def show_strategy_modal(n_clicks_list, close_clicks, ids):
    ctx_cb = dash.callback_context
    if not ctx_cb.triggered:
        return False, "", html.Div()
    trigger = ctx_cb.triggered[0]["prop_id"]
    if "strat-modal-close" in trigger:
        return False, "", html.Div()
    if not any(n_clicks_list):
        return False, "", html.Div()

    import json as _json
    tid = _json.loads(trigger.split(".")[0])["index"]
    from alphalens.core.strategy.library import get_strategy
    s = get_strategy(tid)
    if not s:
        return False, "", html.Div()

    def rule_block(title, rules):
        if not rules:
            return html.Div()
        if isinstance(rules, str):
            try:
                rules = _json.loads(rules)
            except Exception:
                return html.Pre(rules, style={"color": "#9ca3af", "fontSize": "0.78rem"})
        return html.Div([
            html.Div(title, style={"color": GOLD_COLOR, "fontFamily": "JetBrains Mono",
                                    "fontSize": "0.72rem", "textTransform": "uppercase",
                                    "letterSpacing": "0.1em", "marginBottom": "4px",
                                    "marginTop": "12px"}),
            html.Pre(_json.dumps(rules, indent=2),
                     style={"background": "#0d0d0d", "color": "#93c5fd", "padding": "10px",
                            "borderRadius": "4px", "fontSize": "0.78rem",
                            "overflowX": "auto", "border": f"1px solid {BORDER_COLOR}"}),
        ])

    body = html.Div([
        html.P(s.get("description", ""), style={"color": "#9ca3af", "fontSize": "0.85rem"}),
        rule_block("Entry Rules",     s.get("entry_rules")),
        rule_block("Exit Rules",      s.get("exit_rules")),
        rule_block("Stop-Loss Rules", s.get("stoploss_rules")),
        rule_block("Parameters",      s.get("parameters")),
    ])
    return True, s.get("name", ""), body
