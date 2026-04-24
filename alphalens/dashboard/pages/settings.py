"""alphalens/dashboard/pages/settings.py — Page 10: Settings & Configuration"""
import dash, dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, html
dash.register_page(__name__, path="/settings", name="Settings", order=10)
from alphalens.dashboard.components.shared import *

layout = page_layout([
    section_header("Settings", "Portfolio slots · capital allocation · signal thresholds · notifications"),
    dbc.Row([
        # ── Portfolio Configuration ────────────────────────────────────
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Portfolio Configuration",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody([
                    html.Div("Slot Limits", style={"fontWeight": "700", "marginBottom": "8px",
                                                    "color": GOLD_COLOR, "fontSize": "0.78rem",
                                                    "fontFamily": "JetBrains Mono"}),
                    *[_config_input(label, key, default, type_="number")
                      for label, key, default in [
                          ("Intraday slots",  "intraday_slots",   3),
                          ("Swing slots",     "swing_slots",      5),
                          ("Medium slots",    "medium_slots",     8),
                          ("Long-term slots", "longterm_slots",   15),
                      ]],
                    html.Hr(style={"borderColor": BORDER_COLOR, "margin": "12px 0"}),
                    html.Div("Capital Allocation (₹)", style={"fontWeight": "700", "marginBottom": "8px",
                                                               "color": GOLD_COLOR, "fontSize": "0.78rem",
                                                               "fontFamily": "JetBrains Mono"}),
                    *[_config_input(label, key, default, type_="number")
                      for label, key, default in [
                          ("Total capital",     "total_capital",     2_500_000),
                          ("Intraday capital",  "intraday_capital",    250_000),
                          ("Swing capital",     "swing_capital",       500_000),
                          ("Medium capital",    "medium_capital",      750_000),
                          ("Long-term capital", "longterm_capital",  1_000_000),
                      ]],
                    dbc.Button("Save Portfolio Config", id="save-portfolio-config",
                               n_clicks=0, color="warning",
                               style={"width": "100%", "marginTop": "12px", "fontWeight": "700"}),
                    html.Div(id="portfolio-config-result", style={"marginTop": "6px"}),
                ], style={"padding": "16px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=4),

        # ── Signal Thresholds ──────────────────────────────────────────
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Signal Thresholds & ML",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody([
                    html.Div("Cycle-conditioned confidence thresholds:",
                             style={"color": "#9ca3af", "fontSize": "0.78rem", "marginBottom": "10px"}),
                    *[_config_input(label, key, default, type_="number", step=0.05)
                      for label, key, default in [
                          ("Bull market threshold",    "signal_threshold_bull",    0.65),
                          ("Neutral market threshold", "signal_threshold_neutral",  0.75),
                          ("Bear market threshold",    "signal_threshold_bear",     0.85),
                          ("Min R:R ratio",            "min_risk_reward",           1.5),
                          ("Drawdown alert %",         "drawdown_alert_pct",        0.10),
                          ("Strategy min Sharpe",      "strategy_min_sharpe",       1.0),
                          ("Strategy min win rate",    "strategy_min_winrate",      0.52),
                      ]],
                    dbc.Button("Save Signal Config", id="save-signal-config",
                               n_clicks=0, color="warning",
                               style={"width": "100%", "marginTop": "12px", "fontWeight": "700"}),
                    html.Div(id="signal-config-result", style={"marginTop": "6px"}),
                ], style={"padding": "16px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=4),

        # ── Notifications ──────────────────────────────────────────────
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Notification Settings",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase"}),
                dbc.CardBody([
                    html.Div("Telegram", style={"fontWeight": "700", "marginBottom": "8px",
                                                 "color": GOLD_COLOR, "fontSize": "0.78rem",
                                                 "fontFamily": "JetBrains Mono"}),
                    dbc.Input(id="tg-token", type="password", placeholder="Bot token (from .env)",
                              style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                                     "color": "#e5e7eb", "fontSize": "0.83rem", "marginBottom": "6px"}),
                    dbc.Input(id="tg-chat", type="text", placeholder="Chat ID",
                              style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
                                     "color": "#e5e7eb", "fontSize": "0.83rem", "marginBottom": "12px"}),
                    dbc.Button("Test Telegram", id="test-telegram", n_clicks=0, size="sm",
                               color="secondary", outline=True, style={"marginBottom": "12px"}),
                    html.Div(id="telegram-test-result"),
                    html.Hr(style={"borderColor": BORDER_COLOR, "margin": "12px 0"}),
                    html.Div("System", style={"fontWeight": "700", "marginBottom": "8px",
                                               "color": GOLD_COLOR, "fontSize": "0.78rem",
                                               "fontFamily": "JetBrains Mono"}),
                    html.Div(id="system-status"),
                ], style={"padding": "16px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=4),
    ], className="g-3"),
], padding="20px 24px")


@callback(Output("portfolio-config-result", "children"),
          Input("save-portfolio-config", "n_clicks"),
          [State(f"cfg-{k}", "value") for k in [
              "intraday_slots", "swing_slots", "medium_slots", "longterm_slots",
              "total_capital", "intraday_capital", "swing_capital",
              "medium_capital", "longterm_capital"]],
          prevent_initial_call=True)
def save_portfolio_config(n, *values):
    keys = ["intraday_slots", "swing_slots", "medium_slots", "longterm_slots",
            "total_capital", "intraday_capital", "swing_capital",
            "medium_capital", "longterm_capital"]
    from alphalens.core.database import set_config
    for k, v in zip(keys, values):
        if v is not None:
            set_config(k, v)
    return dbc.Alert("✓ Portfolio configuration saved.", color="success", style={"fontSize": "0.82rem"})


@callback(Output("signal-config-result", "children"),
          Input("save-signal-config", "n_clicks"),
          [State(f"cfg-{k}", "value") for k in [
              "signal_threshold_bull", "signal_threshold_neutral", "signal_threshold_bear",
              "min_risk_reward", "drawdown_alert_pct",
              "strategy_min_sharpe", "strategy_min_winrate"]],
          prevent_initial_call=True)
def save_signal_config(n, *values):
    keys = ["signal_threshold_bull", "signal_threshold_neutral", "signal_threshold_bear",
            "min_risk_reward", "drawdown_alert_pct",
            "strategy_min_sharpe", "strategy_min_winrate"]
    from alphalens.core.database import set_config
    for k, v in zip(keys, values):
        if v is not None:
            set_config(k, v)
    return dbc.Alert("✓ Signal configuration saved.", color="success", style={"fontSize": "0.82rem"})


@callback(Output("telegram-test-result", "children"),
          Input("test-telegram", "n_clicks"),
          prevent_initial_call=True)
def test_telegram(_):
    try:
        from alphalens.core.notifications.telegram import TelegramNotifier
        tg = TelegramNotifier()
        ok = tg.send("🧪 <b>AlphaLens test message</b> — notifications are working!")
        if ok:
            return dbc.Alert("✓ Test message sent!", color="success", style={"fontSize": "0.82rem"})
        return dbc.Alert("Telegram not configured or failed. Check .env file.", color="warning",
                         style={"fontSize": "0.82rem"})
    except Exception as e:
        return dbc.Alert(f"Error: {e}", color="danger", style={"fontSize": "0.82rem"})


@callback(Output("system-status", "children"), Input("save-portfolio-config", "n_clicks"),
          Input("save-signal-config", "n_clicks"))
def render_system_status(_, __):
    from alphalens.core.database import get_duck
    from alphalens.core.portfolio.manager import PortfolioManager
    con = get_duck()
    pm  = PortfolioManager()

    price_count = con.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0]
    ind_count   = con.execute("SELECT COUNT(*) FROM technical_indicators").fetchone()[0]
    strat_count = con.execute("SELECT COUNT(*) FROM strategies WHERE is_active = true").fetchone()[0]
    sig_count   = con.execute("SELECT COUNT(*) FROM market_cycles").fetchone()[0]
    all_cap     = pm.get_all_capacity()

    items = [
        ("Price bars",     f"{price_count:,}",    BULL_COLOR),
        ("Indicator rows", f"{ind_count:,}",       BULL_COLOR),
        ("Active strategies", str(strat_count),   GOLD_COLOR),
        ("Cycle labels",   f"{sig_count:,}",       NEUTRAL_COLOR),
    ]
    for tf, cap in all_cap.items():
        items.append((
            tf.replace("_","-").capitalize(),
            f"{cap['used']}/{cap['max']} slots",
            BULL_COLOR if cap["available"] > 0 else NEUTRAL_COLOR,
        ))

    return html.Div([
        html.Div([
            html.Span(lbl, style={"fontSize": "0.72rem", "color": "#6b7280", "flex": "1"}),
            html.Span(val, style={"fontFamily": "JetBrains Mono", "fontSize": "0.78rem",
                                   "color": col, "fontWeight": "600"}),
        ], style={"display": "flex", "padding": "4px 0",
                  "borderBottom": f"1px solid {BORDER_COLOR}"})
        for lbl, val, col in items
    ])


def _config_input(label: str, key: str, default, type_: str = "text", step=1):
    from alphalens.core.database import get_config
    current = get_config(key, default)
    return dbc.Row([
        dbc.Col(html.Div(label, style={"fontSize": "0.78rem", "color": "#9ca3af",
                                        "paddingTop": "6px"}), width=7),
        dbc.Col(dbc.Input(id=f"cfg-{key}", type=type_, value=current, step=step,
                           style={"background": "#0d0d0d", "border": f"1px solid {BORDER_COLOR}",
                                  "color": "#e5e7eb", "fontSize": "0.83rem",
                                  "padding": "4px 8px", "height": "30px"}), width=5),
    ], className="mb-1", align="center")
