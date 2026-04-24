"""alphalens/dashboard/pages/backtest.py — Page 9: Backtest Explorer"""
import dash, dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html
dash.register_page(__name__, path="/backtest", name="Backtest", order=9)
from alphalens.dashboard.components.shared import *

layout = page_layout([
    section_header("Backtest Explorer", "Walk-forward backtest results · strategy × symbol drill-down"),
    dbc.Row([
        dbc.Col(dcc.Dropdown(id="bt-strategy-filter", placeholder="All strategies…",
                             style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}", "fontSize": "0.83rem"}), width=4),
        dbc.Col(dcc.Dropdown(id="bt-tf-filter",
                             options=[{"label": l, "value": v} for l, v in [
                                 ("All TF", "all"), ("Intraday", "intraday"), ("Swing", "swing"),
                                 ("Medium", "medium"), ("Long-term", "long_term")]],
                             value="all", clearable=False,
                             style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}", "fontSize": "0.83rem"}), width=2),
        dbc.Col(dcc.Slider(id="bt-sharpe-min", min=0, max=3, step=0.1, value=0,
                           marks={0: "0", 1: "1", 1.5: "1.5", 2: "2", 3: "3"},
                           tooltip={"always_visible": False, "placement": "top"}), width=4),
        dbc.Col(html.Div("Min Sharpe", style={"color": "#6b7280", "fontSize": "0.75rem",
                                               "marginTop": "8px"}), width=2),
    ], className="mb-3 g-2", align="center"),
    html.Div(id="bt-results-table"),
    dcc.Interval(id="bt-refresh", interval=300_000, n_intervals=0),
])

@callback(Output("bt-strategy-filter", "options"), Input("bt-refresh", "n_intervals"))
def load_strategies(_):
    from alphalens.core.strategy.library import get_all_strategies
    strats = get_all_strategies(active_only=False)
    return [{"label": f"{s['strategy_id']} – {s['name']}", "value": s["strategy_id"]} for s in strats]

@callback(Output("bt-results-table", "children"),
          Input("bt-strategy-filter", "value"), Input("bt-tf-filter", "value"),
          Input("bt-sharpe-min", "value"), Input("bt-refresh", "n_intervals"))
def render_backtest_table(strat_id, tf, min_sharpe, _):
    from alphalens.core.database import get_duck
    con = get_duck()
    where = ["1=1"]
    params = []
    if strat_id:       where.append("strategy_id = ?"); params.append(strat_id)
    if tf != "all":    where.append("timeframe = ?"); params.append(tf)
    if min_sharpe > 0: where.append("sharpe_ratio >= ?"); params.append(float(min_sharpe))

    df = con.execute(f"""
        SELECT run_id, strategy_id, symbol, timeframe,
               sharpe_ratio, win_rate, max_drawdown, total_return,
               profit_factor, total_trades, from_date, to_date
        FROM backtest_results
        WHERE {" AND ".join(where)}
        ORDER BY sharpe_ratio DESC NULLS LAST
        LIMIT 200
    """, params).fetchdf()

    if df.empty:
        return dbc.Alert("No backtest results yet. Run backtests via IEx: Backtester().run_all_strategies()",
                         color="secondary", style={"fontSize": "0.82rem", "marginTop": "12px"})

    rows = []
    for _, r in df.iterrows():
        sharpe = r.get("sharpe_ratio")
        wr     = r.get("win_rate")
        dd     = r.get("max_drawdown")
        ret    = r.get("total_return")
        pf     = r.get("profit_factor")
        sc = BULL_COLOR if sharpe and sharpe >= 1.5 else (NEUTRAL_COLOR if sharpe and sharpe >= 1.0 else BEAR_COLOR)
        rows.append(html.Tr([
            html.Td(r.get("strategy_id", "–"),
                    style={"fontFamily": "JetBrains Mono", "fontSize": "0.75rem", "color": GOLD_COLOR}),
            html.Td(r.get("symbol") or "ALL",
                    style={"fontFamily": "JetBrains Mono", "fontWeight": "600"}),
            html.Td((r.get("timeframe") or "–").replace("_","-").upper(),
                    style={"fontFamily": "JetBrains Mono", "fontSize": "0.72rem", "color": NEUTRAL_COLOR}),
            html.Td(html.Span(f"{sharpe:.2f}" if sharpe else "–",
                              style={"color": sc, "fontFamily": "JetBrains Mono", "fontWeight": "700"})),
            html.Td(f"{wr*100:.1f}%" if wr else "–",
                    style={"color": BULL_COLOR if wr and wr >= 0.52 else BEAR_COLOR,
                           "fontFamily": "JetBrains Mono"}),
            html.Td(f"{dd:.1f}%" if dd else "–",
                    style={"color": BEAR_COLOR, "fontFamily": "JetBrains Mono"}),
            html.Td(f"{ret:.1f}%" if ret else "–",
                    style={"color": pnl_color(ret or 0), "fontFamily": "JetBrains Mono"}),
            html.Td(f"{pf:.2f}" if pf else "–",
                    style={"fontFamily": "JetBrains Mono"}),
            html.Td(str(int(r.get("total_trades", 0) or 0))),
            html.Td(f"{str(r.get('from_date',''))[:10]} → {str(r.get('to_date',''))[:10]}",
                    style={"fontSize": "0.72rem", "color": "#6b7280"}),
        ]))

    return dbc.Table([
        html.Thead(html.Tr([html.Th(h, style={"color": "#6b7280", "fontSize": "0.7rem",
                                               "fontFamily": "JetBrains Mono", "textTransform": "uppercase"})
                            for h in ["Strategy", "Symbol", "TF", "Sharpe", "Win%",
                                      "MaxDD", "Return", "PF", "Trades", "Period"]])),
        html.Tbody(rows),
    ], bordered=False, hover=True, responsive=True,
       style={"fontSize": "0.82rem", "background": CARD_BG,
              "border": f"1px solid {BORDER_COLOR}", "borderRadius": "6px"})
