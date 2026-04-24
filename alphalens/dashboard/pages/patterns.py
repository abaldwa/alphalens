"""alphalens/dashboard/pages/patterns.py — Page 8: Stock Pattern Analysis (HMM)"""
import dash, dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html
dash.register_page(__name__, path="/patterns", name="Patterns", order=8)
from alphalens.dashboard.components.shared import *
from alphalens.core.ingestion.universe import get_all_symbols

SYMBOLS = get_all_symbols()

layout = page_layout([
    section_header("Stock Pattern Analysis", "Hidden Markov Model regime detection per stock"),
    dbc.Row([
        dbc.Col(dcc.Dropdown(id="pat-symbol", options=[{"label": s, "value": s} for s in SYMBOLS],
                             value="RELIANCE", placeholder="Select symbol…",
                             style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}", "fontSize": "0.83rem"}), width=4),
        dbc.Col(dbc.Button("Run HMM Analysis", id="pat-run", n_clicks=0, color="warning", outline=True,
                           style={"fontSize": "0.8rem"}), width=2),
    ], className="mb-3 g-2"),
    html.Div(id="pattern-output"),
])

@callback(Output("pattern-output", "children"),
          Input("pat-symbol", "value"), Input("pat-run", "n_clicks"))
def render_patterns(symbol, _):
    if not symbol:
        return html.Div()
    from alphalens.core.database import get_duck
    import json as _json
    con = get_duck()
    row = con.execute("SELECT * FROM stock_patterns WHERE symbol = ?", [symbol]).fetchdf()

    if row.empty:
        return dbc.Alert([
            html.Strong("No HMM model trained for this stock yet. "),
            html.Span("Models are trained during the monthly discovery run or manually via IEx: "),
            html.Code("from alphalens.core.patterns.hmm import StockPatternDetector; StockPatternDetector().fit('" + symbol + "')",
                      style={"fontSize": "0.75rem", "color": "#93c5fd"}),
        ], color="secondary", style={"fontSize": "0.82rem"})

    r = row.iloc[0].to_dict()
    n_states = r.get("n_states", 3)
    current  = r.get("current_state")
    try:
        labels = _json.loads(r["state_labels"]) if r.get("state_labels") else {}
        history = _json.loads(r["state_history"]) if r.get("state_history") else []
    except Exception:
        labels, history = {}, []

    state_colors = {0: BULL_COLOR, 1: NEUTRAL_COLOR, 2: BEAR_COLOR}
    current_label = labels.get(str(current), f"State {current}")
    current_color = state_colors.get(current, NEUTRAL_COLOR)

    # State badge
    state_display = html.Div([
        html.Div("Current Regime", style={"fontSize": "0.7rem", "color": "#6b7280", "marginBottom": "4px"}),
        html.Span(current_label, style={"fontSize": "1.2rem", "fontWeight": "700",
                                         "fontFamily": "JetBrains Mono", "color": current_color}),
    ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
              "borderRadius": "6px", "padding": "12px 16px", "marginBottom": "16px"})

    # State history chart
    fig = go.Figure()
    if history:
        import pandas as pd
        hist_df = pd.DataFrame(history, columns=["date", "state"])
        hist_df["date"] = pd.to_datetime(hist_df["date"])
        for state_id in range(n_states):
            state_data = hist_df[hist_df["state"] == state_id]
            lbl = labels.get(str(state_id), f"State {state_id}")
            col = state_colors.get(state_id, NEUTRAL_COLOR)
            fig.add_trace(go.Scatter(
                x=state_data["date"], y=[state_id] * len(state_data),
                mode="markers", name=lbl,
                marker={"color": col, "size": 4, "symbol": "circle"},
            ))

    fig.update_layout(paper_bgcolor="#1f2937", plot_bgcolor="#1f2937",
                      font={"color": "#9ca3af", "family": "JetBrains Mono", "size": 10},
                      margin={"l": 50, "r": 10, "t": 10, "b": 30},
                      yaxis={"tickmode": "array", "tickvals": list(range(n_states)),
                             "ticktext": [labels.get(str(i), f"S{i}") for i in range(n_states)]},
                      xaxis={"gridcolor": "#374151"}, hovermode="x unified",
                      legend={"bgcolor": "rgba(0,0,0,0)"})

    chart = dbc.Card([
        dbc.CardHeader("Regime History", style={"background": "#111827", "color": "#9ca3af",
                                                  "fontSize": "0.75rem", "textTransform": "uppercase"}),
        dbc.CardBody(dcc.Graph(figure=fig, config={"displayModeBar": False}, style={"height": "220px"}),
                     style={"padding": "8px"}),
    ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"})

    return html.Div([state_display, chart])


"""alphalens/dashboard/pages/backtest.py — Page 9: Backtest Explorer"""
import dash as _dash, dash_bootstrap_components as _dbc
from dash import Input as _I, Output as _O, callback as _cb, dcc as _dcc, html as _html
_dash.register_page(__name__.replace("patterns", "backtest") if "patterns" in __name__ else __name__,
                    path="/backtest", name="Backtest", order=9)


"""alphalens/dashboard/pages/settings.py — Page 10: Settings"""
