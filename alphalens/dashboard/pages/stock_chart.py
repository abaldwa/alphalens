"""
alphalens/dashboard/pages/stock_chart.py — Page 4: Stock Chart

Interactive candlestick chart with:
  - Full OHLCV candlestick (Plotly go.Candlestick)
  - Overlay: EMA lines, Bollinger Bands, Supertrend, Volume
  - Historical entry/exit signal scatter markers (all strategies)
  - Timeframe & indicator selector
  - Cycle badge + current signal display
"""

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Input, Output, State, callback, dcc, html
import pandas as pd

dash.register_page(__name__, path="/chart", name="Chart", order=4)

from alphalens.dashboard.components.shared import (
    BULL_COLOR, BEAR_COLOR, NEUTRAL_COLOR, GOLD_COLOR,
    CARD_BG, BORDER_COLOR, cycle_badge, signal_badge,
    section_header, page_layout, format_inr, format_pct, pnl_color,
)
from alphalens.core.ingestion.universe import get_all_symbols

SYMBOLS     = get_all_symbols()
TIMEFRAMES  = ["intraday", "swing", "medium", "long_term"]
BAR_OPTIONS = [("1M", 22), ("3M", 66), ("6M", 132), ("1Y", 252)]
INDICATORS  = [
    ("EMA 9/20", "ema"),
    ("EMA 50/200", "ema_long"),
    ("Bollinger Bands", "bb"),
    ("Supertrend", "st"),
    ("Volume", "vol"),
]

# ── Layout ─────────────────────────────────────────────────────────────────
layout = page_layout([
    # ── Controls row ──────────────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            dcc.Dropdown(
                id="chart-symbol",
                options=[{"label": s, "value": s} for s in SYMBOLS],
                value="RELIANCE",
                placeholder="Select symbol…",
                style={"background": CARD_BG, "color": "#e5e7eb",
                       "border": f"1px solid {BORDER_COLOR}",
                       "fontFamily": "JetBrains Mono", "fontSize": "0.85rem"},
                className="dark-dropdown",
            ),
        ], width=3),

        dbc.Col([
            dbc.ButtonGroup([
                dbc.Button(label, id={"type": "chart-tf", "index": tf},
                           n_clicks=0, size="sm",
                           color="warning" if tf == "swing" else "secondary",
                           outline=tf != "swing",
                           style={"fontFamily": "JetBrains Mono", "fontSize": "0.72rem"})
                for label, tf in [("1D", "intraday"), ("SWING", "swing"),
                                   ("MED", "medium"), ("LT", "long_term")]
            ]),
        ], width=3),

        dbc.Col([
            dbc.ButtonGroup([
                dbc.Button(label, id={"type": "chart-bars", "index": bars},
                           n_clicks=0, size="sm",
                           color="warning" if bars == 132 else "secondary",
                           outline=bars != 132,
                           style={"fontFamily": "JetBrains Mono", "fontSize": "0.72rem"})
                for label, bars in BAR_OPTIONS
            ]),
        ], width=2),

        dbc.Col([
            dcc.Checklist(
                id="chart-overlays",
                options=[{"label": f" {label}", "value": key}
                         for label, key in INDICATORS],
                value=["ema", "vol"],
                inline=True,
                style={"color": "#9ca3af", "fontSize": "0.75rem",
                       "fontFamily": "JetBrains Mono", "gap": "12px",
                       "display": "flex", "flexWrap": "wrap"},
                labelStyle={"marginRight": "12px"},
            ),
        ], width=4),
    ], className="mb-3 g-2", align="center"),

    # ── Signal bar ────────────────────────────────────────────────────
    html.Div(id="chart-signal-bar", style={"marginBottom": "12px"}),

    # ── Main chart ────────────────────────────────────────────────────
    dbc.Card([
        dbc.CardBody([
            dcc.Graph(
                id="main-chart",
                config={
                    "displayModeBar": True,
                    "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d"],
                    "displaylogo": False,
                    "toImageButtonOptions": {"format": "svg", "scale": 2},
                },
                style={"height": "560px"},
            ),
        ], style={"padding": "8px"}),
    ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
              "marginBottom": "16px"}),

    # ── Signal history table ───────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Historical Signals — This Stock",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase",
                                      "letterSpacing": "0.08em"}),
                dbc.CardBody([html.Div(id="signal-history-table")],
                             style={"padding": "8px", "maxHeight": "260px",
                                    "overflowY": "auto"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=8),

        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Key Indicators",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "textTransform": "uppercase",
                                      "letterSpacing": "0.08em"}),
                dbc.CardBody([html.Div(id="indicator-panel")],
                             style={"padding": "12px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=4),
    ]),

    # Stores
    dcc.Store(id="chart-tf-store", data="swing"),
    dcc.Store(id="chart-bars-store", data=132),
])


# ── Callbacks ──────────────────────────────────────────────────────────────
@callback(
    Output("chart-tf-store", "data"),
    Input({"type": "chart-tf", "index": dash.ALL}, "n_clicks"),
    State({"type": "chart-tf", "index": dash.ALL}, "id"),
    prevent_initial_call=True,
)
def update_tf_store(n_clicks, ids):
    ctx_cb = dash.callback_context
    if not ctx_cb.triggered:
        return "swing"
    trigger = ctx_cb.triggered[0]["prop_id"].split(".")[0]
    import json
    return json.loads(trigger)["index"]


@callback(
    Output("chart-bars-store", "data"),
    Input({"type": "chart-bars", "index": dash.ALL}, "n_clicks"),
    State({"type": "chart-bars", "index": dash.ALL}, "id"),
    prevent_initial_call=True,
)
def update_bars_store(n_clicks, ids):
    ctx_cb = dash.callback_context
    if not ctx_cb.triggered:
        return 132
    trigger = ctx_cb.triggered[0]["prop_id"].split(".")[0]
    import json
    return json.loads(trigger)["index"]


@callback(
    Output("main-chart", "figure"),
    Output("chart-signal-bar", "children"),
    Output("signal-history-table", "children"),
    Output("indicator-panel", "children"),
    Input("chart-symbol", "value"),
    Input("chart-tf-store", "data"),
    Input("chart-bars-store", "data"),
    Input("chart-overlays", "value"),
    Input("slow-interval", "n_intervals"),
)
def update_chart(symbol, timeframe, n_bars, overlays, _n):
    if not symbol:
        return go.Figure(), html.Div(), html.Div(), html.Div()

    from alphalens.core.database import get_duck, get_sqlite, SignalLog
    from alphalens.core.cycle.context import get_cycle_context

    con = get_duck()
    ctx = get_cycle_context()

    # ── Load price data ────────────────────────────────────────────────
    prices = con.execute("""
        SELECT date, open, high, low, close, volume
        FROM daily_prices
        WHERE symbol = ?
        ORDER BY date DESC LIMIT ?
    """, [symbol, n_bars + 50]).fetchdf()

    if prices.empty:
        fig = go.Figure()
        fig.update_layout(paper_bgcolor="#1f2937", plot_bgcolor="#1f2937",
                          font_color="#6b7280",
                          annotations=[{"text": f"No data for {symbol}",
                                        "xref": "paper", "yref": "paper",
                                        "x": 0.5, "y": 0.5, "showarrow": False,
                                        "font": {"size": 16, "color": "#6b7280"}}])
        return fig, html.Div(), html.Div(), html.Div()

    prices = prices.sort_values("date").tail(n_bars)

    # ── Load indicators ────────────────────────────────────────────────
    inds = con.execute("""
        SELECT date, ema_9, ema_20, ema_50, ema_200, sma_200,
               bb_upper, bb_mid, bb_lower, supertrend, supertrend_dir,
               rsi_14, macd, macd_hist, adx_14, atr_14,
               volume_ratio, obv
        FROM technical_indicators
        WHERE symbol = ?
        ORDER BY date DESC LIMIT ?
    """, [symbol, n_bars + 50]).fetchdf().sort_values("date").tail(n_bars)

    # ── Load historical signals (all time, this symbol+timeframe) ──────
    with get_sqlite() as session:
        all_signals = session.query(SignalLog).filter(
            SignalLog.symbol    == symbol,
            SignalLog.timeframe == timeframe,
        ).order_by(SignalLog.generated_at.desc()).limit(100).all()

    signals_df = pd.DataFrame([{
        "date":        s.generated_at.date() if s.generated_at else None,
        "signal_type": s.signal_type,
        "entry_price": s.entry_price,
        "target_price": s.target_price,
        "stop_loss":   s.stop_loss,
        "confidence":  s.confidence,
        "strategy_id": s.strategy_id,
        "risk_reward": s.risk_reward,
    } for s in all_signals if s.generated_at])

    # ── Build chart ────────────────────────────────────────────────────
    fig = _build_candlestick_chart(prices, inds, signals_df, overlays, symbol, timeframe)

    # ── Signal bar (current active signal) ────────────────────────────
    current_signal = all_signals[0] if all_signals else None
    signal_bar     = _build_signal_bar(current_signal, symbol, ctx, timeframe)

    # ── Signal history table ───────────────────────────────────────────
    signal_table = _build_signal_history(all_signals[:20])

    # ── Indicator panel ────────────────────────────────────────────────
    latest_inds = inds.iloc[-1].to_dict() if not inds.empty else {}
    ind_panel   = _build_indicator_panel(latest_inds)

    return fig, signal_bar, signal_table, ind_panel


# ── Chart Builder ─────────────────────────────────────────────────────────

def _build_candlestick_chart(prices, inds, signals_df, overlays, symbol, timeframe):
    """Build the main OHLCV candlestick chart with signal overlays."""

    show_vol = "vol" in (overlays or [])
    rows     = 3 if show_vol else 2
    row_heights = [0.65, 0.20, 0.15] if show_vol else [0.75, 0.25]

    fig = make_subplots(
        rows=rows, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=row_heights,
    )

    dates  = prices["date"].astype(str)
    opens  = prices["open"].astype(float)
    highs  = prices["high"].astype(float)
    lows   = prices["low"].astype(float)
    closes = prices["close"].astype(float)

    # ── Candlestick ────────────────────────────────────────────────────
    fig.add_trace(go.Candlestick(
        x=dates, open=opens, high=highs, low=lows, close=closes,
        name=symbol,
        increasing={"fillcolor": "#1a4d2e", "line": {"color": BULL_COLOR, "width": 1}},
        decreasing={"fillcolor": "#4d1a1a", "line": {"color": BEAR_COLOR, "width": 1}},
        hovertemplate=(
            "<b>%{x}</b><br>"
            "O: %{open:,.2f}  H: %{high:,.2f}<br>"
            "L: %{low:,.2f}   C: %{close:,.2f}<extra></extra>"
        ),
    ), row=1, col=1)

    if not inds.empty:
        ind_dates = inds["date"].astype(str)

        # ── EMA 9/20 ───────────────────────────────────────────────────
        if "ema" in (overlays or []):
            for col, color, label in [
                ("ema_9",  "#60a5fa", "EMA 9"),
                ("ema_20", "#a78bfa", "EMA 20"),
            ]:
                if col in inds.columns:
                    fig.add_trace(go.Scatter(
                        x=ind_dates, y=inds[col].astype(float),
                        mode="lines", name=label,
                        line={"color": color, "width": 1},
                        hovertemplate=f"{label}: %{{y:,.2f}}<extra></extra>",
                    ), row=1, col=1)

        # ── EMA 50/200 ─────────────────────────────────────────────────
        if "ema_long" in (overlays or []):
            for col, color, label in [
                ("ema_50",  "#fbbf24", "EMA 50"),
                ("ema_200", "#f97316", "EMA 200"),
            ]:
                if col in inds.columns:
                    fig.add_trace(go.Scatter(
                        x=ind_dates, y=inds[col].astype(float),
                        mode="lines", name=label,
                        line={"color": color, "width": 1.5, "dash": "dot"},
                        hovertemplate=f"{label}: %{{y:,.2f}}<extra></extra>",
                    ), row=1, col=1)

        # ── Bollinger Bands ────────────────────────────────────────────
        if "bb" in (overlays or []) and all(c in inds.columns for c in ["bb_upper", "bb_lower"]):
            fig.add_trace(go.Scatter(
                x=list(ind_dates) + list(ind_dates[::-1]),
                y=list(inds["bb_upper"].astype(float)) + list(inds["bb_lower"].astype(float)[::-1]),
                fill="toself", fillcolor="rgba(99,102,241,0.08)",
                line={"color": "rgba(99,102,241,0.3)", "width": 1},
                name="BB", hoverinfo="skip",
            ), row=1, col=1)

        # ── Supertrend ─────────────────────────────────────────────────
        if "st" in (overlays or []) and "supertrend" in inds.columns:
            st_up   = inds[inds["supertrend_dir"] == -1]
            st_down = inds[inds["supertrend_dir"] == 1]
            if not st_up.empty:
                fig.add_trace(go.Scatter(
                    x=st_up["date"].astype(str), y=st_up["supertrend"].astype(float),
                    mode="lines", name="ST Buy",
                    line={"color": BULL_COLOR, "width": 1.5, "dash": "dash"},
                    hoverinfo="skip",
                ), row=1, col=1)
            if not st_down.empty:
                fig.add_trace(go.Scatter(
                    x=st_down["date"].astype(str), y=st_down["supertrend"].astype(float),
                    mode="lines", name="ST Sell",
                    line={"color": BEAR_COLOR, "width": 1.5, "dash": "dash"},
                    hoverinfo="skip",
                ), row=1, col=1)

        # ── MACD histogram (row 2) ─────────────────────────────────────
        if "macd_hist" in inds.columns:
            macd_h = inds["macd_hist"].astype(float)
            colors_macd = [BULL_COLOR if v >= 0 else BEAR_COLOR for v in macd_h]
            fig.add_trace(go.Bar(
                x=ind_dates, y=macd_h,
                marker_color=colors_macd,
                name="MACD Hist",
                hovertemplate="MACD: %{y:.4f}<extra></extra>",
            ), row=2, col=1)

    # ── Historical signal markers ──────────────────────────────────────
    if not signals_df.empty:
        sig_dates = signals_df["date"].astype(str)

        buys  = signals_df[signals_df["signal_type"] == "buy"]
        sells = signals_df[signals_df["signal_type"] == "sell"]

        if not buys.empty:
            fig.add_trace(go.Scatter(
                x=buys["date"].astype(str),
                y=buys["entry_price"].astype(float),
                mode="markers",
                name="BUY Signal",
                marker={
                    "symbol": "triangle-up",
                    "size":   12,
                    "color":  BULL_COLOR,
                    "line":   {"color": "#fff", "width": 1},
                },
                hovertemplate=(
                    "<b>BUY %{x}</b><br>"
                    "Entry: ₹%{y:,.2f}<br>"
                    "Conf: %{customdata[0]:.0%}<br>"
                    "Strategy: %{customdata[1]}<extra></extra>"
                ),
                customdata=list(zip(
                    buys["confidence"].fillna(0),
                    buys["strategy_id"].fillna(""),
                )),
            ), row=1, col=1)

        if not sells.empty:
            fig.add_trace(go.Scatter(
                x=sells["date"].astype(str),
                y=sells["entry_price"].astype(float),
                mode="markers",
                name="SELL Signal",
                marker={
                    "symbol": "triangle-down",
                    "size":   12,
                    "color":  BEAR_COLOR,
                    "line":   {"color": "#fff", "width": 1},
                },
                hovertemplate=(
                    "<b>SELL %{x}</b><br>"
                    "Entry: ₹%{y:,.2f}<br>"
                    "Conf: %{customdata[0]:.0%}<extra></extra>"
                ),
                customdata=list(zip(sells["confidence"].fillna(0), )),
            ), row=1, col=1)

        # Target and SL horizontal lines for latest signal
        latest = signals_df.iloc[0]
        if latest.get("target_price"):
            fig.add_hline(
                y=float(latest["target_price"]),
                line={"color": BULL_COLOR, "dash": "dot", "width": 1},
                annotation={"text": f"T ₹{latest['target_price']:,.2f}",
                             "font": {"size": 10, "color": BULL_COLOR}},
                row=1, col=1,
            )
        if latest.get("stop_loss"):
            fig.add_hline(
                y=float(latest["stop_loss"]),
                line={"color": BEAR_COLOR, "dash": "dot", "width": 1},
                annotation={"text": f"SL ₹{latest['stop_loss']:,.2f}",
                             "font": {"size": 10, "color": BEAR_COLOR}},
                row=1, col=1,
            )

    # ── Volume bars (row 3 if enabled) ────────────────────────────────
    if show_vol:
        vol_colors = [
            BULL_COLOR if c >= o else BEAR_COLOR
            for c, o in zip(closes, opens)
        ]
        fig.add_trace(go.Bar(
            x=dates, y=prices["volume"].astype(float),
            marker_color=vol_colors, name="Volume",
            hovertemplate="Vol: %{y:,.0f}<extra></extra>",
        ), row=3, col=1)

    # ── Layout ────────────────────────────────────────────────────────
    fig.update_layout(
        paper_bgcolor="#1f2937",
        plot_bgcolor="#1f2937",
        font={"color": "#9ca3af", "family": "JetBrains Mono", "size": 10},
        margin={"l": 60, "r": 20, "t": 10, "b": 30},
        legend={
            "orientation": "h", "y": 1.02, "x": 0,
            "bgcolor": "rgba(0,0,0,0)",
            "font": {"size": 9},
        },
        hovermode="x unified",
        xaxis={"rangeslider": {"visible": False},
               "gridcolor": "#374151", "showgrid": True},
        xaxis2={"gridcolor": "#374151"},
        yaxis={"gridcolor": "#374151", "tickformat": ",.2f",
               "side": "right", "showgrid": True},
        yaxis2={"gridcolor": "#374151", "showgrid": False,
                "title": {"text": "MACD", "font": {"size": 9}}},
    )
    if show_vol:
        fig.update_layout(
            xaxis3={"gridcolor": "#374151"},
            yaxis3={"gridcolor": "#374151", "showgrid": False,
                    "title": {"text": "Vol", "font": {"size": 9}}},
        )

    return fig


def _build_signal_bar(signal, symbol: str, ctx, timeframe: str):
    if signal is None:
        return dbc.Alert(
            f"No {timeframe} signal for {symbol} yet",
            color="secondary", style={"margin": "0", "padding": "8px 16px",
                                       "fontSize": "0.82rem", "borderRadius": "4px"}
        )

    st   = signal.signal_type or "hold"
    conf = signal.confidence or 0
    sig_color = {"buy": BULL_COLOR, "sell": BEAR_COLOR}.get(st, NEUTRAL_COLOR)

    return html.Div([
        html.Div([
            signal_badge(st),
            html.Span(symbol, style={"fontWeight": "700", "marginLeft": "8px",
                                      "fontFamily": "JetBrains Mono", "fontSize": "0.9rem"}),
            html.Span(f"  [{timeframe.upper().replace('_','-')}]",
                      style={"color": "#6b7280", "fontSize": "0.75rem"}),
        ], style={"display": "flex", "alignItems": "center", "gap": "4px"}),

        html.Div([
            _info_chip("Entry",  f"₹{signal.entry_price:,.2f}" if signal.entry_price else "–"),
            _info_chip("Target", f"₹{signal.target_price:,.2f}" if signal.target_price else "–", BULL_COLOR),
            _info_chip("SL",     f"₹{signal.stop_loss:,.2f}" if signal.stop_loss else "–", BEAR_COLOR),
            _info_chip("R:R",    f"{signal.risk_reward:.1f}x" if signal.risk_reward else "–", GOLD_COLOR),
            _info_chip("Conf",   f"{conf*100:.0f}%", sig_color),
            cycle_badge(ctx.market_cycle, ctx.market_confidence),
        ], style={"display": "flex", "gap": "8px", "alignItems": "center",
                  "flexWrap": "wrap"}),
    ], style={
        "display": "flex", "justifyContent": "space-between", "alignItems": "center",
        "background": CARD_BG, "border": f"1px solid {sig_color}40",
        "borderRadius": "6px", "padding": "10px 16px",
    })


def _info_chip(label: str, value: str, color: str = "#9ca3af"):
    return html.Span([
        html.Span(f"{label}: ", style={"color": "#6b7280", "fontSize": "0.72rem"}),
        html.Span(value, style={"color": color, "fontFamily": "JetBrains Mono",
                                 "fontSize": "0.82rem", "fontWeight": "600"}),
    ])


def _build_signal_history(signals: list):
    if not signals:
        return html.Div("No signals recorded yet",
                        style={"color": "#6b7280", "padding": "12px",
                               "fontSize": "0.82rem", "textAlign": "center"})

    rows = []
    for s in signals:
        st  = s.signal_type or "hold"
        fg  = BULL_COLOR if st == "buy" else (BEAR_COLOR if st == "sell" else NEUTRAL_COLOR)
        rows.append(html.Div([
            html.Span(str(s.generated_at)[:10] if s.generated_at else "–",
                      style={"color": "#6b7280", "fontFamily": "JetBrains Mono",
                             "fontSize": "0.75rem", "width": "85px"}),
            signal_badge(st),
            html.Span(f"₹{s.entry_price:,.2f}" if s.entry_price else "–",
                      style={"fontFamily": "JetBrains Mono", "fontSize": "0.78rem",
                             "color": "#e5e7eb", "width": "80px"}),
            html.Span(f"T ₹{s.target_price:,.2f}" if s.target_price else "",
                      style={"color": BULL_COLOR, "fontSize": "0.72rem",
                             "fontFamily": "JetBrains Mono", "width": "90px"}),
            html.Span(f"SL ₹{s.stop_loss:,.2f}" if s.stop_loss else "",
                      style={"color": BEAR_COLOR, "fontSize": "0.72rem",
                             "fontFamily": "JetBrains Mono", "width": "90px"}),
            html.Span(f"{(s.confidence or 0)*100:.0f}%",
                      style={"color": fg, "fontFamily": "JetBrains Mono",
                             "fontSize": "0.72rem", "width": "40px"}),
            html.Span(s.strategy_id or "",
                      style={"color": "#6b7280", "fontSize": "0.68rem"}),
        ], style={"display": "flex", "alignItems": "center", "gap": "8px",
                  "padding": "5px 0", "borderBottom": f"1px solid {BORDER_COLOR}"}))

    return html.Div(rows)


def _build_indicator_panel(inds: dict):
    items = [
        ("RSI 14",    inds.get("rsi_14"),     "1f", lambda v: BEAR_COLOR if v > 70 else (BULL_COLOR if v < 30 else "#e5e7eb")),
        ("ADX 14",    inds.get("adx_14"),     "1f", lambda v: BULL_COLOR if v > 25 else "#6b7280"),
        ("MACD Hist", inds.get("macd_hist"),  "4f", lambda v: BULL_COLOR if v > 0 else BEAR_COLOR),
        ("BB %B",     inds.get("bb_pct_b"),   "2f", lambda v: "#e5e7eb"),
        ("ATR 14",    inds.get("atr_14"),     "2f", lambda v: "#e5e7eb"),
        ("Vol Ratio", inds.get("volume_ratio"),"2f",lambda v: BULL_COLOR if v > 1.5 else "#e5e7eb"),
        ("Supertrend",inds.get("supertrend_dir"),None,
         lambda v: (BULL_COLOR, "▲ UP") if v == -1 else (BEAR_COLOR, "▼ DN")),
    ]

    rows = []
    for label, val, fmt, color_fn in items:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            display = "–"
            color   = "#6b7280"
        elif fmt is None:
            # Special handling (Supertrend returns tuple)
            result  = color_fn(val)
            color, display = result if isinstance(result, tuple) else (result, str(val))
        else:
            try:
                v       = float(val)
                display = f"{v:.{fmt[:-1]}f}"
                color   = color_fn(v)
            except Exception:
                display = "–"
                color   = "#6b7280"

        rows.append(html.Div([
            html.Span(label, style={"fontSize": "0.72rem", "color": "#6b7280",
                                     "fontFamily": "JetBrains Mono", "flex": "1"}),
            html.Span(display, style={"fontSize": "0.82rem", "color": color,
                                       "fontFamily": "JetBrains Mono", "fontWeight": "600"}),
        ], style={"display": "flex", "justifyContent": "space-between",
                  "padding": "5px 0", "borderBottom": f"1px solid {BORDER_COLOR}"}))

    return html.Div(rows)
