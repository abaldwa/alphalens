"""alphalens/dashboard/pages/market_overview.py — Page 1: Market Overview."""

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html

dash.register_page(__name__, path="/", name="Overview", order=1)

from alphalens.dashboard.components.shared import (
    BULL_COLOR, BEAR_COLOR, NEUTRAL_COLOR, GOLD_COLOR, CARD_BG,
    BORDER_COLOR, SURFACE_BG, cycle_badge, metric_card, section_header,
    page_layout, format_inr, format_pct, pnl_color,
)


# ── Layout ─────────────────────────────────────────────────────────────────
layout = page_layout([
    section_header("Market Overview", "Real-time market cycle · macro context · sector rotation"),

    # ── Cycle + key metrics row ────────────────────────────────────────
    html.Div(id="overview-metrics-row", style={"marginBottom": "20px"}),

    # ── Nifty200 chart + VIX ──────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Nifty200 — Price & Market Cycle",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "fontWeight": "600",
                                      "letterSpacing": "0.08em", "textTransform": "uppercase",
                                      "border": f"0 0 1px 0 solid {BORDER_COLOR}"}),
                dbc.CardBody([
                    dcc.Graph(id="nifty200-chart",
                              config={"displayModeBar": False},
                              style={"height": "320px"}),
                ], style={"padding": "8px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=8),

        dbc.Col([
            dbc.Card([
                dbc.CardHeader("India VIX",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "letterSpacing": "0.08em",
                                      "textTransform": "uppercase"}),
                dbc.CardBody([
                    dcc.Graph(id="vix-gauge",
                              config={"displayModeBar": False},
                              style={"height": "280px"}),
                ], style={"padding": "8px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=4),
    ], className="mb-3"),

    # ── Sector heatmap ─────────────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Sector Cycle Heatmap",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "letterSpacing": "0.08em",
                                      "textTransform": "uppercase"}),
                dbc.CardBody([html.Div(id="sector-heatmap")],
                             style={"padding": "16px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=8),

        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Global Macro",
                               style={"background": "#111827", "color": "#9ca3af",
                                      "fontSize": "0.75rem", "letterSpacing": "0.08em",
                                      "textTransform": "uppercase"}),
                dbc.CardBody([html.Div(id="macro-panel")],
                             style={"padding": "12px"}),
            ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}"}),
        ], width=4),
    ]),
])


# ── Callbacks ──────────────────────────────────────────────────────────────
@callback(
    Output("overview-metrics-row", "children"),
    Output("nifty200-chart", "figure"),
    Output("vix-gauge", "figure"),
    Output("sector-heatmap", "children"),
    Output("macro-panel", "children"),
    Output("navbar-cycle-badge", "children"),
    Input("live-interval", "n_intervals"),
)
def update_overview(n):
    from alphalens.core.cycle.context import get_cycle_context
    from alphalens.core.database import get_duck

    ctx = get_cycle_context()
    con = get_duck()

    # ── Market context latest ──────────────────────────────────────────
    mkt = con.execute("""
        SELECT nifty200_close, nifty200_1d_ret, india_vix, india_vix_1d_chg,
               dxy, brent_crude, usd_inr, nasdaq_close, dow_close,
               fii_10d_sum, advance_decline_ratio, pcr_nifty,
               nifty200_5d_ret, nifty200_20d_ret
        FROM market_context ORDER BY date DESC LIMIT 1
    """).fetchdf()

    if mkt.empty:
        empty_fig = go.Figure()
        empty_fig.update_layout(paper_bgcolor="#1f2937", plot_bgcolor="#1f2937",
                                font_color="#6b7280")
        return (
            _metrics_placeholder(),
            empty_fig, empty_fig,
            html.Div("No data yet. Run --backfill first.",
                     style={"color": "#6b7280", "textAlign": "center", "padding": "20px"}),
            html.Div(),
            _navbar_badge(ctx),
        )

    r = mkt.iloc[0]

    # ── Metrics row ────────────────────────────────────────────────────
    n200     = r.get("nifty200_close", 0) or 0
    n200_1d  = r.get("nifty200_1d_ret", 0) or 0
    vix      = r.get("india_vix", 0) or 0
    fii      = r.get("fii_10d_sum", 0) or 0
    ad       = r.get("advance_decline_ratio", 1) or 1
    pcr      = r.get("pcr_nifty", 1) or 1

    metrics = dbc.Row([
        dbc.Col(metric_card("Market Cycle",
                            cycle_badge(ctx.market_cycle, ctx.market_confidence, "md"),
                            subtext=f"Confidence {ctx.market_confidence*100:.0f}%"), width=2),
        dbc.Col(metric_card("Nifty200",
                            f"{n200:,.2f}",
                            color=pnl_color(n200_1d),
                            subtext=format_pct(n200_1d)), width=2),
        dbc.Col(metric_card("India VIX",
                            f"{vix:.2f}",
                            color=BEAR_COLOR if vix > 20 else (NEUTRAL_COLOR if vix > 14 else BULL_COLOR),
                            subtext="Fear gauge"), width=2),
        dbc.Col(metric_card("FII 10d Flow",
                            format_inr(fii),
                            color=pnl_color(fii),
                            subtext="Crores"), width=2),
        dbc.Col(metric_card("A/D Ratio", f"{ad:.2f}",
                            color=pnl_color(ad - 1),
                            subtext="Advance/Decline"), width=2),
        dbc.Col(metric_card("PCR Nifty", f"{pcr:.2f}",
                            color=BULL_COLOR if pcr > 1 else BEAR_COLOR,
                            subtext="Put/Call ratio"), width=2),
    ], className="g-2 mb-3")

    # ── Nifty200 chart ─────────────────────────────────────────────────
    hist = con.execute("""
        SELECT date, nifty200_close, nifty200_1d_ret
        FROM market_context
        WHERE nifty200_close IS NOT NULL
        ORDER BY date DESC LIMIT 252
    """).fetchdf().sort_values("date")

    cycle_hist = con.execute("""
        SELECT date, cycle FROM market_cycles
        WHERE scope = 'market' AND scope_id IS NULL
        ORDER BY date DESC LIMIT 252
    """).fetchdf().sort_values("date")

    nifty_fig = _build_nifty_chart(hist, cycle_hist)

    # ── VIX gauge ──────────────────────────────────────────────────────
    vix_fig = _build_vix_gauge(vix)

    # ── Sector heatmap ─────────────────────────────────────────────────
    sector_grid = _build_sector_heatmap(ctx.sector_cycles)

    # ── Macro panel ────────────────────────────────────────────────────
    macro = _build_macro_panel(r)

    return metrics, nifty_fig, vix_fig, sector_grid, macro, _navbar_badge(ctx)


def _build_nifty_chart(hist, cycle_hist):
    """Nifty200 line chart with cycle colour shading."""
    fig = go.Figure()

    if hist.empty:
        fig.update_layout(paper_bgcolor="#1f2937", plot_bgcolor="#1f2937")
        return fig

    # Price line
    fig.add_trace(go.Scatter(
        x=hist["date"], y=hist["nifty200_close"],
        mode="lines",
        line={"color": GOLD_COLOR, "width": 1.5},
        name="Nifty200",
        hovertemplate="%{x}: %{y:,.2f}<extra></extra>",
    ))

    # Cycle background shading
    if not cycle_hist.empty:
        merged = hist.merge(cycle_hist, on="date", how="left")
        merged["cycle"] = merged["cycle"].fillna("neutral")

        for cycle, color in [("bull", "#1a4d2e40"), ("bear", "#4d1a1a40")]:
            mask    = merged["cycle"] == cycle
            x_fill  = list(merged.loc[mask, "date"])
            y_fill  = list(merged.loc[mask, "nifty200_close"])
            if x_fill:
                fig.add_trace(go.Scatter(
                    x=x_fill, y=y_fill,
                    fill="tozeroy", fillcolor=color,
                    line={"width": 0}, mode="lines",
                    name=f"{cycle.capitalize()} cycle",
                    showlegend=True,
                    hoverinfo="skip",
                ))

    fig.update_layout(
        paper_bgcolor="#1f2937", plot_bgcolor="#1f2937",
        font={"color": "#9ca3af", "family": "JetBrains Mono"},
        margin={"l": 50, "r": 10, "t": 10, "b": 30},
        legend={"orientation": "h", "y": 1.05, "x": 0,
                "bgcolor": "rgba(0,0,0,0)", "font": {"size": 10}},
        xaxis={"gridcolor": "#374151", "showgrid": True, "tickfont": {"size": 10}},
        yaxis={"gridcolor": "#374151", "showgrid": True, "tickfont": {"size": 10},
               "tickformat": ",.0f"},
        hovermode="x unified",
    )
    return fig


def _build_vix_gauge(vix: float):
    color = BEAR_COLOR if vix > 25 else (NEUTRAL_COLOR if vix > 14 else BULL_COLOR)
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=vix,
        delta={"reference": 15},
        gauge={
            "axis":       {"range": [0, 45], "tickcolor": "#6b7280", "tickfont": {"size": 9}},
            "bar":        {"color": color},
            "bgcolor":    "#374151",
            "bordercolor": "#4b5563",
            "steps": [
                {"range": [0, 14],  "color": "#1a4d2e40"},
                {"range": [14, 25], "color": "#78350f40"},
                {"range": [25, 45], "color": "#4d1a1a40"},
            ],
            "threshold": {"line": {"color": "#f87171", "width": 2},
                          "thickness": 0.75, "value": 25},
        },
        number={"font": {"size": 32, "color": color, "family": "JetBrains Mono"},
                "suffix": ""},
        title={"text": "INDIA VIX", "font": {"size": 11, "color": "#6b7280"}},
    ))
    fig.update_layout(
        paper_bgcolor="#1f2937", font={"color": "#9ca3af"},
        margin={"l": 20, "r": 20, "t": 40, "b": 20},
        height=280,
    )
    return fig


def _build_sector_heatmap(sector_cycles: dict):
    if not sector_cycles:
        return html.Div("No sector data", style={"color": "#6b7280", "padding": "20px"})

    cells = []
    for sector, info in sorted(sector_cycles.items()):
        cycle = info.get("cycle", "neutral") if isinstance(info, dict) else str(info)
        conf  = info.get("confidence", 0) if isinstance(info, dict) else 0
        color_map = {"bull": "#1a4d2e", "bear": "#4d1a1a", "neutral": "#1f2937"}
        text_map  = {"bull": BULL_COLOR, "bear": BEAR_COLOR, "neutral": "#9ca3af"}
        bg    = color_map.get(cycle, "#1f2937")
        fg    = text_map.get(cycle, "#9ca3af")
        border_color = fg + "40"

        cells.append(html.Div([
            html.Div(sector,
                     style={"fontSize": "0.7rem", "color": "#9ca3af",
                            "marginBottom": "3px", "whiteSpace": "nowrap",
                            "overflow": "hidden", "textOverflow": "ellipsis"}),
            html.Div(cycle.upper(),
                     style={"fontSize": "0.78rem", "fontFamily": "JetBrains Mono",
                            "fontWeight": "700", "color": fg}),
            html.Div(f"{conf*100:.0f}%",
                     style={"fontSize": "0.65rem", "color": "#6b7280", "marginTop": "2px"}),
        ], style={
            "background": bg, "border": f"1px solid {border_color}",
            "borderRadius": "6px", "padding": "8px 10px",
            "minWidth": "80px", "flex": "1",
        }))

    return html.Div(cells, style={
        "display": "grid",
        "gridTemplateColumns": "repeat(auto-fill, minmax(100px, 1fr))",
        "gap": "6px",
    })


def _build_macro_panel(r):
    rows = [
        ("DXY",    r.get("dxy"),          "US Dollar Index"),
        ("Crude",  r.get("brent_crude"),  "Brent $/bbl"),
        ("Nasdaq", r.get("nasdaq_close"), "NDX"),
        ("Dow",    r.get("dow_close"),    "DJIA"),
        ("USD/INR", r.get("usd_inr"),     "Exchange rate"),
    ]
    items = []
    for label, val, desc in rows:
        items.append(html.Div([
            html.Div(label, style={"fontSize": "0.7rem", "color": "#6b7280",
                                    "fontFamily": "JetBrains Mono", "width": "60px"}),
            html.Div(f"{val:,.2f}" if val else "–",
                     style={"fontSize": "0.83rem", "fontFamily": "JetBrains Mono",
                            "color": "#e5e7eb", "fontWeight": "600"}),
            html.Div(desc, style={"fontSize": "0.65rem", "color": "#4b5563",
                                   "marginLeft": "auto"}),
        ], style={"display": "flex", "alignItems": "center", "gap": "8px",
                  "padding": "6px 0", "borderBottom": f"1px solid {BORDER_COLOR}"}))
    return html.Div(items)


def _navbar_badge(ctx):
    from alphalens.dashboard.components.navbar import _cycle_badge
    return _cycle_badge(ctx.market_cycle, ctx.market_confidence)


def _metrics_placeholder():
    return html.Div("Loading market data…",
                    style={"color": "#6b7280", "padding": "12px 0"})
