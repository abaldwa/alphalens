"""
alphalens/dashboard/app.py

Dash application factory. Creates and configures the multi-page Dash app.

Pages:
  /               → Market Overview (cycle + macro dashboard)
  /portfolio      → Portfolio Dashboard (all 4 timeframes)
  /watchlist      → Watchlist (buy/sell signals)
  /chart          → Stock Chart (candlestick + signal overlays)
  /strategies     → Strategy Library
  /pnl            → P&L Reports
  /entry          → Portfolio Entry (manual + Zerodha CSV)
  /patterns       → Stock Pattern Analysis (HMM)
  /backtest       → Backtest Explorer
  /settings       → Settings & Configuration
"""

import dash
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc

from alphalens.dashboard.components.navbar import create_navbar


def create_app() -> Dash:
    app = Dash(
        __name__,
        use_pages=True,
        pages_folder="pages",
        external_stylesheets=[
            dbc.themes.DARKLY,
            "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600&family=Playfair+Display:ital,wght@0,700;1,400&family=Lato:wght@300;400;700&display=swap",
        ],
        suppress_callback_exceptions=True,
        title="AlphaLens",
        update_title=None,
        meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    )

    app.layout = html.Div([
        # Live update interval (every 60s during market hours)
        dcc.Interval(id="live-interval", interval=60_000, n_intervals=0),
        dcc.Interval(id="slow-interval", interval=300_000, n_intervals=0),

        # Store for shared state
        dcc.Store(id="cycle-store",     storage_type="memory"),
        dcc.Store(id="portfolio-store", storage_type="memory"),
        dcc.Store(id="selected-symbol", storage_type="memory", data="RELIANCE"),

        # Navigation
        create_navbar(),

        # Page content
        html.Div(
            dash.page_container,
            style={"minHeight": "calc(100vh - 56px)", "backgroundColor": "#0d0d0d"}
        ),
    ], style={"backgroundColor": "#0d0d0d", "fontFamily": "'Lato', sans-serif"})

    return app
