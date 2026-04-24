"""alphalens/dashboard/components/navbar.py"""

import dash_bootstrap_components as dbc
from dash import html

NAV_LINKS = [
    ("Overview",    "/"),
    ("Portfolio",   "/portfolio"),
    ("Watchlist",   "/watchlist"),
    ("Chart",       "/chart"),
    ("Strategies",  "/strategies"),
    ("P&L",         "/pnl"),
    ("Entry",       "/entry"),
    ("Patterns",    "/patterns"),
    ("Backtest",    "/backtest"),
    ("Settings",    "/settings"),
]

CYCLE_BADGE_STYLE = {
    "bull":    {"background": "#1a4d2e", "color": "#4ade80",  "border": "1px solid #1a4d2e"},
    "bear":    {"background": "#4d1a1a", "color": "#f87171",  "border": "1px solid #4d1a1a"},
    "neutral": {"background": "#2a2a2a", "color": "#9ca3af",  "border": "1px solid #374151"},
}


def create_navbar():
    return dbc.Navbar(
        dbc.Container([
            # Brand
            html.A(
                html.Span("◈ AlphaLens",
                          style={"fontFamily": "'Playfair Display',serif",
                                 "color": "#b8860b", "fontSize": "1.05rem",
                                 "fontWeight": "700", "letterSpacing": "0.03em"}),
                href="/", style={"textDecoration": "none", "marginRight": "1.5rem"}
            ),

            # Nav links
            dbc.Nav([
                dbc.NavLink(
                    label, href=href,
                    active="exact",
                    style={"color": "#9ca3af", "fontSize": "0.78rem",
                           "letterSpacing": "0.05em", "textTransform": "uppercase",
                           "padding": "4px 10px"},
                )
                for label, href in NAV_LINKS
            ], navbar=True, className="me-auto"),

            # Market cycle badge (updated by callback)
            html.Div(id="navbar-cycle-badge",
                     children=_cycle_badge("neutral", 0.0),
                     style={"marginLeft": "auto"}),

        ], fluid=True),
        dark=True,
        style={"backgroundColor": "#111827", "borderBottom": "2px solid #b8860b",
               "padding": "0 12px", "minHeight": "52px"},
        sticky="top",
    )


def _cycle_badge(cycle: str, confidence: float):
    styles = CYCLE_BADGE_STYLE.get(cycle, CYCLE_BADGE_STYLE["neutral"])
    return html.Span(
        f"Market: {cycle.upper()}  {confidence*100:.0f}%",
        style={
            **styles,
            "padding": "4px 12px", "borderRadius": "4px",
            "fontFamily": "'JetBrains Mono',monospace",
            "fontSize": "0.72rem", "fontWeight": "600",
        }
    )
