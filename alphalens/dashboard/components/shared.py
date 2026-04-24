"""alphalens/dashboard/components/shared.py — reusable UI components."""

import dash_bootstrap_components as dbc
from dash import html

# ── Colour constants ──────────────────────────────────────────────────────
BULL_COLOR    = "#4ade80"
BEAR_COLOR    = "#f87171"
NEUTRAL_COLOR = "#9ca3af"
GOLD_COLOR    = "#b8860b"
SURFACE_BG    = "#111827"
CARD_BG       = "#1f2937"
BORDER_COLOR  = "#374151"

# ── Cycle colours ─────────────────────────────────────────────────────────
CYCLE_COLORS = {"bull": BULL_COLOR, "bear": BEAR_COLOR, "neutral": NEUTRAL_COLOR}


def cycle_badge(cycle: str, confidence: float = None, size: str = "sm"):
    color = CYCLE_COLORS.get(cycle, NEUTRAL_COLOR)
    label = cycle.upper() if cycle else "–"
    conf_str = f" {confidence*100:.0f}%" if confidence is not None else ""
    font_size = "0.65rem" if size == "sm" else "0.82rem"
    padding   = "2px 8px" if size == "sm" else "5px 14px"
    return html.Span(
        f"{label}{conf_str}",
        style={
            "color": color, "background": f"{color}18",
            "border": f"1px solid {color}40",
            "borderRadius": "3px", "padding": padding,
            "fontFamily": "'JetBrains Mono',monospace",
            "fontSize": font_size, "fontWeight": "600",
        }
    )


def signal_badge(signal_type: str):
    colors = {"buy": (BULL_COLOR, "#1a4d2e"), "sell": (BEAR_COLOR, "#4d1a1a"),
              "hold": (NEUTRAL_COLOR, "#1f2937")}
    fg, bg = colors.get(signal_type, (NEUTRAL_COLOR, "#1f2937"))
    return html.Span(
        (signal_type or "–").upper(),
        style={
            "color": fg, "background": bg,
            "border": f"1px solid {fg}40",
            "borderRadius": "3px", "padding": "2px 8px",
            "fontFamily": "'JetBrains Mono',monospace",
            "fontSize": "0.68rem", "fontWeight": "700",
        }
    )


def metric_card(label: str, value, color: str = "#e5e7eb",
                subtext: str = None, mono: bool = True):
    font = "'JetBrains Mono',monospace" if mono else "'Lato',sans-serif"
    return dbc.Card([
        dbc.CardBody([
            html.Div(label, style={"fontSize": "0.65rem", "color": "#6b7280",
                                    "textTransform": "uppercase", "letterSpacing": "0.1em",
                                    "marginBottom": "2px"}),
            html.Div(value, style={"fontSize": "1.25rem", "fontWeight": "700",
                                    "color": color, "fontFamily": font}),
            html.Div(subtext, style={"fontSize": "0.72rem", "color": "#6b7280",
                                      "marginTop": "2px"}) if subtext else None,
        ], style={"padding": "12px 16px"}),
    ], style={"background": CARD_BG, "border": f"1px solid {BORDER_COLOR}",
              "borderRadius": "6px"})


def section_header(title: str, subtitle: str = None):
    return html.Div([
        html.Div(title, style={"fontFamily": "'Playfair Display',serif",
                                "fontSize": "1.3rem", "color": "#f9fafb",
                                "fontWeight": "700"}),
        html.Div(subtitle, style={"fontSize": "0.78rem", "color": "#6b7280",
                                   "marginTop": "2px"}) if subtitle else None,
    ], style={"marginBottom": "16px"})


def pnl_color(value: float) -> str:
    if value is None:
        return NEUTRAL_COLOR
    return BULL_COLOR if value >= 0 else BEAR_COLOR


def format_inr(value, decimals: int = 2) -> str:
    if value is None:
        return "–"
    try:
        v = float(value)
        if abs(v) >= 1_00_000:
            return f"₹{v/1_00_000:,.2f}L"
        if abs(v) >= 1_000:
            return f"₹{v:,.{decimals}f}"
        return f"₹{v:.{decimals}f}"
    except Exception:
        return "–"


def format_pct(value, decimals: int = 1, show_sign: bool = True) -> str:
    if value is None:
        return "–"
    try:
        v = float(value)
        sign = "+" if v >= 0 and show_sign else ""
        return f"{sign}{v:.{decimals}f}%"
    except Exception:
        return "–"


def timeframe_tabs(active: str = "swing"):
    TF = [("1D", "intraday"), ("SW", "swing"), ("MED", "medium"), ("LT", "long_term")]
    return html.Div([
        html.Button(
            label, id={"type": "tf-tab", "index": tf},
            n_clicks=0,
            style={
                "background": GOLD_COLOR if tf == active else CARD_BG,
                "color": "#0d0d0d" if tf == active else "#9ca3af",
                "border": f"1px solid {GOLD_COLOR if tf == active else BORDER_COLOR}",
                "borderRadius": "3px", "padding": "4px 12px",
                "fontFamily": "'JetBrains Mono',monospace",
                "fontSize": "0.72rem", "cursor": "pointer",
                "fontWeight": "600" if tf == active else "400",
            }
        )
        for label, tf in TF
    ], style={"display": "flex", "gap": "4px"})


def loading_spinner():
    return dbc.Spinner(color="warning", size="sm",
                        spinner_style={"width": "1.2rem", "height": "1.2rem"})


# ── Page layout wrapper ────────────────────────────────────────────────────
def page_layout(content, padding: str = "20px 24px"):
    return html.Div(content, style={
        "padding": padding,
        "backgroundColor": "#0d0d0d",
        "minHeight": "calc(100vh - 56px)",
        "color": "#e5e7eb",
    })
